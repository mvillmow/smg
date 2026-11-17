"""Bounded SMG encoder control registry and the native P receiver control facade."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import grpc
from smg_grpc_proto.generated import sglang_encoder_pb2 as ep
from smg_grpc_proto.generated import sglang_encoder_pb2_grpc as rpc
from smg_grpc_proto.generated import sglang_managed_pb2 as pb
from smg_grpc_proto.generated import sglang_scheduler_pb2 as sg
from smg_grpc_proto.generated import sglang_scheduler_pb2_grpc as sg_rpc

from .config import encoder_window, healthy
from .encoder_bridge import ARTIFACT_KEY, NativeReferences, OwnedBatchSubmitter


class Rejected(Exception):
    """Typed control refusal, before any native state is created."""

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def require(condition, message, code=grpc.StatusCode.INVALID_ARGUMENT):
    """Keep validation failures separate from unexpected native errors."""
    if not condition:
        raise Rejected(code, message)


@dataclass
class Entry:
    """Only this E registry owns admission, CPU/native tasks, sends and final release."""

    expiry: int
    deadline: float
    rid: str
    request: object = None
    cancelled: bool = False
    quiesced: bool = False
    terminal: bool = False
    # Retain the error message, never traceback frames holding native tensors.
    error: str | None = None
    finished: asyncio.Event = field(default_factory=asyncio.Event)
    cancellation: asyncio.Event = field(default_factory=asyncio.Event)
    delivered: asyncio.Event = field(default_factory=asyncio.Event)
    refs: NativeReferences = field(default_factory=NativeReferences)
    sends: dict = field(default_factory=dict)
    receivers: int | None = None
    destinations: set = field(default_factory=set)
    task: object = None


class EncoderService(rpc.SglangEncoderServicer, sg_rpc.SglangSchedulerServicer):
    """Single-loop admission ledger shared by gRPC and the native HTTP facade."""

    async def HealthCheck(self, request, context):
        """Use the ordinary SMG worker health workflow for encoder admission."""
        return sg.HealthCheckResponse(
            healthy=not self.draining and healthy(self.config["max_clock_error_ms"])
        )

    async def GetModelInfo(self, request, context):
        """The native served name, rather than its local weight path, identifies this pool."""
        args = self.server_args
        return sg.GetModelInfoResponse(
            model_path=args.model_path,
            tokenizer_path=args.tokenizer_path or args.model_path,
            served_model_name=args.served_model_name or args.model_path,
            is_generation=True,
            supports_vision=True,
            model_type=self.backend.encoder.model_config.hf_config.model_type,
        )

    async def GetServerInfo(self, request, context):
        """Publish native transport contracts through existing worker registration."""
        from google.protobuf.struct_pb2 import Struct

        capability = self.capabilities()
        values = {
            field.name: getattr(capability, field.name)
            for field in capability.DESCRIPTOR.fields
            if field.name != "instance"
        }
        values["instance"] = dict(
            worker_id=self.instance.worker_id, generation=self.instance.generation
        )
        args = self.server_args
        labels = dict(
            model_path=args.model_path,
            served_model_name=args.served_model_name or args.model_path,
            supports_vision=True,
            mm_processor="smg",
            mm_media_ref_schemes="http,https,data",
            sg_computation_role="encoder",
            sglang_prepared=json.dumps(values),
            max_running_requests=self.capacity,
        )
        info = Struct()
        info.update(labels)
        return sg.GetServerInfoResponse(
            server_args=info,
            active_requests=sum(not entry.quiesced for entry in self.entries.values()),
        )

    async def GetLoads(self, request, context):
        """Feed the shared SMG load monitor from the bounded owners, including pending cleanup."""
        waiting = len(self.queue)
        running = sum(not entry.quiesced for entry in self.entries.values()) - waiting
        return sg.GetLoadsResponse(
            dp_rank_count=1,
            loads=[
                sg.SchedulerLoad(
                    dp_rank=0,
                    num_running_reqs=running,
                    num_waiting_reqs=waiting,
                    num_total_reqs=running + waiting,
                    max_running_requests=self.capacity,
                )
            ],
            aggregate=sg.AggregateMetrics(
                total_running_reqs=running,
                total_waiting_reqs=waiting,
                total_reqs=running + waiting,
            ),
        )

    def __init__(self, config, backend, processor):
        self.config, self.backend, self.processor = config, backend, processor
        self.capacity = config["max_active"]
        # Waiting requests own bounded controls, not yet materialized tensors. Reserve
        # room for every queued control, then bound the CPU/native/send window separately.
        self.execution_capacity, self.execution_bytes = encoder_window(config)
        self.execution = asyncio.BoundedSemaphore(self.execution_capacity)
        self.instance = pb.Instance(worker_id=config["worker_id"], generation=config["generation"])
        self.entries, self.parts, self.early = {}, {}, {}
        self.draining = False
        self.queue = deque()
        self.queued = asyncio.Event()
        self.cpu = asyncio.Semaphore(config["max_cpu"])
        self.cpu_active = 0
        self.dispatcher = None
        self.early_count = 0
        self.submitter = OwnedBatchSubmitter(backend.scheduler)

    def start(self):
        """Start one admission dispatcher; request tasks remain in the bounded registry."""
        self.backend.start()
        self.dispatcher = asyncio.create_task(self._dispatch())

    def _collect(self, now):
        """Only safe terminal records past their original acceptance horizon may expire."""
        expired = [
            key
            for key, entry in self.entries.items()
            if entry.quiesced and now > entry.expiry + self.config["retention_ms"]
        ]
        for key in expired:
            entry = self.entries.pop(key)
            self.parts.pop(entry.rid, None)

    def _admit(self, request, remaining):
        """Bound native controls before preprocessing; every attempt has a fresh fenced rid."""
        require(
            healthy(self.config["max_clock_error_ms"]),
            "encoder clock unhealthy",
            grpc.StatusCode.UNAVAILABLE,
        )
        now = int(time.time() * 1000)
        self._collect(now)
        require(
            remaining is not None and 0 < remaining <= 122, "Encode requires a bounded deadline"
        )
        require(
            0 < len(request.req_id) <= 256
            and request.req_id.startswith(self.instance.generation + "."),
            "stale encoder instance",
        )
        require(request.num_parts == 1 and request.part_idx == 0, "v1 uses one encoder per prompt")
        require(0 < len(request.mm_items) <= self.config["max_images"], "invalid image count")
        require(request.ByteSize() <= self.config["max_control_bytes"], "control request too large")
        require(bool(request.prefill_host), "missing native receiver host")
        require(
            request.req_id not in self.parts,
            "duplicate native request",
            grpc.StatusCode.ALREADY_EXISTS,
        )
        require(
            not self.draining
            and len(self.entries) < self.config["max_records"]
            and sum(not entry.quiesced for entry in self.entries.values()) < self.capacity,
            "encoder admission full",
            grpc.StatusCode.RESOURCE_EXHAUSTED,
        )
        entry = Entry(
            now + int(remaining * 1000),
            time.monotonic() + remaining,
            request.req_id,
            request=request,
        )
        self.entries[entry.rid] = self.parts[entry.rid] = entry
        if waiter := self.early.get(entry.rid):
            waiter[0].set()
        self.queue.append(entry)
        self.queued.set()
        return entry

    def _release_cpu(self):
        """Release only completed CPU owners; backpressure never cancels running work."""
        self.cpu_active -= 1
        self.cpu.release()

    async def _dispatch(self):
        """Grant tensor and CPU slots in FIFO order while completed CPU work overlaps GPU/send."""
        while True:
            await self.queued.wait()
            await self.execution.acquire()
            await self.cpu.acquire()
            self.cpu_active += 1
            if not self.queue:
                self._release_cpu()
                self.execution.release()
                self.queued.clear()
                continue
            entry = self.queue.popleft()
            entry.task = asyncio.create_task(self._run(entry, execution_held=True))
            if not self.queue:
                self.queued.clear()

    async def _run(self, entry, *, cpu_held=True, execution_held=False):
        """Do not cancel CPU, a native batch or a zero-copy send when the request expires."""
        from sglang.srt.disaggregation.encoder.runtime import execute_encode_pipeline

        native_started = False

        async def abandon_cancelled_native():
            """Wake missing-destination waits through the native lifecycle owner, never cancel CUDA."""
            try:
                await asyncio.wait_for(
                    entry.cancellation.wait(), max(0, entry.deadline - time.monotonic())
                )
            except asyncio.TimeoutError:  # noqa: UP041 - Python 3.10 compatibility
                self._cancel(entry)
            if native_started and entry.cancelled:
                try:
                    await self.backend.encoder.abandon_request(entry.rid)
                except Exception as error:
                    # The pipeline may still be waiting for this native wakeup.
                    # Stop admission immediately, even before its owner exits.
                    self.draining = True
                    entry.error = str(error)[:1024] or type(error).__name__
                    raise

        cancellation = asyncio.create_task(abandon_cancelled_native())
        try:
            require(
                not entry.cancelled and time.monotonic() < entry.deadline,
                "Encode cancelled before CPU",
                grpc.StatusCode.CANCELLED,
            )
            async with entry.refs.retain():
                artifact = await asyncio.to_thread(
                    self.processor.prepare,
                    list(entry.request.mm_items),
                    self.config["image_token_id"],
                )
            self._release_cpu()
            cpu_held = False
            require(
                not entry.cancelled and time.monotonic() < entry.deadline,
                "Encode expired after CPU",
                grpc.StatusCode.CANCELLED,
            )
            from sglang.srt.managers.req_types import Modality
            from smg_grpc_proto.generated import sglang_scheduler_pb2 as sg

            prepared = sg.GenerateRequest.FromString(artifact)
            tokens = sum(
                item.length
                for batch in [prepared.mm_inputs, *prepared.extra_mm_inputs]
                for item in batch.mm_placeholders
            )
            native_bytes = (
                tokens
                * self.backend.encoder._embedding_dims[Modality.IMAGE]
                * self.backend.encoder._element_size
            )
            require(
                native_bytes <= self.config["max_staging_bytes_per_request"],
                "embedding exceeds reserved staging budget",
                grpc.StatusCode.RESOURCE_EXHAUSTED,
            )
            del prepared
            native = {
                "req_id": entry.rid,
                "num_parts": 1,
                "part_idx": 0,
                "modality": "image",
                "mm_items": [None] * len(entry.request.mm_items),
                ARTIFACT_KEY: artifact,
            }
            async with entry.refs.retain():
                native_started = True
                await execute_encode_pipeline(self.backend.encoder, self.submitter, native)
            if self.backend.encoder.transfer_backend == "mooncake":
                delivered = asyncio.create_task(entry.delivered.wait())
                cancelled = asyncio.create_task(entry.cancellation.wait())
                try:
                    await asyncio.wait(
                        [delivered, cancelled],
                        timeout=max(0, entry.deadline - time.monotonic()),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    for waiter in (delivered, cancelled):
                        waiter.cancel()
                    await asyncio.gather(delivered, cancelled, return_exceptions=True)
                require(
                    entry.delivered.is_set() and not entry.cancelled,
                    "Encode delivery cancelled or expired",
                    grpc.StatusCode.CANCELLED,
                )
            require(
                not entry.cancelled,
                "Encode delivery cancelled",
                grpc.StatusCode.CANCELLED,
            )
        except Exception as error:
            entry.error = str(error)[:1024] or type(error).__name__
            if not native_started:
                # A P metadata waiter may already hold a reference to this queued
                # request. Publish failure before waiting for that reader to leave.
                from sglang.srt.disaggregation.encoder.server import meta_registry

                try:
                    await meta_registry.publish(entry.rid, 0, 0, 0, error=str(error)[:1024])
                except Exception:
                    logging.getLogger(__name__).exception(
                        "native metadata failure publication failed"
                    )
        finally:
            if cpu_held:
                self._release_cpu()
            entry.refs.cancel()
            entry.cancellation.set()
            # Native abandonment wakes ZMQ destination waits but retains active encode/send
            # owners. Their scopes still have to finish before SMG removes any charges.
            try:
                await cancellation
                await entry.refs.wait_quiesced()
                await self.backend.encoder.release_request(entry.rid)
                if native_started:
                    self.backend.encoder.clear_abandoned_request(entry.rid)
            except Exception as error:
                # Release may have partially progressed; retrying it is not proven safe.
                # Stop admitting work while retaining this owner's charges and buffers.
                self.draining = True
                logging.getLogger(__name__).exception("native E cleanup failed; draining worker")
                entry.error = str(error)[:1024] or type(error).__name__
            else:
                # Retain only the replay fence and bounded events after native owners exit.
                entry.request = None
                # Failed send tasks retain traceback frames holding native embeddings.
                # Closed parts cannot replay these tasks after quiescence.
                entry.sends.clear()
                entry.quiesced = True
                if execution_held:
                    self.execution.release()
            entry.terminal = True
            entry.finished.set()

    async def Encode(self, request, context):
        """Use SGLang's existing Encode RPC; the native owner outlives this observer."""
        try:
            entry = self._admit(request, context.time_remaining())
        except Rejected as error:
            await context.abort(error.code, str(error))
        try:
            await entry.finished.wait()
            if entry.error:
                await context.abort(grpc.StatusCode.INTERNAL, str(entry.error)[:1024])
            return ep.EncodeResponse()
        finally:
            if not entry.terminal:
                self._cancel(entry)

    def _cancel(self, entry):
        """Close new work; cancellation acknowledgement never frees resource charges."""
        entry.cancelled = True
        entry.refs.cancel()
        entry.cancellation.set()
        if entry.task is None:
            try:
                self.queue.remove(entry)
            except ValueError:
                return
            # There is no CPU/native execution permit to release. Any early receiver
            # still owns its references until the same normal cleanup confirms safety.
            entry.task = asyncio.create_task(self._run(entry, cpu_held=False))

    def capabilities(self):
        """Publish this process fence and the explicit validated deployment contract."""
        return pb.Capabilities(
            version=1,
            instance=self.instance,
            role="encoder",
            model_revision=self.config["model_revision"],
            processor_contract=self.config["processor_contract"],
            layout_contract="sglang_native_manifest_v1",
            embedding_dtype=self.config["embedding_dtype"],
            kv_backend=self.config["kv_backend"],
            embedding_backend=self.backend.encoder.transfer_backend,
            topology_contract=self.config["topology_contract"],
            receive_endpoint=self.config["receive_endpoint"],
            capacity=self.capacity,
            tp_size=self.config["tp_size"],
            wall_time_unix_ms=int(time.time() * 1000),
            ready=not self.draining and healthy(self.config["max_clock_error_ms"]),
        )

    async def part(self, rid):
        """Bound pull-before-Encode waiters without creating speculative native state."""
        require(
            isinstance(rid, str)
            and len(rid) <= 512
            and rid.startswith(self.instance.generation + "."),
            "stale native part",
        )
        if rid not in self.parts:
            require(
                self.early_count < self.config["max_early_waiters"],
                "early facade capacity full",
                grpc.StatusCode.RESOURCE_EXHAUSTED,
            )
            state = self.early.setdefault(rid, [asyncio.Event(), 0])
            state[1] += 1
            self.early_count += 1
            try:
                await asyncio.wait_for(state[0].wait(), self.config["early_wait_seconds"])
            finally:
                self.early_count -= 1
                state[1] -= 1
                if not state[1]:
                    self.early.pop(rid, None)
        entry = self.parts[rid]
        require(
            not entry.cancelled and not entry.quiesced,
            "native part closed",
            grpc.StatusCode.CANCELLED,
        )
        require(
            time.monotonic() < entry.deadline,
            "native facade deadline expired",
            grpc.StatusCode.DEADLINE_EXCEEDED,
        )
        return entry

    async def _receiver_control(self, entry, operation):
        """Cancel only metadata/registration waits; these own no GPU or transport buffers."""
        work = asyncio.create_task(operation)
        cancelled = asyncio.create_task(entry.cancellation.wait())
        try:
            done, _ = await asyncio.wait((work, cancelled), return_when=asyncio.FIRST_COMPLETED)
            require(
                work in done and not entry.cancelled,
                "native receiver control cancelled",
                grpc.StatusCode.CANCELLED,
            )
            return await work
        finally:
            for waiter in (work, cancelled):
                if not waiter.done():
                    waiter.cancel()
            await asyncio.gather(work, cancelled, return_exceptions=True)

    async def facade(self, kind, request):
        """Native receive calls and Encode resolve the exact same E operation and buffer owner."""
        from sglang.srt.disaggregation.encoder import server as native
        from sglang.srt.disaggregation.encoder.runtime import send_staged_embedding

        entry = await self.part(request.get("req_id"))
        if kind == "metadata":
            require(request.get("part_idx") == 0, "invalid part index")
            async with entry.refs.retain():
                metadata = await self._receiver_control(
                    entry,
                    asyncio.wait_for(
                        native.meta_registry.wait(entry.rid),
                        max(0, entry.deadline - time.monotonic()),
                    ),
                )
            require(
                metadata and not metadata.get("error"),
                "native preprocessing failed",
                grpc.StatusCode.INTERNAL,
            )
            return {"req_id": entry.rid, "part_idx": 0, **metadata}
        count = request.get("receive_count")
        require(
            type(count) is int and 0 < count <= self.config["max_receivers"],
            "invalid native receive count",
        )
        require(entry.receivers in (None, count), "native receive count changed")
        entry.receivers = count
        destination = (
            request.get("receive_url")
            if kind == "register"
            else (request.get("prefill_host"), request.get("embedding_port"))
        )
        require(bool(destination), "missing native receiver")
        expected_host = entry.request.prefill_host
        if kind == "register":
            require(
                self.backend.encoder.transfer_backend == "zmq_to_scheduler",
                "unexpected ZMQ receiver",
            )
            # The native receiver sends NetworkAddress.to_host_port_str(), and
            # the native sender adds the transport scheme itself.
            require(isinstance(destination, str), "invalid native receiver address")
            address = urlsplit("//" + destination)
            require(
                address.hostname == expected_host
                and bool(address.port)
                and not any(
                    (
                        address.username,
                        address.password,
                        address.path,
                        address.query,
                        address.fragment,
                    )
                ),
                "receiver differs from frozen P host",
            )
        else:
            require(
                self.backend.encoder.transfer_backend == "mooncake", "unexpected Mooncake receiver"
            )
            require(
                destination[0] == expected_host
                and type(destination[1]) is int
                and 0 < destination[1] <= 65535,
                "receiver differs from frozen P host",
            )
        key = (kind, destination)
        digest = hashlib.sha256(repr(sorted(request.items())).encode()).digest()
        if key in entry.sends:
            old_digest, task = entry.sends[key]
            require(
                digest == old_digest,
                "conflicting duplicate native receive call",
                grpc.StatusCode.ALREADY_EXISTS,
            )
            return await asyncio.shield(task)
        require(
            len(entry.sends) < self.config["max_receivers"],
            "too many native destinations",
            grpc.StatusCode.RESOURCE_EXHAUSTED,
        )

        async def run():
            """Retain the actual send even if its HTTP response consumer disconnects."""
            async with entry.refs.retain():
                if kind == "register":
                    await self._receiver_control(
                        entry,
                        self.backend.encoder.register_embedding_destinations(
                            entry.rid, count, [destination]
                        ),
                    )
                else:
                    sent = await send_staged_embedding(
                        self.backend.encoder, request, release_without_count=False
                    )
                    require(sent, "native embedding unavailable", grpc.StatusCode.INTERNAL)
                    entry.destinations.add(destination)
                    if len(entry.destinations) == entry.receivers:
                        entry.delivered.set()
            return None

        task = asyncio.create_task(run())
        entry.sends[key] = (digest, task)
        return await asyncio.shield(task)

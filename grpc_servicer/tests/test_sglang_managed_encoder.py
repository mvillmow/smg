"""CPU ownership regressions; these are not GPU/TP or transport acceptance tests."""

import asyncio
import inspect
from types import SimpleNamespace

import pytest
from smg_grpc_servicer.sglang.managed.config import complete
from smg_grpc_servicer.sglang.managed.encoder_bridge import NativeReferences


def test_native_send_reference_outlives_cancel_acknowledgement():
    """Cancellation closes new work but only the send owner can establish quiescence."""

    async def scenario():
        refs = NativeReferences()
        sending, finish = asyncio.Event(), asyncio.Event()

        async def native_send():
            async with refs.retain():
                sending.set()
                await finish.wait()

        sender = asyncio.create_task(native_send())
        await sending.wait()
        refs.cancel()
        quiesced = asyncio.create_task(refs.wait_quiesced())
        await asyncio.sleep(0)
        assert not quiesced.done()
        with pytest.raises(asyncio.CancelledError):
            async with refs.retain():
                pytest.fail("cancelled part admitted new native work")
        finish.set()
        await sender
        await quiesced
        assert refs.count == 0

    asyncio.run(scenario())


def test_pull_before_encode_shares_bounded_waiter_for_native_tp_receivers():
    """Two native TP receivers can wait for the same part without duplicating admission."""
    from smg_grpc_servicer.sglang.managed.encoder_service import EncoderService

    async def scenario():
        service = object.__new__(EncoderService)
        service.instance = SimpleNamespace(generation="epoch")
        service.config = {"max_early_waiters": 2, "early_wait_seconds": 1}
        service.parts, service.early, service.early_count = {}, {}, 0
        first = asyncio.create_task(service.part("epoch.part"))
        second = asyncio.create_task(service.part("epoch.part"))
        await asyncio.sleep(0)
        assert service.early_count == 2 and len(service.early) == 1
        entry = SimpleNamespace(cancelled=False, quiesced=False, deadline=float("inf"))
        service.parts["epoch.part"] = entry
        service.early["epoch.part"][0].set()
        assert await first is entry and await second is entry
        assert not service.early and service.early_count == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("release_fails", [False, True])
def test_request_payload_released_only_after_native_quiescence(monkeypatch, release_fails):
    """Replay fences cannot retain uncharged media or failed-send traceback buffers."""
    import gc
    import sys
    import weakref
    from unittest.mock import AsyncMock

    from smg_grpc_servicer.sglang.managed.encoder_service import EncoderService, Entry

    monkeypatch.setitem(
        sys.modules,
        "sglang.srt.disaggregation.encoder.runtime",
        SimpleNamespace(execute_encode_pipeline=AsyncMock()),
    )
    metadata = SimpleNamespace(publish=AsyncMock())
    server = SimpleNamespace(meta_registry=metadata)
    monkeypatch.setitem(sys.modules, "sglang.srt.disaggregation.encoder.server", server)
    if parent := sys.modules.get("sglang.srt.disaggregation.encoder"):
        monkeypatch.setattr(parent, "server", server, raising=False)

    async def scenario():
        releasing, finish = asyncio.Event(), asyncio.Event()

        async def release(_rid):
            releasing.set()
            await finish.wait()
            if release_fails:
                raise RuntimeError("native release has not established quiescence")

        service = object.__new__(EncoderService)
        service.backend = SimpleNamespace(encoder=SimpleNamespace(release_request=release))
        service.cpu = asyncio.Semaphore(0)
        service.config = {"max_cpu": 1, "image_token_id": 1, "max_staging_bytes_per_request": 1024}
        service.cpu_active = 1
        service.execution = asyncio.BoundedSemaphore(1)
        await service.execution.acquire()
        payload = SimpleNamespace(mm_items=["x" * 1048576])
        entry = Entry(1, float("inf"), "rid", request=payload, cancelled=True)

        class SendBuffer:
            """Stand in for an embedding held by a failed native send's frame."""

        references = []

        async def failed_send():
            """A completed task keeps its exception traceback and native frame locals."""
            buffer = SendBuffer()
            references.append(weakref.ref(buffer))
            raise RuntimeError("native send failed")

        sender = asyncio.create_task(failed_send())
        try:
            await sender
        except RuntimeError:
            pass
        entry.sends["destination"] = (b"digest", sender)
        del sender
        task = asyncio.create_task(service._run(entry, execution_held=True))
        await asyncio.wait_for(releasing.wait(), 2)
        assert entry.request is payload
        assert not entry.quiesced
        finish.set()
        await task
        assert entry.terminal
        gc.collect()
        assert service.execution._value == (0 if release_fails else 1)
        if release_fails:
            assert service.draining
            assert entry.request is payload
            assert not entry.quiesced
            assert references[0]() is not None
        else:
            assert entry.request is None
            assert entry.quiesced
            assert references[0]() is None

    asyncio.run(scenario())


def native_args():
    """Use explicit native values without importing or launching an engine."""
    return SimpleNamespace(
        host="10.0.0.1",
        port=43000,
        dtype="bfloat16",
        tp_size=2,
        disaggregation_transfer_backend="mooncake",
        encoder_transfer_backend="zmq_to_scheduler",
        disaggregation_bootstrap_port=43002,
        model_path="/models/qwen3",
        revision="weights",
    )


@pytest.mark.parametrize("role", ["encoder", "prefill", "decode"])
def test_minimal_config_derives_contract_and_finite_bounds(role):
    """All roles share the same model contract without copying TP/dtype/transport settings."""
    config = complete(native_args(), role)
    contract = config if role == "encoder" else config["contract"]
    assert contract["model_revision"] == "weights"
    assert contract["tp_size"] == 2
    assert contract["embedding_backend"] == "zmq_to_scheduler"
    assert contract["embedding_dtype"] == "bfloat16"
    assert config["max_records"] > 0
    if role == "encoder":
        assert config["max_records"] >= config["max_active"] > 0
    else:
        assert "max_active" not in config  # P/D admission belongs to the shared gateway path.
    assert config["max_duration_ms"] >= 120000 + config["max_clock_error_ms"]
    if role == "encoder":
        assert config["control_listen"] == "0.0.0.0:43010"
        assert config["receive_endpoint"] == "http://10.0.0.1:43000"


@pytest.mark.parametrize(
    "field,value",
    [
        ("nnodes", 2),
        ("pp_size", 2),
        ("dp_size", 2),
        ("attn_dp_size", 2),
        ("enable_unified_memory", True),
    ],
)
def test_native_layout_cannot_advertise_an_unsupported_topology(field, value):
    """Reject layouts the adapter cannot represent before advertising a single TP group."""
    args = native_args()
    setattr(args, field, value)
    with pytest.raises(ValueError, match="does not support"):
        complete(args, "prefill")


@pytest.mark.parametrize("backend", ["mooncake", "zmq_to_scheduler"])
def test_receiver_fanout_preserves_memory_bound_and_advertised_capacity(backend):
    """Eight P ranks are allowed without overcommitting the E owner's context budget."""
    from smg_grpc_servicer.sglang.managed.encoder_service import EncoderService

    args = native_args()
    args.encoder_transfer_backend = backend
    config = complete(args, "encoder")
    config["generation"] = "generation"
    service = EncoderService(config, SimpleNamespace(scheduler=None), None)
    assert config["max_receivers"] == 8
    # Physical storage is shared only for native Mooncake GPU-direct sends.
    assert service.execution_capacity == (14 if backend == "mooncake" else 5)
    controls = service.capacity * config["max_control_bytes"]
    reserved = service.execution_bytes
    assert service.capacity == config["max_active"]
    assert service.execution_capacity < service.capacity
    assert controls + reserved * service.execution_capacity <= config["max_context_bytes"]
    assert controls + reserved * (service.execution_capacity + 1) > config["max_context_bytes"]
    assert config["max_early_waiters"] >= service.capacity * config["max_receivers"]
    assert config["max_http_connections"] > config["max_early_waiters"]
    config["max_context_bytes"] = controls + reserved - 1
    with pytest.raises(ValueError, match="cannot admit"):
        EncoderService(config, SimpleNamespace(scheduler=None), None)


def test_native_mooncake_staging_owns_per_request_storage():
    """Optional native staging checks do not skip dependency-free capacity coverage."""
    # Exercise native staging on CPU tensors: this checks the ownership
    # contract, not GPU/RDMA completion (covered by real transport tests).
    torch = pytest.importorskip("torch")
    server = pytest.importorskip("sglang.srt.disaggregation.encoder.server")
    Modality = pytest.importorskip("sglang.srt.managers.req_types").Modality
    MMEncoder = server.MMEncoder

    staged = []
    native = SimpleNamespace(
        rank=0,
        _register_shared_mr=lambda *args: None,
        _deregister_shared_mr=lambda *args: None,
        _stage_embedding_batch=staged.extend,
    )
    batch = torch.arange(12, dtype=torch.float32).reshape(6, 2)
    ctx = SimpleNamespace(
        items_per_req=[1, 1],
        preprocess_result=SimpleNamespace(
            token_counts=[2, 4], grid_thw=torch.ones((2, 3), dtype=torch.int64)
        ),
        aux_data={},
        modality=Modality.IMAGE,
        use_global_cache=False,
    )
    requests = [{"req_id": str(i), "num_parts": 1, "part_idx": 0} for i in range(2)]
    result = MMEncoder._stage_embeddings(native, ctx, requests, batch, keep_on_gpu=True)
    if inspect.isawaitable(result):
        asyncio.run(result)
    assert len(staged) == len(requests)
    for item, expected in zip(staged, (batch[:2], batch[2:])):
        assert torch.equal(item.embedding, expected)
        assert item.embedding.data_ptr() != expected.data_ptr()
        assert item.embedding.untyped_storage().nbytes() == item.embedding.nbytes


def test_receiver_errors_retain_native_timeout_and_admission_semantics():
    """SGLang reads message and maps HTTP 504/429 to deadline/capacity failures."""
    import grpc
    from smg_grpc_servicer.sglang.managed.encoder_service import Rejected
    from smg_grpc_servicer.sglang.managed.launch import receiver_error

    for error, status in [
        (Rejected(grpc.StatusCode.DEADLINE_EXCEEDED, "expired"), 504),
        (Rejected(grpc.StatusCode.RESOURCE_EXHAUSTED, "full"), 429),
        (TimeoutError(), 504),
        (ValueError("invalid receiver"), 400),
    ]:
        body, actual_status = receiver_error(error)
        assert actual_status == status
        assert body["message"]


def test_queued_cancel_does_not_wait_for_or_release_a_native_execution_slot(monkeypatch):
    """Queued controls can retire while all tensor windows belong to other requests."""
    import sys
    from unittest.mock import AsyncMock

    from smg_grpc_servicer.sglang.managed.encoder_service import EncoderService, Entry

    monkeypatch.setitem(
        sys.modules,
        "sglang.srt.disaggregation.encoder.runtime",
        SimpleNamespace(execute_encode_pipeline=AsyncMock()),
    )
    metadata = SimpleNamespace(publish=AsyncMock())
    server = SimpleNamespace(meta_registry=metadata)
    monkeypatch.setitem(sys.modules, "sglang.srt.disaggregation.encoder.server", server)
    if parent := sys.modules.get("sglang.srt.disaggregation.encoder"):
        monkeypatch.setattr(parent, "server", server, raising=False)

    async def scenario():
        config = complete(native_args(), "encoder")
        config["generation"] = "generation"
        backend = SimpleNamespace(
            scheduler=None, encoder=SimpleNamespace(release_request=AsyncMock())
        )
        service = EncoderService(config, backend, None)
        for _ in range(service.execution_capacity):
            await service.execution.acquire()
        entry = Entry(1, float("inf"), "cancelled", request=object())
        sibling = Entry(1, float("inf"), "sibling")
        service.queue.append(entry)
        service.queue.append(sibling)
        waiting, failure = asyncio.Event(), asyncio.Event()
        metadata.publish.side_effect = lambda *_args, **_kwargs: failure.set()

        async def early_metadata_reader():
            async with entry.refs.retain():
                waiting.set()
                await failure.wait()

        reader = asyncio.create_task(early_metadata_reader())
        await waiting.wait()
        service._cancel(entry)
        await asyncio.wait_for(entry.task, 1)
        await reader
        metadata.publish.assert_awaited_once()
        assert entry.quiesced and entry.request is None
        assert service.execution._value == 0
        assert service.cpu._value == config["max_cpu"]
        assert service.queue.popleft() is sibling
        assert not service.queue
        backend.encoder.release_request.assert_awaited_once_with("cancelled")

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["metadata", "register"])
def test_cancelled_queued_encode_retires_early_receiver_control(monkeypatch, kind):
    """Metadata or registration waits cannot pin a cancelled admission until its full deadline."""
    import sys
    from unittest.mock import AsyncMock

    from smg_grpc_servicer.sglang.managed.encoder_service import EncoderService, Entry, Rejected

    monkeypatch.setitem(
        sys.modules,
        "sglang.srt.disaggregation.encoder.runtime",
        SimpleNamespace(send_staged_embedding=AsyncMock()),
    )

    async def scenario():
        started, retired = asyncio.Event(), asyncio.Event()

        async def register(*_args):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                retired.set()

        server = SimpleNamespace(meta_registry=SimpleNamespace(wait=register))
        monkeypatch.setitem(sys.modules, "sglang.srt.disaggregation.encoder.server", server)
        if parent := sys.modules.get("sglang.srt.disaggregation.encoder"):
            monkeypatch.setattr(parent, "server", server, raising=False)
        else:
            monkeypatch.setitem(
                sys.modules, "sglang.srt.disaggregation.encoder", SimpleNamespace(server=server)
            )
        service = object.__new__(EncoderService)
        service.config = {"max_receivers": 2}
        service.backend = SimpleNamespace(
            encoder=SimpleNamespace(
                transfer_backend="zmq_to_scheduler", register_embedding_destinations=register
            )
        )
        entry = Entry(
            1,
            float("inf"),
            "rid",
            request=SimpleNamespace(prefill_host="10.0.0.2"),
        )
        service.part = AsyncMock(return_value=entry)
        task = asyncio.create_task(
            service.facade(
                kind,
                {
                    "req_id": "rid",
                    "part_idx": 0,
                    "receive_count": 2,
                    "receive_url": "10.0.0.2:1234",
                },
            )
        )
        await asyncio.wait_for(started.wait(), 1)
        assert entry.refs.count == 1
        entry.cancelled = True
        entry.refs.cancel()
        entry.cancellation.set()
        with pytest.raises(Rejected, match="control cancelled"):
            await asyncio.wait_for(task, 1)
        assert retired.is_set() and entry.refs.count == 0
        assert not entry.quiesced  # Only the actual Encode owner can remove its admission.

    asyncio.run(scenario())


@pytest.mark.parametrize("expire", [False, True])
@pytest.mark.parametrize("abandon_fails", [False, True])
def test_zmq_abandonment_wakes_native_wait_without_releasing_active_send(
    monkeypatch, expire, abandon_fails
):
    """Cancellation requests native abandonment, but charges survive the actual send owner."""
    import sys
    from unittest.mock import AsyncMock, Mock

    from smg_grpc_proto.generated import sglang_scheduler_pb2 as sg
    from smg_grpc_servicer.sglang.managed.encoder_service import EncoderService, Entry

    async def scenario():
        started, abandoned, finish = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def pipeline(*_args):
            started.set()
            await abandoned.wait()
            await finish.wait()  # Represents a send that already owns the native buffer.

        async def abandon(_rid):
            abandoned.set()
            if abandon_fails:
                raise RuntimeError("injected partial abandonment failure")

        monkeypatch.setitem(
            sys.modules,
            "sglang.srt.disaggregation.encoder.runtime",
            SimpleNamespace(execute_encode_pipeline=pipeline),
        )
        monkeypatch.setitem(
            sys.modules,
            "sglang.srt.managers.req_types",
            SimpleNamespace(Modality=SimpleNamespace(IMAGE="image")),
        )
        encoder = SimpleNamespace(
            _embedding_dims={"image": 1},
            _element_size=2,
            transfer_backend="zmq_to_scheduler",
            abandon_request=abandon,
            release_request=AsyncMock(),
            clear_abandoned_request=Mock(),
        )
        service = object.__new__(EncoderService)
        service.backend = SimpleNamespace(encoder=encoder)
        service.config = {"image_token_id": 1}
        service.processor = SimpleNamespace(
            prepare=lambda *_args: sg.GenerateRequest().SerializeToString()
        )
        service.submitter = object()
        service.cpu = asyncio.Semaphore(0)
        service.config = {"max_cpu": 1, "image_token_id": 1, "max_staging_bytes_per_request": 1024}
        service.cpu_active = 1
        service.execution = asyncio.BoundedSemaphore(1)
        await service.execution.acquire()
        entry = Entry(1, float("inf"), "rid", request=SimpleNamespace(mm_items=["image"]))
        if expire:

            async def expire_after_native_start():
                await started.wait()
                raise asyncio.TimeoutError  # noqa: UP041 - Python 3.10 compatibility

            # Exercise deadline expiry only after native ownership exists; CPU
            # scheduling delays must not turn this into a pre-admission test.
            entry.cancellation.wait = expire_after_native_start
        entry.task = asyncio.create_task(service._run(entry, execution_held=True))
        await asyncio.wait_for(started.wait(), 1)
        if not expire:
            service._cancel(entry)
        await asyncio.wait_for(abandoned.wait(), 1)
        assert entry.cancelled
        assert entry.request is not None and not entry.quiesced
        encoder.release_request.assert_not_awaited()
        assert service.execution._value == 0
        if abandon_fails:
            assert service.draining
            assert isinstance(entry.error, str) and entry.error
        finish.set()
        await asyncio.wait_for(entry.task, 1)
        if abandon_fails:
            assert service.draining and entry.terminal
            assert not entry.quiesced
            assert service.execution._value == 0
            encoder.release_request.assert_not_awaited()
            encoder.clear_abandoned_request.assert_not_called()
            return
        encoder.release_request.assert_awaited_once_with("rid")
        encoder.clear_abandoned_request.assert_called_once_with("rid")
        assert isinstance(entry.error, str) and entry.error
        assert entry.quiesced and service.execution._value == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("cancel", [False, True])
def test_artifact_materialization_keeps_control_loop_live_and_owns_cpu_work(cancel):
    """Control callbacks run during CPU work, and cancellation waits for its actual exit."""
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from smg_grpc_servicer.sglang.managed.encoder_bridge import RustPreprocessorBridge

    async def scenario():
        loop = asyncio.get_running_loop()
        started = asyncio.Event()
        finish = threading.Event()
        bridge = RustPreprocessorBridge(SimpleNamespace(preproc_executor=executor), 1024)
        requests = [object()]
        result = object()

        def materialize(actual_requests, modality):
            assert actual_requests is requests
            assert threading.get_ident() != control_thread
            loop.call_soon_threadsafe(started.set)
            assert finish.wait(2), "CPU owner was not released by the control loop"
            return result

        control_thread = threading.get_ident()
        bridge._materialize_batch = materialize
        task = asyncio.create_task(bridge.process_batch_mm_items(requests, None))
        try:
            await asyncio.wait_for(started.wait(), 1)
            if cancel:
                task.cancel()
                await asyncio.sleep(0)
            assert not task.done()
        finally:
            finish.set()
        if cancel:
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            assert await task is result

    with ThreadPoolExecutor(max_workers=1) as executor:
        asyncio.run(scenario())


def test_cpu_capacity_respects_affinity_and_container_quota(monkeypatch):
    """Host core counts must not override a smaller container CPU allocation."""
    from smg_grpc_servicer.sglang.managed import config

    monkeypatch.setattr(config.os, "sched_getaffinity", lambda _: set(range(16)))
    monkeypatch.setattr(config.Path, "read_text", lambda _: "250000 100000")
    assert config.available_cpus() == 2
    monkeypatch.setattr(config.Path, "read_text", lambda _: "max 100000")
    assert config.available_cpus() == 16


@pytest.mark.parametrize("processor_limit", [0, -1, None, True, 1.5])
def test_adaptive_cpu_limit_rejects_invalid_processor_bound(processor_limit):
    """A zero or nonintegral hard ceiling must fail instead of stalling dispatch."""
    from smg_grpc_servicer.sglang.managed.config import encoder_cpu_limit

    config = complete(native_args(), "encoder")
    config["processor_options"]["max_inflight"] = processor_limit
    with pytest.raises(ValueError, match="positive integer"):
        encoder_cpu_limit(config, 8)


def test_adaptive_cpu_limit_intersects_native_memory_and_host_bounds(monkeypatch):
    """An explicit ceiling cannot overrun actual CPU or retained tensor capacity."""
    from smg_grpc_servicer.sglang.managed import config as settings

    config = complete(native_args(), "encoder")
    config["max_cpu"] = config["processor_options"]["max_inflight"] = 32
    monkeypatch.setattr(settings, "available_cpus", lambda: 16)
    assert settings.encoder_cpu_limit(config, 1) == 2
    assert settings.encoder_cpu_limit(config, 32) == settings.encoder_window(config)[0]
    monkeypatch.setattr(settings, "available_cpus", lambda: 1)
    assert settings.encoder_cpu_limit(config, 8) == 1


@pytest.mark.parametrize("role", ["encoder", "prefill", "decode"])
def test_native_revision_needs_no_managed_config_file(role):
    """Existing native identity and role arguments are sufficient for default deployment."""
    args = native_args()
    args.revision = "pinned-weights"
    config = complete(args, role)
    contract = config if role == "encoder" else config["contract"]
    assert contract["model_revision"] == args.revision
    assert contract["tp_size"] == args.tp_size


def test_staging_budget_tracks_model_width_and_native_dtype():
    """Larger Qwen3-VL embeddings need a larger reservation, not a deployment override."""
    from smg_grpc_servicer.sglang.managed.config import encoder_staging_bytes

    assert encoder_staging_bytes(4096, "bfloat16") == 32 * 1024**2
    assert encoder_staging_bytes(5120, "bfloat16") == 40 * 1024**2
    assert encoder_staging_bytes(5120, "float32") == 80 * 1024**2
    with pytest.raises(ValueError, match="dimensions or dtype"):
        encoder_staging_bytes(0, "bfloat16")


@pytest.mark.parametrize("bind_host", ["0.0.0.0", "::"])
def test_wildcard_bind_uses_native_address_discovery(monkeypatch, bind_host):
    """Containers without resolvable hostnames reuse the native engine's route/NIC discovery."""
    import sys

    monkeypatch.setitem(
        sys.modules,
        "sglang.srt.utils.network",
        SimpleNamespace(get_local_ip_auto=lambda: "10.0.0.2"),
    )
    args = native_args()
    args.host = bind_host
    args.revision = "weights"
    config = complete(args, "encoder")
    assert config["receive_endpoint"] == "http://10.0.0.2:43000"
    assert config["worker_id"] == "encoder-10.0.0.2:43000"


@pytest.mark.parametrize("batch_size", [1, 2])
def test_materialized_pixels_remain_owned_after_artifact_release(batch_size):
    """Single and fused inputs retain writable storage without mutating wire artifacts."""
    import gc
    import struct

    torch = pytest.importorskip("torch")
    pytest.importorskip("sglang.srt.disaggregation.encoder.preprocessor")
    from smg_grpc_proto.generated import sglang_scheduler_pb2 as sg
    from smg_grpc_servicer.sglang.managed.encoder_bridge import ARTIFACT_KEY, RustPreprocessorBridge

    request = sg.GenerateRequest()
    mm = request.mm_inputs
    mm.pixel_values.dtype = "float32"
    mm.pixel_values.shape.extend([4, 1])
    mm.pixel_values.data = struct.pack("4f", 1, 2, 3, 4)
    for name, shape, values in (
        ("image_grid_thw", [1, 3], [1, 2, 2]),
        ("patches_per_image", [1], [4]),
    ):
        tensor = mm.model_specific_tensors[name]
        tensor.dtype = "int64"
        tensor.shape.extend(shape)
        tensor.data = struct.pack(f"{len(values)}q", *values)
    mm.mm_placeholders.add(length=1)
    wire = request.SerializeToString()
    requests = [{ARTIFACT_KEY: wire, "mm_items": [None]} for _ in range(batch_size)]
    result, counts = RustPreprocessorBridge(None, 4096)._materialize_batch(
        requests, SimpleNamespace(name="IMAGE")
    )
    del requests, request
    gc.collect()
    pixels = result.mm_inputs["pixel_values"]
    assert torch.equal(pixels.flatten(), torch.tensor([1, 2, 3, 4] * batch_size))
    pixels[0, 0] = 9
    unchanged = sg.GenerateRequest.FromString(wire)
    assert struct.unpack("4f", unchanged.mm_inputs.pixel_values.data) == (1, 2, 3, 4)
    assert counts == [1] * batch_size
    assert result.token_counts == [1] * batch_size
    assert result.grid_thw.tolist() == [[1, 2, 2]] * batch_size


def test_receiver_recognizes_pre311_asyncio_timeout(monkeypatch):
    """Python 3.10 asyncio.TimeoutError is distinct from the builtin exception."""
    from smg_grpc_servicer.sglang.managed.launch import receiver_error

    class AsyncTimeout(Exception):
        pass

    monkeypatch.setattr(asyncio, "TimeoutError", AsyncTimeout)
    assert receiver_error(AsyncTimeout("expired"))[1] == 504


def test_native_encode_refuses_duplicate_and_stale_instance(monkeypatch):
    """The standard encoder wire retains one owner and never replays a completed rid."""
    from smg_grpc_proto.generated import sglang_encoder_pb2 as ep
    from smg_grpc_servicer.sglang.managed.encoder_service import EncoderService, Rejected

    monkeypatch.setattr("smg_grpc_servicer.sglang.managed.encoder_service.healthy", lambda _: True)

    async def scenario():
        config = complete(native_args(), "encoder")
        config["generation"] = "generation"
        service = EncoderService(config, SimpleNamespace(scheduler=None), None)
        request = ep.EncodeRequest(
            req_id="generation.r_local_part_0", mm_items=["image"], num_parts=1, prefill_host="p"
        )
        entry = service._admit(request, 120)
        assert entry.rid == request.req_id and entry.request is request
        load = (await service.GetLoads(None, None)).loads[0]
        assert (load.num_running_reqs, load.num_waiting_reqs, load.max_running_requests) == (
            0,
            1,
            64,
        )
        with pytest.raises(Rejected, match="duplicate"):
            service._admit(request, 120)
        request.req_id = "old.r"
        with pytest.raises(Rejected, match="stale"):
            service._admit(request, 120)

    asyncio.run(scenario())


def test_collection_preserves_unknown_work_and_original_retention_horizon():
    """A cleanup optimization must neither expire live owners nor shorten replay retention."""
    from smg_grpc_servicer.sglang.managed.encoder_service import EncoderService, Entry

    service = object.__new__(EncoderService)
    service.config = {"retention_ms": 10}
    expired = Entry(99, 0, "expired", quiesced=True)
    boundary = Entry(100, 0, "boundary", quiesced=True)
    unknown = Entry(1, 0, "unknown", terminal=True, quiesced=False)
    service.entries = {entry.rid: entry for entry in (expired, boundary, unknown)}
    service.parts = dict(service.entries)
    service._collect(110)
    assert service.entries == service.parts == {"boundary": boundary, "unknown": unknown}
    service._collect(111)
    assert service.entries == service.parts == {"unknown": unknown}


@pytest.mark.parametrize(
    "route,kind",
    [
        ("scheduler_receive_meta_data", "metadata"),
        ("scheduler_receive_url", "register"),
        ("send", "send"),
    ],
)
def test_native_receiver_http_body_reaches_owner(route, kind):
    """Native JSON requests must bind the ASGI request, not a query parameter."""
    TestClient = pytest.importorskip("fastapi.testclient").TestClient
    from smg_grpc_servicer.sglang.managed.launch import receiver_app

    calls = []

    async def facade(operation, payload):
        calls.append((operation, payload))
        return {"req_id": payload["req_id"]}

    app = receiver_app({"max_control_bytes": 128}, SimpleNamespace(facade=facade))
    with TestClient(app) as client:
        payload = {"req_id": "admitted-request", "part_idx": 0}
        response = client.post("/" + route, json=payload)
        assert response.status_code == 200
        assert response.json() == {"req_id": "admitted-request"}
        assert calls == [(kind, payload)]
        assert client.post("/" + route, content=b"x" * 129).status_code == 413
        assert client.post("/" + route, json=[]).status_code == 400
        assert calls == [(kind, payload)]


def test_encoder_forward_wait_yields_and_retains_owner_through_repeated_cancel(monkeypatch):
    """Cancellation cannot outlive the GPU completion owner; other loop tasks still run."""
    import sys
    import threading

    from smg_grpc_servicer.sglang.managed.plugin import await_encoder_forward

    async def scenario():
        loop = asyncio.get_running_loop()
        started = asyncio.Event()
        finish = threading.Event()
        event_loop_thread = threading.get_ident()
        device = object()
        stream = object()
        result = SimpleNamespace(is_cuda=True, device=device)

        class Event:
            def record(self, selected):
                assert selected is stream
                assert threading.get_ident() == event_loop_thread

            def synchronize(self):
                assert threading.get_ident() != event_loop_thread
                loop.call_soon_threadsafe(started.set)
                assert finish.wait(5)

        def current_stream(selected):
            assert selected is device
            return stream

        monkeypatch.setitem(
            sys.modules,
            "torch",
            SimpleNamespace(cuda=SimpleNamespace(Event=Event, current_stream=current_stream)),
        )

        async def original(_encoder):
            return result

        task = asyncio.create_task(await_encoder_forward(original, object()))
        try:
            await asyncio.wait_for(started.wait(), 2)
            for _ in range(2):
                task.cancel()
                await asyncio.sleep(0)
                assert not task.done()
            # asyncio.run shutdown cancels every Task, including a to_thread Task.
            # The private executor Future must remain owned until GPU completion.
            for pending in asyncio.all_tasks():
                if pending is not asyncio.current_task():
                    pending.cancel()
            for _ in range(4):
                await asyncio.sleep(0)
            assert not task.done()
        finally:
            finish.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        assert await await_encoder_forward(original, object()) is result

    asyncio.run(scenario())


@pytest.mark.parametrize("enabled", [None, "", "0", "1", "invalid"])
def test_native_launch_switch_preserves_default_and_parsed_arguments(monkeypatch, enabled):
    """Only explicit opt-in changes dispatch; invalid values cannot silently use legacy HTTP."""
    from smg_grpc_servicer.sglang.managed import launch, plugin

    calls = []
    args = SimpleNamespace(encoder_only=True, tp_size=2)
    monkeypatch.delenv(plugin.ENABLE_ENV, raising=False)
    if enabled is not None:
        monkeypatch.setenv(plugin.ENABLE_ENV, enabled)
    monkeypatch.setattr(launch, "run_server", lambda value: calls.append(("managed", value)))
    if enabled == "invalid":
        with pytest.raises(ValueError, match="must be 0 or 1"):
            plugin.native_launch(lambda value: calls.append(("native", value)), args)
        assert not calls
    else:
        plugin.native_launch(lambda value: calls.append(("native", value)), args)
        assert calls == [("managed" if enabled == "1" else "native", args)]


@pytest.mark.parametrize("module", ["sglang.launch_server", "sglang.cli.main"])
def test_native_entry_hook_targets_running_module(monkeypatch, module):
    """The -m entry is a distinct module; the CLI imports the canonical run_server."""
    import sys

    from smg_grpc_servicer.sglang.managed import plugin

    registrations = []
    monkeypatch.setitem(
        sys.modules,
        "sglang.srt.plugins.hook_registry",
        SimpleNamespace(
            HookRegistry=SimpleNamespace(register=lambda *args: registrations.append(args)),
            HookType=SimpleNamespace(AROUND="around"),
        ),
    )
    monkeypatch.setattr(sys.modules["__main__"], "__spec__", SimpleNamespace(name=module))
    monkeypatch.delenv(plugin.ROLE_ENV, raising=False)
    monkeypatch.delenv(plugin.ENABLE_ENV, raising=False)
    plugin.register()
    assert not registrations
    monkeypatch.setenv(plugin.ENABLE_ENV, "1")
    plugin.register()
    target = "__main__" if module == "sglang.launch_server" else "sglang.launch_server"
    assert registrations == [(target + ".run_server", plugin.native_launch, "around")]


def test_role_hooks_install_after_native_plugins_loaded_once(monkeypatch):
    """An idempotent native loader must not hide the role discovered after CLI parsing."""
    import sys

    from smg_grpc_servicer.sglang.managed import plugin

    registrations, applied = [], []
    monkeypatch.setattr(plugin, "_registered_role", None)
    monkeypatch.setenv(plugin.ROLE_ENV, "encoder")
    monkeypatch.setitem(
        sys.modules, "sglang.srt.plugins", SimpleNamespace(load_plugins=lambda: None)
    )
    monkeypatch.setitem(
        sys.modules,
        "sglang.srt.plugins.hook_registry",
        SimpleNamespace(
            HookRegistry=SimpleNamespace(
                register=lambda *args: registrations.append(args),
                apply_hooks=lambda: applied.append(True),
            ),
            HookType=SimpleNamespace(AROUND="around", AFTER="after"),
        ),
    )
    plugin.load_role_hooks()
    plugin.load_role_hooks()
    assert len(registrations) == 2
    assert registrations[1][1] is plugin.await_encoder_forward
    assert applied == [True, True]


@pytest.mark.parametrize("override", [None, "4"])
def test_encoder_defaults_precede_config_and_runtime(monkeypatch, override):
    """Native startup inherits media defaults without replacing deployment overrides."""
    import os

    from smg_grpc_servicer.sglang.managed import config, launch

    for name in ("SMG_MM_THREADS", "SMG_MM_IMAGE_CONCURRENCY", "SMG_SERVICER_WORKER_THREADS"):
        monkeypatch.delenv(name, raising=False)
    if override is not None:
        monkeypatch.setenv("SMG_MM_THREADS", override)

    class StopBeforeRuntime(Exception):
        pass

    def inspect_defaults(*args):
        assert os.environ["SMG_MM_THREADS"] == (override or "32")
        assert os.environ["SMG_MM_IMAGE_CONCURRENCY"] == "8"
        assert os.environ["SMG_SERVICER_WORKER_THREADS"] == "8"
        raise StopBeforeRuntime

    monkeypatch.setattr(config, "complete", inspect_defaults)
    with pytest.raises(StopBeforeRuntime):
        launch.encoder_main(SimpleNamespace())

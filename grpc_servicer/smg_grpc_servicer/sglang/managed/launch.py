"""Managed lifecycle behind native SGLang launch; the module entry remains a compatibility shim."""

import asyncio
import json
import logging
import os
import signal
import sys
import tempfile
import uuid
from contextlib import contextmanager

from .plugin import EVENTS_ENV, INSTANCE_ENV, ROLE_ENV, WORKER_ENV


def set_identity(config, role):
    """Give every native role a fresh process fence before plugins or TP ranks load."""
    config["generation"] = uuid.uuid4().hex
    os.environ[ROLE_ENV] = role
    os.environ[WORKER_ENV] = config["worker_id"]
    os.environ[INSTANCE_ENV] = config["generation"]


class ManagedBootstrap:
    """Own only native encoder registration; no frontend encode dispatcher or processor."""

    def __init__(self, args):
        self.args = args
        self.server = None

    def start(self):
        """Start the native receiver bootstrap before native scheduler ranks register."""
        from sglang.srt.disaggregation.encoder.receiver import EncoderBootstrapServer

        self.server = EncoderBootstrapServer(
            host=self.args.host,
            port=self.args.encoder_bootstrap_port,
            urls=[],
        )

    def shutdown(self):
        """Release the native bootstrap server with the scheduler process group."""
        if self.server is not None:
            self.server.close()


def launcher(config):
    """Construct a separate lifecycle without changing the default SGLang launcher."""
    from smg_grpc_servicer.sglang.rust import SglangLauncher, launch_scheduler_ranks

    class ManagedLauncher(SglangLauncher):
        ipc_prefix = "sglang-managed-servicer"
        warmup_env = ""

        def server_extras(self, args, tokenizer_dir):
            """Managed startup never constructs the legacy encode-dispatch bridge."""
            if args.disaggregation_mode != config["role"]:
                raise ValueError("managed role must equal native disaggregation mode")
            if not args.language_only:
                raise ValueError("managed P/D requires --language-only")
            if os.environ.get("SMG_SGLANG_MM_PROCESSOR"):
                raise ValueError("managed P/D refuses legacy media preprocessing")
            return {"managed_config": json.dumps(config)}

        def launch(self, args, handshake_port, facts):
            """Use unchanged native TP/DP spawning and P's native receiver bootstrap."""
            bootstrap = ManagedBootstrap(args) if config["role"] == "prefill" else None
            return launch_scheduler_ranks(
                args, handshake_port=handshake_port, encode_bridge=bootstrap
            )

    return ManagedLauncher()


def main():
    """Load finite limits explicitly and assign a new process fence on every launch."""
    from sglang.srt.server_args import prepare_server_args

    return run_server(prepare_server_args(sys.argv[1:]))


def run_server(args):
    """Derive the role from native arguments; own initialization and shutdown exactly once."""
    from smg_grpc_servicer.sglang.rust import resolve_servicer_impl

    from .config import complete

    if resolve_servicer_impl(args) != "rust":
        raise ValueError("managed EPD requires the Rust servicer")
    if args.encoder_only:
        return encoder_main(args)
    config = complete({}, args, args.disaggregation_mode)
    if config["role"] not in ("prefill", "decode"):
        raise ValueError("managed P/D requires native --disaggregation-mode prefill or decode")
    set_identity(config, config["role"])
    # The temporary directory is kept until the native ranks and Rust servicer have exited.
    with tempfile.TemporaryDirectory(prefix="smg-managed-") as directory:
        endpoint = f"ipc://{directory}/metadata.sock" if config["role"] == "prefill" else None
        config["metadata_endpoint"] = endpoint
        if endpoint:
            os.environ[EVENTS_ENV] = endpoint
        from .plugin import load_role_hooks

        load_role_hooks()
        raise SystemExit(asyncio.run(launcher(config).serve(args)))


@contextmanager
def shutdown_signals(server):
    """Let the owner drain before exiting; Uvicorn otherwise re-raises SIGTERM first."""
    previous = {
        sig: signal.signal(sig, server.handle_exit) for sig in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def receiver_error(error):
    """Preserve the native receiver's message field and timeout/admission status semantics."""
    import grpc

    from .encoder_service import Rejected

    if isinstance(error, (TimeoutError, asyncio.TimeoutError)):
        status = 504
    elif isinstance(error, Rejected):
        status = {
            grpc.StatusCode.RESOURCE_EXHAUSTED: 429,
            grpc.StatusCode.DEADLINE_EXCEEDED: 504,
        }.get(error.code, 400)
    else:
        status = 400
    return {"message": str(error) or "native receiver timeout"}, status


def receiver_app(config, service):
    """Bind native receiver controls with concrete ASGI request annotations."""
    from fastapi import FastAPI, Request
    from fastapi.responses import JSONResponse

    from .encoder_service import Rejected

    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @app.post("/{route}")
    async def receive(route: str, request: Request):
        """Expose only receiver controls; every call resolves an admitted managed operation."""
        kinds = {
            "scheduler_receive_meta_data": "metadata",
            "scheduler_receive_url": "register",
            "send": "send",
        }
        if route not in kinds:
            return JSONResponse(
                {"message": "unsupported managed receiver operation"}, status_code=404
            )
        data = bytearray()
        async for chunk in request.stream():
            data.extend(chunk)
            if len(data) > config["max_control_bytes"]:
                return JSONResponse({"message": "control request too large"}, status_code=413)
        try:
            payload = json.loads(data)
            if not isinstance(payload, dict):
                raise ValueError("receiver control must be an object")
            result = await service.facade(kinds[route], payload)
            return result or {"status": "ok"}
        except (Rejected, ValueError, TimeoutError, asyncio.TimeoutError) as error:  # noqa: UP041 - Python 3.10
            body, status = receiver_error(error)
            return JSONResponse(body, status_code=status)

    return app


async def serve(config, args):
    """Bind bounded control listeners only after native TP ranks initialize successfully."""
    import grpc
    import uvicorn
    from smg.smg_rs import ManagedEncoderProcessor
    from smg_grpc_proto.generated import sglang_managed_pb2_grpc as rpc

    from .config import encoder_cpu_limit, encoder_staging_bytes
    from .encoder_bridge import launch_runtime
    from .encoder_service import EncoderService

    backend = launch_runtime(args, config["max_active"])
    service = server = None
    try:
        # These are ceilings, not a fixed running concurrency. The registry starts
        # small and grows only when CPU supply leaves the native scheduler idle.
        config["max_staging_bytes_per_request"] = encoder_staging_bytes(
            backend.encoder.model_config.hidden_size, config["embedding_dtype"]
        )
        config["max_cpu"] = encoder_cpu_limit(config, backend.scheduler.max_batch_size)
        config["processor_options"]["max_inflight"] = config["max_cpu"]
        # Match the native visual input cast before serializing and copying pixels.
        config["processor_options"]["encoder_dtype"] = str(
            backend.encoder.model.visual.dtype
        ).removeprefix("torch.")
        processor = ManagedEncoderProcessor(
            config["processor_options"],
            args.tokenizer_path or args.model_path,
            config["max_artifact_bytes"],
        )
        service = EncoderService(config, backend, processor)
        app = receiver_app(config, service)

        server = grpc.aio.server(
            options=[("grpc.max_receive_message_length", config["max_control_bytes"])]
        )
        rpc.add_ManagedEncoderServicer_to_server(service, server)
        if not server.add_insecure_port(config["control_listen"]):
            raise RuntimeError("managed encoder control port could not be bound")
        service.start()
        await server.start()

        class ManagedHttpServer(uvicorn.Server):
            """Keep process termination behind the managed native-owner drain."""

            def handle_exit(self, sig, frame):
                """Close admission immediately, before HTTP observers finish their grace period."""
                service.draining = True
                super().handle_exit(sig, frame)

            def capture_signals(self):
                """Consume shutdown signals without Uvicorn re-raising them before cleanup."""
                return shutdown_signals(self)

        http = ManagedHttpServer(
            uvicorn.Config(
                app,
                host=args.host,
                port=args.port,
                limit_concurrency=config["max_http_connections"],
                # HTTP observers cannot indefinitely postpone the native-owner drain.
                timeout_graceful_shutdown=config["shutdown_timeout_seconds"],
            )
        )
        await http.serve()
    finally:
        await shutdown(service, server, backend, config.get("shutdown_timeout_seconds", 30))


async def stop_followers(backend):
    """Stop only this launcher's native TP child processes with bounded joins."""
    for process in backend.tp_processes:
        if process.is_alive():
            process.terminate()
    for process in backend.tp_processes:
        await asyncio.to_thread(process.join, 1)
        if process.is_alive():
            process.kill()
            await asyncio.to_thread(process.join, 1)


async def shutdown(service, server, backend, timeout):
    """Drain owned work, or terminate the entire E process without false quiescence receipts."""
    if service is not None:
        service.draining = True
        for entry in service.entries.values():
            service._cancel(entry)

    async def drain():
        if service is not None:
            while any(not entry.quiesced for entry in service.entries.values()):
                await asyncio.sleep(0.05)

    try:
        await asyncio.wait_for(drain(), timeout)
    except asyncio.TimeoutError:  # noqa: UP041 - Python 3.10
        logging.getLogger(__name__).error(
            "managed E drain expired; terminating owned native processes"
        )
        if server is not None:
            await server.stop(0)
        await stop_followers(backend)
        # Rank zero and its CUDA/send threads live here. Do not unwind Python
        # buffers under those threads or announce safe release while they run.
        os._exit(1)
    # Keep reconciliation reachable while native owners drain, then give terminal
    # events a bounded opportunity to reach their existing observers.
    if server is not None:
        await server.stop(1)
    if service is not None and service.dispatcher is not None:
        service.dispatcher.cancel()
        await asyncio.gather(service.dispatcher, return_exceptions=True)
    try:
        await backend.stop()
    finally:
        await stop_followers(backend)


def encoder_main(args):
    """Require finite deployment limits and assign a fresh instance generation at startup."""
    from smg_grpc_servicer.sglang.mm_defaults import configure_threads

    from .config import complete, encoder_window

    # Initialize before native TP children or Rust preprocessing thread pools.
    configure_threads()
    config = complete({}, args, "encoder")
    positive = (
        "max_records",
        "max_active",
        "max_work",
        "max_context_bytes",
        "retention_ms",
        "max_duration_ms",
        "max_clock_error_ms",
        "max_cpu",
        "max_artifact_bytes",
        "max_staging_bytes_per_request",
        "max_receivers",
        "max_control_bytes",
        "max_images",
        "drr_quantum",
        "aging_seconds",
        "max_early_waiters",
        "early_wait_seconds",
        "load_interval_ms",
        "max_http_connections",
    )
    if any(
        type(config.get(key)) not in (int, float) or not 0 < config[key] < 2**53 for key in positive
    ):
        raise ValueError("managed encoder requires finite positive resource and deadline limits")
    if any(
        type(config[key]) is not int
        for key in positive
        if key not in ("aging_seconds", "early_wait_seconds")
    ):
        raise ValueError(
            "managed encoder resource counts, bytes and millisecond limits must be integers"
        )
    if config["max_images"] > 32 or config["max_active"] > config["max_records"]:
        raise ValueError("invalid managed encoder admission bounds")
    if any(type(weight) is not int or weight <= 0 for weight in config["tenant_weights"].values()):
        raise ValueError("DRR weights must be positive integers")
    shutdown_timeout = config.get("shutdown_timeout_seconds", 30)
    if type(shutdown_timeout) not in (int, float) or not 0 < shutdown_timeout <= 300:
        raise ValueError("managed shutdown timeout must be in (0, 300] seconds")
    set_identity(config, "encoder")
    os.environ["SMG_SGLANG_MANAGED_PROCESSOR_CONTRACT"] = config["processor_contract"]
    native_window, _ = encoder_window(config)
    os.environ["SMG_SGLANG_MANAGED_BATCH_BYTES"] = str(config["max_artifact_bytes"] * native_window)
    from .plugin import load_role_hooks

    load_role_hooks()
    if not args.encoder_only or args.language_only:
        raise ValueError("managed E requires native --encoder-only")
    if args.encoder_transfer_backend not in ("mooncake", "zmq_to_scheduler"):
        raise ValueError("managed E supports only Mooncake or scheduler ZMQ transfers")
    asyncio.run(serve(config, args))


if __name__ == "__main__":
    main()

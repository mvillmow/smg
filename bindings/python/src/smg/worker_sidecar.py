"""Standalone two-tier SMG Worker process for engine gRPC endpoints."""

from __future__ import annotations

import argparse
import signal
import socket
import threading
import time

from smg.worker import WorkerControlServer


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Rust SMG Worker sidecar")
    parser.add_argument("--bind-address", required=True)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--engine-type", choices=["sglang", "vllm", "tokenspeed"], required=True)
    parser.add_argument("--engine-transport", choices=["grpc", "zmq"], default="grpc")
    parser.add_argument("--engine-endpoint", required=True)
    parser.add_argument("--zmq-handshake-address")
    parser.add_argument("--engine-count", type=int, default=1)
    parser.add_argument("--model-id", action="append", dest="model_ids", required=True)
    parser.add_argument("--max-concurrent-requests", type=int, default=0)
    parser.add_argument("--drain-secs", type=float, default=5.0)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    if args.drain_secs < 0:
        raise ValueError("--drain-secs must be non-negative")
    if args.engine_count <= 0:
        raise ValueError("--engine-count must be positive")
    if args.engine_transport == "zmq" and args.engine_type == "sglang":
        raise ValueError("SGLang Worker transport does not support ZMQ yet")
    stopped = threading.Event()
    server = WorkerControlServer(
        bind_address=args.bind_address,
        worker_id=args.worker_id,
        engine_type=args.engine_type,
        hostname=socket.gethostname(),
        engine_endpoint=args.engine_endpoint,
        model_ids=args.model_ids,
        features=["generate", "stream", "abort"],
        max_concurrent_requests=max(0, args.max_concurrent_requests),
        inference_enabled=True,
        engine_attributes={
            "model_path": args.model_ids[0],
            "tokenizer_path": args.model_ids[0],
            "engine_transport": args.engine_transport,
        },
        engine_transport=args.engine_transport,
        zmq_handshake_address=args.zmq_handshake_address,
        engine_count=args.engine_count,
    )
    server.set_health("serving", "ready")

    def stop(_signum: int, _frame: object) -> None:
        if stopped.is_set():
            return
        server.set_health("draining", "draining")
        stopped.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    stopped.wait()
    time.sleep(args.drain_secs)
    server.set_health("not_serving", "stopped")
    server.stop(max(1.0, args.drain_secs))


if __name__ == "__main__":
    main()

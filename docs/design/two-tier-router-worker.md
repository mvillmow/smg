# Two-Tier SMG Router/Worker Architecture

Status: draft implementation on `feat/two-tier-router-worker`

## Decision

Split SMG into a fleet-level Router and a node-local Worker. The Router owns
admission, fleet membership, and request placement. The Worker owns engine
discovery, readiness, topology, and request execution.

```mermaid
flowchart LR
    C[Client] --> R[SMG Router]
    R -->|WorkerControl gRPC| WC[SMG Worker control plane]
    R -->|WorkerInference gRPC| DP[SMG Worker data plane]
    WC --> E[Engine coordinator]
    DP -->|native gRPC or IPC| S[Engine scheduler]
```

Control and inference are separate versioned Rust gRPC services. They may share
a process, but they must have distinct endpoint identities so the Router never
sends inference traffic to a control-only listener.

## Protocol boundary

`WorkerControl` is the discovery and lifecycle API:

- `GetIdentity`: stable Worker ID plus a per-process instance ID;
- `GetCapabilities`: API version and supported operations;
- `GetHealth`: starting, serving, degraded, draining, or not serving;
- `GetTopology`: the Worker-owned inference endpoints and rank layout.

`WorkerInference` is the request path. Its first slice should cover regular
generation, token streaming, and abort. Unsupported workloads must fail closed
until their contracts are explicit.

The Router uses `WorkerControl` for registration and periodic health. It uses
only `WorkerInference` for requests. It must not bypass the Worker and connect
to an engine endpoint advertised as node-local implementation detail.

## Python binding

Some engine coordinators are Python processes. A small PyO3 binding may start
the Rust tonic server in that coordinator after scheduler processes fork.
Python only announces lifecycle transitions such as starting, serving, and
draining; health reads are served from Rust memory and do not acquire the GIL.

The binding is a compatibility and lifecycle hook, not the inference path. The
same constructor and lifecycle calls should work across engines, while each
engine supplies its own endpoint, model IDs, features, and readiness timing.

## Performance assessment

Changing only the gRPC implementation from Python to Rust does not guarantee an
end-to-end gain. The material improvement comes when parsing, streaming,
backpressure, cancellation, and scheduler IPC stay in Rust. A Rust server that
calls Python on every request retains GIL and conversion costs.

Evaluate the architecture with:

1. generate TTFT and inter-token latency at p50/p99;
2. Router and Worker CPU per generated token;
3. cancellation propagation and disconnect cleanup;
4. admission/backpressure behavior under overload;
5. health polling cost and coordinator shutdown time.

## Lifecycle

The embedded Worker control server starts in the engine coordinator after
multiprocessing forks. It begins in `STARTING`, becomes `SERVING` only after
engine warmup, moves to `DRAINING` before data-plane shutdown, and stops after
in-flight requests have had a bounded drain window.

Configuration is opt-in and shared across engines:

- `SMG_WORKER_CONTROL_BIND_ADDRESS`;
- `SMG_WORKER_ID` and optional `SMG_WORKER_INSTANCE_ID`;
- `SMG_WORKER_ZONE`;
- `SMG_WORKER_ENGINE_ENDPOINT`, which must be Router-reachable and must not
  advertise an unspecified address such as `0.0.0.0`.

If the control server is explicitly enabled but cannot start, the coordinator
must fail closed. An unexpected control-server exit also initiates shutdown.

## Current implementation

- `WorkerMode::{Engine, Smg}` keeps endpoint identity separate from transport
  and engine runtime.
- `WorkerControl` protobuf and Rust client/server bindings implement identity,
  capabilities, health, and topology.
- The Rust mock worker serves control and scheduler APIs for GPU-free tests.
- The PyO3 binding hosts the Rust control server and exposes lifecycle health
  transitions without putting Python on the request path.
- Router registration performs a versioned Worker handshake before admitting
  an explicit SMG Worker.
- Worker specs can carry separate `control_url` and inference `url` values;
  registration and periodic health use only the control endpoint.
- SGLang's coordinator can opt in to the embedded control server, transitions
  it after warmup and before drain, and treats unexpected exit as fatal.

## Remaining work

1. Preserve Worker mode and discovered metadata through every registration,
   replacement, mesh, and service-discovery path.
2. Persist discovered Worker identity, capabilities, and topology as observed
   metadata instead of treating the handshake as a boolean probe.
3. Add the minimal Rust `WorkerInference` service and client.
4. Route Two-Tier pools only to SMG Workers and keep legacy engine pools
   isolated.
5. Validate streaming, cancellation, and coordinator shutdown with an actual
   model on a multi-GPU dev host.

## Upstream alignment

- [SGLang native Rust gRPC RFC](https://github.com/sgl-project/sglang/issues/22558)
  proposes progressive migration to direct Rust scheduler IPC while retaining
  Python for cold coordinator operations.
- [vLLM Rust frontend roadmap](https://github.com/vllm-project/vllm/issues/44280)
  uses the EngineCore boundary for a Rust request path.
- [vLLM Rust frontend RFC](https://github.com/vllm-project/vllm/issues/40846)
  attributes frontend-heavy gains to moving frontend work into Rust.

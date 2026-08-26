# API Stability Policy

SMG protects externally consumed contracts. Every regular package under
`crates/**` must remain a workspace member, inherit workspace lints, and pass the
repository's existing workspace formatting, Clippy, and test jobs. The machine
inventory in `governance/api-surfaces.toml` is the authoritative package
classification.

## Public boundary

| Category | Boundary |
| --- | --- |
| Published crate | Rust public API is SemVer-compatible for its documented public feature profiles. |
| `smg-client` | Public Rust SDK and declared endpoint surface are SemVer-compatible. |
| `mock-worker` | Quality-controlled infrastructure; no independent public Rust API promise. |
| `model_gateway` | HTTP/OpenAPI, CLI, configuration, released artifacts, and documented operational behavior; not internal Rust modules. |
| Python and Go bindings | Quality and integration compatibility while version-locked to core; no independent Rust API promise. |
| Engine and mesh protobuf | Stable wire contracts, including supported generated consumers. |

Stable HTTP routes, methods, required inputs, responses, errors, streaming media
types, and authentication behavior are part of the public boundary. Preview and
internal routes must be explicitly classified. Supported CLI flags, configuration
keys, defaults, exit behavior, and machine-consumed output are also contracts.

## Compatibility

- **Additive** preserves existing supported consumers while adding capability.
- **Deprecated-compatible** provides a documented replacement while retaining the
  old behavior for the full deprecation window.
- **Breaking** removes, restricts, reinterprets, or observably changes a supported
  contract, including a failure to compile, import, send, parse, or operate as
  before.

For Rust crates at 1.0 or later, a breaking public API change requires a major
release. Before 1.0, patch releases remain compatible and a breaking change requires
the next minor release. Protobuf field numbers and meanings remain immutable; removed
field and enum names and numbers remain reserved, and an incompatible RPC change
requires a versioned replacement.

## Deprecation and intentional breaks

Normal removal follows deprecation for at least two published minor releases and 90
days, whichever is later. The deprecation documentation identifies the replacement
and migration path. An emergency security change may shorten the window only when its
pull request records the reason, affected versions, approvers, and replacement path.

An intentional break requires the `api-break-approved` label, the affected contract
and classification, the applicable major or pre-1.0 minor version decision, release
notes, a concrete migration path, and approval from the applicable CODEOWNER and a
Core Maintainer acting as release owner. If one person fills both roles, a second Core
Maintainer approves. Exceptions must be narrow, documented, owned, and expire within
90 days; silent waivers and broad local weakenings are not exceptions.

## Enforcement

The repository's existing workspace checks enforce package quality:

```bash
cargo +nightly fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test
```

The inventory checker confirms all `crates/**` manifests remain covered and that
release and version registries agree with governed packages:

```bash
python3 scripts/check_api_governance.py --check
```

Dedicated compatibility checks enforce the published Rust/SDK, protobuf, and
HTTP/OpenAPI boundaries directly in the existing pull-request workflow.

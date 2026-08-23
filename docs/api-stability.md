# API Stability Policy

SMG treats compatibility as a property of every externally consumed contract, not
only its network protocols. The required end state applies strict formatting,
linting, testing, and standards-drift rules to every package under `crates/`.
Published libraries and other public surfaces receive the additional compatibility
guarantees defined below. Policy decisions and review requirements apply immediately;
mechanical blocking is activated in stages as described under Enforcement commands.

## Contract categories

| Category | Surface | Compatibility promise |
| --- | --- | --- |
| `published-library` | The 15 publishable libraries under `crates/` | Rust public-API compatibility under the package's version and supported feature profiles |
| `quality-only` | `crates/mock_worker` (package `mock-worker`, with `publish = false`) | Strict workspace quality gates, but no independent public Rust API promise |
| `public-sdk` | `clients/rust` (package `smg-client`) | Independent Rust SemVer and a declared supported endpoint surface |
| `external-application` | `model_gateway` | Compatibility for HTTP/OpenAPI, CLI, configuration, released binary and image behavior, and documented operational behavior; no Rust module-level SemVer promise |
| `version-locked-binding` | `bindings/python` and `bindings/golang` | Quality and integration compatibility while version-locked to core; no independent Rust API promise |

The engine and mesh protobuf definitions are stable wire contracts. Generated
clients, stable HTTP endpoints, and supported CLI and configuration interfaces are
also contracts even though they are not Rust packages. Public WIT worlds and
versioned data schemas remain explicit contracts; their versioned compatibility
fixtures and upgrade-test enforcement activate only after the Rust, protobuf,
HTTP/OpenAPI, and all-crates quality gates are blocking and the combined exit audit
passes.

Every governed change uses one of three compatibility classifications:

- **Additive** adds a capability while preserving compilation, import, wire,
  request/response, generated-client, CLI, configuration, and operational behavior
  for every supported consumer.
- **Deprecated-compatible** introduces a documented replacement while retaining the
  old contract and its observable behavior for the full deprecation window.
- **Breaking** removes, restricts, reinterprets, or observably changes a supported
  contract, or causes a supported consumer to fail to compile, import, send, parse,
  or operate as before.

## Rust package compatibility

The following publishable `crates/` packages are SemVer-governed until an explicit
manifest change reclassifies them:

1. `data-connector`
2. `engine-zmq-client`
3. `kv-index`
4. `llm-multimodal`
5. `llm-tokenizer`
6. `openai-protocol`
7. `reasoning-parser`
8. `smg-auth`
9. `smg-grpc-client`
10. `smg-mcp`
11. `smg-mesh`
12. `smg-mm-rdma`
13. `smg-wasm`
14. `tool-parser`
15. `wfaas`

For packages at `1.0.0` or later, an incompatible public-API change requires a
major-version release. For `0.y.z` packages, patch releases remain compatible; an
incompatible change increments `y` and resets `z` to zero. Compatibility covers the
public API emitted by rustdoc for every documented supported feature profile.

For a published Rust library or the public Rust SDK, a change is additive only when
all supported feature profiles remain SemVer-compatible and the SDK's declared
endpoint surface remains compatible. Keeping the old item or endpoint usable while
marking it deprecated and documenting its replacement is deprecated-compatible. A
SemVer-incompatible item change, supported-feature removal, incompatible SDK API, or
endpoint change is breaking; adding a required trait item or exhaustively matched
enum variant is not additive merely because it adds syntax.

Under the GOV-02 inventory gate, a publishable package cannot silently escape
governance. Adding a package or changing its publication state requires updating the
authoritative API-surface inventory, release registry, ownership, and compatibility
checks in the same change.

## Protobuf wire compatibility

The protobuf definitions under `crates/grpc_client/proto` and
`crates/mesh/src/proto` are stable wire contracts. Once the protobuf lane is
activated, lint and breaking-change checks compare them with the pull request's merge
base.

- Existing field numbers and meanings are immutable within a supported package.
- Removed fields and enum values reserve both their numbers and names.
- An RPC cannot be removed or changed incompatibly in place; introduce a versioned
  replacement instead.
- Additive optional fields, messages, enum values, and RPCs are permitted when
  generated consumers continue to compile or import.

An addition is additive only under that generated-consumer condition. Marking and
retaining an existing field, enum value, or RPC while a replacement is available is
deprecated-compatible. Reusing or changing a field number or meaning, removing a
field or enum value before its support window ends, changing an RPC in place, or
breaking a generated consumer is breaking. Any later field or enum removal still
reserves its number and name.

## HTTP and OpenAPI compatibility

Stable routes, methods, required inputs, response schemas, error behavior, streaming
media types, and authentication behavior form the HTTP contract. Preview and internal
routes must be explicitly classified; omission from the generated OpenAPI document is
not itself a classification.

The required mechanical end state uses the generated OpenAPI document as the
canonical description of the declared HTTP surface. In that state, generated clients
remain synchronized with it, and the activated compatibility lane assesses changes
against the pull request's merge base.

A new route, method, or optional input is additive only when existing requests,
responses, and generated clients remain compatible. A response field or enum value
is additive only when supported consumers accept it; it is breaking when strict or
generated consumers cannot parse or compile against it. Retaining an old route,
schema, authentication behavior, and generated-client interface while documenting a
replacement is deprecated-compatible. Removing or renaming a stable operation,
making an input required, narrowing accepted values, or incompatibly changing a
schema, error, streaming media type, authentication behavior, or generated-client
interface is breaking.

## CLI and configuration compatibility

Supported flags, configuration keys, defaults, exit behavior, and machine-consumed
output remain compatible for their documented support window. Renames provide an
alias or migration path for the normal deprecation window. A default change that
alters observable behavior requires explicit release notes and compatibility review.

A new optional flag or key is additive only when its default preserves existing
behavior and machine-consumed output remains parseable. A rename is
deprecated-compatible only while the old flag or key remains as an alias or a
behavior-preserving migration for the full window. Removing an interface, adding a
required setting, changing a default or exit behavior, or incompatibly changing
machine-consumed output is breaking.

Application internals may change without a compatibility process when none of these
external behaviors change.

## Deprecation and removal

A public item must be deprecated before normal removal. The default removal window is
two published minor releases and at least 90 days, whichever is later. Documentation
must identify the replacement and migration path when deprecation begins.

Removal or another incompatible change requires a migration note and the version
decision appropriate to the affected contract. An emergency security change may
shorten the window, but its pull request must record the reason, approvers, affected
versions, and replacement path.

## Intentional breaking changes

An intentional break must be visible and reviewable. Its pull request must include:

- the `api-break-approved` label;
- the affected contract and compatibility classification;
- the major or pre-1.0 minor version decision, where applicable;
- release notes and a concrete migration path; and
- approval from an applicable CODEOWNER and a Core Maintainer acting as release owner.

Compatibility and lint exceptions must be narrow, documented, owned, and expire no
more than 90 days after approval. A repository-local weakening of a shared rule or a
silent gate waiver is not an acceptable exception.

## Ownership and review

`.github/CODEOWNERS` identifies the reviewers for policy, inventory, SDK, OpenAPI, and
protobuf changes. Significant public-contract changes are proposed through a GitHub
issue or design discussion in accordance with `GOVERNANCE.md`.

The author must identify every affected contract in the pull request and classify the
change as additive, deprecated-compatible, or breaking. A release owner is a current
Core Maintainer listed in `GOVERNANCE.md`. Request and record both approvals on the
pull request. If one person is both the applicable CODEOWNER and release owner, a
second Core Maintainer must approve so every intentional break has two distinct human
approvers.

## Enforcement commands

The policy's classifications, deprecation rules, migration requirements, and approval
path are normative now. At publication, the repository's existing formatting,
Clippy, test, and pre-commit checks are the active mechanical checks, with coverage
defined by the current workflows and manifests. Full strict-rule coverage for all 16
`crates/` packages and the compatibility blockers below are staged work, not current
blocking claims.

The required all-crates end state runs:

```bash
cargo +nightly fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test
```

[GOV-02 (#2289)](https://github.com/smg-project/smg/issues/2289) tracks creation of
the authoritative package inventory and this checker:

```bash
python3 scripts/check_api_governance.py --check
```

The command is not available until its implementation lands and is not a blocking
gate until the rollout status marks it blocking.

[GOV-03 (#2288)](https://github.com/smg-project/smg/issues/2288) tracks
`docs/api-stability-ci.md`, which defines the pinned tools, exact CI job contracts,
merge-base selection, artifacts, rollout status, and exception mechanics. The
required compatibility lanes use `cargo-semver-checks` for governed Rust packages,
Buf lint and breaking checks for protobuf contracts, and canonical generation and
diff checks for OpenAPI and generated clients. A job becomes blocking only after its
implementation exists and the GOV-03 rollout status marks it blocking.

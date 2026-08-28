# 2026-08-27 round 3 — corrected reduced-scale runs

> **Reduced-scale cache-semantics results.** 8 gateways × 120 mock workers
> (1 gateway × 120 for the single-replica leg), ~10× time/body compression
> per the design doc. Cache-hit, affinity, and balance conclusions transfer;
> CPU/RSS/fd figures do NOT represent production-scale resources.

All scenarios: 3 loadgen seeds per leg, Student-t 95% CIs (n=3 → t=4.303),
steady-state measurement window (warmup and drain excluded; drain volume
reported separately). Token-weighted cache ratios (`Σcached/Σprompt`) are
the headline metric; per-request means are reported alongside. The
`body path streamed share` row (from `smg_router_request_body_path_total`)
verifies each leg's routing regime instead of assuming it.

Corrections applied since round 2 (all six blocking issues):
gateway rebased to latest main; active-request prompt KV stays pinned and
counted until completion; measurement windowing; Student-t CIs; body-path
metric capture; hygiene (no absolute paths, no exact fleet/load values —
run with an untracked `profiles/full.local.json` for real numbers).

Per leg: `seed-rows.json` (all seeds' extracted rows), `report-seed42.json`
and `meta-seed42.json` (one full representative report with provenance:
git commit, binary sha256s, profile hash). `token-totals.json` aggregates
Σprompt/Σcached per leg. Binary identity for the revision A/B is verified
by `binary_sha256.smg` differing across legs.

## revision-ab — deployed revision vs latest main vs min_group

Under the production flag set (sticky routing-key override + valid key +
disable-retries) every leg streams header-only (`streamed share 1±0`,
`hash_hit 0±0`): the policy code that changed between revisions never
executes, and all outcome metrics are statistically identical.

| metric | deployed-rev | latest-main | latest-main min_group |
|---|---|---|---|
| AGG cached tokens (Σ/Σ) | 0.4732 ±0.003 | 0.4732 ±0.0029 | 0.4732 ±0.0029 |
| e2e p50 ms | 7798 ±54 | 7799 ±55 | 7799 ±55 |
| follow-up same-worker | 1 ±0 | 1 ±0 | 1 ±0 |
| overall fleet CoV | 0.0217 ±0.0006 | 0.0829 ±0.021 | 0.0291 ±0.0031 |

The one real difference is placement spread: latest main's expected-wait
selection balances ~3× wider than the deployed raw min-load — on request
count AND on work-weighted CoV. Mechanism (code-level): with streamed
bodies the router never sees tokens, so expected-wait credits each
dispatch `DEFAULT_MEAN_PREFILL_TOKENS = 1024` against a real ~9.5k-token
mean — the score lags between load polls and placements mildly herd.
Raw request counting is size-blind but exactly self-correcting. Absolute
spread stays tiny with zero latency/queueing effect at this utilization;
not actionable, but the `hint-sticky` runs measure the practical fix
(clients send `x-smg-routing-tokens`, credit becomes exact).

## hit-rate-calibration — what moves aggregate cache-hit

| leg | AGG cached tokens (Σ/Σ) |
|---|---|
| baseline ~1.5 turns/session | 0.4732 ±0.0029 |
| multiturn ~4.1 turns/session | 0.8008 ±0.0017 |
| multiturn + 4k shared prefix | 0.8432 ±0.0012 |

~4 turns/session reaches the 0.80 target on its own; the shared prefix
adds ~4 points. Follow-up turns run 0.949–0.975 cached; turn 1 stays
~0.20 without a shared prefix.

## key-stability — routing-key discipline dominates

| leg | AGG (Σ/Σ) | follow-up cached | follow-up same-worker |
|---|---|---|---|
| stable key per session | 0.4732 ±0.003 | 0.9491 | 1.000 |
| fresh key every turn | 0.1901 ±0.0009 | 0.1689 | 0.008 (≈1/120) |
| 32 keys shared by all | 0.1899 ±0.0003 | 0.1690 | 0.008 |

Both failure modes collapse to the incidental block-overlap floor. Shared
keys fail via the sticky per-worker cap (45k `cap_respill` events scatter
follow-ups); per-turn keys never register as a returning session
(`occupied_hit` 0).

## router-restart — mid-window gateway kill + relaunch

Multiturn workload, all 8 SMGs killed and relaunched (with worker
re-registration) at t=60s: 3879 ±76 errors during the blackout,
follow-up same-worker drops to 0.9748 ±0.0033 (pins are process state and
rebuild), aggregate hit 0.7313 ±0.0063 vs the 0.8008 undisturbed
multiturn baseline — a ~7-point cache penalty until sessions re-pin.
Recovery is complete within the run; streamed share stays ~0.99.

## radix-replica — per-replica tree accuracy

Approximate radix trees (`cache_index=tree`) are per-gateway state learned
from each replica's own placements — over HTTP there are no worker KV
events and no gateway-to-gateway sync. These legs drop the sticky override
so cache_aware actually consults the tree (all-buffered legs verified
`streamed share 0±0`; the hint leg `1±0`). Images off; otherwise the
baseline workload (~1.5 turns, 2k shared prefix).

| leg | AGG (Σ/Σ) | follow-up cached | follow-up same-worker | overall CoV |
|---|---|---|---|---|
| token tree, affine ingress, 8 SMG | 0.4442 ±0.0038 | 0.8684 | 0.789 | 0.351 |
| token tree, sprayed, 8 SMG | 0.2221 ±0.0013 | 0.2565 | 0.107 | 0.388 |
| token tree, 1 SMG (saturated) | 0.3122 ±0.0034 | 0.5487 | 0.476 | 0.282 |
| string (text) tree, affine, 8 SMG | 0.4100 ±0.0026 | 0.7744 | 0.832 | 0.103 |
| string tree, sprayed, 8 SMG | 0.2178 ±0.0014 | 0.2447 | 0.112 | 0.105 |
| token tree via hint, streamed, 8 SMG | 0.1908 ±0.0016 | 0.1722 | 0.012 | 0.303 |

Findings:

- **Per-replica trees only predict for sessions they saw.** Spraying
  session turns across 8 replicas collapses follow-up affinity from 0.79
  to 0.11 same-worker (≈ the 1/8 × tree-accuracy expectation) and
  follow-up cached tokens from 0.87 to 0.26. Replica count is harmless
  ONLY while ingress keeps a session on one gateway (consistent-hash LB)
  — the trees never synchronize, and nothing corrects a wrong replica.
- **The single-replica control is capacity-confounded**: one gateway
  buffering the full 417 rps ran at 54% CPU / 1.46 GB RSS with TTFT p50
  of 10.7 s and achieved only 324 rps, and its own balance spill broke
  affinity (0.476 same-worker). Treat its cache numbers as saturation
  behavior, not clean tree accuracy.
- **The string tree matches the token tree's affinity** (0.83 vs 0.79
  same-worker; slightly lower matched-token credit) **and balances far
  better** (CoV 0.10 vs 0.35): char-granularity matching spreads load
  where the token tree concentrates it.
- **Negative result — the routing-tokens hint cannot drive the tree
  here**: the hint carries at most 512 ids while the shared system prefix
  is 2048 tokens, so every request presents an identical head; the tree
  cannot distinguish sessions and affinity is random (0.012 same-worker).
  A hint-driven tree needs a cap larger than the shared prefix.
- Production comparison: the sticky-override hash-index config still beats
  every tree leg at equal workload (0.473 AGG, 1.000 follow-up
  same-worker, CoV 0.02–0.08) while streaming bodies (RSS 130 MiB vs
  366–615 MiB buffered). No reason to switch modes for this traffic.

## kv-events — worker-broadcast cache events (gRPC), the radix rematch

Same fleet shape as radix-replica, but the mock workers serve gRPC and
stream real KV cache events (`SubscribeKvEvents` → `PositionalIndexer`):
every gateway replica independently hears every worker's cache ground
truth — no gateway-to-gateway sync, no shared store. Gateways run IGW
mode (dynamic gRPC registration), tree index, no sticky override on the
event legs; loadgen non-streaming, no images, and no reliance on routing
keys at all for the event legs.

| leg | AGG (Σ/Σ) | follow-up cached | same-worker | overall CoV |
|---|---|---|---|---|
| events, affine ingress, 8 SMG | 0.4623 ±0.0026 | 0.917 | 0.941 | 0.25 |
| **events, sprayed ingress, 8 SMG** | **0.4653 ±0.0063** | **0.928** | **0.958** | 0.36 |
| sticky-control (override on, tree vacant) | 0.4324 ±0.012 | 0.837 | 1.000 | 0.96 |

Findings:

- **Worker broadcast repairs sprayed ingress completely.** The
  approximate tree collapsed to 0.257 follow-up cached / 0.107
  same-worker under spray; with events the same spray holds 0.928 /
  0.958 — statistically indistinguishable from the affine leg. Replica
  count and ingress affinity stop mattering because every replica
  converges on the same worker-reported truth.
- **No dedicated index storage is needed** for accurate multi-replica
  prediction: the workers are the storage, and the per-replica indexes
  are caches of their broadcasts. The cost is the gRPC event stream (one
  per gateway×worker pair) and IGW mode.
- Event routing with sprayed ingress and NO routing keys reaches 0.465
  aggregate — within a point of the production sticky config's 0.473 —
  at the price of wider balance (CoV 0.36 vs 0.02–0.08) and the gRPC
  worker requirement. For HTTP fleets the sticky-key conclusion stands.
- The sticky-control leg exposes an interaction: with the override ON and
  a tree index, vacant placements route by tree affinity and concentrate
  — only ~65 of 120 workers ever pinned, CoV 0.96, follow-up cached down
  to 0.837 (hot workers evict more), e2e p50 +7%. The production config
  avoids this because its hash index cannot engage on the streamed path
  (placement falls back to least-load). Sticky override + tree index is
  a combination to avoid.

## hint-sticky — production config + routing-tokens hint

Three seeded baseline-profile runs (latest main) with
`x-smg-routing-tokens` enabled, versus the no-hint latest-main leg of
revision-ab:

| metric | no hint | with hint |
|---|---|---|
| AGG cached tokens (Σ/Σ) | 0.4732 ±0.0029 | 0.4732 ±0.0029 |
| body path streamed share | 1 ±0 | 1 ±0 |
| e2e p50 ms | 7799 ±55 | 7798 ±56 |
| per-worker count CoV | 0.0828 ±0.0208 | 0.0609 ±0.0176 |
| per-worker work CoV | 0.0853 ±0.0174 | 0.0624 ±0.0091 |

The hint changes nothing material under the production sticky config:
cache, latency, and streaming are identical, and the balance CoVs overlap
the no-hint intervals (both far from the deployed binary's 0.022/0.026).
The gateway caps the hint at 512 ids, below both the smallest cache
boundary (3072 — so hash placement still cannot quantize) and the real
~9.5k-token prompt mean (so the expected-wait in-flight credit still
undercounts ~20×). Sending the hint at today's cap is not a balance or
cache lever; `--assignment-mode min_group` (0.029 CoV) remains the only
measured way to tighten placement spread on latest main.

## hint-sticky — production config + routing-tokens hint (pending)

Three seeded baseline-profile runs with `x-smg-routing-tokens` enabled:
measures what the hint changes under the sticky streamed regime
(placement balance via exact expected-wait credit, hash-index engagement).

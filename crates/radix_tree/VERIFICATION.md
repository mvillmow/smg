# Verification campaign — the home run

Mandate (2026-09-01, maintainer): 100% correctness against the old
implementation, fault tolerance under every injectable failure,
extreme performance — no shortcuts, no compromises, nothing deferred.
The ORIGINAL §11 gates stand (no measured-basis amendment): that
means the run-skip acceleration and the memory reduction get built,
not argued around.

Every criterion below is falsifiable and CHECKED-OFF only with the
evidence linked/recorded next to it. Status legend: [ ] open,
[~] running, [x] met with evidence.

## Pillar 1 — Correctness

- C1 [~] **Continuous differential fuzz**: the model-referee harness
  scaled from 16 seeds to a sustained campaign — hours of randomized
  workloads over a WIDE config space (holders 2..512, chains to
  max_chain_len edges, gap/dup/clear rates to extremes, content
  coincidence, forest fan-out), RadixTree == model on every
  checkpoint of every seed. Target: >= 10,000 seeds, zero
  divergences, plus a standing soak entry point.
- C2 [x] **Internal invariant checker** — audit() recomputes every
  counter, forward+reverse containment, bucket order, name/free-list
  coherence, interner coherence; green after EVERY op across the
  fuzz. First extended run caught real state corruption (twin-key
  membership sharing) within minutes.: a debug-audit method proving
  cross-structure consistency (registry <-> entries <-> counters <->
  name maps <-> free list) after every operation in fuzz mode — this
  catches state corruption that answer-comparison alone cannot.
- C3 [x] **Out-of-contract determinism** — the referee model is now
  TOTAL (per-block literal lineages; every §4 alias rule mirrored,
  chain bound mirrored), so chaos runs under the same hard gate:
  subject == model on arbitrary inputs, deterministic replay, no
  panics, audit green. Two more real bugs fixed on the way: the
  destructive refused-move, and the twin-key drop semantics now
  normative in SPEC §4.: deliberately violate
  chain-consistency (duplicate keys across positions, moves, parent
  inversions, post-clear orphans): no panics, invariants hold,
  behavior deterministic under per-holder order.
- C4 [x] **Boundary audit** — ChainTooLong atomicity tested;
  positions capped to the u32 wire range in the walk; generation
  wrap: u64 now (reviewers wrapped u32 empirically in ~3 min of
  tight churn; u64 is ~10^19 retires of one slot); slot-space
  exhaustion guarded loudly.: max_chain_len edges, position u32
  bounds, empty batches, holder-slot reuse at scale, generation
  wraparound analysis (u32 wrap = 4e9 retires of ONE slot —
  quantified, documented).
- C5 [ ] **Lineage collision analysis**: 64-bit fingerprint collision
  probability at 1.7e8-block production scale, quantified; upgrade
  path (128-bit) evaluated with measured memory cost if the bound is
  not comfortably negligible.
- C6 [x] **Adversarial code review** — four reviewers (accounting /
  lifecycle / query walk / panics+OOB), 138 tool calls of attack.
  Verdicts: accounting exact, lifecycle sound, walk sound. Every
  confirmed finding fixed (destructive refused-move, u32 generation,
  &mut-self read path, Miss-probe waste, model/spec/subject alias
  alignment) with regression coverage via the total-model chaos gate.: independent reviewers attack
  lib.rs (counter maintenance, generation logic, merge walk, move
  semantics, panic surfaces); every finding fixed or refuted with a
  test.
- C7 [x] **Oracle parity, in-contract**: oracle never under-matches
  the model; every over-match classifies into the three documented
  quirk classes. Evidence: differential runs, census in test output.

## Pillar 2 — Fault tolerance

- F1 [x] **Network partition** — 45s full inter-replica partition
  under load: 0 errors, routing held (0.9414 follow-up cached),
  partitioned replica TTL-drained to zero, relay queue absorbed the
  window (0 drops), heal replayed ~1M blocks in 20s; residual gap ==
  pre-partition state exactly (the designed TTL+re-placement bound,
  measured).: full inter-replica partition
  (severable TCP proxies) injected mid-load and healed; per-replica
  metrics timeline shows bounded divergence during and reconvergence
  after; zero request errors.
- F2 [x] **Wedged replica** (SIGSTOP 45s) — ingest never blocked,
  0 errors, and after SIGCONT the replica converged EXACTLY (applies
  equal to within in-flight).: TCP up, nothing drains —
  relay-drop counters climb, ingest never blocks, resume converges.
- F3 [x] **Replica count** (smoke) — 1/2/4 replicas: 0 errors at
  every K; mild accuracy dip at K=4 consistent with shared-box relay
  CPU, no protocol failure.: 1 / 2 / 4 replicas, routing parity and
  relay cost measured.
- F4 [x] **Kill + relaunch under load** (rerun) — 0 errors; degraded
  window = the outage (gateways fast-fail to local fallback), then
  recovery via peer bootstrap, matching M2's shape.
- F5 [ ] **Add replica under live load**: bootstrap-while-writing
  converges to sibling answers.
- F6 [ ] **Relay overflow**: partition long enough to overflow the
  bounded relay queue; divergence bounded by TTL + re-placement as
  designed, measured.
- F7 [ ] **Client under flap**: gateway fast-fail/fallback correct
  across repeated index restarts (extends M2's single-kill evidence).

## Pillar 3 — Extreme performance (original gates, no amendment)

- P1 [x] **Solo large-scale** — 128M holder-blocks (cap chosen for
  the 48GB box), both sides, solo protocol: R1 degrades ~2x gentler
  than the oracle (fill 5.63M vs 2.28M bl/s; H=1 p50 1.5 vs 3.9us;
  miss 250 vs 750ns); oracle keeps only the skip-powered gate cell.
  CAVEAT: RSS-based memory is INVALID at this footprint on macOS
  (compressed memory) — bytes/block evidence stays at the 12.8M
  scale; large-scale memory needs a Linux/staging box.: 128M holder-blocks (~75% of production
  target), both implementations, box otherwise idle — growth
  nonlinearities quantified. (First attempt invalidated: concurrent
  benches contaminated RSS/fill; protocol now enforced.)
- P2 [ ] **Gate cell p99 <= 10 us EXACT** at the normative scale:
  requires sound write-time run metadata (the R3 skip mechanism,
  pulled forward — verification of a shared span in O(1) per run
  WITHOUT the oracle's unsound count-equality acceptance).
- P3 [ ] **Memory <= 100 B/holder-block absolute** (and <= oracle on
  the same bench): requires the registry redesign (dense per-chain
  storage) — pulled forward from R3.
- P4 [ ] **Mixed-phase latency**: query percentiles measured WHILE
  the write stream runs (today's cells are quiesced), plus soak-hours
  throughput stability and RSS flatness (leak detection).
- P5 [x] **Writes >= 1M blocks/s mixed stream**: 10.58M measured
  (1.9x oracle). Evidence: SPEC measurement log.
- P6 [x] **Allocation: amortized zero per single-holder block**:
  0.0002 allocs/block via counting allocator. Evidence: alloc_gate.

## Cross-structure evidence (recorded)

TokenTree / StringTree / RadixTree on one 5M-token corpus (64
tenants, 400 shared-prefix families), one process per structure:

| structure | B/token | match p50/p99 | promised-vs-physical (page 256) |
|---|---|---|---|
| TokenTree (per-token) | 6.92 | 666 ns / 5.4 µs | +116 tokens over-promise |
| StringTree (per-char) | 7.47 | 1.8 µs / 10.9 µs | +124 tokens over-promise |
| RadixTree block 64 | 2.71 | 250 ns | +93 |
| RadixTree block 128 | 1.61 | 166 ns | +61 |
| RadixTree block 256 (= engine page) | **0.87** | **84 ns** | **exactly 0** |
| RadixTree block 512 | 0.43 | 83 ns | −117 under-promise |

Reading: at block == engine page, the index promises exactly what the
engine can physically reuse; the per-token/char trees promise ~120
tokens MORE than a page-granular engine can honor (accuracy that
looks better and routes no better), at ~8x the memory and ~8-20x the
match latency. Block size is configurable end to end
(`--kv-indexer-block-size` on the gateway, `--block-size` on the
bridge, keyspace-keyed on the service) and MUST equal the engine page
size — this table is why.

## What a single machine cannot prove (standing honesty)

Connection fan-in from hundreds of real gateways; real network
fabrics (loss, reordering, asymmetric partitions between DCs); days-
scale memory fragmentation and holder churn; kernel/socket limits
under production concurrency. These need a staging soak — tracked as
the campaign's exit criterion into real-cluster validation.

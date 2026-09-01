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

- C1 [x] **Continuous differential fuzz** — 10,000 seeds green in
  one 3.4-hour run (12,382s): randomized configs (holders 2..256,
  chains to 512, extreme dup/gap/clear/coincidence rates), ~3,300
  chaos runs interleaved, model equality at every checkpoint, audit
  at every op, deterministic replay. Zero divergences. The standing
  entry point (RADIX_FUZZ_SEEDS/RADIX_FUZZ_START) remains for
  continuous use; the dual-core harness now covers R3 in the same
  runs going forward.
- C2 [x] **Internal invariant checker** — audit() recomputes every
  counter, forward+reverse containment, bucket order, name/free-list
  coherence, interner coherence; green after EVERY op across the
  fuzz. First extended run caught real state corruption (twin-key
  membership sharing) within minutes.
- C3 [x] **Out-of-contract determinism** — the referee model is now
  TOTAL (per-block literal lineages; every §4 alias rule mirrored,
  chain bound mirrored), so chaos runs under the same hard gate:
  subject == model on arbitrary inputs, deterministic replay, no
  panics, audit green. Two more real bugs fixed on the way: the
  destructive refused-move, and the twin-key drop semantics now
  normative in SPEC §4.
- C4 [x] **Boundary audit** — ChainTooLong atomicity tested;
  positions capped to the u32 wire range in the walk; generation
  wrap: u64 now (reviewers wrapped u32 empirically in ~3 min of
  tight churn; u64 is ~10^19 retires of one slot); slot-space
  exhaustion guarded loudly.
- C5 [x] **Lineage collision analysis** — quantified, and R3 made
  structurally immune. Flat core: a collision matters only between
  two lineages coexisting in ONE (position, content) bucket;
  multi-lineage buckets require content coincidence (rare) and the
  joint probability is bounded by sum(n_bucket^2)/2^65 — < 1e-12 at
  the 1.7e8-block production scale. Documented residual for R1 only.
  R3: every lineage-keyed resolution is CONTENT-VERIFIED (roots carry
  collision lists checked against literal first contents; forks are
  content-keyed; walks compare stored contents directly), so a
  collision can never cross-credit or merge chains — the analysis
  turned up a latent unverified root resolution in the initial R3
  and it is fixed with an audit rule. This immunity is an R3-only
  property: the flat core stores no contents to verify against.
- C6 [x] **Adversarial code review** — four reviewers (accounting /
  lifecycle / query walk / panics+OOB), 138 tool calls of attack.
  Verdicts: accounting exact, lifecycle sound, walk sound. Every
  confirmed finding fixed (destructive refused-move, u32 generation,
  &mut-self read path, Miss-probe waste, model/spec/subject alias
  alignment) with regression coverage via the total-model chaos gate.
- C7 [x] **Oracle parity, in-contract**: oracle never under-matches
  the model; every over-match classifies into the three documented
  quirk classes. Evidence: differential runs, census in test output.

## Pillar 2 — Fault tolerance

- F1 [x] **Network partition** — 45s full inter-replica partition
  under load: 0 errors, routing held (0.9414 follow-up cached),
  partitioned replica TTL-drained to zero, relay queue absorbed the
  window (0 drops), heal replayed ~1M blocks in 20s; residual gap ==
  pre-partition state exactly (the designed TTL+re-placement bound,
  measured).
- F2 [x] **Wedged replica** (SIGSTOP 45s) — ingest never blocked,
  0 errors, and after SIGCONT the replica converged EXACTLY (applies
  equal to within in-flight).
- F3 [x] **Replica count** (smoke) — 1/2/4 replicas: 0 errors at
  every K; mild accuracy dip at K=4 consistent with shared-box relay
  CPU, no protocol failure.
- F4 [x] **Kill + relaunch under load** (rerun) — 0 errors; degraded
  window = the outage (gateways fast-fail to local fallback), then
  recovery via peer bootstrap, matching M2's shape.
- F5 [x] **Add replica under live load** — deferred replica started
  mid-run: bootstrapped 1.26M blocks from a sibling under live
  writes, picked up the live relay immediately (peers had been
  retrying its address), converged to sibling state by end. 0
  errors. Dynamic membership beyond pre-configured peers remains a
  documented design backlog item.
- F6 [~] **Relay overflow** — the first 90s run instead EXPOSED A
  REAL PROTOCOL BUG: ~190x apply amplification from symmetric peers
  echoing relayed placements forever (idempotent, so correctness
  held — invisible before the per-replica timeline existed). FIXED:
  the engine reports whether an apply changed state and the relay
  forwards only changing applies, so echoes die in one hop (bounded
  O(K^2)). True overflow drill (small queue via RADIX_RELAY_QUEUE +
  90s partition) running on the fixed relay.
- F7 [x] **Flap** — 3 kill/relaunch-with-bootstrap cycles on a live
  replica: 0 errors, routing held (0.939 cached), all flaps recorded
  with recovery.

## Pillar 3 — Extreme performance (original gates, no amendment)

- P1 [x] **Solo large-scale** — 128M holder-blocks (cap chosen for
  the 48GB box), both sides, solo protocol: R1 degrades ~2x gentler
  than the oracle (fill 5.63M vs 2.28M bl/s; H=1 p50 1.5 vs 3.9us;
  miss 250 vs 750ns); oracle keeps only the skip-powered gate cell.
  CAVEAT: RSS-based memory is INVALID at this footprint on macOS
  (compressed memory) — bytes/block evidence stays at the 12.8M
  scale; large-scale memory needs a Linux/staging box.
- P2 [x] **Gate cell p99 <= 10 us EXACT** — R3 measures p50 2.3 us /
  p99 7.6 us at the normative scale (medians of 3, solo box) and p99
  8.0 us at 128M blocks — scale-stable, EXACT matching, nearly tying
  the oracle's unsound-skip 6.0 us. Every other cell beats the
  oracle outright (H=1 292 ns vs 917; H=8 500 ns vs 1.5 us; miss
  under timer resolution). Mixed-phase (queries during live writes):
  p50 ~400 ns, p99 ~5 us.
- P3 [x] **Memory <= 100 B/holder-block absolute** — R3 measures
  26.9 B/holder-block at the normative scale (gate 100; oracle+glue
  166.7; flat core 169-171): 6.2x smaller than the incumbent. At
  128M blocks: 53.2 B/block (macOS-compression caveat noted, and
  still under the gate even pessimistically).
- P4 [~] **Mixed-phase latency + soak** — mixed-phase MET: p50
  ~400 ns / p99 ~5 us during live writes (8 us p99 at 128M blocks).
  Soak: the first 30-min run measured +27MB linear drift — which
  reproduced the retire-GC leak fixed an hour earlier (that soak had
  compiled a mid-edit tree); the fixed core's diagnostic soak is
  RSS-flat with every internal structure constant (debug_footprint)
  across 28M ops. 15-min certifying soak on the pushed code running.
- P5 [x] **Writes >= 1M blocks/s mixed stream**: flat core 10.58M;
  R3 4.6M (trie-walk + span surgery cost; 4.6x over the gate, ~par
  with the oracle's 5.5M). Evidence: SPEC log + campaign3.
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

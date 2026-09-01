# R3 core — chain-native layout (design note)

Status: design for the second-generation storage layout behind the
UNCHANGED §4 API and §6/§7 contract. The R1 flat core stays in-tree
until R3 passes the identical referee (model equality on in-contract
AND chaos inputs, audit, determinism) and the ORIGINAL §11 gates.

## Why the flat layout cannot meet the last two gates (measured)

- Cold query: one hash probe per position. Depth-78 answers cost 78
  independent DRAM/TLB misses into a multi-GB table (~16 µs measured;
  the walk itself is 542 ns warm — memory is the whole cost).
- Memory: every block owns a hash-map slot (~40–56 B) plus a
  per-holder registry entry (~42 B), so ~170 B/holder-block against
  the 100 B gate, regardless of how boring the block's neighborhood
  is.

## The R3 shape: store chains, not positions

The lineage value at position p identifies the entire prefix 0..=p
(injective modulo 64-bit fingerprint collisions — same assumption R1
already rests on, quantified under C5). Therefore prefixes form a
TREE, and consecutive positions of one chain are CONTIGUOUS DATA, not
independent map entries.

Structures (one instance per keyspace, single-writer as today):

1. **ChainData** (one per distinct chain path): `parent:
   Option<(chain, pos)>` (trie edge for divergent tails), `base_pos`,
   `contents: Vec<u64>`, `keys: Vec<u64>` — 16 B per position ONCE
   per chain, shared by every holder of that chain.
2. **node_by_lineage**: `FxHashMap<lineage → (chain, offset)>` for
   chain STARTS and BRANCH POINTS only — not per position. This is
   the query's single entry probe.
3. **Membership spans** per chain: `Vec<(start, len, SetRef)>` — the
   canonical MAXIMAL-RUN partition of position→holder-set, reusing
   the R1 interner for the sets. Canonicality is free: the maximal-run
   partition is the unique normal form of the underlying per-position
   mapping, so §7 convergence holds whenever the underlying mapping
   converges — split/merge operations must simply renormalize
   neighbors, and the fuzz+audit prove they do.
4. **Per-holder key map**: `FxHashMap<key → (chain, pos)>` (8 B
   value; per-holder because out-of-contract inputs can register one
   key differently across holders — chaos covers this). Parent
   resolution and removal route through it.
5. Per-holder coverage list (chain → intervals) for truncate_tail /
   enumerate / holder_blocks — cold paths derive order on demand as
   in R1.

## The query walk

Probe `node_by_lineage[l_0]` once → (chain, offset). Then:
- **Divergence localization is a linear scan of `contents`**: compare
  query contents against the chain's contiguous array — ~8 B per
  position, prefetcher-friendly, ~10 cache lines for depth 78 versus
  78 random lines today.
- At chain end, follow the child edge by next content (in-node map or
  a `node_by_lineage` probe) — hash probes total ≈ 1 + branch hops.
- Holder answers come from the span list overlapping [0, depth):
  typically 1–3 spans; per-holder depth = extent of coverage from 0
  intersected with the walk, sets compared by interned pointer as in
  R1.

Cold estimate: entry probe (~100 ns) + contiguous content stream
(~100 ns) + span reads (~100 ns) ≈ well under 1 µs at the gate shape —
versus the 10 µs gate. Memory estimate per holder-block: chain data
16/H + key map ~30 + span amortization ≈ ~45 B at H=1, ~1–5 B at
H≥8; blended ~30–50 B against the 100 B gate. Both leave real margin
for implementation slack — and both get MEASURED, not asserted.

## Semantics carried over exactly (the referee enforces them)

- §4 alias rules: same-triple twin keys are duplicates (never
  registered); refused moves are non-destructive; same-key
  re-placement is a move. "Same triple" = (position, content,
  lineage), all of which R3 derives identically.
- Removal punches holes: spans stop covering a position; chain data
  is freed when no spans and no children reference it (refcount GC —
  audited).
- Duplicate re-publish lands on the SAME chain object via
  node_by_lineage (canonical chain identity), which is what makes
  cross-publisher placement idempotence structural.
- Epoch clear / retire: drop the holder from every span it covers
  (via the coverage list), renormalizing runs; audit proves no
  orphaned chain data.

## Risks and their instruments

1. Span split/merge renormalization bugs → the total-model referee +
   audit-after-every-op chaos fuzz (already proven to catch exactly
   this class).
2. Chain GC leaks/dangles → audit extends: every chain reachable from
   spans/children/key-maps and vice versa; bytes bounded under churn
   (existing churn property).
3. Fingerprint collisions now also select chain identity → C5
   quantifies; the bound is unchanged from R1 (collisions already
   decided membership identity there).
4. Perf regressions on writes (span surgery per store) → the pinned
   bench write gate (≥ 1M blocks/s) stands; batch appends amortize to
   O(1) span updates in-contract.

# radix-tree — specification (pre-implementation)

Status: SPEC ONLY. No code yet. This crate is the planned ground-up
replacement for the radix-index service's current direct import of
`kv_index::PositionalIndexer`. The import stays in place as the
stopgap; `kv_index` remains untouched in the production gateway and
serves as the differential-testing reference and the measured
performance baseline throughout.

Derived from the adversarial design review of 2026-08-29 (five
code-reading lenses + two advocates over `kv_index/src/event_tree.rs`
and `radix_index/src/engine.rs`), then revised against a second
adversarial pass on this spec itself (engine-fit, semantics, and
performance reviewers; 8 blocking findings incorporated — the notable
ones are flagged inline as "revised:").

## 1. Purpose

A generic, standalone prefix-membership index: given per-holder chains
of content-addressed blocks, answer "which holders hold the longest
prefix of this chain, and how deep?" — the routing question — plus the
write and lifecycle operations a long-lived, multi-tenant index
service needs as first-class API rather than caller-side bookkeeping.

Zero dependencies on SMG crates. One instance indexes one keyspace;
the service maps keyspaces to instances (as it does today).

## 2. Non-goals

- Distribution protocol: epochs, per-holder sequence dedup, feed
  authority (event vs placement), and capacity/TTL *policy* stay in
  the service engine. Revised: an epoch bump maps onto `clear()` —
  same holder, same id, engine-side posture retained — exactly what
  the engine does today with `apply_cleared`. `retire_holder` is
  reserved for holder *disappearance* (TTL sweep, permanent drop),
  never for restarts.
- Networking, serialization, keyspace routing.
- The wire hash scheme. This crate is hash-agnostic; the service owns
  the wire vocabulary (§10.2).
- Replacing `kv_index`'s StringTree/TokenTree in the gateway. Out of
  scope; the only concession to that future is that this crate only
  ever sees hashes.
- Internal concurrency. Single-writer per instance (§8).

## 3. Vocabulary

- **Block**: one quantum of cache (a token page / byte page),
  identified two ways:
  - `ContentHash(u64)` — position-independent content identity; the
    matching currency on the query path and the wire.
  - `BlockKey(u64)` — the publisher's removal key (the backend's block
    hash on the event feed; a synthesized chain hash on the placement
    feed). Opaque to matching; needed because event-feed removals
    arrive as backend keys and are not derivable from content.
- **Chain**: blocks at consecutive positions `0..n`, each linked to
  the prefix that produced it. A chain is what a *query* presents.
- **Holder state is a forest** (revised): a holder holds MANY chains —
  one per cached prompt prefix on the placement feed (the engine's own
  convergence tests feed one holder several disjoint parent-None
  chains). Every per-holder operation below is defined on the forest.
- **Lineage**: a stored block's identity includes the specific chain
  prefix it was stored under, not just (position, content). Two chains
  that coincide in content at position p are still different blocks —
  a block's KV values depend on its whole prefix. Internally this is a
  rolling lineage fingerprint (an implementation detail, NOT the wire
  hash scheme).
- **Holder**: the entity that holds blocks (a worker). Identified by a
  generational `HolderId` (§5).
- **Position**: `u32` depth in a chain, starting at 0.

## 4. Public API (normative sketch)

```rust
pub struct RadixTree { /* one keyspace's index */ }

pub struct Config {
    pub max_chain_len: u32,        // hard bound; default 65_536
}

// --- holder lifecycle ---
pub fn create_holder(&mut self, name: &str) -> HolderId;
pub fn holder_name(&self, id: HolderId) -> Option<&str>;
pub fn retire_holder(&mut self, id: HolderId);
pub fn holder_blocks(&self, id: HolderId) -> u64;   // O(1)

// --- writes ---
pub struct StoreOutcome {
    pub applied: u32,              // blocks newly inserted
    pub duplicates: u32,           // already present (idempotent hits)
}
pub enum StoreError {
    UnknownHolder,                 // includes stale generation (§5)
    ParentNotFound,
    ChainTooLong,
}
pub fn store(
    &mut self,
    holder: HolderId,
    parent: Option<BlockKey>,      // None = anchor a new chain at 0
    blocks: &[(BlockKey, ContentHash)],
) -> Result<StoreOutcome, StoreError>;

pub fn remove(&mut self, holder: HolderId, keys: &[BlockKey]) -> u32;
pub fn truncate_tail(&mut self, holder: HolderId, keep: u64) -> u64;
pub fn clear(&mut self, holder: HolderId);

// --- reads ---
pub struct Overlap {
    pub holder: HolderId,
    pub depth: u32,
    pub total_blocks: u64,         // revised: the wire answer carries
                                   // it per hit; O(1) at query time
}
pub fn overlap(&self, chain: &[ContentHash], out: &mut Vec<Overlap>);
pub fn enumerate(&self, holder: HolderId)
    -> impl Iterator<Item = (u32, BlockKey, ContentHash)> + '_;
pub fn stats(&self) -> Stats;
```

Write semantics (revised, all normative):

- **`store` is all-or-nothing**: on any `Err`, no effect was applied.
  `ParentNotFound` is the ONLY error for which re-anchoring at
  position 0 is the sanctioned caller recovery; `ChainTooLong` is
  terminal for the batch (the caller drops it — re-anchoring a
  too-long batch cannot succeed and must not be attempted).
- `store` with `parent: Some(k)` anchors at (position of k) + 1 *on
  the chain k belongs to*; the batch extends that chain's lineage.
  `parent: None` starts a new chain in the forest at position 0.
- Re-registering an existing `BlockKey` at a different placement
  **moves** the block (the prior placement is removed first) — the
  deterministic resolution the engine's re-anchor recovery needs.
  Chain-consistent workloads (§7) never do this.
- `remove` of an unknown key is a no-op (counts only actual removals).
- **`truncate_tail` on the forest**: remove blocks in strictly
  decreasing position order across ALL chains, ties at a position
  broken deterministically by `BlockKey`, until `keep` remain. A pure
  function of (state, keep). This is prefix-closed for every chain
  simultaneously. Note: this deliberately *fixes* today's engine,
  whose parallel chain Vec tracks only the most recent chain and lets
  earlier chains escape capacity accounting until TTL.
- **Return values are advisory** (`StoreOutcome`, `remove`'s count):
  they are order-dependent under duplication and are explicitly
  outside the convergence contract (§7).

`Stats` (revised — two block quantities, both defined):

```rust
pub struct Stats {
    pub holders: u64,
    pub holder_blocks: u64,    // sum of holder_blocks(h) — capacity math
    pub distinct_entries: u64, // unique (position, content, lineage)
                               // — the oracle-parity metric the
                               // engine's entry_count() tests assert
    pub bytes_estimate: u64,
}
```

Deliberate differences from the oracle's API, each traced to a review
finding:

| finding (oracle) | this API |
|---|---|
| caller-owned `WorkerBlockMap` threaded into every write | registry is internal |
| engine keeps a parallel position-ordered chain Vec for eviction | `truncate_tail` is native, and forest-correct |
| `OverlapScores` keyed by `u32` with no reverse lookup | `holder_name` on the query path |
| `WorkerIdExhausted` surfaced as a false `KeyspaceMismatch` | generational ids; truthful errors |
| no enumeration API; snapshot sorts an inverted caller map | `enumerate` in position order |
| parent-not-found handled by copied gateway heuristics | `ParentNotFound` is an explicit, recoverable error |
| global `prune` breaking prefix-closure; per-entry touch atomics | no TTL machinery at all; policy is the engine's |
| `total_blocks` from a side structure per answer | carried on `Overlap`, O(1) |

## 5. Holder lifecycle (the service-class fix)

The oracle interns worker ids monotonically and never frees them —
invisible in a gateway restarted every deploy, unbounded in a
long-lived service under pod churn.

- **`HolderId` is generational** (revised): index + generation.
  `retire_holder` bumps the generation; every operation through a
  stale id fails loudly (`UnknownHolder` / `None`), never silently
  addressing a recycled slot. This is what makes recycling safe for a
  caller that caches ids (the engine does, in `HolderState`).
- `retire_holder` releases every byte attributable to the holder and
  returns the slot to a free list; name maps shrink.
- Property tests: (a) create/store/retire cycled N times leaves
  `stats().bytes_estimate` bounded by a constant independent of N;
  (b) operations through a retired id fail loudly, never alias.

## 6. Matching semantics (the contract)

Revised — depth is defined over *lineage*, not bare positional
content:

> depth(h) = the largest `d` such that holder `h` holds the chain
> `chain[0..d]` — i.e., for every position `p < d`, `h` holds a block
> with content `chain[p]` at position `p` whose lineage is exactly
> `chain[0..p]`.

Exact — no probabilistic skip acceptance, no lineage shortcuts. A
holder holding two content-coincident chains never over-matches on
the strength of the wrong chain's block.

The oracle deviates from this contract in three known ways (the
adjudication classes for §10):

1. **Jump-landing acceptance on holder-count equality** without
   membership verification — can credit a holder across a mid-chain
   gap or past its true depth.
2. **The linear-scan retain guard**, which skips verification whenever
   a position's matching set is at least as large as the active set —
   same over-count direction, and it operates even on gap-free states
   (count parity is not set equality).
3. **The Single-entry lineage skip**: the oracle checks lineage only
   for Multi entries; a Single-represented block matches on
   (position, content) alone, so the oracle may over-match where this
   crate is exact — representation-dependent semantics this contract
   deliberately does not reproduce.

All three deviate in the same direction: oracle ≥ truth ≥ this crate
is NOT the invariant — the invariant is that *this crate equals the
model* (§10.1); the oracle may exceed the model through exactly these
three mechanisms and no others.

## 7. Convergence requirement (the load-bearing property)

Revised — the earlier "any arrival order" claim was false (store and
remove do not commute: `{store k, remove k}` ends present or absent by
order, in the oracle and in any sane implementation).

**Chain-consistent** (definition): a per-holder store multiset is
chain-consistent iff there exists a single mapping
`BlockKey → (position, content, lineage)` such that every batch places
every key at that mapping's placement, every `parent` resolves under
the mapping to the batch anchor's position − 1 on the same chain, and
no two distinct keys map to the same (chain, position) for the holder.

**The guarantee**: for any workload where

- each holder's own operation order is preserved whenever that
  holder's sequence contains `remove` or `clear`,
- remove-free, clear-free per-holder store multisets may additionally
  be reordered arbitrarily,
- cross-holder interleaving is arbitrary, and individual operations
  may be duplicated (re-applied) arbitrarily,
- all stores are chain-consistent,

the resulting `overlap` answers (as a full holder→depth map),
`enumerate` output, and `stats()` block quantities are identical.

Excluded from the guarantee: return values (§4, advisory);
`truncate_tail` and `retire_holder` (engine-sequenced local decisions,
not replicated effects); `ParentNotFound` caller compensation — a
re-anchor is a NEW operation whose convergence is the caller's
obligation, and the engine must derive it deterministically from
update content, never from local arrival history.

The oracle satisfies this scoped property because its index inserts
are set-idempotent — but its caller-side reverse map is
last-writer-wins and needs the same restrictions; "trivially
convergent" overstates it. Every candidate layout here re-earns the
same scoped property, and it is the R1-vs-R3 fork's dividing question
(§9).

Snapshot note (revised): replaying `enumerate` output through
`store()` collapses mid-chain gaps into contiguous chains — exactly
what the engine's Pull/bootstrap does today. Bootstrap-equivalence
assertions are therefore scoped to gap-free states; gapped holders
converge through the feeds, not through snapshot transfer.

## 8. Concurrency model

Single writer, `&mut self` writes, `&self` reads. No internal locks,
no atomics, no shards. The consumer already serializes per keyspace;
the review measured the imported structure's concurrency machinery as
unexercised there, and its per-entry atomics as pure overhead on every
store *and every query probe*. Concurrent-read designs, if ever
needed, are an engine-layer snapshot-publication concern.

## 9. Storage layouts: R1 flat core, R3 linked tree

The public API and every contract above are layout-independent.

**R1 — flat positional core (ships first).**

- Entry side: `FxHashMap<(u32, ContentHash), Membership>` where
  `Membership` disambiguates by lineage fingerprint and holds the
  common case inline: one `(lineage, holder)` with **zero heap
  allocation** — one step past the oracle's Single-entry
  representation, which still heap-allocates a one-element worker set
  per entry — spilling to a small lineage→holders map only when
  shared.
- Registry side (per holder): `BlockKey → (position, content)` map
  plus a position-ordered `(BlockKey, ContentHash)` index — ≈36–40
  B/holder-block with hashmap overhead, within ~10% of the oracle's
  externalized `WorkerBlockMap`. **The R1 memory saving comes from the
  entry side** (no per-entry heap set, no atomics, no shard tables),
  not from the registry.
- Convergence (§7) holds by construction (set-idempotent inserts under
  the chain-consistency scope).
- Historical reference: the oracle measured 274.7 B/entry on the old
  unique-chain bench — superseded as a target by the §11 gates, which
  are defined on the pinned shared-prefix workload.

**R3 — path-compressed linked tree (gated).**

Arena-allocated trie over positions branching on lineage, with
membership stored per *run* (maximal span of positions with identical
holder set). Structural prefix-closure (leaf-pop eviction, O(subtree)
holder retirement). Canonicality: a path-compressed trie's shape is a
pure function of its key set; the genuinely hard, novel work is
keeping run boundaries and membership canonical under interleaved
store/remove with duplication — splits and merges must depend only on
the resulting key/membership set, never on arrival order. This is the
part expected to be much harder than R1; it gets its own design note
before any code.

Honest arithmetic (revised): run compression only collapses the entry
side; the per-holder registry (~36–40 B/holder-block) is a floor it
cannot touch. At sharing factor H=1 the projected total win over R1 is
≈2×; at H=32 it decays toward ≈1.2×. **A ≥2× total win therefore also
requires a registry redesign** — e.g. replacing the per-holder hashmap
with a dense position-indexed `Vec<(BlockKey, ContentHash)>` per chain
(≈16 B/holder-block, gap-tolerant via run splits) — and the R3 design
note must carry both. Go/no-go is computed from run statistics
*measured on R1* under a written per-run byte model (§11), not from
this projection.

## 10. Verification (written BEFORE the core — the R0 deliverable)

1. **Model-referee differential harness** (revised) in
   `tests/differential.rs`, `kv-index` as a dev-dependency only. The
   harness maintains a trivially-correct model (per-holder
   chain-forest map; depth computed literally per §6). Assertions:
   - `RadixTree == model`, on every state, always — this is the hard
     gate;
   - full holder→depth *map* equality (holders at depth 0 absent from
     both) — never a bare multiset of depths;
   - oracle-vs-model divergences are recorded and each must classify
     into one of §6's three oracle-quirk classes; an unclassifiable
     divergence fails the run (it means the model, the spec, or our
     understanding of the oracle is wrong) — but oracle divergence
     never fails `RadixTree` while it equals the model.
   Workloads: many holders sharing deep prefixes, divergent tails,
   duplicate/reordered batches within §7's scope, event-feed gap
   patterns, placement re-publishes, forest-shaped placement holders.
2. **Golden hash vectors**: the wire hash scheme (XXH3-64 seed 1337,
   LE u32 full-block chunking, chain rule, position-0
   prefix==content convention) pinned as hard-coded u64 constants
   captured from production `kv_index` output. Revised: after R2
   drops the service's `kv_index` import, the wire scheme's
   implementation lives in a small service-side `wire_hash` module in
   `radix_index` — NOT in this crate, which stays hash-agnostic — and
   the golden vectors test that module. The service's proto grows a
   `hash_scheme` version field so drift fails loudly instead of as
   silent empty matches.
3. **Property tests**: §5 lifecycle boundedness and stale-id
   loudness, §6 exactness on constructed gap and content-coincidence
   cases, §7 convergence interleavings within scope, forest
   `truncate_tail` prefix-closure and determinism (tie-break
   included).
4. **Ported engine acceptance tests**: the service's existing
   structure-independent tests must pass after the R2 switch, with
   two documented behavior changes: capacity eviction becomes
   forest-correct (§4), and dropped holders release memory (§12).

## 11. Performance gates (measured, not argued)

**The pinned workload** (revised — normative constants, so gates are
not gameable after the fact): ≥1e7 holder-blocks total; 256 holders;
sharing mixture H ∈ {1, 8, 64} at 50%/35%/15% of blocks; shared-depth
distribution log-uniform over [8, 512] blocks; divergent tails
uniform over [4, 64] blocks; query mix replaying stored prefixes plus
20% misses, depth distribution matching stores; 5% duplicate
re-stores; 2% gap injection via removes. Numbers quoted as the median
of ≥3 runs; same global allocator both sides; RSS sampled after fill
and before query-phase allocations; no asserts or allocation inside
timed regions; latency reported per (depth, active-set-size) cell.

**R1 adoption gates:**

- *Memory*: RSS-delta / holder-blocks ≤ the oracle measured on the
  SAME workload with its production glue replicated (indexer +
  per-holder `WorkerBlockMap` + chain Vec + `by_worker_id`, exactly
  as `engine.rs` holds them) — and, independently, **≤ 100
  B/holder-block absolute**, so beating the glue-laden oracle cannot
  hide a bloated layout.
- *Allocation*: storing a fresh single-holder block performs zero
  heap allocations amortized beyond map growth (counting-allocator
  property test) — the §13.4 risk gated directly, since the RSS gate
  alone cannot catch it.
- *Latency* (revised): exactness forbids the oracle's count-equality
  jump skip, so ≤-oracle is not the bar. Gate: **overlap p99 ≤ 10 µs
  at depth 78 with 64 candidate holders** on the pinned workload
  (~200× headroom under the service's 2 ms routing deadline); the
  oracle's number on the same bench is recorded as reference, not as
  a gate.
- *Writes*: **≥ 1M blocks/s sustained single-thread on the mixed
  write stream** (batch 8, duplicates and removes included as part of
  the gated stream). Absolute, not vs the oracle: write load has ≥10×
  headroom at 1M, while reads and memory are the scaling constraints
  — R1 is permitted to be slower than the oracle's 4.6M pure-fill
  blocks/s.

**R3 gates** (revised — go/no-go and acceptance are different
measurements):

- *Go/no-go* (before building): projected total win ≥ 2×, computed
  from run statistics measured on R1 under the pinned workload (count
  and length of maximal identical-membership runs, membership sizes,
  registry byte share) under the written per-run byte model of §9.
- *Acceptance* (after building): measured RSS/holder-block ≤ 0.5× R1
  on the pinned workload, and overlap p99 ≤ R1's on the same query
  mix.

## 12. Milestones

- **R0** — this spec reviewed; model-referee rig, golden vectors,
  `wire_hash` module carve-out, and the pinned bench built and run
  against the *oracle alone* (baseline numbers recorded). No core
  code before the harness exists.
- **R1** — flat core passes differential + property + perf gates.
- **R2** — service engine rewires to the new API: deletes the chain
  Vec, `by_worker_id`, and caller-owned maps; epoch bump calls
  `clear()`; `sweep_idle` retires idle inferred holders via
  `retire_holder` **and drops the engine's own `HolderState` in
  lockstep** (stale-id hygiene, §5); revised: holders marked
  `dropped` are retired immediately — including event-fed ones, which
  today leak forever — with an engine-side tombstone carrying the
  last-seen epoch for a grace window so a late lower-epoch relay
  straggler cannot resurrect pre-restart state. The service's
  `kv_index` import is dropped (`wire_hash` takes over, §10.2). Live
  smoke + one sim leg rerun for sanity. Gateway untouched.
- **R3** — linked-tree core behind the same API, gated per §11, with
  its canonical-maintenance design note written and reviewed first.

## 13. Risks, ranked

1. **R3 canonical incremental maintenance** (§9) — the one genuinely
   hard algorithmic problem; contained by being last, optional, and
   gated on measured run statistics rather than projections.
2. **Oracle-quirk adjudication** — the model-referee design (§10.1)
   bounds it: divergences classify against three enumerated
   mechanisms or the run fails loudly; no case-by-case judgment calls
   inside the harness.
3. **Silent hash drift** — the failure mode is empty matches, not
   errors; mitigated by golden vectors + the proto version field +
   one owning module.
4. **Perf regression subtlety** — gated directly by the
   counting-allocator test and the absolute byte budget, not just the
   relative RSS comparison.
5. **Two implementations in-tree during the window** — bounded by R2
   removing the service's import; the gateway's `kv_index` and this
   crate never share a caller afterward.

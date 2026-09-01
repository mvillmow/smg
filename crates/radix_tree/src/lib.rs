//! Generic prefix-membership index — the R1 flat core.
//!
//! The contract lives in `SPEC.md`; the referee lives in
//! `tests/differential.rs` (this implementation must equal the model
//! there, always). Shape per §9: a flat positional entry map keyed by
//! `(position, content)` with lineage-disambiguated membership, plus
//! an internal per-holder registry — order-insensitive set semantics,
//! so §7 convergence holds by construction. Single-writer (§8): no
//! locks, no atomics, no shards.

#![forbid(unsafe_code)]

use rustc_hash::FxHashMap;

/// Position-independent content identity (the matching currency).
pub type ContentHash = u64;
/// The publisher's removal key (backend block hash / placement chain
/// hash). Opaque to matching.
pub type BlockKey = u64;

/// Generational holder id (§5): operations through a retired id fail
/// loudly, never alias a recycled slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HolderId {
    index: u32,
    generation: u32,
}

impl HolderId {
    /// Raw (index, generation) — for callers keeping side tables
    /// keyed by holder. Stale ids stay detectable through the
    /// generation.
    pub fn parts(self) -> (u32, u32) {
        (self.index, self.generation)
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    /// Hard bound on chain length; a store extending past it is
    /// rejected whole (`StoreError::ChainTooLong`).
    pub max_chain_len: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_chain_len: 65_536,
        }
    }
}

/// Advisory (§4): outside the convergence contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreOutcome {
    pub applied: u32,
    pub duplicates: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreError {
    /// Unknown or stale-generation holder id.
    UnknownHolder,
    /// Parent key not registered; the ONLY error whose sanctioned
    /// recovery is re-anchoring at position 0 (§4).
    ParentNotFound,
    /// Batch would extend past `max_chain_len`; terminal, do not
    /// re-anchor.
    ChainTooLong,
}

/// One query answer row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Overlap {
    pub holder: HolderId,
    pub depth: u32,
    pub total_blocks: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    pub holders: u64,
    /// Sum of per-holder block counts (capacity arithmetic).
    pub holder_blocks: u64,
    /// Unique (position, content, lineage) memberships-bearing
    /// entries — the oracle-parity metric (§4).
    pub distinct_entries: u64,
    /// Rough resident estimate; drift-tolerant, monotone with state.
    pub bytes_estimate: u64,
}

/// A block's placement inside its holder: position, content, and the
/// lineage fingerprint of the chain prefix it was stored under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockInfo {
    pos: u32,
    content: ContentHash,
    lineage: u64,
}

#[derive(Debug, Default)]
struct HolderState {
    name: String,
    /// BlockKey -> placement. Internal — never caller-owned (§4).
    /// Deliberately the ONLY per-block holder structure: position
    /// order (truncate_tail/enumerate — cold capacity/snapshot
    /// paths) is derived from it on demand, costing those calls an
    /// O(n log n) scan instead of costing every block 16 resident
    /// bytes of standing order log.
    registry: FxHashMap<BlockKey, BlockInfo>,
}

impl HolderState {
    /// (position, key) pairs in ascending order, derived on demand.
    fn ordered(&self) -> Vec<(u32, BlockKey)> {
        let mut v: Vec<(u32, BlockKey)> = self.registry.iter().map(|(&k, i)| (i.pos, k)).collect();
        v.sort_unstable();
        v
    }
}

#[derive(Debug)]
struct HolderSlot {
    generation: u32,
    /// `None` = retired slot awaiting reuse.
    state: Option<HolderState>,
}

/// Lineage fingerprint chain: an INTERNAL rolling 64-bit mix — not
/// the wire hash scheme (the crate is hash-agnostic, §2).
#[inline]
fn lineage_root(content: ContentHash) -> u64 {
    splitmix(0xA0761D6478BD642F ^ content)
}

#[inline]
fn lineage_step(prev: u64, content: ContentHash) -> u64 {
    splitmix(prev.rotate_left(23) ^ content.wrapping_mul(0x9E3779B97F4A7C15))
}

#[inline]
fn splitmix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

/// Membership at one (position, content) entry, lineage-disambiguated.
/// The common case — one holder, one lineage — is inline and
/// allocation-free (§9). Shared entries hold one dense sorted holder
/// array per lineage: 4 B/holder, slice-comparable, streamed by the
/// query merge.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Membership {
    One {
        lineage: u64,
        holder: u32,
    },
    /// Buckets sorted by lineage; holders sorted ascending within.
    Many(Vec<(u64, Vec<u32>)>),
}

impl Membership {
    /// Insert; true when newly added.
    fn insert(&mut self, lineage: u64, holder: u32) -> bool {
        match self {
            Membership::One {
                lineage: l,
                holder: h,
            } => {
                if *l == lineage && *h == holder {
                    false
                } else {
                    let mut buckets = vec![(*l, vec![*h])];
                    match buckets[0].0.cmp(&lineage) {
                        std::cmp::Ordering::Equal => {
                            buckets[0].1.push(holder);
                            buckets[0].1.sort_unstable();
                        }
                        std::cmp::Ordering::Less => buckets.push((lineage, vec![holder])),
                        std::cmp::Ordering::Greater => buckets.insert(0, (lineage, vec![holder])),
                    }
                    *self = Membership::Many(buckets);
                    true
                }
            }
            Membership::Many(buckets) => {
                match buckets.binary_search_by_key(&lineage, |&(l, _)| l) {
                    Ok(bi) => match buckets[bi].1.binary_search(&holder) {
                        Ok(_) => false,
                        Err(at) => {
                            buckets[bi].1.insert(at, holder);
                            true
                        }
                    },
                    Err(bi) => {
                        buckets.insert(bi, (lineage, vec![holder]));
                        true
                    }
                }
            }
        }
    }

    /// Remove; true when the whole membership is now empty.
    fn remove(&mut self, lineage: u64, holder: u32) -> bool {
        match self {
            Membership::One {
                lineage: l,
                holder: h,
            } => *l == lineage && *h == holder,
            Membership::Many(buckets) => {
                if let Ok(bi) = buckets.binary_search_by_key(&lineage, |&(l, _)| l) {
                    if let Ok(at) = buckets[bi].1.binary_search(&holder) {
                        buckets[bi].1.remove(at);
                        if buckets[bi].1.is_empty() {
                            buckets.remove(bi);
                        }
                    }
                }
                buckets.is_empty()
            }
        }
    }

    /// Does any OTHER holder share (lineage) here?
    fn lineage_shared_beyond(&self, lineage: u64, holder: u32) -> bool {
        match self {
            Membership::One {
                lineage: l,
                holder: h,
            } => *l == lineage && *h != holder,
            Membership::Many(buckets) => {
                match buckets.binary_search_by_key(&lineage, |&(l, _)| l) {
                    Ok(bi) => buckets[bi].1.iter().any(|&h| h != holder),
                    Err(_) => false,
                }
            }
        }
    }
}

pub struct RadixTree {
    cfg: Config,
    entries: FxHashMap<(u32, ContentHash), Membership>,
    slots: Vec<HolderSlot>,
    by_name: FxHashMap<String, u32>,
    free: Vec<u32>,
    holder_blocks_total: u64,
    distinct_lineage_entries: u64,
    /// Scratch for the query walk (kept to stay allocation-free on
    /// the hot path once warm).
    scratch_active: Vec<u32>,
    scratch_next: Vec<u32>,
    scratch_lineages: Vec<u64>,
}

impl RadixTree {
    pub fn new(cfg: Config) -> Self {
        Self {
            cfg,
            entries: FxHashMap::default(),
            slots: Vec::new(),
            by_name: FxHashMap::default(),
            free: Vec::new(),
            holder_blocks_total: 0,
            distinct_lineage_entries: 0,
            scratch_active: Vec::new(),
            scratch_next: Vec::new(),
            scratch_lineages: Vec::new(),
        }
    }

    // ---- holder lifecycle (§5) ----

    /// Create (or return the live holder of this name). Recycles
    /// retired slots; the returned id's generation detects staleness.
    pub fn create_holder(&mut self, name: &str) -> HolderId {
        if let Some(&index) = self.by_name.get(name) {
            return HolderId {
                index,
                generation: self.slots[index as usize].generation,
            };
        }
        let state = HolderState {
            name: name.to_string(),
            ..HolderState::default()
        };
        let index = if let Some(index) = self.free.pop() {
            let slot = &mut self.slots[index as usize];
            slot.state = Some(state);
            index
        } else {
            self.slots.push(HolderSlot {
                generation: 0,
                state: Some(state),
            });
            (self.slots.len() - 1) as u32
        };
        self.by_name.insert(name.to_string(), index);
        HolderId {
            index,
            generation: self.slots[index as usize].generation,
        }
    }

    pub fn holder_name(&self, id: HolderId) -> Option<&str> {
        self.live(id).map(|s| s.name.as_str())
    }

    /// Release every byte attributable to the holder; the id becomes
    /// stale (generation bumped) and the slot reusable.
    pub fn retire_holder(&mut self, id: HolderId) {
        if self.live(id).is_none() {
            return;
        }
        self.clear(id);
        let slot = &mut self.slots[id.index as usize];
        let state = slot.state.take().expect("checked live");
        self.by_name.remove(&state.name);
        slot.generation = slot.generation.wrapping_add(1);
        self.free.push(id.index);
    }

    /// O(1).
    pub fn holder_blocks(&self, id: HolderId) -> u64 {
        self.live(id).map_or(0, |s| s.registry.len() as u64)
    }

    // ---- writes (§4) ----

    /// All-or-nothing on error; see `StoreError` for the recovery
    /// contract per variant.
    pub fn store(
        &mut self,
        id: HolderId,
        parent: Option<BlockKey>,
        blocks: &[(BlockKey, ContentHash)],
    ) -> Result<StoreOutcome, StoreError> {
        if self.live(id).is_none() {
            return Err(StoreError::UnknownHolder);
        }
        if blocks.is_empty() {
            return Ok(StoreOutcome {
                applied: 0,
                duplicates: 0,
            });
        }
        let state = self.slots[id.index as usize]
            .state
            .as_ref()
            .expect("checked live");
        // Resolve the anchor BEFORE any mutation (all-or-nothing).
        let (start_pos, mut lineage_prev) = match parent {
            Some(parent_key) => match state.registry.get(&parent_key) {
                None => return Err(StoreError::ParentNotFound),
                Some(info) => (info.pos + 1, Some(info.lineage)),
            },
            None => {
                // Re-publish dedup: a parent-None batch whose first
                // key already anchors a chain at position 0 extends
                // that chain (the model's rule; in-contract the fresh
                // lineage recomputation matches the stored one).
                (0, None)
            }
        };
        if start_pos as u64 + blocks.len() as u64 > self.cfg.max_chain_len as u64 {
            return Err(StoreError::ChainTooLong);
        }

        let mut applied = 0u32;
        let mut duplicates = 0u32;
        for (i, &(key, content)) in blocks.iter().enumerate() {
            let pos = start_pos + i as u32;
            let lineage = match lineage_prev {
                None => lineage_root(content),
                Some(prev) => lineage_step(prev, content),
            };
            lineage_prev = Some(lineage);
            let info = BlockInfo {
                pos,
                content,
                lineage,
            };
            let state = self.slots[id.index as usize]
                .state
                .as_mut()
                .expect("checked live");
            match state.registry.get(&key) {
                Some(existing) if *existing == info => {
                    duplicates += 1;
                    continue;
                }
                Some(&existing) => {
                    // §4: re-registration MOVES the block.
                    Self::unindex(
                        &mut self.entries,
                        &mut self.distinct_lineage_entries,
                        id.index,
                        existing,
                    );
                    self.holder_blocks_total -= 1;
                }
                None => {}
            }
            let state = self.slots[id.index as usize]
                .state
                .as_mut()
                .expect("checked live");
            state.registry.insert(key, info);
            self.holder_blocks_total += 1;
            self.index_block(id.index, info);
            applied += 1;
        }
        Ok(StoreOutcome {
            applied,
            duplicates,
        })
    }

    /// Idempotent; returns blocks actually removed (advisory).
    pub fn remove(&mut self, id: HolderId, keys: &[BlockKey]) -> u32 {
        if self.live(id).is_none() {
            return 0;
        }
        let mut removed = 0u32;
        for &key in keys {
            let state = self.slots[id.index as usize]
                .state
                .as_mut()
                .expect("checked live");
            let Some(info) = state.registry.remove(&key) else {
                continue;
            };
            self.holder_blocks_total -= 1;
            Self::unindex(
                &mut self.entries,
                &mut self.distinct_lineage_entries,
                id.index,
                info,
            );
            removed += 1;
        }
        removed
    }

    /// Forest-wide prefix-closed eviction (§4): drop blocks in
    /// strictly decreasing position order, ties by key, until `keep`
    /// remain. Returns dropped count.
    pub fn truncate_tail(&mut self, id: HolderId, keep: u64) -> u64 {
        if self.live(id).is_none() {
            return 0;
        }
        let state = self.slots[id.index as usize]
            .state
            .as_mut()
            .expect("checked live");
        if state.registry.len() as u64 <= keep {
            return 0;
        }
        let mut ordered = state.ordered();
        let mut dropped = 0u64;
        while ordered.len() as u64 > keep {
            let (pos, key) = ordered.pop().expect("non-empty");
            let state = self.slots[id.index as usize]
                .state
                .as_mut()
                .expect("checked live");
            let info = state.registry.remove(&key).expect("derived from registry");
            debug_assert_eq!(info.pos, pos);
            self.holder_blocks_total -= 1;
            Self::unindex(
                &mut self.entries,
                &mut self.distinct_lineage_entries,
                id.index,
                info,
            );
            dropped += 1;
        }
        dropped
    }

    /// Drop all blocks; the holder (id, name, epoch posture at the
    /// caller) survives — this is the epoch-bump primitive (§2).
    pub fn clear(&mut self, id: HolderId) {
        if self.live(id).is_none() {
            return;
        }
        let state = self.slots[id.index as usize]
            .state
            .as_mut()
            .expect("checked live");
        let registry = std::mem::take(&mut state.registry);
        self.holder_blocks_total -= registry.len() as u64;
        for (_, info) in registry {
            Self::unindex(
                &mut self.entries,
                &mut self.distinct_lineage_entries,
                id.index,
                info,
            );
        }
    }

    // ---- reads ----

    /// §6 exact overlap: lineage-true consecutive depth per holder.
    /// `out` is cleared and filled (holders at depth 0 absent).
    ///
    /// Two-phase walk: probing each position's entry has no data
    /// dependency on its neighbors (lineages are a pure rolling
    /// function of the QUERY), so phase 1 issues all map probes
    /// back-to-back — the CPU overlaps their cache misses — and only
    /// phase 2's cheap merge is sequential. This is what makes exact
    /// matching latency-competitive without the oracle's unsound
    /// skip heuristics.
    pub fn overlap(&mut self, chain: &[ContentHash], out: &mut Vec<Overlap>) {
        out.clear();
        if chain.is_empty() {
            return;
        }
        // Phase 0: lineages up front.
        let mut lineages = std::mem::take(&mut self.scratch_lineages);
        lineages.clear();
        let mut lineage = 0u64;
        for (p, &content) in chain.iter().enumerate() {
            lineage = if p == 0 {
                lineage_root(content)
            } else {
                lineage_step(lineage, content)
            };
            lineages.push(lineage);
        }
        // Phase 1: independent probes resolving each position's
        // dense holder run, endpoints touched so the run data rides
        // the same overlapped misses. Stops at the first absent
        // entry (no deeper position can matter).
        enum RunProbe<'a> {
            One(u32),
            Slice(&'a [u32]),
            Miss,
        }
        let mut probes: Vec<RunProbe> = Vec::with_capacity(chain.len().min(1024));
        for (p, &content) in chain.iter().enumerate() {
            match self.entries.get(&(p as u32, content)) {
                Some(Membership::One { lineage, holder }) => {
                    probes.push(if *lineage == lineages[p] {
                        RunProbe::One(*holder)
                    } else {
                        RunProbe::Miss
                    })
                }
                Some(Membership::Many(buckets)) => {
                    let run = if buckets.len() == 1 {
                        (buckets[0].0 == lineages[p]).then(|| buckets[0].1.as_slice())
                    } else {
                        buckets
                            .binary_search_by_key(&lineages[p], |&(l, _)| l)
                            .ok()
                            .map(|bi| buckets[bi].1.as_slice())
                    };
                    match run {
                        Some(run) => {
                            // Endpoint touches: pull the run's first
                            // and last cache lines now, overlapped.
                            std::hint::black_box(run[0]);
                            std::hint::black_box(run[run.len() - 1]);
                            probes.push(RunProbe::Slice(run));
                        }
                        None => probes.push(RunProbe::Miss),
                    }
                }
                None => break,
            }
        }
        // Phase 2: sequential merge over dense holder runs.
        let mut active = std::mem::take(&mut self.scratch_active);
        let mut next = std::mem::take(&mut self.scratch_next);
        active.clear();
        next.clear();
        let mut survivors_depth = 0u32;
        let mut one_hold = [0u32; 1];
        for (p, probe) in probes.iter().enumerate() {
            let run: &[u32] = match probe {
                RunProbe::One(h) => {
                    one_hold[0] = *h;
                    &one_hold[..]
                }
                RunProbe::Slice(r) => r,
                RunProbe::Miss => &[],
            };
            if p == 0 {
                if run.is_empty() {
                    break;
                }
                active.extend_from_slice(run);
            } else {
                if run == active.as_slice() {
                    // Unchanged membership (the common shared-run
                    // case): one memcmp, no rebuild.
                } else {
                    // Merge-intersect two sorted runs; dropped
                    // holders get depth p in the same pass.
                    next.clear();
                    let mut ai = 0usize;
                    for &h in run {
                        while ai < active.len() && active[ai] < h {
                            self.push_answer(active[ai], p as u32, out);
                            ai += 1;
                        }
                        if ai < active.len() && active[ai] == h {
                            next.push(h);
                            ai += 1;
                        }
                    }
                    while ai < active.len() {
                        self.push_answer(active[ai], p as u32, out);
                        ai += 1;
                    }
                    std::mem::swap(&mut active, &mut next);
                }
            }
            if active.is_empty() {
                break;
            }
            survivors_depth = p as u32 + 1;
        }
        // Holders still active survived every visited position.
        for &h in active.iter() {
            self.push_answer(h, survivors_depth, out);
        }
        drop(probes);
        self.scratch_active = active;
        self.scratch_next = next;
        self.scratch_lineages = lineages;
    }

    /// Position-ordered enumeration (snapshot/Pull; §4). Order is
    /// derived on demand — snapshot is a cold path.
    pub fn enumerate(
        &self,
        id: HolderId,
    ) -> impl Iterator<Item = (u32, BlockKey, ContentHash)> + '_ {
        let state = self.live(id);
        let ordered = state.map(|s| s.ordered()).unwrap_or_default();
        ordered.into_iter().map(move |(pos, key)| {
            let info = state
                .expect("ordered non-empty implies live")
                .registry
                .get(&key)
                .expect("derived from registry");
            (pos, key, info.content)
        })
    }

    pub fn stats(&self) -> Stats {
        let holders = self.slots.iter().filter(|s| s.state.is_some()).count() as u64;
        // Rough model: entry map + memberships + registries.
        let bytes = self.entries.len() as u64 * 48 + self.holder_blocks_total * 72 + holders * 128;
        Stats {
            holders,
            holder_blocks: self.holder_blocks_total,
            distinct_entries: self.distinct_lineage_entries,
            bytes_estimate: bytes,
        }
    }

    // ---- internals ----

    fn live(&self, id: HolderId) -> Option<&HolderState> {
        let slot = self.slots.get(id.index as usize)?;
        if slot.generation != id.generation {
            return None;
        }
        slot.state.as_ref()
    }

    fn push_answer(&self, holder: u32, depth: u32, out: &mut Vec<Overlap>) {
        if depth == 0 {
            return;
        }
        let slot = &self.slots[holder as usize];
        let Some(state) = slot.state.as_ref() else {
            return;
        };
        out.push(Overlap {
            holder: HolderId {
                index: holder,
                generation: slot.generation,
            },
            depth,
            total_blocks: state.registry.len() as u64,
        });
    }

    fn index_block(&mut self, holder: u32, info: BlockInfo) {
        match self.entries.entry((info.pos, info.content)) {
            std::collections::hash_map::Entry::Vacant(vacant) => {
                vacant.insert(Membership::One {
                    lineage: info.lineage,
                    holder,
                });
                self.distinct_lineage_entries += 1;
            }
            std::collections::hash_map::Entry::Occupied(mut occupied) => {
                let had_lineage = occupied.get().lineage_shared_beyond(info.lineage, holder)
                    || matches!(occupied.get(), Membership::One { lineage, holder: h }
                        if *lineage == info.lineage && *h == holder);
                if occupied.get_mut().insert(info.lineage, holder) && !had_lineage {
                    self.distinct_lineage_entries += 1;
                }
            }
        }
    }

    fn unindex(
        entries: &mut FxHashMap<(u32, ContentHash), Membership>,
        distinct: &mut u64,
        holder: u32,
        info: BlockInfo,
    ) {
        let Some(membership) = entries.get_mut(&(info.pos, info.content)) else {
            return;
        };
        let shared = membership.lineage_shared_beyond(info.lineage, holder);
        if membership.remove(info.lineage, holder) {
            entries.remove(&(info.pos, info.content));
        }
        if !shared {
            *distinct -= 1;
        }
    }
}

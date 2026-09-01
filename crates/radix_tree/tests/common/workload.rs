//! Seeded workload generator producing §7-scoped operation streams.
//!
//! Shape (mirrors SPEC.md §11's pinned-workload structure at test
//! scale): prefix FAMILIES — shared base chains that several holders
//! store identically (same keys, same contents: exactly what the
//! placement feed produces) — plus per-holder divergent tails,
//! duplicate re-stores, gap-punching removes, rare clears, and
//! optional cross-family content coincidence to arm the oracle's
//! Single-entry lineage skip.
//!
//! Ordering scope: the emitted stream preserves each holder's own
//! order (ops interleave only ACROSS holders), matching §7. The
//! convergence tests additionally reorder within that scope.

use super::{Op, Rng};

#[derive(Debug, Clone)]
pub struct Config {
    pub holders: usize,
    pub families: usize,
    /// Blocks per family base chain: uniform in [min, max].
    pub family_len: (usize, usize),
    /// Holders per family: uniform in [min, max].
    pub holders_per_family: (usize, usize),
    /// Divergent tail blocks per holder per family: uniform [min, max].
    pub tail_len: (usize, usize),
    pub store_batch: usize,
    /// Percent of batches re-sent verbatim later (duplicates).
    pub duplicate_pct: u64,
    /// Percent of (holder, family) pairs that lose a mid-chain block
    /// (gap injection via remove).
    pub gap_pct: u64,
    /// Percent of holders cleared once mid-stream.
    pub clear_pct: u64,
    /// Reuse content values across families at this percent per
    /// block, arming content coincidence (oracle quirk class 3).
    pub coincidence_pct: u64,
}

impl Config {
    pub fn small() -> Self {
        Self {
            holders: 24,
            families: 8,
            family_len: (8, 96),
            holders_per_family: (1, 8),
            tail_len: (2, 16),
            store_batch: 8,
            duplicate_pct: 10,
            gap_pct: 15,
            clear_pct: 8,
            coincidence_pct: 0,
        }
    }

    pub fn with_coincidence() -> Self {
        Self {
            coincidence_pct: 25,
            ..Self::small()
        }
    }
}

/// The generated workload: an op stream (per-holder order already
/// legal per §7) plus the query set derived from stored prefixes and
/// misses.
#[derive(Debug, Clone)]
pub struct Workload {
    pub ops: Vec<Op>,
    pub queries: Vec<Vec<u64>>,
    pub holders: usize,
}

pub fn generate(seed: u64, cfg: &Config) -> Workload {
    let mut rng = Rng::new(seed);
    let mut content_pool: Vec<u64> = Vec::new();
    let fresh_content = |rng: &mut Rng, pool: &mut Vec<u64>, coincidence_pct: u64| -> u64 {
        if !pool.is_empty() && rng.chance(coincidence_pct) {
            pool[rng.below(pool.len())]
        } else {
            let c = rng.next() | 1; // avoid 0 (the model's lineage filler)
            pool.push(c);
            c
        }
    };

    // Families: shared (key, content) base chains. Keys are the
    // deterministic placement chain of the contents — identical
    // across holders for identical prefixes, as on the wire.
    let mut families: Vec<Vec<(u64, u64)>> = Vec::new();
    for _ in 0..cfg.families {
        let len = cfg.family_len.0 + rng.below(cfg.family_len.1 - cfg.family_len.0 + 1);
        let mut chain = Vec::with_capacity(len);
        let mut prev_key = 0u64;
        for i in 0..len {
            // Position 0 contents stay unique: pos-0 keys ARE the
            // content (wire position-0 rule), so coincidence there
            // would fuse two families into one chain — out of the §7
            // chain-consistent scope this generator promises.
            let coincidence = if i == 0 { 0 } else { cfg.coincidence_pct };
            let content = fresh_content(&mut rng, &mut content_pool, coincidence);
            // Chain-hash-shaped keys without depending on the wire
            // scheme: mix prev key and content deterministically.
            let key = if i == 0 {
                content
            } else {
                let mut k = prev_key ^ content.rotate_left(17);
                k = k.wrapping_mul(0x2545F4914F6CDD1D) | 1;
                k
            };
            chain.push((key, content));
            prev_key = key;
        }
        families.push(chain);
    }

    // Assignment + per-holder scripts (sequences that MUST keep their
    // relative order for that holder).
    let mut per_holder: Vec<Vec<Op>> = vec![Vec::new(); cfg.holders];
    for (fi, family) in families.iter().enumerate() {
        let count = cfg.holders_per_family.0
            + rng.below(cfg.holders_per_family.1 - cfg.holders_per_family.0 + 1);
        for _ in 0..count {
            let holder = rng.below(cfg.holders);
            // Base chain in parent-linked batches.
            let mut parent = None;
            let mut batches = Vec::new();
            for batch in family.chunks(cfg.store_batch) {
                batches.push(Op::Store {
                    holder,
                    parent,
                    blocks: batch.to_vec(),
                });
                parent = Some(batch.last().expect("non-empty").0);
            }
            // Divergent tail: unique contents, keys mixed with holder
            // and family so tails never collide.
            let tail_len = cfg.tail_len.0 + rng.below(cfg.tail_len.1 - cfg.tail_len.0 + 1);
            let mut tail = Vec::with_capacity(tail_len);
            let mut prev_key = parent.expect("family non-empty");
            for _ in 0..tail_len {
                let content = fresh_content(&mut rng, &mut content_pool, 0);
                let key = (prev_key ^ content.rotate_left(29))
                    .wrapping_mul(0x9E3779B97F4A7C15)
                    .wrapping_add(holder as u64 ^ (fi as u64) << 32)
                    | 1;
                tail.push((key, content));
                prev_key = key;
            }
            for batch in tail.chunks(cfg.store_batch) {
                batches.push(Op::Store {
                    holder,
                    parent,
                    blocks: batch.to_vec(),
                });
                parent = Some(batch.last().expect("non-empty").0);
            }
            // Duplicates: re-send some batches verbatim (later in the
            // holder's script — legal §7 duplication).
            let mut dups = Vec::new();
            for b in &batches {
                if rng.chance(cfg.duplicate_pct) {
                    dups.push(b.clone());
                }
            }
            // Gap: remove one mid-chain family block.
            let mut gaps = Vec::new();
            if rng.chance(cfg.gap_pct) && family.len() > 2 {
                let victim = family[1 + rng.below(family.len() - 2)].0;
                gaps.push(Op::Remove {
                    holder,
                    keys: vec![victim],
                });
            }
            let script = &mut per_holder[holder];
            script.extend(batches);
            script.extend(dups);
            script.extend(gaps);
        }
    }
    for (holder, script) in per_holder.iter_mut().enumerate() {
        if rng.chance(cfg.clear_pct) && !script.is_empty() {
            // Clear mid-script: everything before it is discarded
            // state; everything after rebuilds. Insert at a random
            // point rather than the end so post-clear stores exist.
            let at = rng.below(script.len());
            script.insert(at, Op::Clear { holder });
        }
    }

    // Interleave across holders preserving each holder's order.
    let mut cursors = vec![0usize; cfg.holders];
    let mut ops = Vec::new();
    loop {
        let live: Vec<usize> = (0..cfg.holders)
            .filter(|&h| cursors[h] < per_holder[h].len())
            .collect();
        if live.is_empty() {
            break;
        }
        let h = live[rng.below(live.len())];
        ops.push(per_holder[h][cursors[h]].clone());
        cursors[h] += 1;
    }

    // Post-clear stores may reference parents wiped by the clear;
    // both sides must reject those identically (ParentNotFound), so
    // they stay in the stream on purpose.

    // Queries: family prefixes at random depths (+tails), plus misses.
    let mut queries = Vec::new();
    for family in &families {
        for _ in 0..4 {
            let d = 1 + rng.below(family.len());
            queries.push(family[..d].iter().map(|&(_, c)| c).collect());
        }
    }
    for _ in 0..families.len() {
        let miss: Vec<u64> = (0..1 + rng.below(24)).map(|_| rng.next() | 1).collect();
        queries.push(miss);
    }
    Workload {
        ops,
        queries,
        holders: cfg.holders,
    }
}

/// Reorder a stream within §7 scope: per-holder order preserved for
/// order-bearing scripts, arbitrary cross-holder interleaving, using
/// a different seed. (Store-only holders could legally reorder
/// further; keeping their order too stays within scope.)
pub fn reinterleave(seed: u64, ops: &[Op], holders: usize) -> Vec<Op> {
    let mut per_holder: Vec<Vec<Op>> = vec![Vec::new(); holders];
    for op in ops {
        per_holder[op.holder()].push(op.clone());
    }
    let mut rng = Rng::new(seed);
    let mut cursors = vec![0usize; holders];
    let mut out = Vec::with_capacity(ops.len());
    loop {
        let live: Vec<usize> = (0..holders)
            .filter(|&h| cursors[h] < per_holder[h].len())
            .collect();
        if live.is_empty() {
            break;
        }
        let h = live[rng.below(live.len())];
        out.push(per_holder[h][cursors[h]].clone());
        cursors[h] += 1;
    }
    out
}

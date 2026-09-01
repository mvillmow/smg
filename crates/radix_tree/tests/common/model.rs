//! The trivially-correct model of SPEC.md §6/§7.
//!
//! Chosen for obviousness over speed: chains store their lineage as
//! literal content vectors, depth is computed by scanning every chain
//! of every holder, and nothing is amortized. If this model and an
//! implementation disagree, the implementation is wrong (or the spec
//! is — either way, loudly).

use std::collections::{BTreeMap, HashMap};

use super::Op;

/// A stored block's placement inside one holder's forest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Placement {
    chain: usize,
    pos: u32,
}

#[derive(Debug, Clone, Default)]
struct Chain {
    /// Positions currently present: pos -> (key, content).
    present: BTreeMap<u32, (u64, u64)>,
    /// Append-only lineage: content by position AS STORED. Removal
    /// leaves lineage intact (a block's identity includes the prefix
    /// it was stored under, §3), so re-parented children keep
    /// matching their original chain.
    lineage: Vec<u64>,
}

#[derive(Debug, Clone, Default)]
struct Holder {
    chains: Vec<Chain>,
    registry: HashMap<u64, Placement>,
}

/// Model-level store outcome, mirroring the §4 error surface the
/// generator and (later) the R1 core must agree on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreResult {
    Applied { applied: u32, duplicates: u32 },
    ParentNotFound,
}

#[derive(Debug, Clone, Default)]
pub struct Model {
    holders: Vec<Holder>,
}

impl Model {
    pub fn new(holder_count: usize) -> Self {
        Self {
            holders: vec![Holder::default(); holder_count],
        }
    }

    pub fn apply(&mut self, op: &Op) -> Option<StoreResult> {
        match op {
            Op::Store {
                holder,
                parent,
                blocks,
            } => Some(self.store(*holder, *parent, blocks)),
            Op::Remove { holder, keys } => {
                self.remove(*holder, keys);
                None
            }
            Op::Clear { holder } => {
                self.clear(*holder);
                None
            }
        }
    }

    fn store(&mut self, holder: usize, parent: Option<u64>, blocks: &[(u64, u64)]) -> StoreResult {
        if blocks.is_empty() {
            return StoreResult::Applied {
                applied: 0,
                duplicates: 0,
            };
        }
        let h = &mut self.holders[holder];
        let (chain_idx, start_pos) = match parent {
            None => {
                // A parent-None batch whose first block is already
                // registered at position 0 extends/duplicates that
                // chain rather than opening a twin (placement
                // re-publish is the common case).
                match h.registry.get(&blocks[0].0) {
                    Some(p) if p.pos == 0 => (p.chain, 0),
                    _ => {
                        h.chains.push(Chain::default());
                        (h.chains.len() - 1, 0)
                    }
                }
            }
            Some(parent_key) => match h.registry.get(&parent_key) {
                None => return StoreResult::ParentNotFound,
                Some(p) => (p.chain, p.pos + 1),
            },
        };
        let mut applied = 0u32;
        let mut duplicates = 0u32;
        for (i, &(key, content)) in blocks.iter().enumerate() {
            let pos = start_pos + i as u32;
            let placement = Placement {
                chain: chain_idx,
                pos,
            };
            match h.registry.get(&key) {
                Some(existing) if *existing == placement => {
                    duplicates += 1;
                    continue;
                }
                Some(&existing) => {
                    // §4: re-registering at a different placement MOVES
                    // the block (out of chain-consistent scope, but
                    // deterministic under per-holder order).
                    h.chains[existing.chain].present.remove(&existing.pos);
                }
                None => {}
            }
            let chain = &mut h.chains[chain_idx];
            if chain.lineage.len() <= pos as usize {
                chain.lineage.resize(pos as usize + 1, 0);
            }
            chain.lineage[pos as usize] = content;
            chain.present.insert(pos, (key, content));
            h.registry.insert(key, placement);
            applied += 1;
        }
        StoreResult::Applied {
            applied,
            duplicates,
        }
    }

    fn remove(&mut self, holder: usize, keys: &[u64]) -> u32 {
        let h = &mut self.holders[holder];
        let mut removed = 0;
        for key in keys {
            if let Some(p) = h.registry.remove(key) {
                h.chains[p.chain].present.remove(&p.pos);
                removed += 1;
            }
        }
        removed
    }

    fn clear(&mut self, holder: usize) {
        self.holders[holder] = Holder::default();
    }

    /// §6 depth: the largest `d` such that ONE chain holds every
    /// position `p < d` with content `query[p]` AND lineage exactly
    /// `query[0..p]`. Lineage prefix equality plus presence.
    pub fn depth(&self, holder: usize, query: &[u64]) -> u32 {
        let mut best = 0u32;
        for chain in &self.holders[holder].chains {
            let mut d = 0u32;
            for (p, &q) in query.iter().enumerate() {
                let lineage_true = chain.lineage.get(p) == Some(&q);
                let present_true = chain.present.get(&(p as u32)).map(|&(_, c)| c) == Some(q);
                if lineage_true && present_true {
                    d = p as u32 + 1;
                } else {
                    break;
                }
            }
            best = best.max(d);
        }
        best
    }

    /// Full holder->depth map for one query (§10.1: map equality,
    /// never a depth multiset). Depth-0 holders are absent.
    pub fn overlap(&self, query: &[u64]) -> BTreeMap<usize, u32> {
        let mut out = BTreeMap::new();
        for holder in 0..self.holders.len() {
            let d = self.depth(holder, query);
            if d > 0 {
                out.insert(holder, d);
            }
        }
        out
    }

    pub fn holder_blocks(&self, holder: usize) -> u64 {
        self.holders[holder].registry.len() as u64
    }

    /// Does `holder` hold content `q` anywhere at `pos` (any lineage)?
    /// The divergence classifier's discriminator (§6 quirk classes).
    pub fn holds_content_at(&self, holder: usize, pos: u32, q: u64) -> bool {
        self.holders[holder]
            .chains
            .iter()
            .any(|c| c.present.get(&pos).map(|&(_, content)| content) == Some(q))
    }

    /// Does `holder` hold a LINEAGE-TRUE block at `pos` for this query
    /// (present, right content, lineage == query[0..pos]) — i.e. the
    /// only reason model depth stopped short of it is a missing
    /// EARLIER position (a gap)?
    pub fn holds_lineage_true_at(&self, holder: usize, pos: u32, query: &[u64]) -> bool {
        self.holders[holder].chains.iter().any(|c| {
            c.present.get(&pos).map(|&(_, content)| content) == Some(query[pos as usize])
                && c.lineage.len() > pos as usize
                && c.lineage[..=pos as usize] == query[..=pos as usize]
        })
    }
}

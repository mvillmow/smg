//! Campaign C1/C3/C4: wide-config differential fuzz plus
//! out-of-contract chaos.
//!
//! In-contract mode: randomized workload configs far beyond the
//! normative shapes, RadixTree == model at every checkpoint, full
//! `audit()` at every checkpoint (and after EVERY op on small
//! workloads).
//!
//! Chaos mode: deliberately violates every §7 precondition — random
//! parents (including keys inside the same batch and never-seen
//! keys), keys reused across positions and chains, interleaved
//! retires/recreates, operations through stale ids, truncates at
//! random keeps. The contract here is: NO panics, `audit()` holds
//! after EVERY op, stale ids are loud no-ops, and a bit-for-bit
//! replay of the same sequence lands in an identical observable
//! state (determinism).
//!
//! `fuzz_quick` (32+8 seeds) always runs. The campaign entry point:
//!   RADIX_FUZZ_SEEDS=10000 cargo test -p radix-tree --release \
//!     --test fuzz_differential -- --ignored --nocapture

mod common;

use std::collections::BTreeMap;

use common::{
    model::{Model, StoreResult},
    workload::{self, Config as WlConfig},
    Op, Rng,
};
use radix_tree::{Config, HolderId, OverlapScratch, RadixTree, StoreError};

fn random_config(rng: &mut Rng) -> WlConfig {
    let holders = 2 + rng.below(255);
    WlConfig {
        holders,
        families: 1 + rng.below(64),
        family_len: {
            let lo = 1 + rng.below(64);
            (lo, lo + 1 + rng.below(448))
        },
        holders_per_family: {
            let lo = 1 + rng.below(4);
            (lo, lo + rng.below(holders.min(24)))
        },
        tail_len: (1 + rng.below(8), 9 + rng.below(56)),
        store_batch: 1 + rng.below(16),
        duplicate_pct: rng.below(41) as u64,
        gap_pct: rng.below(41) as u64,
        clear_pct: rng.below(31) as u64,
        coincidence_pct: rng.below(51) as u64,
    }
}

struct Subject {
    tree: RadixTree,
    ids: Vec<HolderId>,
    scratch: Vec<radix_tree::Overlap>,
    qscratch: OverlapScratch,
}

impl Subject {
    fn new(holders: usize) -> Self {
        let mut tree = RadixTree::new(Config::default());
        let ids = (0..holders)
            .map(|h| tree.create_holder(&format!("holder-{h}")))
            .collect();
        Self {
            tree,
            ids,
            scratch: Vec::new(),
            qscratch: OverlapScratch::default(),
        }
    }
    fn apply(&mut self, op: &Op) -> bool {
        match op {
            Op::Store {
                holder,
                parent,
                blocks,
            } => match self.tree.store(self.ids[*holder], *parent, blocks) {
                Ok(_) => true,
                Err(StoreError::ParentNotFound) => false,
                Err(e) => panic!("unexpected store error {e:?}"),
            },
            Op::Remove { holder, keys } => {
                self.tree.remove(self.ids[*holder], keys);
                true
            }
            Op::Clear { holder } => {
                self.tree.clear(self.ids[*holder]);
                true
            }
        }
    }
    fn overlap(&mut self, query: &[u64]) -> BTreeMap<usize, u32> {
        let scratch = &mut self.scratch;
        self.tree.overlap(query, &mut self.qscratch, scratch);
        let mut out = BTreeMap::new();
        for o in scratch.iter() {
            out.insert(o.holder.parts().0 as usize, o.depth);
        }
        out
    }
}

fn run_one_in_contract(seed: u64) {
    let mut rng = Rng::new(seed ^ 0xF00D);
    let cfg = random_config(&mut rng);
    let wl = workload::generate(seed, &cfg);
    let audit_every_op = wl.ops.len() < 4000;
    let mut model = Model::new(wl.holders);
    let mut subject = Subject::new(wl.holders);
    let checkpoint_every = (wl.ops.len() / 6).max(1);
    for (i, op) in wl.ops.iter().enumerate() {
        let model_outcome = model.apply(op);
        let subject_ok = subject.apply(op);
        if let Op::Store { .. } = op {
            let model_ok = !matches!(model_outcome, Some(StoreResult::ParentNotFound));
            assert_eq!(
                model_ok, subject_ok,
                "acceptance diverged: seed {seed} op {i}"
            );
        }
        if audit_every_op {
            subject
                .tree
                .audit()
                .unwrap_or_else(|e| panic!("audit failed: seed {seed} op {i}: {e}"));
        }
        if i % checkpoint_every == 0 || i + 1 == wl.ops.len() {
            if !audit_every_op {
                subject
                    .tree
                    .audit()
                    .unwrap_or_else(|e| panic!("audit failed: seed {seed} op {i}: {e}"));
            }
            for query in &wl.queries {
                assert_eq!(
                    subject.overlap(query),
                    model.overlap(query),
                    "subject != model: seed {seed} op {i}"
                );
            }
        }
    }
    for h in 0..wl.holders {
        assert_eq!(
            subject.tree.holder_blocks(subject.ids[h]),
            model.holder_blocks(h),
            "holder_blocks diverged: seed {seed} holder {h}"
        );
    }
}

/// Chaos: arbitrary op soup. Contract: no panic, audit always green,
/// stale-id ops are loud no-ops, replay is deterministic.
fn run_one_chaos(seed: u64) {
    #[derive(Clone, Debug)]
    enum COp {
        Store {
            slot: usize,
            parent_pick: u64,
            blocks: Vec<(u64, u64)>,
        },
        Remove {
            slot: usize,
            keys: Vec<u64>,
        },
        Clear {
            slot: usize,
        },
        Truncate {
            slot: usize,
            keep: u64,
        },
        Retire {
            slot: usize,
        },
        Recreate {
            slot: usize,
        },
        StaleProbe {
            slot: usize,
        },
        Query {
            probe: Vec<u64>,
        },
    }
    let mut rng = Rng::new(seed ^ 0xC4A05);
    let slots = 2 + rng.below(12);
    let ops_count = 400 + rng.below(1200);
    // Key pool encourages collisions across positions and holders.
    let key_pool: Vec<u64> = (0..64).map(|_| rng.next() | 1).collect();
    let content_pool: Vec<u64> = (0..48).map(|_| rng.next() | 1).collect();
    let mut script = Vec::with_capacity(ops_count);
    for _ in 0..ops_count {
        let slot = rng.below(slots);
        script.push(match rng.below(100) {
            0..=44 => {
                let len = 1 + rng.below(12);
                let blocks: Vec<(u64, u64)> = (0..len)
                    .map(|_| {
                        (
                            key_pool[rng.below(key_pool.len())],
                            content_pool[rng.below(content_pool.len())],
                        )
                    })
                    .collect();
                COp::Store {
                    slot,
                    parent_pick: rng.next(),
                    blocks,
                }
            }
            45..=59 => COp::Remove {
                slot,
                keys: (0..1 + rng.below(6))
                    .map(|_| key_pool[rng.below(key_pool.len())])
                    .collect(),
            },
            60..=66 => COp::Clear { slot },
            67..=74 => COp::Truncate {
                slot,
                keep: rng.below(40) as u64,
            },
            75..=81 => COp::Retire { slot },
            82..=88 => COp::Recreate { slot },
            89..=92 => COp::StaleProbe { slot },
            _ => COp::Query {
                probe: (0..1 + rng.below(16))
                    .map(|_| content_pool[rng.below(content_pool.len())])
                    .collect(),
            },
        });
    }

    let run = |script: &[COp]| -> (Vec<BTreeMap<usize, u32>>, u64, u64) {
        let mut tree = RadixTree::new(Config { max_chain_len: 64 });
        let mut ids: Vec<Option<HolderId>> = (0..slots)
            .map(|s| Some(tree.create_holder(&format!("chaos-{s}"))))
            .collect();
        let mut stale: Vec<HolderId> = Vec::new();
        let mut answers = Vec::new();
        let mut scratch = Vec::new();
        let mut qscratch = OverlapScratch::default();
        for (i, op) in script.iter().enumerate() {
            match op {
                COp::Store {
                    slot,
                    parent_pick,
                    blocks,
                } => {
                    if let Some(id) = ids[*slot] {
                        // Parent: none / a pooled key / a never-seen key.
                        let parent = match parent_pick % 3 {
                            0 => None,
                            1 => Some(key_pool[(*parent_pick as usize / 3) % key_pool.len()]),
                            _ => Some(parent_pick | 1),
                        };
                        let _ = tree.store(id, parent, blocks);
                    }
                }
                COp::Remove { slot, keys } => {
                    if let Some(id) = ids[*slot] {
                        tree.remove(id, keys);
                    }
                }
                COp::Clear { slot } => {
                    if let Some(id) = ids[*slot] {
                        tree.clear(id);
                    }
                }
                COp::Truncate { slot, keep } => {
                    if let Some(id) = ids[*slot] {
                        tree.truncate_tail(id, *keep);
                    }
                }
                COp::Retire { slot } => {
                    if let Some(id) = ids[*slot].take() {
                        tree.retire_holder(id);
                        stale.push(id);
                    }
                }
                COp::Recreate { slot } => {
                    if ids[*slot].is_none() {
                        ids[*slot] = Some(tree.create_holder(&format!("chaos-{slot}-re{i}")));
                    }
                }
                COp::StaleProbe { slot } => {
                    // Every stale id must be a loud no-op forever.
                    if let Some(&old) = stale.last() {
                        assert_eq!(
                            tree.store(old, None, &[(1, 1)]),
                            Err(StoreError::UnknownHolder),
                            "stale id accepted a store (seed {seed} op {i} slot {slot})"
                        );
                        assert_eq!(tree.remove(old, &[1]), 0);
                        assert_eq!(tree.holder_blocks(old), 0);
                        assert_eq!(tree.holder_name(old), None);
                    }
                }
                COp::Query { probe } => {
                    tree.overlap(probe, &mut qscratch, &mut scratch);
                    let mut m = BTreeMap::new();
                    for o in scratch.iter() {
                        m.insert(o.holder.parts().0 as usize, o.depth);
                    }
                    answers.push(m);
                }
            }
            tree.audit()
                .unwrap_or_else(|e| panic!("chaos audit failed: seed {seed} op {i}: {e}"));
        }
        let stats = tree.stats();
        (answers, stats.holder_blocks, stats.distinct_entries)
    };
    // Determinism: two independent executions of the same soup agree.
    let a = run(&script);
    let b = run(&script);
    assert_eq!(a, b, "chaos replay diverged (seed {seed})");
}

#[test]
fn fuzz_quick() {
    for seed in 1..=32u64 {
        run_one_in_contract(seed);
    }
    for seed in 1..=8u64 {
        run_one_chaos(seed);
    }
}

#[test]
#[ignore = "campaign entry point; set RADIX_FUZZ_SEEDS"]
fn fuzz_campaign() {
    let seeds: u64 = std::env::var("RADIX_FUZZ_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let start: u64 = std::env::var("RADIX_FUZZ_START")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    for seed in start..start + seeds {
        run_one_in_contract(seed);
        if seed % 3 == 0 {
            run_one_chaos(seed);
        }
        if (seed - start) % 250 == 249 {
            println!("fuzz: {} seeds green", seed - start + 1);
        }
    }
    println!("fuzz campaign: {seeds} seeds green");
}

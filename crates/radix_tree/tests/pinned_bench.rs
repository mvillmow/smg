//! The pinned performance workload (SPEC.md §11), oracle baseline.
//!
//! Normative constants live HERE; gates pass on this configuration
//! only. R0 runs it against the oracle plus the engine's replicated
//! glue (per-holder reverse map, last-chain Vec, id maps) to record
//! the baseline; R1 adds the RadixTree side under the same driver.
//!
//! Run (numbers are meaningless in debug):
//!   cargo test -p radix-tree --release --test pinned_bench \
//!     -- --ignored --nocapture
//!
//! Protocol (§11): RSS sampled after fill, before query-phase
//! allocations; no asserts or allocation inside timed regions;
//! quote the median of >=3 runs.

mod common;

use std::time::Instant;

use common::{oracle::Oracle, Op, Rng};
use radix_tree::{Config, HolderId, OverlapScratch, RadixTree, RadixTree3};

// ---- §11 normative constants (default scale) ----
// RADIX_BENCH_SCALE=large runs 8x blocks / 8x holders (~20 GB peak,
// ~60% of the 1.7e8 production target) to expose growth
// nonlinearities the normative scale can't: hash-table growth stalls,
// TLB pressure on a ~17 GB resident structure, query latency vs
// table size. Gates are quoted at the DEFAULT scale; large-scale
// runs are diagnostics.
fn target_blocks() -> u64 {
    match std::env::var("RADIX_BENCH_SCALE").as_deref() {
        Ok("large") => 100_000_000,
        _ => 10_000_000,
    }
}
fn holders() -> usize {
    match std::env::var("RADIX_BENCH_SCALE").as_deref() {
        Ok("large") => 2048,
        _ => 256,
    }
}
/// (sharing factor H, share of total holder-blocks in percent).
const SHARING_MIX: [(usize, u64); 3] = [(1, 50), (8, 35), (64, 15)];
const SHARED_DEPTH: (u32, u32) = (8, 512); // log-uniform
const TAIL_LEN: (u32, u32) = (4, 64); // uniform
const BATCH: usize = 8;
const DUPLICATE_PCT: u64 = 5;
const GAP_PCT: u64 = 2;
const MISS_QUERY_PCT: u64 = 20;
/// The gate cell: depth 78 with 64 candidate holders.
const GATE_DEPTH: u32 = 78;

fn log_uniform(rng: &mut Rng, lo: u32, hi: u32) -> u32 {
    let (llo, lhi) = ((lo as f64).ln(), (hi as f64).ln());
    let u = (rng.next() >> 11) as f64 / (1u64 << 53) as f64;
    (llo + u * (lhi - llo)).exp().round() as u32
}

struct Family {
    blocks: Vec<(u64, u64)>,
    holders: Vec<usize>,
}

fn build_families(rng: &mut Rng) -> Vec<Family> {
    let mut families = Vec::new();
    let next_content = |rng: &mut Rng| rng.next() | 1;
    // One forced gate family: H=64, shared length >= GATE_DEPTH.
    let mut budgets: Vec<(usize, u64)> = SHARING_MIX
        .iter()
        .map(|&(h, pct)| (h, target_blocks() * pct / 100))
        .collect();
    let mut force_gate = true;
    for (h, budget) in budgets.iter_mut() {
        let mut used = 0u64;
        while used < *budget {
            let shared_len = if force_gate && *h == 64 {
                force_gate = false;
                96
            } else {
                log_uniform(rng, SHARED_DEPTH.0, SHARED_DEPTH.1)
            };
            let mut blocks = Vec::with_capacity(shared_len as usize);
            let mut prev_key = 0u64;
            for i in 0..shared_len {
                let content = next_content(rng);
                let key = if i == 0 {
                    content
                } else {
                    (prev_key ^ content.rotate_left(17)).wrapping_mul(0x2545F4914F6CDD1D) | 1
                };
                blocks.push((key, content));
                prev_key = key;
            }
            let holder_count = holders();
            let mut members = Vec::with_capacity(*h);
            let base = rng.below(holder_count);
            for k in 0..*h {
                members.push((base + k * 7) % holder_count);
            }
            members.sort_unstable();
            members.dedup();
            used += shared_len as u64 * members.len() as u64;
            families.push(Family {
                blocks,
                holders: members,
            });
        }
    }
    families
}

/// Expand families into the mixed write stream (stores + duplicates +
/// gap removes), per-holder order preserved, §7-scoped interleave.
fn build_ops(rng: &mut Rng, families: &[Family]) -> (Vec<Op>, u64) {
    let mut per_holder: Vec<Vec<Op>> = vec![Vec::new(); holders()];
    let mut holder_blocks = 0u64;
    for family in families {
        for &holder in &family.holders {
            let mut parent = None;
            let mut batches = Vec::new();
            for chunk in family.blocks.chunks(BATCH) {
                batches.push(Op::Store {
                    holder,
                    parent,
                    blocks: chunk.to_vec(),
                });
                parent = Some(chunk.last().expect("non-empty").0);
            }
            // Divergent tail.
            let tail_len = TAIL_LEN.0 + (rng.next() % (TAIL_LEN.1 - TAIL_LEN.0 + 1) as u64) as u32;
            let mut prev_key = parent.expect("family non-empty");
            let mut tail = Vec::with_capacity(tail_len as usize);
            for _ in 0..tail_len {
                let content = rng.next() | 1;
                let key = (prev_key ^ content.rotate_left(29))
                    .wrapping_mul(0x9E3779B97F4A7C15)
                    .wrapping_add(holder as u64)
                    | 1;
                tail.push((key, content));
                prev_key = key;
            }
            for chunk in tail.chunks(BATCH) {
                batches.push(Op::Store {
                    holder,
                    parent,
                    blocks: chunk.to_vec(),
                });
                parent = Some(chunk.last().expect("non-empty").0);
            }
            holder_blocks += (family.blocks.len() + tail.len()) as u64;
            let mut dups = Vec::new();
            for b in &batches {
                if rng.chance(DUPLICATE_PCT) {
                    dups.push(b.clone());
                }
            }
            let mut gaps = Vec::new();
            if rng.chance(GAP_PCT * 10) && family.blocks.len() > 2 {
                // ~GAP_PCT of blocks overall: one block per ~10% of
                // instances at these lengths.
                let victim = family.blocks[1 + rng.below(family.blocks.len() - 2)].0;
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
    let mut cursors = vec![0usize; holders()];
    let mut ops = Vec::new();
    let mut live: Vec<usize> = (0..holders())
        .filter(|&h| !per_holder[h].is_empty())
        .collect();
    while !live.is_empty() {
        let li = rng.below(live.len());
        let h = live[li];
        ops.push(per_holder[h][cursors[h]].clone());
        cursors[h] += 1;
        if cursors[h] == per_holder[h].len() {
            live.swap_remove(li);
        }
    }
    (ops, holder_blocks)
}

fn rss_kib() -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(0)
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

/// Which implementation this process measures. One side per process
/// so RSS deltas are clean (§11 protocol):
///   RADIX_BENCH_SIDE=oracle (default) | r1
fn side() -> String {
    std::env::var("RADIX_BENCH_SIDE").unwrap_or_else(|_| "oracle".into())
}

#[allow(clippy::large_enum_variant)] // bench-local, two instances ever
enum Sider {
    Oracle(Oracle, Vec<Vec<u64>>),
    R1(
        RadixTree,
        Vec<HolderId>,
        Vec<radix_tree::Overlap>,
        OverlapScratch,
    ),
    R3(
        RadixTree3,
        Vec<HolderId>,
        Vec<radix_tree::Overlap>,
        OverlapScratch,
    ),
}

impl Sider {
    fn apply(&mut self, op: &Op) {
        match self {
            Sider::Oracle(oracle, chains) => {
                oracle.apply(op);
                if let Op::Store {
                    holder,
                    parent,
                    blocks,
                } = op
                {
                    // Engine glue the §11 oracle side must carry:
                    // last-chain Vec (reset on parent-None anchors).
                    let chain = &mut chains[*holder];
                    if parent.is_none() {
                        chain.clear();
                    }
                    chain.extend(blocks.iter().map(|&(k, _)| k));
                }
            }
            Sider::R1(tree, ids, _, _) => match op {
                Op::Store {
                    holder,
                    parent,
                    blocks,
                } => {
                    let _ = tree.store(ids[*holder], *parent, blocks);
                }
                Op::Remove { holder, keys } => {
                    tree.remove(ids[*holder], keys);
                }
                Op::Clear { holder } => tree.clear(ids[*holder]),
            },
            Sider::R3(tree, ids, _, _) => match op {
                Op::Store {
                    holder,
                    parent,
                    blocks,
                } => {
                    let _ = tree.store(ids[*holder], *parent, blocks);
                }
                Op::Remove { holder, keys } => {
                    tree.remove(ids[*holder], keys);
                }
                Op::Clear { holder } => tree.clear(ids[*holder]),
            },
        }
    }

    fn query(&mut self, q: &[u64]) -> usize {
        match self {
            Sider::Oracle(oracle, _) => oracle.overlap(q).len(),
            Sider::R1(tree, _, out, qscratch) => {
                tree.overlap(q, qscratch, out);
                out.len()
            }
            Sider::R3(tree, _, out, qscratch) => {
                tree.overlap(q, qscratch, out);
                out.len()
            }
        }
    }
}

#[test]
#[ignore = "pinned benchmark; run --release --ignored --nocapture"]
fn pinned_workload() {
    let mut rng = Rng::new(20260901);
    let families = build_families(&mut rng);
    let (ops, holder_blocks) = build_ops(&mut rng, &families);
    let total_blocks: u64 = ops
        .iter()
        .map(|op| match op {
            Op::Store { blocks, .. } => blocks.len() as u64,
            Op::Remove { keys, .. } => keys.len() as u64,
            Op::Clear { .. } => 0,
        })
        .sum();
    println!(
        "workload: {} families, {} ops, {} stream blocks, {} resident holder-blocks",
        families.len(),
        ops.len(),
        total_blocks,
        holder_blocks
    );

    let side_name = side();
    println!("side: {side_name}");
    let rss_before = rss_kib();
    let mut sider = match side_name.as_str() {
        "r1" => {
            let mut tree = RadixTree::new(Config::default());
            let ids = (0..holders())
                .map(|h| tree.create_holder(&format!("holder-{h}")))
                .collect();
            Sider::R1(tree, ids, Vec::new(), OverlapScratch::default())
        }
        "r3" => {
            let mut tree = RadixTree3::new(Config::default());
            let ids = (0..holders())
                .map(|h| tree.create_holder(&format!("holder-{h}")))
                .collect();
            Sider::R3(tree, ids, Vec::new(), OverlapScratch::default())
        }
        _ => Sider::Oracle(Oracle::new(holders()), vec![Vec::new(); holders()]),
    };
    let fill_start = Instant::now();
    for op in &ops {
        sider.apply(op);
    }
    let fill = fill_start.elapsed();
    let rss_after = rss_kib();
    println!(
        "fill: {:.2}s -> {:.2}M stream blocks/s",
        fill.as_secs_f64(),
        total_blocks as f64 / fill.as_secs_f64() / 1e6
    );
    println!(
        "memory: {} KiB delta -> {:.1} B/holder-block ({} holder-blocks)",
        rss_after - rss_before,
        (rss_after - rss_before) as f64 * 1024.0 / holder_blocks as f64,
        holder_blocks
    );

    // Query phase: prefix probes per sharing cell + misses. Build all
    // query vectors BEFORE timing (no allocation inside the region).
    let mut cells: Vec<(String, Vec<Vec<u64>>)> = Vec::new();
    let mut gate_queries = Vec::new();
    let warm = |fam: &Family, depth: u32| -> Vec<u64> {
        fam.blocks[..depth as usize]
            .iter()
            .map(|&(_, c)| c)
            .collect()
    };
    for &(h, _) in &SHARING_MIX {
        let mut queries = Vec::new();
        for fam in families.iter().filter(|f| f.holders.len() == h) {
            if queries.len() >= 2000 {
                break;
            }
            let d = 1 + rng.below(fam.blocks.len());
            queries.push(warm(fam, d as u32));
        }
        cells.push((format!("H={h}"), queries));
    }
    for fam in families.iter().filter(|f| f.holders.len() == 64) {
        if fam.blocks.len() >= GATE_DEPTH as usize && gate_queries.len() < 2000 {
            gate_queries.push(warm(fam, GATE_DEPTH));
        }
    }
    cells.push((format!("gate d={GATE_DEPTH} W=64"), gate_queries));
    let mut misses = Vec::new();
    for _ in 0..1000 {
        let len = 1 + rng.below(96);
        misses.push((0..len).map(|_| rng.next() | 1).collect::<Vec<u64>>());
    }
    cells.push(("miss".to_string(), misses));

    for (label, queries) in &cells {
        if queries.is_empty() {
            println!("cell {label}: EMPTY (workload bug)");
            continue;
        }
        // MISS_QUERY_PCT is embodied by the dedicated miss cell; warm
        // cells stay pure so percentiles are per-shape.
        let _ = MISS_QUERY_PCT;
        let mut lat: Vec<u64> = Vec::with_capacity(queries.len());
        for q in queries {
            let t = Instant::now();
            let n = sider.query(q);
            let ns = t.elapsed().as_nanos() as u64;
            std::hint::black_box(n);
            lat.push(ns);
        }
        lat.sort_unstable();
        println!(
            "cell {label}: n={} p50={}ns p99={}ns",
            lat.len(),
            percentile(&lat, 0.50),
            percentile(&lat, 0.99),
        );
    }
    std::hint::black_box(&sider as *const _);
}

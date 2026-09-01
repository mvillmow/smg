//! §11 allocation gate: storing fresh single-holder blocks performs
//! zero heap allocations amortized beyond map growth. The relative
//! RSS gate cannot catch a regression to per-block heap sets (the
//! §13.4 risk), so this counts allocations directly.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicU64, Ordering},
};

use radix_tree::{Config, RadixTree, RadixTree3};

struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static A: Counting = Counting;

#[test]
fn fresh_single_holder_stores_amortize_to_map_growth() {
    run_flat();
    run_chain();
}

fn run_flat() {
    let mut tree = RadixTree::new(Config::default());
    let h = tree.create_holder("h");
    // Warm: one chain so first-touch allocations (scratch, maps) are
    // out of the measured window.
    let warm: Vec<(u64, u64)> = (1..=64u64).map(|i| (i, i ^ 0xABCD)).collect();
    tree.store(h, None, &warm).expect("warm");

    // Many realistic-length chains (512 blocks), fresh single-holder
    // blocks throughout — never near max_chain_len.
    const BLOCKS: u64 = 100_000;
    const CHAIN_LEN: u64 = 512;
    let mut batch = Vec::with_capacity(8);
    let before = ALLOCS.load(Ordering::Relaxed);
    let mut key = 1000u64;
    let mut stored = 0u64;
    'outer: loop {
        let mut parent = None;
        let mut in_chain = 0u64;
        while in_chain < CHAIN_LEN {
            batch.clear();
            for _ in 0..8 {
                key += 1;
                batch.push((key, key.wrapping_mul(0x9E3779B97F4A7C15) | 1));
            }
            tree.store(h, parent, &batch).expect("store");
            parent = Some(key);
            stored += 8;
            in_chain += 8;
            if stored >= BLOCKS {
                break 'outer;
            }
        }
    }
    let allocs = ALLOCS.load(Ordering::Relaxed) - before;
    let per_block = allocs as f64 / stored as f64;
    println!("{allocs} allocations for {stored} single-holder blocks = {per_block:.4}/block");
    // Membership::One is inline (zero per-block); what remains is
    // amortized structure growth: BTreeSet nodes (~1 per 11 inserts)
    // and hash-map doublings. 0.5/block is generous headroom over
    // that floor while still failing any per-block heap regression
    // (a heap set per block would cost >= 1.0).
    assert!(
        per_block < 0.5,
        "flat allocation regression: {per_block:.4} allocs/block (gate: < 0.5)"
    );
}

/// Same gate for the chain-native core: contents-Vec growth and span
/// bookkeeping must amortize, never per-block allocate.
fn run_chain() {
    let mut tree = RadixTree3::new(Config::default());
    let h = tree.create_holder("h");
    let warm: Vec<(u64, u64)> = (1..=64u64).map(|i| (i, i ^ 0xABCD)).collect();
    tree.store(h, None, &warm).expect("warm");
    const BLOCKS: u64 = 100_000;
    const CHAIN_LEN: u64 = 512;
    let mut batch = Vec::with_capacity(8);
    let before = ALLOCS.load(Ordering::Relaxed);
    let mut key = 10_000_000u64;
    let mut stored = 0u64;
    'outer: loop {
        let mut parent = None;
        let mut in_chain = 0u64;
        while in_chain < CHAIN_LEN {
            batch.clear();
            for _ in 0..8 {
                key += 1;
                batch.push((key, key.wrapping_mul(0x9E3779B97F4A7C15) | 1));
            }
            tree.store(h, parent, &batch).expect("store");
            parent = Some(key);
            stored += 8;
            in_chain += 8;
            if stored >= BLOCKS {
                break 'outer;
            }
        }
    }
    let allocs = ALLOCS.load(Ordering::Relaxed) - before;
    let per_block = allocs as f64 / stored as f64;
    println!("chain core: {allocs} allocations for {stored} blocks = {per_block:.4}/block");
    assert!(
        per_block < 0.5,
        "chain allocation regression: {per_block:.4} allocs/block (gate: < 0.5)"
    );
}

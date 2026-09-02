//! Multi-writer load bench: the goal-doc G1–G3 baseline instrument.
//!
//! N concurrent PUBLISHER streams (one per simulated gateway) push a
//! duplicate-dominated placement stream at ONE live gRPC service
//! instance over loopback, while querier streams measure routing-time
//! latency — first idle, then under the full write load. Everything is
//! measured through the real wire: tonic, HTTP/2 flow control, the
//! ingest channel, the engine lock — exactly the path production
//! gateways hit.
//!
//!   radix-index-loadbench [--publishers 16] [--queriers 2]
//!     [--workers 64] [--chain-len 256] [--hot-per-worker 8]
//!     [--dup-pct 90] [--secs 15]
//!
//! Output is a flat key/value report; nothing is written to disk.
#![allow(
    clippy::disallowed_methods,
    clippy::too_many_arguments,
    clippy::type_complexity,
    reason = "standalone load-generator binary: fire-and-forget tasks and wide per-run knobs"
)]

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use radix_index::{
    proto, proto::radix_index_client::RadixIndexClient, server, wire_hash, ContentHash, Engine,
    EngineConfig,
};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

const MODEL: &str = "loadbench";
const BLOCK_SIZE: u32 = 128;

fn parse_flag<T: std::str::FromStr>(args: &[String], flag: &str) -> Option<T> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
}

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

/// Deterministic hot-chain contents for (worker, slot).
fn hot_contents(worker: usize, slot: usize, len: usize) -> Vec<ContentHash> {
    let seed = ((worker as u64) << 20) | slot as u64;
    (0..len as u64)
        .map(|pos| ContentHash(splitmix(seed.wrapping_mul(0x1000) ^ pos) | 1))
        .collect()
}

fn placement_update(holder: &str, contents: &[ContentHash]) -> proto::Update {
    let blocks = wire_hash::placement_chain(contents)
        .into_iter()
        .map(|(seq_hash, content_hash)| proto::Block {
            seq_hash: seq_hash.0,
            content_hash: content_hash.0,
        })
        .collect();
    proto::Update {
        keyspace: Some(proto::Keyspace {
            model: MODEL.into(),
            symbol_kind: proto::SymbolKind::Tokens as i32,
            block_size: BLOCK_SIZE,
            hash_scheme: wire_hash::HASH_SCHEME_V1,
        }),
        holder: holder.into(),
        epoch: 1,
        seq: 0,
        events: vec![proto::Event {
            kind: Some(proto::event::Kind::Stored(proto::Stored {
                parent_seq_hash: None,
                blocks,
            })),
        }],
        added: None,
        dropped: false,
    }
}

fn worker_url(w: usize) -> String {
    format!("grpc://10.0.0.{}:{}", w % 250, 9000 + w / 250)
}

struct Percentiles {
    n: usize,
    p50: u64,
    p90: u64,
    p99: u64,
}

fn percentiles(mut ns: Vec<u64>) -> Percentiles {
    ns.sort_unstable();
    let pick = |p: f64| ns[((ns.len() as f64 * p) as usize).min(ns.len() - 1)];
    Percentiles {
        n: ns.len(),
        p50: pick(0.50),
        p90: pick(0.90),
        p99: pick(0.99),
    }
}

/// One querier: serial queries against hot chains through a real
/// Subscribe stream; returns latencies (ns) recorded while `running`.
async fn run_querier(
    url: String,
    workers: usize,
    hot_per_worker: usize,
    chain_len: usize,
    seed: u64,
    running: Arc<AtomicBool>,
) -> Vec<u64> {
    let mut client = RadixIndexClient::connect(url)
        .await
        .expect("querier connect");
    let (tx, rx) = mpsc::channel::<proto::Query>(16);
    let mut answers = client
        .subscribe(tonic::Request::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
        .await
        .expect("subscribe")
        .into_inner();
    let mut rng = seed;
    let mut lat = Vec::new();
    let mut query_id = 1u64;
    while running.load(Ordering::Relaxed) {
        rng = splitmix(rng);
        let w = (rng % workers as u64) as usize;
        let slot = ((rng >> 32) % hot_per_worker as u64) as usize;
        let contents: Vec<u64> = hot_contents(w, slot, chain_len)
            .iter()
            .map(|c| c.0)
            .collect();
        let started = Instant::now();
        tx.send(proto::Query {
            query_id,
            keyspace: Some(proto::Keyspace {
                model: MODEL.into(),
                symbol_kind: proto::SymbolKind::Tokens as i32,
                block_size: BLOCK_SIZE,
                hash_scheme: wire_hash::HASH_SCHEME_V1,
            }),
            content_hashes: contents,
        })
        .await
        .expect("query send");
        let answer = answers.next().await.expect("answer").expect("answer ok");
        assert_eq!(answer.query_id, query_id, "serial stream must correlate");
        assert!(
            !answer.scores.is_empty(),
            "hot query must match (warm fill covered it)"
        );
        lat.push(started.elapsed().as_nanos() as u64);
        query_id += 1;
    }
    lat
}

/// One publisher: a gateway identity pushing the duplicate-dominated
/// placement stream. Returns blocks sent after `count_from`.
async fn run_publisher(
    url: String,
    workers: usize,
    hot_per_worker: usize,
    chain_len: usize,
    dup_pct: u64,
    seed: u64,
    running: Arc<AtomicBool>,
    count_from: Instant,
    sent_blocks: Arc<AtomicU64>,
) {
    let client = RadixIndexClient::connect(url)
        .await
        .expect("publisher connect");
    let mut client = client
        .max_decoding_message_size(64 * 1024 * 1024)
        .max_encoding_message_size(64 * 1024 * 1024);
    let (tx, rx) = mpsc::channel::<proto::Update>(64);
    let outbound = tokio_stream::wrappers::ReceiverStream::new(rx);
    let mut acks = client
        .publish(tonic::Request::new(outbound))
        .await
        .expect("publish stream")
        .into_inner();
    // Drain acks so the server's advisory ack channel never backs up.
    tokio::spawn(async move { while let Some(_ack) = acks.next().await {} });

    let mut rng = seed ^ 0xF00D;
    let mut nonce = 0u64;
    while running.load(Ordering::Relaxed) {
        rng = splitmix(rng);
        let w = (rng % workers as u64) as usize;
        let slot = ((rng >> 32) % hot_per_worker as u64) as usize;
        let update = if rng % 100 < dup_pct {
            // The multi-gateway steady state: another gateway routed the
            // same hot prefix and re-publishes an identical chain.
            let contents = hot_contents(w, slot, chain_len);
            placement_update(&worker_url(w), &contents)
        } else {
            // Fresh traffic: the hot prefix extended by a new tail (a
            // follow-up turn), attributed to the same worker.
            nonce += 1;
            let mut contents = hot_contents(w, slot, chain_len);
            let tail_seed = seed.wrapping_mul(0x51D) ^ nonce;
            contents.extend((0..32u64).map(|p| ContentHash(splitmix(tail_seed ^ p) | 1)));
            placement_update(&worker_url(w), &contents)
        };
        let blocks: u64 = update
            .events
            .iter()
            .map(|e| match &e.kind {
                Some(proto::event::Kind::Stored(s)) => s.blocks.len() as u64,
                _ => 0,
            })
            .sum();
        if tx.send(update).await.is_err() {
            break;
        }
        if Instant::now() >= count_from {
            sent_blocks.fetch_add(blocks, Ordering::Relaxed);
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let publishers: usize = parse_flag(&args, "--publishers").unwrap_or(16);
    let queriers: usize = parse_flag(&args, "--queriers").unwrap_or(2);
    let workers: usize = parse_flag(&args, "--workers").unwrap_or(64);
    let chain_len: usize = parse_flag(&args, "--chain-len").unwrap_or(256);
    let hot_per_worker: usize = parse_flag(&args, "--hot-per-worker").unwrap_or(8);
    let dup_pct: u64 = parse_flag(&args, "--dup-pct").unwrap_or(90);
    let secs: u64 = parse_flag(&args, "--secs").unwrap_or(15);

    let engine = Arc::new(Engine::new(EngineConfig::default()));
    let stats = Arc::new(server::ServiceStats::default());
    let port = {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe port");
        probe.local_addr().expect("probe addr").port()
    };
    let url = format!("http://127.0.0.1:{port}");
    {
        let engine = Arc::clone(&engine);
        let stats = Arc::clone(&stats);
        tokio::spawn(server::serve_until(
            engine,
            format!("127.0.0.1:{port}").parse().unwrap(),
            Vec::new(),
            Duration::from_secs(60),
            Duration::ZERO,
            Duration::ZERO,
            stats,
            std::future::pending::<()>(),
        ));
    }
    // Wait for the service to accept.
    let mut attempt = 0;
    loop {
        match RadixIndexClient::connect(url.clone()).await {
            Ok(_) => break,
            Err(_) if attempt < 50 => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => panic!("service never came up: {error}"),
        }
    }

    // Warm fill: every hot chain once, through one stream.
    let fill_start = Instant::now();
    {
        let mut client = RadixIndexClient::connect(url.clone()).await.expect("fill");
        let (tx, rx) = mpsc::channel::<proto::Update>(64);
        let fill = tokio::spawn(async move {
            for w in 0..workers {
                for slot in 0..hot_per_worker {
                    let contents = hot_contents(w, slot, chain_len);
                    tx.send(placement_update(&worker_url(w), &contents))
                        .await
                        .expect("fill send");
                }
            }
        });
        let mut acks = client
            .publish(tonic::Request::new(
                tokio_stream::wrappers::ReceiverStream::new(rx),
            ))
            .await
            .expect("fill stream")
            .into_inner();
        let expected = workers * hot_per_worker;
        let mut acked = 0usize;
        while acked < expected {
            acks.next().await.expect("fill ack").expect("fill ack ok");
            acked += 1;
        }
        fill.await.expect("fill task");
    }
    let hot_blocks = workers * hot_per_worker * chain_len;
    println!(
        "warm_fill_blocks {hot_blocks} in {:.2}s",
        fill_start.elapsed().as_secs_f64()
    );

    // Phase 1: idle queries.
    let idle = {
        let running = Arc::new(AtomicBool::new(true));
        let mut tasks = Vec::new();
        for q in 0..queriers {
            tasks.push(tokio::spawn(run_querier(
                url.clone(),
                workers,
                hot_per_worker,
                chain_len,
                0xA11CE ^ q as u64,
                Arc::clone(&running),
            )));
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
        running.store(false, Ordering::Relaxed);
        let mut lat = Vec::new();
        for t in tasks {
            lat.extend(t.await.expect("querier"));
        }
        percentiles(lat)
    };
    println!(
        "idle_query_ns n={} p50={} p90={} p99={}",
        idle.n, idle.p50, idle.p90, idle.p99
    );

    // Phase 2: full write load + queries.
    let running = Arc::new(AtomicBool::new(true));
    let sent_blocks = Arc::new(AtomicU64::new(0));
    let warmup = Duration::from_secs(2);
    let count_from = Instant::now() + warmup;
    let mut pubs = Vec::new();
    for p in 0..publishers {
        pubs.push(tokio::spawn(run_publisher(
            url.clone(),
            workers,
            hot_per_worker,
            chain_len,
            dup_pct,
            p as u64,
            Arc::clone(&running),
            count_from,
            Arc::clone(&sent_blocks),
        )));
    }
    tokio::time::sleep(warmup).await;
    let applies_before = stats.applies.load(Ordering::Relaxed);
    let window_start = Instant::now();
    let mut qtasks = Vec::new();
    for q in 0..queriers {
        qtasks.push(tokio::spawn(run_querier(
            url.clone(),
            workers,
            hot_per_worker,
            chain_len,
            0xB0B ^ q as u64,
            Arc::clone(&running),
        )));
    }
    tokio::time::sleep(Duration::from_secs(secs)).await;
    running.store(false, Ordering::Relaxed);
    let window = window_start.elapsed().as_secs_f64();
    let applies = stats.applies.load(Ordering::Relaxed) - applies_before;
    let mut lat = Vec::new();
    for t in qtasks {
        lat.extend(t.await.expect("loaded querier"));
    }
    for p in pubs {
        p.await.expect("publisher");
    }
    let loaded = percentiles(lat);
    let blocks = sent_blocks.load(Ordering::Relaxed);

    let gauges = engine.stats();
    println!(
        "loaded_publish blocks_per_sec {:.0} updates_per_sec {:.0} (window {window:.1}s, {publishers} publishers, dup {dup_pct}%)",
        blocks as f64 / window,
        applies as f64 / window,
    );
    println!(
        "loaded_query_ns n={} p50={} p90={} p99={}",
        loaded.n, loaded.p50, loaded.p90, loaded.p99
    );
    println!(
        "isolation p99_loaded/p99_idle {:.2} (goal G2: <= 2.0)",
        loaded.p99 as f64 / idle.p99.max(1) as f64
    );
    println!(
        "engine keyspaces={} holders={} blocks={}",
        gauges.keyspaces, gauges.holders, gauges.blocks
    );
}

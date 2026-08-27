//! Per-request records and the end-of-run summary: latency percentiles,
//! cache-hit ratios, turn-2 worker affinity, and the turn-1 worker spread.

use std::collections::{BTreeMap, HashMap};

use serde_json::{json, Value};

use crate::args::Args;

/// A request's `cached_tokens / prompt_tokens` at or above this counts as a
/// cache hit (the design's hit definition).
const CACHE_HIT_RATIO: f64 = 0.3;

/// One completed (or failed) `/generate` request.
#[derive(Debug)]
pub struct RequestRecord {
    pub turn: u8,
    pub session: u64,
    pub key: String,
    pub smg: usize,
    pub worker_port: Option<u64>,
    /// Local count of the input ids sent, not the server's echo.
    pub prompt_tokens: usize,
    pub cached_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub max_new: u32,
    /// Elapsed to the first SSE data frame; `None` for non-streaming requests.
    pub ttft_ms: Option<f64>,
    pub e2e_ms: f64,
    /// HTTP status; 0 for transport failures (connect or mid-stream).
    pub status: u16,
    /// Request start, milliseconds since the Unix epoch.
    pub start_ms: u64,
}

impl RequestRecord {
    pub fn is_ok(&self) -> bool {
        (200..300).contains(&self.status)
    }

    fn finish_ms(&self) -> u64 {
        self.start_ms.saturating_add(self.e2e_ms as u64)
    }

    fn cached_ratio(&self) -> Option<f64> {
        let cached = self.cached_tokens? as f64;
        if self.prompt_tokens == 0 {
            return None;
        }
        Some(cached / self.prompt_tokens as f64)
    }

    /// One JSONL line.
    pub fn to_json(&self) -> Value {
        json!({
            "turn": self.turn,
            "session": self.session,
            "key": self.key,
            "smg": self.smg,
            "worker_port": self.worker_port,
            "prompt_tokens": self.prompt_tokens,
            "cached_tokens": self.cached_tokens,
            "completion_tokens": self.completion_tokens,
            "max_new": self.max_new,
            "ttft_ms": self.ttft_ms,
            "e2e_ms": self.e2e_ms,
            "status": self.status,
            "start_ms": self.start_ms,
        })
    }
}

/// Build the summary document. Requests finishing during `--warmup-secs` are
/// excluded from every statistic but still counted in the totals.
pub fn summarize(
    args: &Args,
    records: &[RequestRecord],
    run_start_ms: u64,
    elapsed_secs: f64,
    sessions: u64,
) -> Value {
    let warmup_end_ms = run_start_ms.saturating_add(args.warmup_secs.saturating_mul(1000));
    let measured: Vec<&RequestRecord> = records
        .iter()
        .filter(|r| r.finish_ms() >= warmup_end_ms)
        .collect();
    let measured_secs = (elapsed_secs - args.warmup_secs as f64).max(1e-9);

    let mut errors: BTreeMap<String, u64> = BTreeMap::new();
    for record in records {
        if !record.is_ok() {
            *errors.entry(record.status.to_string()).or_insert(0) += 1;
        }
    }

    let ok: Vec<&RequestRecord> = measured.iter().copied().filter(|r| r.is_ok()).collect();
    let ttfts: Vec<f64> = ok.iter().filter_map(|r| r.ttft_ms).collect();
    let e2es: Vec<f64> = ok.iter().map(|r| r.e2e_ms).collect();

    // Turn-1 worker per session, from the whole run: a warmup turn 1 still
    // anchors its session's turn-2 affinity.
    let mut t1_ports: HashMap<u64, u64> = HashMap::new();
    for record in records {
        if record.turn == 1 && record.is_ok() {
            if let Some(port) = record.worker_port {
                t1_ports.entry(record.session).or_insert(port);
            }
        }
    }
    let t2_matches: Vec<bool> = ok
        .iter()
        .filter(|r| r.turn == 2)
        .filter_map(|r| {
            let port = r.worker_port?;
            Some(port == *t1_ports.get(&r.session)?)
        })
        .collect();
    let same_worker_rate = if t2_matches.is_empty() {
        None
    } else {
        Some(t2_matches.iter().filter(|&&same| same).count() as f64 / t2_matches.len() as f64)
    };

    let mut worker_counts: BTreeMap<u64, u64> = BTreeMap::new();
    for record in ok.iter().filter(|r| r.turn == 1) {
        if let Some(port) = record.worker_port {
            *worker_counts.entry(port).or_insert(0) += 1;
        }
    }
    let t1_total: u64 = worker_counts.values().sum();
    let distinct = worker_counts.len();
    let max_share = if t1_total > 0 {
        worker_counts
            .values()
            .max()
            .map(|&m| m as f64 / t1_total as f64)
    } else {
        None
    };
    // Normalized over the observed workers; a single worker is complete
    // concentration, so it reports 0 rather than dividing by ln(1).
    let normalized_entropy = if t1_total == 0 {
        None
    } else if distinct > 1 {
        let h: f64 = worker_counts
            .values()
            .map(|&c| {
                let p = c as f64 / t1_total as f64;
                -p * p.ln()
            })
            .sum();
        Some(h / (distinct as f64).ln())
    } else {
        Some(0.0)
    };

    let mut per_smg: Vec<u64> = vec![0; args.smg_urls.len()];
    for record in records {
        if let Some(slot) = per_smg.get_mut(record.smg) {
            *slot += 1;
        }
    }
    let per_smg_requests: Vec<Value> = args
        .smg_urls
        .iter()
        .zip(&per_smg)
        .map(|(url, &requests)| json!({"url": url, "requests": requests}))
        .collect();

    json!({
        "config": config_json(args),
        "totals": {
            "sessions": sessions,
            "requests": records.len(),
            "errors": errors,
        },
        "elapsed_secs": elapsed_secs,
        "achieved_rps": measured.len() as f64 / measured_secs,
        "ttft_ms": stats(&ttfts),
        "e2e_ms": stats(&e2es),
        "turns": {
            "turn1": turn_block(&measured, 1),
            "turn2": turn_block(&measured, 2),
        },
        "turn2_same_worker_rate": same_worker_rate,
        "turn1_workers": {
            "distinct": distinct,
            "max_share": max_share,
            "normalized_entropy": normalized_entropy,
        },
        "per_smg_requests": per_smg_requests,
    })
}

fn config_json(args: &Args) -> Value {
    json!({
        "smg_urls": args.smg_urls,
        "duration_secs": args.duration_secs,
        "session_rps": args.session_rps,
        "t2_ratio": args.t2_ratio,
        "think_secs": args.think_secs,
        "stream": args.stream,
        "http2": args.http2,
        "ingress": args.ingress.as_str(),
        "turn2_ingress": args.turn2_ingress.as_str(),
        "routing_key_reuse": args.routing_key_reuse,
        "system_prefix_tokens": args.system_prefix_tokens,
        "image_count": args.image_count,
        "image_bytes": args.image_bytes,
        "image_placeholder_id": args.image_placeholder_id,
        "image_placeholder_run": args.image_placeholder_run,
        "t2_suffix_tokens": args.t2_suffix_tokens,
        "prompt_cdf": cdf_json(&args.prompt_cdf),
        "prompt_max": args.prompt_max,
        "output_cdf": cdf_json(&args.output_cdf),
        "output_max": args.output_max,
        "tokens_hint": args.tokens_hint,
        "max_inflight": args.max_inflight,
        "warmup_secs": args.warmup_secs,
        "seed": args.seed,
        "out": args.out,
    })
}

fn cdf_json(anchors: &[(u32, f64)]) -> Vec<Value> {
    anchors
        .iter()
        .map(|&(tokens, cum)| json!([tokens, cum]))
        .collect()
}

fn turn_block(measured: &[&RequestRecord], turn: u8) -> Value {
    let of_turn: Vec<&RequestRecord> = measured
        .iter()
        .copied()
        .filter(|r| r.turn == turn)
        .collect();
    let ok: Vec<&RequestRecord> = of_turn.iter().copied().filter(|r| r.is_ok()).collect();
    let ratios: Vec<f64> = ok.iter().filter_map(|r| r.cached_ratio()).collect();
    let hit_rate = if ratios.is_empty() {
        None
    } else {
        Some(ratios.iter().filter(|&&r| r >= CACHE_HIT_RATIO).count() as f64 / ratios.len() as f64)
    };
    let ttfts: Vec<f64> = ok.iter().filter_map(|r| r.ttft_ms).collect();
    let e2es: Vec<f64> = ok.iter().map(|r| r.e2e_ms).collect();
    json!({
        "count": of_turn.len(),
        "ok": ok.len(),
        "cached_ratio_mean": mean(&ratios),
        "hit_rate": hit_rate,
        "ttft_ms": stats(&ttfts),
        "e2e_ms": stats(&e2es),
    })
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

/// {mean, p50, p90, p99} with nearest-rank percentiles from a sorted copy.
fn stats(values: &[f64]) -> Value {
    if values.is_empty() {
        return json!({"mean": null, "p50": null, "p90": null, "p99": null});
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable_by(f64::total_cmp);
    let pct = |q: f64| {
        let rank = ((q * (sorted.len() - 1) as f64).round() as usize).min(sorted.len() - 1);
        sorted[rank]
    };
    json!({
        "mean": sorted.iter().sum::<f64>() / sorted.len() as f64,
        "p50": pct(0.50),
        "p90": pct(0.90),
        "p99": pct(0.99),
    })
}

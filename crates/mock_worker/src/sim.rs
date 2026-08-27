//! Scale-simulation engine (`--engine sim`): SGLang-native `/generate`
//! semantics — `input_ids`, base64 multimodal identity, per-worker prefix
//! cache, long request holds — at a cost that lets one machine host
//! thousands of endpoints under hundreds of thousands of concurrent
//! requests.
//!
//! Unlike the realistic engine's stepped continuous-batching actor (whose
//! per-step wakeups are prohibitive at fleet scale), sim mode computes each
//! request's timeline analytically at admission and then sleeps: one timer
//! to first token, one to completion. Cache and KV accounting stay per
//! worker and O(prompt blocks) per request.
//!
//! The multimodal model reproduces production's routing/cache mismatch: the
//! gateway routes on `input_ids`, where images appear only as placeholder
//! runs (identical regardless of pixel content), while the worker's cached
//! sequence splices in ids derived from the image bytes — so two requests
//! that look identical to routing can still miss each other's KV.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use serde_json::{json, Value};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Tunables for the sim engine, resolved from CLI flags.
#[derive(Debug, Clone)]
pub struct SimParams {
    /// Per-output-token decode latency (ms); with the output-length
    /// distribution this sets the mean request lifetime.
    pub itl_ms: f64,
    /// Fixed pre-first-token overhead (ms) added to prefill time.
    pub ttft_base_ms: f64,
    /// Prefill throughput over UNCACHED prompt tokens (tokens/sec).
    pub prefill_tps: f64,
    /// Admission width: requests beyond it queue FIFO as "waiting".
    pub max_running: usize,
    /// KV capacity in tokens, shared by running requests and the prefix
    /// cache (cache evicts LRU to make room for running work).
    pub kv_capacity_tokens: u64,
    /// Cache block (page) size in tokens.
    pub block_size: usize,
    /// Token id marking an image's position inside `input_ids`.
    pub image_placeholder_id: u32,
    /// Cached-sequence tokens contributed per image; `0` derives the count
    /// from the payload size via `image_bytes_per_token`.
    pub image_tokens_per_image: usize,
    /// Bytes of (base64) image payload per derived image token.
    pub image_bytes_per_token: usize,
}

impl Default for SimParams {
    fn default() -> Self {
        Self {
            itl_ms: 43.0,
            ttft_base_ms: 30.0,
            prefill_tps: 8000.0,
            max_running: 256,
            kv_capacity_tokens: 1_200_000,
            block_size: 128,
            image_placeholder_id: 151_655,
            image_tokens_per_image: 0,
            image_bytes_per_token: 2800,
        }
    }
}

/// One simulated worker: admission semaphore + mutexed accounting state.
pub struct SimWorker {
    params: SimParams,
    admission: Arc<Semaphore>,
    state: Mutex<SimState>,
    port: u16,
}

/// Point-in-time load view for `/v1/loads`.
pub struct SimLoad {
    pub num_running_reqs: i64,
    pub num_waiting_reqs: i64,
    pub num_waiting_uncached_tokens: i64,
    pub num_used_tokens: i64,
    pub max_total_num_tokens: i64,
    pub token_usage: f64,
    pub gen_throughput: f64,
    pub cache_hit_rate: f64,
    pub max_running_requests: i64,
}

struct SimState {
    running: usize,
    waiting: usize,
    waiting_uncached_tokens: i64,
    /// Tokens pinned by running requests (prompt + reserved output).
    running_tokens: u64,
    cache: BlockLru,
    /// EWMA of per-request cached/prompt.
    hit_rate_ewma: f64,
}

/// Everything a response needs, computed at admission under one lock hold.
pub struct Admitted {
    pub prompt_tokens: usize,
    pub cached_tokens: usize,
    pub output_ids: Vec<u32>,
    pub ttft: Duration,
    pub decode: Duration,
    pub request_id: String,
    pub worker_port: u16,
    /// Held for the request's lifetime; dropping it releases admission and
    /// KV accounting and folds the sequence into the cache. Never read —
    /// its Drop is the point.
    _guard: CompletionGuard,
}

impl SimWorker {
    pub fn new(params: SimParams, port: u16) -> Arc<Self> {
        let admission = Arc::new(Semaphore::new(params.max_running.max(1)));
        Arc::new(Self {
            admission,
            state: Mutex::new(SimState {
                running: 0,
                waiting: 0,
                waiting_uncached_tokens: 0,
                running_tokens: 0,
                cache: BlockLru::new(),
                hit_rate_ewma: 0.0,
            }),
            params,
            port,
        })
    }

    pub fn load(&self) -> SimLoad {
        let st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let used = st.running_tokens + st.cache.tokens(self.params.block_size);
        let cap = self.params.kv_capacity_tokens.max(1);
        SimLoad {
            num_running_reqs: st.running as i64,
            num_waiting_reqs: st.waiting as i64,
            num_waiting_uncached_tokens: st.waiting_uncached_tokens,
            num_used_tokens: used.min(cap) as i64,
            max_total_num_tokens: cap as i64,
            token_usage: (used as f64 / cap as f64).min(1.0),
            // Decode-rate estimate: every running request emits one token
            // per ITL. Good enough for expected-wait ranking.
            gen_throughput: st.running as f64 * 1000.0 / self.params.itl_ms.max(1e-6),
            cache_hit_rate: st.hit_rate_ewma,
            max_running_requests: self.params.max_running as i64,
        }
    }

    /// Effective cached sequence: `input_ids` with each placeholder run
    /// replaced positionally by ids derived from the matching image's
    /// bytes; leftover images append. The gateway never sees this
    /// substitution — that blindness is the point.
    pub fn effective_sequence(&self, input_ids: &[u32], images: &[&str]) -> Vec<u32> {
        let ph = self.params.image_placeholder_id;
        let mut out = Vec::with_capacity(input_ids.len());
        let mut image_idx = 0usize;
        let mut i = 0usize;
        while i < input_ids.len() {
            if input_ids[i] == ph {
                let run_start = i;
                while i < input_ids.len() && input_ids[i] == ph {
                    i += 1;
                }
                let run_len = i - run_start;
                match images.get(image_idx) {
                    Some(image) => {
                        let seed = splitmix64(fnv64(image.as_bytes()));
                        for k in 0..run_len {
                            out.push((splitmix64(seed ^ k as u64) % 1_000_000) as u32 + 1_000_000);
                        }
                    }
                    // No payload for this run: keep the placeholders so the
                    // sequence still matches other payload-less requests.
                    None => out.extend(std::iter::repeat_n(ph, run_len)),
                }
                image_idx += 1;
            } else {
                out.push(input_ids[i]);
                i += 1;
            }
        }
        // Images beyond the placeholder runs contribute appended tokens
        // sized by config or payload length.
        for image in images.iter().skip(image_idx) {
            let n = if self.params.image_tokens_per_image > 0 {
                self.params.image_tokens_per_image
            } else {
                (image.len() / self.params.image_bytes_per_token.max(1)).max(1)
            };
            let seed = splitmix64(fnv64(image.as_bytes()));
            for k in 0..n {
                out.push((splitmix64(seed ^ k as u64) % 1_000_000) as u32 + 1_000_000);
            }
        }
        out
    }

    /// Queue for admission, then compute this request's cache outcome and
    /// timeline. The returned [`Admitted`] carries a guard that releases
    /// admission and KV accounting when dropped — including when the
    /// client disconnects mid-stream.
    pub async fn admit(self: &Arc<Self>, effective: Vec<u32>, max_new: u32) -> Admitted {
        let uncached_estimate = effective.len() as i64;
        {
            let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
            st.waiting += 1;
            st.waiting_uncached_tokens += uncached_estimate;
        }
        // Queue wait happens here; it is naturally part of client TTFT.
        let permit = Arc::clone(&self.admission)
            .acquire_owned()
            .await
            .expect("admission semaphore never closes");

        let prompt_tokens = effective.len();
        let seq_tokens = prompt_tokens as u64 + u64::from(max_new);
        let cached_tokens = {
            let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
            st.waiting -= 1;
            st.waiting_uncached_tokens -= uncached_estimate;
            st.running += 1;
            st.running_tokens += seq_tokens;
            let cached = st
                .cache
                .match_prefix(&effective, self.params.block_size)
                .min(prompt_tokens);
            // Prompt blocks are cached from admission on, so concurrent
            // requests sharing a prefix hit each other's work.
            st.cache.insert_chain(&effective, self.params.block_size);
            let ratio = if prompt_tokens == 0 {
                0.0
            } else {
                cached as f64 / prompt_tokens as f64
            };
            st.hit_rate_ewma = st.hit_rate_ewma * 0.95 + ratio * 0.05;
            let budget = self
                .params
                .kv_capacity_tokens
                .saturating_sub(st.running_tokens);
            st.cache.evict_to(budget, self.params.block_size);
            cached
        };

        let uncached = (prompt_tokens - cached_tokens) as f64;
        let ttft = Duration::from_secs_f64(
            (self.params.ttft_base_ms / 1000.0) + uncached / self.params.prefill_tps.max(1.0),
        );
        let decode = Duration::from_secs_f64(f64::from(max_new) * self.params.itl_ms / 1000.0);

        let request_seed =
            splitmix64(NEXT_SEED.fetch_add(1, Ordering::Relaxed) ^ fnv64_u32(&effective));
        let output_ids: Vec<u32> = (0..max_new as u64)
            .map(|k| (splitmix64(request_seed ^ k) % 30_000) as u32)
            .collect();

        Admitted {
            prompt_tokens,
            cached_tokens,
            ttft,
            decode,
            request_id: format!("sim-{}-{request_seed:016x}", self.port),
            worker_port: self.port,
            _guard: CompletionGuard {
                worker: Arc::clone(self),
                effective,
                output_ids: output_ids.clone(),
                seq_tokens,
                permit: Some(permit),
            },
            output_ids,
        }
    }
}

static NEXT_SEED: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);

/// Releases a request's admission slot and KV pin, and folds its full
/// sequence (prompt ⊕ output) into the prefix cache — turn-2 affinity
/// depends on turn-1 output being cached. Runs on normal completion AND on
/// client disconnect (response stream dropped).
struct CompletionGuard {
    worker: Arc<SimWorker>,
    effective: Vec<u32>,
    output_ids: Vec<u32>,
    seq_tokens: u64,
    permit: Option<OwnedSemaphorePermit>,
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        let params = self.worker.params.clone();
        let mut st = self.worker.state.lock().unwrap_or_else(|p| p.into_inner());
        st.running = st.running.saturating_sub(1);
        st.running_tokens = st.running_tokens.saturating_sub(self.seq_tokens);
        let mut full = std::mem::take(&mut self.effective);
        full.extend_from_slice(&self.output_ids);
        st.cache.insert_chain(&full, params.block_size);
        let budget = params.kv_capacity_tokens.saturating_sub(st.running_tokens);
        st.cache.evict_to(budget, params.block_size);
        drop(st);
        self.permit.take();
    }
}

// ── Block-hash LRU prefix cache ─────────────────────────────────────────────

/// Chained block-hash set with amortized-O(1) LRU eviction (lazy-deletion
/// queue). One entry = one `block_size`-token block.
struct BlockLru {
    /// block hash → last-touch tick.
    live: HashMap<u64, u64>,
    /// (hash, tick) in touch order; stale pairs skipped at eviction.
    order: std::collections::VecDeque<(u64, u64)>,
    tick: u64,
}

impl BlockLru {
    fn new() -> Self {
        Self {
            live: HashMap::new(),
            order: std::collections::VecDeque::new(),
            tick: 0,
        }
    }

    fn tokens(&self, block_size: usize) -> u64 {
        (self.live.len() * block_size) as u64
    }

    /// Longest cached block-aligned prefix of `seq`, in tokens. Touches
    /// matched blocks so hot prefixes stay resident.
    fn match_prefix(&mut self, seq: &[u32], block_size: usize) -> usize {
        let mut matched = 0usize;
        let mut chain = FNV_SEED;
        for block in seq.chunks_exact(block_size) {
            chain = chain_hash(chain, block);
            if self.live.contains_key(&chain) {
                self.touch(chain);
                matched += block_size;
            } else {
                break;
            }
        }
        matched
    }

    /// Insert every full block of `seq` (chained hashes).
    fn insert_chain(&mut self, seq: &[u32], block_size: usize) {
        let mut chain = FNV_SEED;
        for block in seq.chunks_exact(block_size) {
            chain = chain_hash(chain, block);
            self.touch(chain);
        }
    }

    fn touch(&mut self, hash: u64) {
        self.tick += 1;
        self.live.insert(hash, self.tick);
        self.order.push_back((hash, self.tick));
    }

    /// Evict least-recently-touched blocks until cached tokens fit
    /// `budget_tokens`.
    fn evict_to(&mut self, budget_tokens: u64, block_size: usize) {
        while self.tokens(block_size) > budget_tokens {
            match self.order.pop_front() {
                Some((hash, tick)) => {
                    if self.live.get(&hash) == Some(&tick) {
                        self.live.remove(&hash);
                    }
                }
                None => break,
            }
        }
    }
}

const FNV_SEED: u64 = 0xcbf2_9ce4_8422_2325;

fn chain_hash(prev: u64, block: &[u32]) -> u64 {
    let mut h = prev ^ 0x100_0000_01b3;
    for &id in block {
        h ^= u64::from(id);
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}

fn fnv64(bytes: &[u8]) -> u64 {
    let mut h = FNV_SEED;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}

fn fnv64_u32(ids: &[u32]) -> u64 {
    let mut h = FNV_SEED;
    for &id in ids.iter().take(64) {
        h ^= u64::from(id);
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

// ── Request-body extraction (SGLang /generate shape) ────────────────────────

/// `input_ids` as u32s: single sequence or first of a batch; absent → None.
pub fn extract_input_ids(v: &Value) -> Option<Vec<u32>> {
    let ids = v.get("input_ids")?;
    let seq = match ids.as_array()?.first() {
        Some(Value::Array(_)) => ids.as_array()?.first()?.as_array()?,
        _ => ids.as_array()?,
    };
    Some(
        seq.iter()
            .filter_map(Value::as_u64)
            .map(|id| id as u32)
            .collect(),
    )
}

/// Image payloads (raw base64 strings; never decoded — identity is bytes).
pub fn extract_images(v: &Value) -> Vec<&str> {
    match v.get("image_data") {
        Some(Value::String(s)) => vec![s.as_str()],
        Some(Value::Array(items)) => items.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    }
}

/// `sampling_params.max_new_tokens`, else top-level
/// `max_new_tokens`/`max_tokens`.
pub fn extract_max_new_tokens(v: &Value) -> Option<u32> {
    if let Some(n) = v
        .get("sampling_params")
        .and_then(|sp| sp.get("max_new_tokens"))
        .and_then(Value::as_u64)
    {
        return Some(n as u32);
    }
    for key in ["max_new_tokens", "max_tokens"] {
        if let Some(n) = v.get(key).and_then(Value::as_u64) {
            return Some(n as u32);
        }
    }
    None
}

/// SGLang-native `/generate` response body.
pub fn native_response(adm: &Admitted, finished: bool) -> Value {
    let completion = if finished { adm.output_ids.len() } else { 0 };
    json!({
        "text": if finished { "mock" } else { "" },
        "output_ids": if finished { Value::from(adm.output_ids.clone()) } else { Value::from(Vec::<u32>::new()) },
        "meta_info": {
            "id": adm.request_id,
            "prompt_tokens": adm.prompt_tokens,
            "completion_tokens": completion,
            "cached_tokens": adm.cached_tokens,
            "finish_reason": if finished {
                json!({"type": "length", "length": completion})
            } else {
                Value::Null
            },
            "worker_port": adm.worker_port,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(block: usize) -> SimParams {
        SimParams {
            itl_ms: 1.0,
            ttft_base_ms: 0.0,
            prefill_tps: 1_000_000.0,
            max_running: 8,
            kv_capacity_tokens: 100_000,
            block_size: block,
            ..SimParams::default()
        }
    }

    #[tokio::test]
    async fn repeat_prompt_hits_cache() {
        let w = SimWorker::new(params(4), 9100);
        let ids: Vec<u32> = (0..64).collect();
        let a = w.admit(ids.clone(), 4).await;
        assert_eq!(a.cached_tokens, 0);
        drop(a);
        let b = w.admit(ids, 4).await;
        assert_eq!(b.cached_tokens, 64, "identical prompt must fully hit");
    }

    #[tokio::test]
    async fn turn_two_hits_turn_one_prompt_and_output() {
        let w = SimWorker::new(params(4), 9100);
        let t1: Vec<u32> = (0..64).collect();
        let a = w.admit(t1.clone(), 8).await;
        let output = a.output_ids.clone();
        drop(a); // completion caches prompt ⊕ output
        let mut t2 = t1;
        t2.extend_from_slice(&output);
        t2.extend(1000..1040u32);
        let b = w.admit(t2, 4).await;
        // 64 prompt + 8 output = 72 tokens, block 4 → 72 cached.
        assert_eq!(b.cached_tokens, 72, "turn 2 must hit turn 1 prompt+output");
    }

    #[tokio::test]
    async fn image_identity_changes_effective_sequence() {
        let w = SimWorker::new(params(4), 9100);
        let mut ids: Vec<u32> = (0..16).collect();
        ids.extend(std::iter::repeat_n(w.params.image_placeholder_id, 8));
        ids.extend(100..116u32);

        let eff_a = w.effective_sequence(&ids, &["imagebytesA"]);
        let eff_b = w.effective_sequence(&ids, &["imagebytesB"]);
        assert_eq!(eff_a.len(), ids.len());
        assert_ne!(
            eff_a, eff_b,
            "different image bytes must change the cached sequence"
        );
        assert_eq!(eff_a[..16], ids[..16], "text before the image is untouched");

        // Same text, different image ⇒ the cache misses past the image.
        let a = w.admit(eff_a.clone(), 2).await;
        drop(a);
        let b = w.admit(eff_b, 2).await;
        assert_eq!(b.cached_tokens, 16, "hit stops at the image boundary");
        let c = w.admit(eff_a, 2).await;
        assert_eq!(c.cached_tokens, 40, "same image hits through it");
    }

    #[tokio::test]
    async fn admission_queues_beyond_max_running_and_reports_waiting() {
        let w = SimWorker::new(
            SimParams {
                max_running: 1,
                ..params(4)
            },
            9100,
        );
        let first = w.admit((0..8).collect(), 4).await;
        let w2 = Arc::clone(&w);
        let mut queued = tokio::task::JoinSet::new();
        queued.spawn(async move { w2.admit((100..108).collect(), 4).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let load = w.load();
        assert_eq!(load.num_running_reqs, 1);
        assert_eq!(load.num_waiting_reqs, 1);
        assert!(load.num_waiting_uncached_tokens > 0);
        drop(first);
        let second = queued.join_next().await.unwrap().unwrap();
        assert_eq!(w.load().num_waiting_reqs, 0);
        drop(second);
        assert_eq!(w.load().num_running_reqs, 0);
    }

    #[tokio::test]
    async fn kv_accounting_and_eviction_stay_bounded() {
        let w = SimWorker::new(
            SimParams {
                kv_capacity_tokens: 256,
                ..params(4)
            },
            9100,
        );
        for start in (0..2000u32).step_by(100) {
            let a = w.admit((start..start + 64).collect(), 4).await;
            drop(a);
        }
        let load = w.load();
        assert!(
            load.num_used_tokens as u64 <= 256,
            "cache must evict to capacity"
        );
        assert!(load.token_usage <= 1.0);
    }

    #[test]
    fn body_extraction_reads_all_generate_fields() {
        let body = json!({
            "input_ids": [1, 2, 3],
            "image_data": ["aGVsbG8=", "d29ybGQ="],
            "sampling_params": {"max_new_tokens": 7},
            "max_tokens": 99,
        });
        assert_eq!(extract_input_ids(&body), Some(vec![1, 2, 3]));
        assert_eq!(extract_images(&body).len(), 2);
        assert_eq!(extract_max_new_tokens(&body), Some(7));

        let batch = json!({"input_ids": [[4, 5], [6]]});
        assert_eq!(extract_input_ids(&batch), Some(vec![4, 5]));

        let single_image = json!({"image_data": "abc"});
        assert_eq!(extract_images(&single_image), vec!["abc"]);

        let top_level = json!({"max_new_tokens": 3});
        assert_eq!(extract_max_new_tokens(&top_level), Some(3));
    }
}

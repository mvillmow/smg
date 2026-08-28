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
    sync::{Arc, Mutex},
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
    /// Tokens claimed by running requests: uncached prompt + output
    /// reservation, held until COMPLETION (a decoding request depends on
    /// its whole prompt, so none of it may stop counting mid-flight).
    running_tokens: u64,
    kv: KvIndex,
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
    /// KV accounting and folds the finished sequence into the cache.
    guard: CompletionGuard,
}

impl Admitted {
    /// Mark simulated prefill complete: only now do the prompt's freshly
    /// computed blocks become matchable by other requests (a concurrent
    /// request must not hit KV that hasn't been computed). They enter the
    /// index PINNED — this request still decodes against them, so they
    /// stay non-evictable and its KV claim keeps counting until
    /// completion. Idempotent.
    pub fn finish_prefill(&mut self) {
        self.guard.finish_prefill();
    }
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
                kv: KvIndex::new(),
                hit_rate_ewma: 0.0,
            }),
            params,
            port,
        })
    }

    pub fn load(&self) -> SimLoad {
        let st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        // Reported usage covers RUNNING work only: the prefix cache is
        // evictable headroom, and engines report it that way (the
        // production sample — 38 running, 41% usage — only reconciles if
        // cached-but-idle KV is excluded). Counting the cache here drives
        // usage to 1.0 at steady state and trips the gateway's overload
        // veto fleet-wide, which production does not observe.
        let used = st.running_tokens;
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

    /// Test-only view of the evictable cache size in tokens (not part of
    /// reported usage; see [`Self::load`]).
    #[cfg(test)]
    fn cache_tokens_for_test(&self) -> u64 {
        let st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        st.kv.evictable_tokens(self.params.block_size)
    }

    /// Test-only view of pinned (active, non-evictable) KV in tokens.
    #[cfg(test)]
    fn pinned_tokens_for_test(&self) -> u64 {
        let st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        st.kv.pinned_tokens(self.params.block_size)
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

        let block = self.params.block_size;
        let prompt_tokens = effective.len();
        let chain = chain_hashes(&effective, block);
        // The request's KV claim while running: uncached prompt + output
        // reservation, held until COMPLETION. Matched blocks are pinned so
        // eviction cannot remove KV a running request depends on; they stay
        // accounted by whoever computed them, never twice.
        let cached_tokens;
        let private_tokens;
        {
            let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
            st.waiting -= 1;
            st.waiting_uncached_tokens -= uncached_estimate;
            st.running += 1;
            let matched_blocks = st.kv.match_blocks(&chain);
            cached_tokens = (matched_blocks * block).min(prompt_tokens);
            st.kv.pin(&chain[..matched_blocks]);
            private_tokens = (prompt_tokens - cached_tokens) as u64 + u64::from(max_new);
            st.running_tokens += private_tokens;
            let ratio = if prompt_tokens == 0 {
                0.0
            } else {
                cached_tokens as f64 / prompt_tokens as f64
            };
            st.hit_rate_ewma = st.hit_rate_ewma * 0.95 + ratio * 0.05;
            let budget = self
                .params
                .kv_capacity_tokens
                .saturating_sub(st.running_tokens);
            st.kv.evict_to(budget, block);
        }

        let uncached = (prompt_tokens - cached_tokens) as f64;
        let ttft = Duration::from_secs_f64(
            (self.params.ttft_base_ms / 1000.0) + uncached / self.params.prefill_tps.max(1.0),
        );
        let decode = Duration::from_secs_f64(f64::from(max_new) * self.params.itl_ms / 1000.0);

        // Deterministic from request CONTENT alone (full-sequence chain hash
        // + length + output budget) — never from admission order — so the
        // same seed produces byte-identical A/B traffic, and a follow-up
        // echoing these outputs is reproducible across runs.
        let request_seed = splitmix64(
            chain.last().copied().unwrap_or(FNV_SEED)
                ^ (prompt_tokens as u64)
                ^ (u64::from(max_new) << 32),
        );
        let output_ids: Vec<u32> = (0..max_new as u64)
            .map(|k| (splitmix64(request_seed ^ k) % 30_000) as u32)
            .collect();
        let matched_blocks = cached_tokens / block;

        Admitted {
            prompt_tokens,
            cached_tokens,
            ttft,
            decode,
            request_id: format!("sim-{}-{request_seed:016x}", self.port),
            worker_port: self.port,
            guard: CompletionGuard {
                worker: Arc::clone(self),
                effective,
                output_ids: output_ids.clone(),
                chain,
                pinned_blocks: matched_blocks,
                private_tokens,
                prefill_done: false,
                permit: Some(permit),
            },
            output_ids,
        }
    }
}

/// Releases a request's admission slot and KV accounting. The request's
/// claim (uncached prompt + output reservation) is held until COMPLETION —
/// decoding depends on the whole prompt, so none of it stops counting
/// mid-flight. Matchability is staged: matched blocks are pinned from
/// admission; the rest of the prompt becomes matchable (pinned) at prefill
/// completion; at completion everything unpins into the evictable cache
/// with the output appended (later-turn affinity depends on the output
/// being cached). A request dropped before prefill publishes nothing new.
/// Runs on normal completion AND on client disconnect.
struct CompletionGuard {
    worker: Arc<SimWorker>,
    effective: Vec<u32>,
    output_ids: Vec<u32>,
    /// Chained block hashes of the prompt.
    chain: Vec<u64>,
    /// Leading chain blocks this request currently pins.
    pinned_blocks: usize,
    /// Uncached prompt + output reservation, released at drop.
    private_tokens: u64,
    prefill_done: bool,
    permit: Option<OwnedSemaphorePermit>,
}

impl CompletionGuard {
    fn finish_prefill(&mut self) {
        if self.prefill_done {
            return;
        }
        self.prefill_done = true;
        let mut st = self.worker.state.lock().unwrap_or_else(|p| p.into_inner());
        // The freshly computed prompt blocks become matchable, pinned (not
        // evictable) because this request still decodes against them. The
        // KV claim is unchanged — the tokens were counted from admission.
        st.kv.pin(&self.chain[self.pinned_blocks..]);
        self.pinned_blocks = self.chain.len();
    }
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        let params = self.worker.params.clone();
        let mut st = self.worker.state.lock().unwrap_or_else(|p| p.into_inner());
        st.running = st.running.saturating_sub(1);
        st.running_tokens = st.running_tokens.saturating_sub(self.private_tokens);
        st.kv.unpin(&self.chain[..self.pinned_blocks]);
        if self.prefill_done {
            // Publish the finished sequence (prompt ⊕ output) as evictable
            // cache; an abort before prefill publishes nothing new.
            let mut full = std::mem::take(&mut self.effective);
            full.extend_from_slice(&self.output_ids);
            let full_chain = chain_hashes(&full, params.block_size);
            st.kv.insert_evictable(&full_chain);
        }
        let budget = params.kv_capacity_tokens.saturating_sub(st.running_tokens);
        st.kv.evict_to(budget, params.block_size);
        drop(st);
        self.permit.take();
    }
}

// ── KV index: pinned (active) + evictable (LRU) block hashes ────────────────

/// Block-hash view of a worker's KV: `pinned` holds refcounted blocks that
/// running requests depend on (never evicted); `lru` holds completed,
/// evictable cache. Both are matchable.
struct KvIndex {
    pinned: HashMap<u64, u32>,
    lru: BlockLru,
}

impl KvIndex {
    fn new() -> Self {
        Self {
            pinned: HashMap::new(),
            lru: BlockLru::new(),
        }
    }

    /// Leading chain blocks present in pinned or evictable KV.
    fn match_blocks(&mut self, chain: &[u64]) -> usize {
        let mut matched = 0usize;
        for &hash in chain {
            if self.pinned.contains_key(&hash) {
                matched += 1;
            } else if self.lru.contains(hash) {
                self.lru.touch(hash);
                matched += 1;
            } else {
                break;
            }
        }
        matched
    }

    /// Pin blocks (refcounted); a block pinned out of the LRU stops being
    /// evictable until its last unpin.
    fn pin(&mut self, hashes: &[u64]) {
        for &hash in hashes {
            *self.pinned.entry(hash).or_insert(0) += 1;
            self.lru.remove(hash);
        }
    }

    /// Drop one pin per block; the last unpin moves the block into the
    /// evictable LRU.
    fn unpin(&mut self, hashes: &[u64]) {
        for &hash in hashes {
            if let Some(count) = self.pinned.get_mut(&hash) {
                *count -= 1;
                if *count == 0 {
                    self.pinned.remove(&hash);
                    self.lru.touch(hash);
                }
            }
        }
    }

    fn insert_evictable(&mut self, hashes: &[u64]) {
        for &hash in hashes {
            if !self.pinned.contains_key(&hash) {
                self.lru.touch(hash);
            }
        }
    }

    /// Evict LRU blocks until evictable tokens fit `budget_tokens`; pinned
    /// blocks are untouchable by construction.
    fn evict_to(&mut self, budget_tokens: u64, block_size: usize) {
        self.lru.evict_to(budget_tokens, block_size);
    }

    #[cfg(test)]
    fn evictable_tokens(&self, block_size: usize) -> u64 {
        self.lru.tokens(block_size)
    }

    #[cfg(test)]
    fn pinned_tokens(&self, block_size: usize) -> u64 {
        (self.pinned.len() * block_size) as u64
    }
}

/// Chained block hashes for `seq` (one entry per full `block_size` block).
fn chain_hashes(seq: &[u32], block_size: usize) -> Vec<u64> {
    let mut chain = Vec::with_capacity(seq.len() / block_size.max(1));
    let mut prev = FNV_SEED;
    for block in seq.chunks_exact(block_size) {
        prev = chain_hash(prev, block);
        chain.push(prev);
    }
    chain
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

    fn contains(&self, hash: u64) -> bool {
        self.live.contains_key(&hash)
    }

    /// Drop a block outright (used when a block gets pinned); stale queue
    /// entries are skipped lazily at eviction.
    fn remove(&mut self, hash: u64) {
        self.live.remove(&hash);
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
        let mut a = w.admit(ids.clone(), 4).await;
        assert_eq!(a.cached_tokens, 0);
        a.finish_prefill();
        drop(a);
        let b = w.admit(ids, 4).await;
        assert_eq!(b.cached_tokens, 64, "identical prompt must fully hit");
    }

    #[tokio::test]
    async fn cache_not_published_before_prefill_completes() {
        // A cold request's prompt must not be hittable while its prefill is
        // still simulated as running, and never if it aborts before then.
        let w = SimWorker::new(params(4), 9100);
        let ids: Vec<u32> = (0..64).collect();
        let mut a = w.admit(ids.clone(), 4).await;
        let b = w.admit(ids.clone(), 4).await;
        assert_eq!(b.cached_tokens, 0, "concurrent cold request must miss");
        drop(b);

        a.finish_prefill();
        let c = w.admit(ids.clone(), 4).await;
        assert_eq!(c.cached_tokens, 64, "after prefill the prompt is cached");
        drop(c);
        drop(a);

        // An aborted-before-prefill request publishes nothing.
        let novel: Vec<u32> = (5000..5064).collect();
        let aborted = w.admit(novel.clone(), 4).await;
        drop(aborted);
        let d = w.admit(novel, 4).await;
        assert_eq!(d.cached_tokens, 0, "aborted prefill must not publish KV");
    }

    #[tokio::test]
    async fn active_prompt_kv_stays_counted_and_pinned_until_completion() {
        // 64-token cold prompt + 8 reserved output, block 4. A decoding
        // request depends on its whole prompt, so its 72-token claim holds
        // from admission to COMPLETION; prefill completion makes the prompt
        // matchable (pinned, non-evictable) without changing usage or
        // double-counting; completion releases the claim and publishes the
        // full sequence as evictable cache.
        let w = SimWorker::new(params(4), 9100);
        let mut a = w.admit((0..64).collect(), 8).await;
        assert_eq!(w.load().num_used_tokens, 72);
        assert_eq!(w.cache_tokens_for_test(), 0);
        assert_eq!(w.pinned_tokens_for_test(), 0);
        a.finish_prefill();
        assert_eq!(
            w.load().num_used_tokens,
            72,
            "the prompt keeps counting while decode depends on it"
        );
        assert_eq!(w.pinned_tokens_for_test(), 64, "prompt pinned, matchable");
        assert_eq!(w.cache_tokens_for_test(), 0, "nothing evictable yet");
        drop(a);
        assert_eq!(w.load().num_used_tokens, 0);
        assert_eq!(w.pinned_tokens_for_test(), 0);
        assert_eq!(
            w.cache_tokens_for_test(),
            72,
            "prompt \u{2295} output evictable after completion"
        );
        assert_eq!(w.load().num_running_reqs, 0);
    }

    #[tokio::test]
    async fn pinned_blocks_survive_eviction_pressure() {
        // Capacity 128 tokens; an active request's 64-token prompt is
        // pinned after prefill. Churn from completing requests must evict
        // only the evictable cache — the active prompt stays matchable.
        let w = SimWorker::new(
            SimParams {
                kv_capacity_tokens: 128,
                ..params(4)
            },
            9100,
        );
        let ids: Vec<u32> = (0..64).collect();
        let mut active = w.admit(ids.clone(), 8).await;
        active.finish_prefill();
        for start in (1000..3000u32).step_by(100) {
            let mut churn = w.admit((start..start + 32).collect(), 4).await;
            churn.finish_prefill();
            drop(churn);
        }
        assert_eq!(w.pinned_tokens_for_test(), 64, "active prompt not evicted");
        let hit = w.admit(ids, 4).await;
        assert_eq!(hit.cached_tokens, 64, "pinned prompt stays matchable");
    }

    #[tokio::test]
    async fn output_ids_are_deterministic_from_content() {
        // Identical (sequence, max_new) must yield identical outputs on
        // different workers and regardless of admission order; different
        // content must diverge — A/B traffic reproducibility depends on it.
        let w1 = SimWorker::new(params(4), 9100);
        let w2 = SimWorker::new(params(4), 9200);
        let ids: Vec<u32> = (0..64).collect();
        let other = w1.admit((500..564).collect(), 8).await; // admission-order noise
        let a = w1.admit(ids.clone(), 8).await;
        let b = w2.admit(ids.clone(), 8).await;
        assert_eq!(a.output_ids, b.output_ids, "content-determined outputs");
        drop(other);
        let c = w1.admit((2000..2064).collect(), 8).await;
        assert_ne!(a.output_ids, c.output_ids, "different content diverges");
    }

    #[tokio::test]
    async fn turn_two_hits_turn_one_prompt_and_output() {
        let w = SimWorker::new(params(4), 9100);
        let t1: Vec<u32> = (0..64).collect();
        let mut a = w.admit(t1.clone(), 8).await;
        let output = a.output_ids.clone();
        a.finish_prefill();
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
        let mut a = w.admit(eff_a.clone(), 2).await;
        a.finish_prefill();
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
            let mut a = w.admit((start..start + 64).collect(), 4).await;
            a.finish_prefill();
            drop(a);
        }
        assert!(
            w.cache_tokens_for_test() <= 256,
            "cache must evict to capacity"
        );
        let load = w.load();
        assert_eq!(load.num_used_tokens, 0, "nothing running after completion");
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

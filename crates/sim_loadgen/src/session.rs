//! Session lifecycle: build paired turn-1/turn-2 `/generate` requests, send
//! them through the shared client, and parse the SGLang-native responses
//! (single JSON or SSE frames).

use std::{
    fmt::Write as _,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures::StreamExt;
use serde_json::Value;
use tokio::sync::{mpsc::UnboundedSender, Semaphore};

use crate::{
    args::{Args, Ingress, Turn2Ingress},
    dist::{self, PiecewiseCdf, Rng},
    report::RequestRecord,
};

/// Number of routing keys shared by `--routing-key-reuse` sessions.
const SHARED_ROUTING_KEYS: usize = 32;

/// Most input ids a routing-tokens hint carries (the gateway caps it there).
const TOKENS_HINT_CAP: usize = 512;

/// Shared state every session task needs.
pub struct Ctx {
    pub args: Arc<Args>,
    pub client: reqwest::Client,
    pub limiter: Arc<Semaphore>,
    pub records: UnboundedSender<RequestRecord>,
    pub prompt_cdf: PiecewiseCdf,
    pub output_cdf: PiecewiseCdf,
    pub sent: AtomicU64,
    pub done: AtomicU64,
    pub errors: AtomicU64,
}

/// Milliseconds since the Unix epoch.
pub fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Run one session: turn 1, then with probability `--t2-ratio` a think-time
/// pause and turn 2 extending the turn-1 context with its returned output.
pub async fn run(ctx: Arc<Ctx>, sid: u64) {
    let args = &ctx.args;
    let session_seed = dist::sub_seed(dist::sub_seed(args.seed, dist::SALT_SESSION), sid);
    let mut rng = Rng::new(session_seed);

    let reuse_draw = rng.next_f64();
    let key = if reuse_draw < args.routing_key_reuse {
        format!("shared-{}", rng.next_index(SHARED_ROUTING_KEYS))
    } else {
        format!("sess-{sid}")
    };

    let prompt_len = ctx.prompt_cdf.sample(rng.next_f64()) as usize;
    let max_new_1 = ctx.output_cdf.sample(rng.next_f64());

    // Warm runs share one prefix stream; cold runs give each session its own,
    // so no cross-session prefix ever matches.
    let prefix_seed = if args.system_prefix_tokens > 0 {
        dist::sub_seed(args.seed, dist::SALT_PREFIX)
    } else {
        dist::sub_seed(session_seed, dist::SALT_PREFIX)
    };
    let mut input_ids = dist::token_ids(prefix_seed, args.system_prefix_tokens as usize);
    let placeholder_total = args.image_placeholder_run as usize * args.image_count as usize;
    input_ids.resize(
        input_ids.len() + placeholder_total,
        args.image_placeholder_id,
    );
    let pad = prompt_len.saturating_sub(input_ids.len());
    input_ids.extend(dist::token_ids(
        dist::sub_seed(session_seed, dist::SALT_PAD),
        pad,
    ));

    let n = args.smg_urls.len();
    let t1_smg = match args.ingress {
        Ingress::Hash => (dist::hash_str(&key) % n as u64) as usize,
        Ingress::Random => rng.next_index(n),
    };

    // Draw the whole turn-2 plan up front so the session's random stream does
    // not depend on the turn-1 outcome.
    let t2_draw = rng.next_f64();
    let think = rng.next_exp(args.think_secs);
    let max_new_2 = ctx.output_cdf.sample(rng.next_f64());
    let t2_smg_random = rng.next_index(n);

    let output_ids = send_turn(
        &ctx,
        &TurnRequest {
            sid,
            session_seed,
            turn: 1,
            key: &key,
            smg: t1_smg,
            input_ids: &input_ids,
            max_new: max_new_1,
        },
    )
    .await;

    if t2_draw >= args.t2_ratio {
        return;
    }
    // A failed turn 1 has no output to extend; the session ends there.
    let Some(output_ids) = output_ids else {
        return;
    };
    tokio::time::sleep(Duration::from_secs_f64(think)).await;

    let mut t2_ids = input_ids;
    t2_ids.extend(output_ids);
    t2_ids.extend(dist::token_ids(
        dist::sub_seed(session_seed, dist::SALT_SUFFIX),
        args.t2_suffix_tokens as usize,
    ));
    let t2_smg = match args.turn2_ingress {
        Turn2Ingress::Same => t1_smg,
        Turn2Ingress::Hash => (dist::hash_str(&key) % n as u64) as usize,
        Turn2Ingress::Random => t2_smg_random,
    };
    send_turn(
        &ctx,
        &TurnRequest {
            sid,
            session_seed,
            turn: 2,
            key: &key,
            smg: t2_smg,
            input_ids: &t2_ids,
            max_new: max_new_2,
        },
    )
    .await;
}

struct TurnRequest<'a> {
    sid: u64,
    session_seed: u64,
    turn: u8,
    key: &'a str,
    smg: usize,
    input_ids: &'a [u32],
    max_new: u32,
}

/// Send one `/generate` request and record its outcome. Returns the returned
/// `output_ids` on success (empty when the response carried none), `None` on
/// any error.
async fn send_turn(ctx: &Ctx, req: &TurnRequest<'_>) -> Option<Vec<u32>> {
    let args = &ctx.args;
    let permit = match ctx.limiter.clone().acquire_owned().await {
        Ok(permit) => permit,
        // The semaphore is never closed; a close means shutdown.
        Err(_) => return None,
    };

    // Image payloads are regenerated from the session seed inside the permit,
    // so waiting sessions do not hold hundreds of KB, and turn 2 reproduces
    // byte-identical bytes without storing them across the think time.
    let images: Vec<String> = (0..args.image_count)
        .map(|i| {
            dist::base64_blob(
                dist::sub_seed(req.session_seed, dist::SALT_IMAGE + u64::from(i)),
                args.image_bytes,
            )
        })
        .collect();
    let body = build_body(req.input_ids, &images, req.max_new, args.stream);
    drop(images);

    let hint = args.tokens_hint.then(|| {
        let head = &req.input_ids[..req.input_ids.len().min(TOKENS_HINT_CAP)];
        let mut joined = String::with_capacity(head.len() * 7);
        for (i, id) in head.iter().enumerate() {
            if i > 0 {
                joined.push(',');
            }
            let _ = write!(joined, "{id}");
        }
        joined
    });

    ctx.sent.fetch_add(1, Ordering::Relaxed);
    let url = format!("{}/generate", args.smg_urls[req.smg]);
    let start_ms = epoch_ms();
    let started = Instant::now();
    let mut request = ctx
        .client
        .post(&url)
        .header("content-type", "application/json")
        .header("x-smg-routing-key", req.key);
    if let Some(hint) = &hint {
        request = request.header("x-smg-routing-tokens", hint.as_str());
    }

    let mut status: u16 = 0;
    let mut ttft_ms: Option<f64> = None;
    let mut response: Option<Value> = None;
    if let Ok(resp) = request.body(body).send().await {
        status = resp.status().as_u16();
        if resp.status().is_success() {
            if args.stream {
                match consume_sse(resp, started).await {
                    Ok((ttft, last)) => {
                        ttft_ms = ttft;
                        response = last;
                    }
                    // A mid-stream transport failure is an incomplete
                    // request, not a success at the original status.
                    Err(_) => status = 0,
                }
            } else {
                match resp.bytes().await {
                    Ok(bytes) => response = serde_json::from_slice(&bytes).ok(),
                    Err(_) => status = 0,
                }
            }
        } else {
            // Drain the error body so the connection can be reused.
            let _ = resp.bytes().await;
        }
    }
    let e2e_ms = started.elapsed().as_secs_f64() * 1000.0;
    drop(permit);

    let mut worker_port = None;
    let mut cached_tokens = None;
    let mut completion_tokens = None;
    let mut output_ids: Option<Vec<u32>> = None;
    if let Some(value) = &response {
        let meta = &value["meta_info"];
        worker_port = meta["worker_port"].as_u64();
        cached_tokens = meta["cached_tokens"].as_u64();
        completion_tokens = meta["completion_tokens"].as_u64();
        output_ids = value["output_ids"].as_array().map(|ids| {
            ids.iter()
                .filter_map(|id| id.as_u64().map(|id| id as u32))
                .collect()
        });
    }

    let is_error = !(200..300).contains(&status);
    if is_error {
        ctx.errors.fetch_add(1, Ordering::Relaxed);
    }
    ctx.done.fetch_add(1, Ordering::Relaxed);

    let record = RequestRecord {
        turn: req.turn,
        session: req.sid,
        key: req.key.to_string(),
        smg: req.smg,
        worker_port,
        prompt_tokens: req.input_ids.len(),
        cached_tokens,
        completion_tokens,
        max_new: req.max_new,
        ttft_ms,
        e2e_ms,
        status,
        start_ms,
    };
    // A send error means the collector is gone (shutdown); nothing to do.
    let _ = ctx.records.send(record);

    if is_error {
        None
    } else {
        Some(output_ids.unwrap_or_default())
    }
}

/// Serialize the request body by hand: the multi-hundred-KB image strings are
/// appended directly instead of being copied through an intermediate
/// `serde_json::Value` (the base64 alphabet needs no JSON escaping).
fn build_body(input_ids: &[u32], images: &[String], max_new: u32, stream: bool) -> String {
    let image_len: usize = images.iter().map(|image| image.len() + 3).sum();
    let mut body = String::with_capacity(image_len + input_ids.len() * 7 + 96);
    body.push_str("{\"input_ids\":[");
    for (i, id) in input_ids.iter().enumerate() {
        if i > 0 {
            body.push(',');
        }
        let _ = write!(body, "{id}");
    }
    body.push(']');
    if !images.is_empty() {
        body.push_str(",\"image_data\":[");
        for (i, image) in images.iter().enumerate() {
            if i > 0 {
                body.push(',');
            }
            body.push('"');
            body.push_str(image);
            body.push('"');
        }
        body.push(']');
    }
    let _ = write!(
        body,
        ",\"sampling_params\":{{\"max_new_tokens\":{max_new}}},\"stream\":{stream}}}"
    );
    body
}

/// Drain an SSE response: TTFT is the elapsed time to the first data frame,
/// and the last data frame before `[DONE]` carries the same full JSON shape
/// as a non-streaming response. Frames may span chunks, so bytes are buffered
/// and consumed line by line.
async fn consume_sse(
    resp: reqwest::Response,
    started: Instant,
) -> Result<(Option<f64>, Option<Value>), reqwest::Error> {
    let mut stream = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    let mut ttft_ms = None;
    let mut last = None;
    'read: while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(&chunk?);
        while let Some(newline) = buf.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = buf.drain(..=newline).collect();
            let line = line.strip_suffix(b"\n").unwrap_or(&line);
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            let Some(data) = line
                .strip_prefix(b"data: ")
                .or_else(|| line.strip_prefix(b"data:"))
            else {
                continue;
            };
            if data == b"[DONE]".as_slice() {
                break 'read;
            }
            if ttft_ms.is_none() {
                ttft_ms = Some(started.elapsed().as_secs_f64() * 1000.0);
            }
            if let Ok(value) = serde_json::from_slice::<Value>(data) {
                last = Some(value);
            }
        }
    }
    // Drain any trailing bytes after [DONE] so the connection can be reused.
    while let Some(chunk) = stream.next().await {
        if chunk.is_err() {
            break;
        }
    }
    Ok((ttft_ms, last))
}

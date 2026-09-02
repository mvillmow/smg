//! Mock HTTP worker endpoints — the vLLM/SGLang-compatible surface the SMG
//! gateway probes and routes to.
//!
//! In canned mode every response is fixed (no model). In realistic mode each
//! worker is backed by the engine simulator ([`crate::engine`]); since the HTTP
//! path carries text rather than token ids, the prompt is approximated into
//! synthetic token ids (one per whitespace word) so shared text prefixes still
//! produce cache hits and prompt length still drives prefill latency. Token-id
//! KV events (event-driven `cache_aware`) remain a gRPC-path feature.

use std::{
    convert::Infallible,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use axum::{
    body::Bytes,
    extract::State,
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use futures::{stream, Stream};
use serde_json::{json, Value};
use tokio::{net::TcpListener, sync::mpsc};

use crate::{
    config::Config,
    engine::{self, Engine, NewRequest},
    sim::{self, SimWorker},
};

/// Per-listener HTTP state: shared config plus an optional engine simulator.
pub struct AppState {
    cfg: Arc<Config>,
    engine: Option<Engine>,
    /// Scale-simulation worker (`--engine sim`); `None` in the other modes.
    sim: Option<Arc<SimWorker>>,
}

/// Build the router serving the mock HTTP worker contract.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/models", get(models))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/completions", post(completions))
        .route("/generate", post(generate))
        .route("/v1/loads", get(loads))
        .with_state(state)
}

/// Serve the mock HTTP worker contract on `port` until the process exits.
pub async fn serve(cfg: Arc<Config>, host: String, port: u16) {
    let listener = match TcpListener::bind((host.as_str(), port)).await {
        Ok(listener) => listener,
        Err(e) => {
            tracing::error!("http worker bind {host}:{port} failed: {e}");
            return;
        }
    };
    // One simulated engine per listener (i.e. per virtual worker).
    let engine = cfg.realistic.then(|| Engine::spawn(cfg.engine.clone()));
    let sim = cfg
        .sim
        .then(|| SimWorker::new(cfg.sim_params.clone(), port));
    let state = Arc::new(AppState { cfg, engine, sim });
    if let Err(e) = axum::serve(listener, router(state)).await {
        tracing::error!("http worker {port} stopped: {e}");
    }
}

async fn health() -> &'static str {
    "OK"
}

async fn models(State(state): State<Arc<AppState>>) -> Response {
    Json(json!({
        "object": "list",
        "data": [{
            "id": state.cfg.model_id,
            "object": "model",
            "created": 0,
            "owned_by": "sglang",
            "root": state.cfg.model_id,
            "max_model_len": 32768,
        }],
    }))
    .into_response()
}

async fn loads(State(state): State<Arc<AppState>>) -> Response {
    if let Some(sim) = &state.sim {
        let s = sim.load();
        let value = json!({
            "dp_rank": 0,
            "num_running_reqs": s.num_running_reqs,
            "num_waiting_reqs": s.num_waiting_reqs,
            "num_waiting_uncached_tokens": s.num_waiting_uncached_tokens,
            "num_total_reqs": s.num_running_reqs + s.num_waiting_reqs,
            "num_used_tokens": s.num_used_tokens,
            "max_total_num_tokens": s.max_total_num_tokens,
            "token_usage": s.token_usage,
            "gen_throughput": s.gen_throughput,
            "cache_hit_rate": s.cache_hit_rate,
            "utilization": s.token_usage,
            "max_running_requests": s.max_running_requests,
        });
        return Json(json!({ "timestamp": "", "dp_rank_count": 1, "loads": [value] }))
            .into_response();
    }
    let load = state.engine.as_ref().map(|e| e.load());
    let value = match load {
        Some(s) => json!({
            "dp_rank": 0,
            "num_running_reqs": s.num_running_reqs,
            "num_waiting_reqs": s.num_waiting_reqs,
            "num_waiting_uncached_tokens": s.num_waiting_uncached_tokens,
            "num_total_reqs": s.num_running_reqs + s.num_waiting_reqs,
            "num_used_tokens": s.num_used_tokens,
            "max_total_num_tokens": s.max_total_num_tokens,
            "token_usage": s.token_usage,
            "gen_throughput": s.gen_throughput,
            "cache_hit_rate": s.cache_hit_rate,
            "utilization": s.token_usage,
            "max_running_requests": s.max_running_requests,
        }),
        None => json!({
            "dp_rank": 0,
            "num_running_reqs": 0,
            "num_waiting_reqs": 0,
            "num_waiting_uncached_tokens": 0,
            "num_total_reqs": 0,
            "num_used_tokens": 0,
            "max_total_num_tokens": 1_000_000,
            "token_usage": 0.0,
            "gen_throughput": 0.0,
            "cache_hit_rate": 0.0,
            "utilization": 0.0,
            "max_running_requests": 0,
        }),
    };
    Json(json!({ "timestamp": "", "dp_rank_count": 1, "loads": [value] })).into_response()
}

/// OpenAI response shape this worker replies in. Chat emits
/// `choices[].message` / `choices[].delta.content`; completions emits
/// `choices[].text`. The load generator parses the two differently, so
/// `/v1/completions` must not be answered with chat-shaped frames.
#[derive(Clone, Copy)]
enum Endpoint {
    Chat,
    Completions,
}

async fn chat_completions(State(state): State<Arc<AppState>>, body: Bytes) -> Response {
    handle(Endpoint::Chat, state, body).await
}

async fn completions(State(state): State<Arc<AppState>>, body: Bytes) -> Response {
    handle(Endpoint::Completions, state, body).await
}

/// `/generate`: SGLang-native semantics in sim mode; the historical
/// chat-shaped alias in canned/realistic modes (existing rigs depend on it).
async fn generate(State(state): State<Arc<AppState>>, body: Bytes) -> Response {
    match &state.sim {
        Some(sim) => sim_generate(Arc::clone(sim), &state.cfg, body).await,
        None => handle(Endpoint::Chat, state, body).await,
    }
}

/// Sim-mode `/generate`: parse the SGLang request shape, drop the (large)
/// body immediately, admit into the per-worker simulator, then answer on
/// the analytic timeline — first SSE frame at TTFT, completion at the end
/// of decode. All accounting is released by the admission guard even when
/// the client disconnects mid-stream.
async fn sim_generate(sim: Arc<SimWorker>, cfg: &Config, body: Bytes) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    drop(body);
    let stream_requested = parsed
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let input_ids = sim::extract_input_ids(&parsed)
        .unwrap_or_else(|| synth_token_ids(&extract_prompt_text(&parsed)));
    let images = sim::extract_images(&parsed);
    let max_new = sim::extract_max_new_tokens(&parsed).unwrap_or(cfg.output_tokens);
    let effective = sim.effective_sequence(&input_ids, &images);
    drop(parsed);

    let mut adm = sim.admit(effective, max_new).await;
    if !stream_requested {
        tokio::time::sleep(adm.ttft).await;
        adm.finish_prefill();
        tokio::time::sleep(adm.decode).await;
        return Json(sim::native_response(&adm, true)).into_response();
    }

    enum St {
        Ttft(sim::Admitted),
        Decode(sim::Admitted),
        Done,
        End,
    }
    let body = stream::unfold(St::Ttft(adm), |st| async move {
        match st {
            St::Ttft(mut adm) => {
                tokio::time::sleep(adm.ttft).await;
                adm.finish_prefill();
                let frame = sim::native_response(&adm, false);
                Some((
                    Ok::<_, Infallible>(Event::default().data(frame.to_string())),
                    St::Decode(adm),
                ))
            }
            St::Decode(adm) => {
                tokio::time::sleep(adm.decode).await;
                let frame = sim::native_response(&adm, true);
                // `adm` drops here: admission slot + KV pin released, full
                // sequence folded into the prefix cache.
                Some((Ok(Event::default().data(frame.to_string())), St::Done))
            }
            St::Done => Some((Ok(Event::default().data("[DONE]")), St::End)),
            St::End => None,
        }
    });
    Sse::new(body).into_response()
}

async fn handle(endpoint: Endpoint, state: Arc<AppState>, body: Bytes) -> Response {
    let parsed: Option<Value> = serde_json::from_slice(&body).ok();
    let stream_requested = parsed
        .as_ref()
        .and_then(|v| v.get("stream").and_then(Value::as_bool))
        .unwrap_or(false);

    // Realistic mode: drive the engine simulator.
    if let Some(engine) = &state.engine {
        let parsed = parsed.unwrap_or(Value::Null);
        let prompt_ids = synth_token_ids(&extract_prompt_text(&parsed));
        let prompt_tokens = prompt_ids.len() as u32;
        let max_new = extract_max_tokens(&parsed).unwrap_or(state.cfg.output_tokens);
        let request_id = next_request_id();
        let (tx, rx) = mpsc::unbounded_channel();
        engine.submit(NewRequest {
            request_id,
            prompt_token_ids: prompt_ids,
            max_new,
            events: tx,
        });
        let model = state.cfg.model_id.clone();
        return if stream_requested {
            realistic_sse(rx, model, endpoint).into_response()
        } else {
            realistic_completion(rx, model, prompt_tokens, endpoint)
                .await
                .into_response()
        };
    }

    // Canned mode: a single up-front delay, then a fixed response. Always
    // chat-shaped (unchanged) so the existing scale rig is unaffected.
    if !state.cfg.gen_delay.is_zero() {
        tokio::time::sleep(state.cfg.gen_delay).await;
    }
    if stream_requested {
        stream_chat(&state.cfg).into_response()
    } else {
        Json(completion(&state.cfg)).into_response()
    }
}

// ── Canned responses ───────────────────────────────────────────────────────

fn completion(cfg: &Config) -> Value {
    json!({
        "id": "chatcmpl-mock",
        "object": "chat.completion",
        "created": 0,
        "model": cfg.model_id,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "mock"},
            "finish_reason": "stop",
        }],
        "usage": {
            "prompt_tokens": 1,
            "completion_tokens": cfg.output_tokens,
            "total_tokens": u64::from(cfg.output_tokens) + 1,
        },
    })
}

fn stream_chat(cfg: &Config) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut events: Vec<Result<Event, Infallible>> = Vec::new();
    for _ in 0..cfg.output_tokens {
        let frame = json!({
            "id": "chatcmpl-mock",
            "object": "chat.completion.chunk",
            "choices": [{"index": 0, "delta": {"content": "x"}, "finish_reason": null}],
        });
        events.push(Ok(Event::default().data(frame.to_string())));
    }
    let final_frame = json!({
        "id": "chatcmpl-mock",
        "object": "chat.completion.chunk",
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
    });
    events.push(Ok(Event::default().data(final_frame.to_string())));
    events.push(Ok(Event::default().data("[DONE]")));
    Sse::new(stream::iter(events))
}

// ── Realistic responses ─────────────────────────────────────────────────────

/// Non-streaming: drain the engine's events and assemble one completion JSON.
async fn realistic_completion(
    mut rx: mpsc::UnboundedReceiver<engine::GenEvent>,
    model: String,
    prompt_tokens: u32,
    endpoint: Endpoint,
) -> Json<Value> {
    let mut completion_tokens = 0u32;
    let mut cached_tokens = 0u32;
    while let Some(ev) = rx.recv().await {
        match ev {
            engine::GenEvent::Token { .. } => completion_tokens += 1,
            engine::GenEvent::Done {
                completion_tokens: c,
                cached_tokens: cached,
                ..
            } => {
                completion_tokens = c;
                cached_tokens = cached;
            }
        }
    }
    let usage = json!({
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": prompt_tokens + completion_tokens,
        "cached_tokens": cached_tokens,
    });
    Json(match endpoint {
        Endpoint::Chat => json!({
            "id": "chatcmpl-mock",
            "object": "chat.completion",
            "created": 0,
            "model": model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "mock"},
                "finish_reason": "stop",
            }],
            "usage": usage,
        }),
        Endpoint::Completions => json!({
            "id": "cmpl-mock",
            "object": "text_completion",
            "created": 0,
            "model": model,
            "choices": [{
                "index": 0,
                "text": "mock",
                "finish_reason": "stop",
            }],
            "usage": usage,
        }),
    })
}

/// Streaming: map the engine's events to SSE chunks, ending with a finish frame
/// and `[DONE]`. Frame shape follows `endpoint` (chat delta vs completion text).
fn realistic_sse(
    rx: mpsc::UnboundedReceiver<engine::GenEvent>,
    model: String,
    endpoint: Endpoint,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    enum St {
        Active {
            rx: mpsc::UnboundedReceiver<engine::GenEvent>,
            model: String,
            endpoint: Endpoint,
        },
        Closing,
        Ended,
    }

    let body = stream::unfold(
        St::Active {
            rx,
            model,
            endpoint,
        },
        |st| async move {
            match st {
                St::Active {
                    mut rx,
                    model,
                    endpoint,
                } => match rx.recv().await {
                    Some(engine::GenEvent::Token { .. }) => {
                        let frame = token_chunk(endpoint, &model);
                        Some((
                            Ok(Event::default().data(frame.to_string())),
                            St::Active {
                                rx,
                                model,
                                endpoint,
                            },
                        ))
                    }
                    Some(engine::GenEvent::Done { .. }) => {
                        let frame = final_chunk(endpoint, &model);
                        Some((Ok(Event::default().data(frame.to_string())), St::Closing))
                    }
                    None => None,
                },
                St::Closing => Some((Ok(Event::default().data("[DONE]")), St::Ended)),
                St::Ended => None,
            }
        },
    );
    Sse::new(body)
}

/// One streamed token frame in the shape the requesting endpoint expects.
fn token_chunk(endpoint: Endpoint, model: &str) -> Value {
    match endpoint {
        Endpoint::Chat => json!({
            "id": "chatcmpl-mock",
            "object": "chat.completion.chunk",
            "model": model,
            "choices": [{"index": 0, "delta": {"content": "x"}, "finish_reason": null}],
        }),
        Endpoint::Completions => json!({
            "id": "cmpl-mock",
            "object": "text_completion",
            "model": model,
            "choices": [{"index": 0, "text": "x", "finish_reason": null}],
        }),
    }
}

/// The terminal frame (`finish_reason = stop`) for the requesting endpoint.
fn final_chunk(endpoint: Endpoint, model: &str) -> Value {
    match endpoint {
        Endpoint::Chat => json!({
            "id": "chatcmpl-mock",
            "object": "chat.completion.chunk",
            "model": model,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        }),
        Endpoint::Completions => json!({
            "id": "cmpl-mock",
            "object": "text_completion",
            "model": model,
            "choices": [{"index": 0, "text": "", "finish_reason": "stop"}],
        }),
    }
}

// ── HTTP prompt helpers ──────────────────────────────────────────────────────

/// Extract prompt text from a chat/completions/generate body.
fn extract_prompt_text(v: &Value) -> String {
    if let Some(messages) = v.get("messages").and_then(Value::as_array) {
        return messages
            .iter()
            .filter_map(|m| m.get("content").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(" ");
    }
    for key in ["prompt", "text", "inputs"] {
        if let Some(s) = v.get(key).and_then(Value::as_str) {
            return s.to_string();
        }
    }
    String::new()
}

/// Approximate a prompt's token ids from its text: one id per whitespace word,
/// each a stable hash of the word. Identical leading words yield identical
/// leading ids, so shared prefixes still produce cache hits.
fn synth_token_ids(text: &str) -> Vec<u32> {
    text.split_whitespace().map(hash_word).collect()
}

fn hash_word(w: &str) -> u32 {
    let mut h: u32 = 2_166_136_261;
    for b in w.bytes() {
        h ^= u32::from(b);
        h = h.wrapping_mul(16_777_619);
    }
    h % 30_000
}

fn extract_max_tokens(v: &Value) -> Option<u32> {
    for key in ["max_tokens", "max_new_tokens"] {
        if let Some(n) = v.get(key).and_then(Value::as_u64) {
            return Some(n as u32);
        }
    }
    None
}

fn next_request_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!("mock-http-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_prompts_derive_stable_prefix_preserving_ids() {
        // The loadgen's text payload is the token context space-joined as
        // decimal words; extending the context must extend the derived ids
        // so prefix-cache accounting matches the ids path semantically.
        let turn1 = synth_token_ids("12 3");
        let turn2 = synth_token_ids("12 3 45");
        assert_eq!(turn1.len(), 2);
        assert_eq!(turn2.len(), 3);
        assert_eq!(turn2[..2], turn1[..], "shared text head, shared id head");
        // Distinct words diverge (word identity, not position).
        assert_ne!(synth_token_ids("12 4")[1], turn1[1]);

        // The `text` field is one of the accepted prompt carriers.
        let v: Value = serde_json::from_str(r#"{"text":"12 3 45"}"#).unwrap();
        assert_eq!(synth_token_ids(&extract_prompt_text(&v)), turn2);
    }
}

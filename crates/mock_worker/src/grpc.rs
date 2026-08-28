//! Mock gRPC worker implementing the TokenSpeed scheduler service. The gateway
//! tokenizes and sends token ids; this service streams back canned token ids.

use std::{
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::Arc,
};

use futures::{stream, Stream, StreamExt as _};
use smg_grpc_client::{common_proto as common, tokenspeed_scheduler::tokenspeed_proto as ts};
use tokio::sync::mpsc;
use tonic::{transport::Server, Request, Response, Status};
use ts::{
    generate_response::Response as GenResp,
    token_speed_scheduler_server::{TokenSpeedScheduler, TokenSpeedSchedulerServer},
};

use crate::{
    config::Config,
    engine::{self, Engine, NewRequest},
    sim::{SimKvEvent, SimWorker},
};

/// Serve the mock TokenSpeed gRPC service on `port` until the process exits.
pub async fn serve(cfg: Arc<Config>, host: String, port: u16) {
    let ip = match host.parse::<IpAddr>() {
        Ok(ip) => ip,
        Err(e) => {
            tracing::error!("grpc worker host {host} invalid: {e}");
            return;
        }
    };
    let addr = SocketAddr::new(ip, port);
    // One simulated engine per listener (i.e. per virtual worker).
    let engine = cfg.realistic.then(|| Engine::spawn(cfg.engine.clone()));
    // Sim engine with KV-event emission on: gRPC subscribers exist.
    let sim = cfg
        .sim
        .then(|| SimWorker::new_with_events(cfg.sim_params.clone(), port));
    let service = MockScheduler { cfg, engine, sim };
    if let Err(e) = Server::builder()
        .add_service(TokenSpeedSchedulerServer::new(service))
        .serve(addr)
        .await
    {
        tracing::error!("grpc worker {port} stopped: {e}");
    }
}

#[derive(Clone)]
struct MockScheduler {
    cfg: Arc<Config>,
    /// Present iff the worker runs the realistic engine simulator.
    engine: Option<Engine>,
    /// Present iff the worker runs the scale-sim engine (`--engine sim`).
    sim: Option<Arc<SimWorker>>,
}

type GenStream = Pin<Box<dyn Stream<Item = Result<ts::GenerateResponse, Status>> + Send>>;
type KvEventStream = Pin<Box<dyn Stream<Item = Result<common::KvEventBatch, Status>> + Send>>;
type TokenizerStream =
    Pin<Box<dyn Stream<Item = Result<common::GetTokenizerChunk, Status>> + Send>>;

#[tonic::async_trait]
impl TokenSpeedScheduler for MockScheduler {
    type GenerateStream = GenStream;
    type SubscribeKvEventsStream = KvEventStream;
    type GetTokenizerStream = TokenizerStream;

    async fn generate(
        &self,
        request: Request<ts::GenerateRequest>,
    ) -> Result<Response<Self::GenerateStream>, Status> {
        // Sim mode: analytic timeline — one sleep to first token, one to
        // completion — mirroring the HTTP sim handler. The gateway
        // tokenizes upstream, so the request carries token ids and never
        // image payloads; the effective sequence is the ids as sent. The
        // timeline is driven by the response stream itself, so a dropped
        // stream (gateway abort) cancels mid-sleep and the admission guard
        // releases immediately.
        if let Some(sim) = &self.sim {
            let sim = Arc::clone(sim);
            let req = request.into_inner();
            let request_id = req.request_id;
            let input_ids = req.tokenized.map(|t| t.input_ids).unwrap_or_default();
            let max_new = req
                .sampling_params
                .and_then(|s| s.max_new_tokens)
                .unwrap_or(self.cfg.output_tokens);
            let init = SimGen::Admit {
                sim,
                request_id,
                input_ids,
                max_new,
            };
            let stream = stream::unfold(init, |state| async move {
                match state {
                    SimGen::Admit {
                        sim,
                        request_id,
                        input_ids,
                        max_new,
                    } => {
                        let mut adm = sim.admit(input_ids, max_new).await;
                        tokio::time::sleep(adm.ttft).await;
                        adm.finish_prefill();
                        let first = ts::GenerateResponse {
                            request_id: request_id.clone(),
                            response: Some(GenResp::Chunk(ts::GenerateStreamChunk {
                                token_ids: adm.output_ids.first().copied().into_iter().collect(),
                                prompt_tokens: adm.prompt_tokens as u32,
                                completion_tokens: 1,
                                cached_tokens: adm.cached_tokens as u32,
                                output_logprobs: None,
                                index: 0,
                            })),
                        };
                        let next = SimGen::Decode {
                            request_id,
                            max_new,
                            adm: Box::new(adm),
                        };
                        Some((Ok(first), next))
                    }
                    SimGen::Decode {
                        request_id,
                        max_new,
                        mut adm,
                    } => {
                        tokio::time::sleep(adm.decode).await;
                        let done = ts::GenerateResponse {
                            request_id,
                            response: Some(GenResp::Complete(ts::GenerateComplete {
                                output_ids: std::mem::take(&mut adm.output_ids),
                                finish_reason: "stop".to_string(),
                                prompt_tokens: adm.prompt_tokens as u32,
                                completion_tokens: max_new,
                                cached_tokens: adm.cached_tokens as u32,
                                output_logprobs: None,
                                matched_stop: None,
                                index: 0,
                            })),
                        };
                        Some((Ok(done), SimGen::Done))
                    }
                    SimGen::Done => None,
                }
            });
            return Ok(Response::new(Box::pin(stream)));
        }

        // Realistic mode: submit to the engine simulator and stream its output.
        if let Some(engine) = &self.engine {
            let req = request.into_inner();
            let request_id = req.request_id;
            let prompt_token_ids = req.tokenized.map(|t| t.input_ids).unwrap_or_default();
            // Omitted limit falls back to the worker default, matching the HTTP
            // path; `unwrap_or(0)` here would make unbounded requests generate
            // nothing (zero tokens), starving the routing signals.
            let max_new = req
                .sampling_params
                .and_then(|s| s.max_new_tokens)
                .unwrap_or(self.cfg.output_tokens);
            let stream_chunks = req.stream;
            let (tx, rx) = mpsc::unbounded_channel();
            engine.submit(NewRequest {
                request_id: request_id.clone(),
                prompt_token_ids,
                max_new,
                events: tx,
            });
            return Ok(Response::new(generate_stream(
                rx,
                stream_chunks,
                request_id,
            )));
        }

        // Canned mode: a single up-front delay, then synthetic token ids.
        let request_id = request.into_inner().request_id;
        if !self.cfg.gen_delay.is_zero() {
            tokio::time::sleep(self.cfg.gen_delay).await;
        }
        let ids: Vec<u32> = (0..self.cfg.output_tokens).map(|i| 100 + i).collect();

        let mut items: Vec<Result<ts::GenerateResponse, Status>> = Vec::new();
        for id in &ids {
            items.push(Ok(ts::GenerateResponse {
                request_id: request_id.clone(),
                response: Some(GenResp::Chunk(ts::GenerateStreamChunk {
                    token_ids: vec![*id],
                    prompt_tokens: 1,
                    completion_tokens: 1,
                    cached_tokens: 0,
                    output_logprobs: None,
                    index: 0,
                })),
            }));
        }
        items.push(Ok(ts::GenerateResponse {
            request_id,
            response: Some(GenResp::Complete(ts::GenerateComplete {
                output_ids: ids,
                finish_reason: "stop".to_string(),
                prompt_tokens: 1,
                completion_tokens: self.cfg.output_tokens,
                cached_tokens: 0,
                output_logprobs: None,
                matched_stop: None,
                index: 0,
            })),
        }));

        Ok(Response::new(Box::pin(stream::iter(items))))
    }

    async fn health_check(
        &self,
        _request: Request<ts::HealthCheckRequest>,
    ) -> Result<Response<ts::HealthCheckResponse>, Status> {
        Ok(Response::new(ts::HealthCheckResponse {
            healthy: true,
            message: "ok".to_string(),
        }))
    }

    async fn abort(
        &self,
        _request: Request<ts::AbortRequest>,
    ) -> Result<Response<ts::AbortResponse>, Status> {
        Ok(Response::new(ts::AbortResponse {
            success: true,
            message: String::new(),
        }))
    }

    async fn get_model_info(
        &self,
        _request: Request<ts::GetModelInfoRequest>,
    ) -> Result<Response<ts::GetModelInfoResponse>, Status> {
        Ok(Response::new(ts::GetModelInfoResponse {
            model_path: self.cfg.model_id.clone(),
            tokenizer_path: self.cfg.tokenizer_path.clone(),
            served_model_name: self.cfg.model_id.clone(),
            model_type: "mock".to_string(),
            architectures: vec!["MockForCausalLM".to_string()],
            max_context_length: 32768,
            max_req_input_len: 32768,
            vocab_size: 32000,
            eos_token_ids: vec![2],
            pad_token_id: 0,
            bos_token_id: 1,
            weight_version: "mock".to_string(),
            default_sampling_params_json: String::new(),
            supports_vision: false,
            ..Default::default()
        }))
    }

    async fn get_server_info(
        &self,
        _request: Request<ts::GetServerInfoRequest>,
    ) -> Result<Response<ts::GetServerInfoResponse>, Status> {
        Ok(Response::new(ts::GetServerInfoResponse {
            server_args: None,
            scheduler_info: None,
            active_requests: 0,
            is_paused: false,
            uptime_seconds: 0.0,
            max_total_num_tokens: 1_000_000,
            tokenspeed_version: "mock".to_string(),
            start_time: None,
        }))
    }

    async fn get_loads(
        &self,
        _request: Request<ts::GetLoadsRequest>,
    ) -> Result<Response<ts::GetLoadsResponse>, Status> {
        let load = match (&self.sim, &self.engine) {
            (Some(sim), _) => sim_to_scheduler_load(&sim.load()),
            (None, Some(engine)) => snapshot_to_scheduler_load(&engine.load()),
            (None, None) => ts::SchedulerLoad {
                dp_rank: 0,
                num_running_reqs: 0,
                num_waiting_reqs: 0,
                num_waiting_uncached_tokens: 0,
                num_total_reqs: 0,
                num_used_tokens: 0,
                max_total_num_tokens: 1_000_000,
                max_running_requests: 0,
                token_usage: 0.0,
                gen_throughput: 0.0,
                cache_hit_rate: 0.0,
                utilization: 0.0,
                memory: None,
                queues: None,
            },
        };
        Ok(Response::new(ts::GetLoadsResponse {
            timestamp: String::new(),
            version: "mock".to_string(),
            dp_rank_count: 1,
            loads: vec![load],
            aggregate: None,
        }))
    }

    async fn subscribe_kv_events(
        &self,
        request: Request<common::SubscribeKvEventsRequest>,
    ) -> Result<Response<Self::SubscribeKvEventsStream>, Status> {
        // Sim mode: replay the ring past the cursor, then the live stream.
        if let Some(sim) = &self.sim {
            let start = request.into_inner().start_sequence_number;
            let Some((replay, live)) = sim.subscribe_kv_events(start) else {
                return Err(Status::unimplemented("mock-worker (sim KV events off)"));
            };
            let block_size = sim.block_size() as i32;
            let replayed = stream::iter(
                replay
                    .into_iter()
                    .map(move |b| Ok::<_, Status>(sim_batch_to_proto(&b, block_size))),
            );
            // A lagged receiver skips ahead; the gateway sees the sequence
            // gap and reconnects with its cursor, replaying from the ring.
            let live = stream::unfold(live, move |mut rx| async move {
                loop {
                    match rx.recv().await {
                        Ok(batch) => return Some((Ok(sim_batch_to_proto(&batch, block_size)), rx)),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                    }
                }
            });
            return Ok(Response::new(Box::pin(replayed.chain(live))));
        }

        match &self.engine {
            // Realistic mode with prefix caching: stream the engine's KV events.
            Some(engine) if engine.kv_enabled() => {
                let start = request.into_inner().start_sequence_number;
                Ok(Response::new(engine.subscribe_kv(start)))
            }
            // Otherwise Unimplemented makes the gateway's KvEventMonitor give up
            // cleanly (no idle per-worker task), exactly as before this RPC existed.
            _ => Err(Status::unimplemented(
                "mock-worker (KV events require --engine realistic with --prefix-cache true)",
            )),
        }
    }

    async fn flush_cache(
        &self,
        _request: Request<common::FlushCacheRequest>,
    ) -> Result<Response<common::FlushCacheResponse>, Status> {
        Err(Status::unimplemented("mock-worker"))
    }

    async fn start_profile(
        &self,
        _request: Request<common::StartProfileRequest>,
    ) -> Result<Response<common::ProfileResponse>, Status> {
        Err(Status::unimplemented("mock-worker"))
    }

    async fn stop_profile(
        &self,
        _request: Request<common::StopProfileRequest>,
    ) -> Result<Response<common::ProfileResponse>, Status> {
        Err(Status::unimplemented("mock-worker"))
    }

    async fn get_tokenizer(
        &self,
        _request: Request<common::GetTokenizerRequest>,
    ) -> Result<Response<Self::GetTokenizerStream>, Status> {
        // The mock has no tokenizer artifacts to serve; the gateway's
        // remote-tokenizer fallback treats Unimplemented as "not supported".
        Err(Status::unimplemented("mock-worker"))
    }
}

/// Map the engine's [`engine::GenEvent`] channel to the gRPC generate stream.
/// In streaming mode each token becomes a `Chunk`; otherwise tokens are
/// accumulated and only the final `Complete` is sent. After `Complete` the
/// engine has dropped the sender, so the next `recv()` yields `None` and the
/// stream ends.
fn generate_stream(
    rx: mpsc::UnboundedReceiver<engine::GenEvent>,
    stream_chunks: bool,
    request_id: String,
) -> GenStream {
    let init = (rx, Vec::<u32>::new(), stream_chunks, request_id);
    Box::pin(stream::unfold(
        init,
        |(mut rx, mut output_ids, stream_chunks, request_id)| async move {
            loop {
                match rx.recv().await {
                    Some(engine::GenEvent::Token {
                        token_id,
                        prompt_tokens,
                        cached_tokens,
                    }) => {
                        output_ids.push(token_id);
                        if stream_chunks {
                            let resp = ts::GenerateResponse {
                                request_id: request_id.clone(),
                                response: Some(GenResp::Chunk(ts::GenerateStreamChunk {
                                    token_ids: vec![token_id],
                                    prompt_tokens,
                                    completion_tokens: output_ids.len() as u32,
                                    cached_tokens,
                                    output_logprobs: None,
                                    index: 0,
                                })),
                            };
                            return Some((Ok(resp), (rx, output_ids, stream_chunks, request_id)));
                        }
                        // Non-streaming: keep accumulating until Done.
                    }
                    Some(engine::GenEvent::Done {
                        finish_reason,
                        prompt_tokens,
                        completion_tokens,
                        cached_tokens,
                    }) => {
                        let resp = ts::GenerateResponse {
                            request_id: request_id.clone(),
                            response: Some(GenResp::Complete(ts::GenerateComplete {
                                output_ids: std::mem::take(&mut output_ids),
                                finish_reason: finish_reason.to_string(),
                                prompt_tokens,
                                completion_tokens,
                                cached_tokens,
                                output_logprobs: None,
                                matched_stop: None,
                                index: 0,
                            })),
                        };
                        return Some((Ok(resp), (rx, output_ids, stream_chunks, request_id)));
                    }
                    None => return None,
                }
            }
        },
    ))
}

/// Sim generate timeline as stream states: admission + prefill sleep yields
/// the first chunk; the decode sleep yields the completion. Dropping the
/// stream mid-state drops the admission guard immediately.
enum SimGen {
    Admit {
        sim: Arc<SimWorker>,
        request_id: String,
        input_ids: Vec<u32>,
        max_new: u32,
    },
    Decode {
        request_id: String,
        max_new: u32,
        adm: Box<crate::sim::Admitted>,
    },
    Done,
}

/// Map one sim KV batch to the wire type. The sim's chained block hash is
/// the wire `block_hash` (parent/removal key); token ids are what the
/// gateway hashes for matching.
fn sim_batch_to_proto(batch: &crate::sim::SimKvBatch, block_size: i32) -> common::KvEventBatch {
    let events = batch
        .events
        .iter()
        .map(|event| {
            let data = match event {
                SimKvEvent::Stored { parent, blocks } => {
                    common::kv_cache_event::Data::Stored(common::KvBlocksStored {
                        blocks: blocks
                            .iter()
                            .map(|b| common::KvBlock {
                                block_hash: b.hash as i64,
                                token_ids: b.token_ids.clone(),
                                block_size,
                                lora_id: None,
                                cache_level: None,
                            })
                            .collect(),
                        parent_block_hash: parent.map(|h| h as i64),
                    })
                }
                SimKvEvent::Removed { hashes } => {
                    common::kv_cache_event::Data::Removed(common::KvBlocksRemoved {
                        block_hashes: hashes.iter().map(|&h| h as i64).collect(),
                        cache_level: None,
                    })
                }
            };
            common::KvCacheEvent {
                event_id: batch.seq,
                data: Some(data),
            }
        })
        .collect();
    common::KvEventBatch {
        sequence_number: batch.seq,
        timestamp: 0.0,
        events,
        dp_rank: None,
    }
}

/// Map a sim load view to the TokenSpeed `SchedulerLoad` wire type.
fn sim_to_scheduler_load(s: &crate::sim::SimLoad) -> ts::SchedulerLoad {
    ts::SchedulerLoad {
        dp_rank: 0,
        num_running_reqs: s.num_running_reqs as i32,
        num_waiting_reqs: s.num_waiting_reqs as i32,
        num_waiting_uncached_tokens: s.num_waiting_uncached_tokens as i32,
        num_total_reqs: (s.num_running_reqs + s.num_waiting_reqs) as i32,
        num_used_tokens: s.num_used_tokens as i32,
        max_total_num_tokens: s.max_total_num_tokens as i32,
        max_running_requests: s.max_running_requests as i32,
        token_usage: s.token_usage,
        gen_throughput: s.gen_throughput,
        cache_hit_rate: s.cache_hit_rate,
        utilization: s.token_usage,
        memory: None,
        queues: None,
    }
}

/// Map an engine load snapshot to the TokenSpeed `SchedulerLoad` wire type.
fn snapshot_to_scheduler_load(s: &engine::LoadSnapshot) -> ts::SchedulerLoad {
    ts::SchedulerLoad {
        dp_rank: 0,
        num_running_reqs: s.num_running_reqs,
        num_waiting_reqs: s.num_waiting_reqs,
        num_waiting_uncached_tokens: s.num_waiting_uncached_tokens,
        num_total_reqs: s.num_running_reqs + s.num_waiting_reqs,
        num_used_tokens: s.num_used_tokens,
        max_total_num_tokens: s.max_total_num_tokens,
        max_running_requests: s.max_running_requests,
        token_usage: s.token_usage,
        gen_throughput: s.gen_throughput,
        cache_hit_rate: s.cache_hit_rate,
        utilization: s.token_usage,
        memory: None,
        queues: None,
    }
}

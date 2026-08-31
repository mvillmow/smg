//! Engine-neutral Router-to-Worker inference client.
//!
//! The wire contract is deliberately independent of the engine behind the
//! Worker. Conversion helpers bridge the first text-generation implementation
//! to the router's existing TokenSpeed-shaped internal request/response model;
//! that shape does not escape onto the WorkerInference wire.

use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use futures::{Stream, StreamExt};
use tonic::{transport::Channel, Request, Response, Status};
use tracing::{debug, warn};

use crate::{
    sglang_runtime as sglang, tokenspeed_proto as ts, AbortOnDropClient, BoxedTraceInjector,
    NoopTraceInjector,
};

#[expect(clippy::allow_attributes)]
pub mod proto {
    #![allow(
        clippy::all,
        clippy::absolute_paths,
        clippy::trivially_copy_pass_by_ref,
        unused_qualifications
    )]
    tonic::include_proto!("smg.worker.v1");
}

pub type AbortOnDropStream =
    crate::AbortOnDropStream<proto::GenerateResponse, WorkerInferenceClient>;

/// Client for the stable Worker SMG data plane.
#[derive(Clone)]
pub struct WorkerInferenceClient {
    client: proto::worker_inference_client::WorkerInferenceClient<Channel>,
    trace_injector: BoxedTraceInjector,
}

impl AbortOnDropClient for WorkerInferenceClient {
    fn abort_for_drop(
        self,
        request_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Status>> + Send>> {
        Box::pin(async move {
            self.abort_request(request_id, "Stream dropped".to_string())
                .await
        })
    }
}

impl WorkerInferenceClient {
    pub async fn connect(endpoint: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::connect_with_trace_injector(endpoint, Arc::new(NoopTraceInjector)).await
    }

    pub async fn connect_with_trace_injector(
        endpoint: &str,
        trace_injector: BoxedTraceInjector,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        debug!(endpoint, "Connecting to WorkerInference");
        let channel = crate::channel::connect_channel(endpoint).await?;
        Ok(Self {
            client: proto::worker_inference_client::WorkerInferenceClient::new(channel),
            trace_injector,
        })
    }

    pub async fn generate(
        &self,
        request: proto::GenerateRequest,
    ) -> Result<AbortOnDropStream, Status> {
        let request_id = request.request_id.clone();
        let mut request = Request::new(request);
        if let Err(error) = self.trace_injector.inject(request.metadata_mut()) {
            warn!(%error, "Failed to inject WorkerInference trace context");
        }
        let response = self.client.clone().generate(request).await?;
        Ok(AbortOnDropStream::new(
            response.into_inner(),
            request_id,
            self.clone(),
        ))
    }

    pub async fn abort_request(&self, request_id: String, reason: String) -> Result<(), Status> {
        let mut request = Request::new(proto::AbortRequest { request_id, reason });
        if let Err(error) = self.trace_injector.inject(request.metadata_mut()) {
            warn!(%error, "Failed to inject WorkerInference trace context");
        }
        let response = self.client.clone().abort(request).await?.into_inner();
        if response.success {
            Ok(())
        } else {
            Err(Status::failed_precondition(response.message))
        }
    }
}

/// Worker-side adapter for SGLang's native Rust gRPC service.
///
/// The Router only sees [`proto::WorkerInference`]. This adapter owns the
/// engine-specific translation and can later be embedded directly in the
/// engine coordinator through the Python binding.
#[derive(Clone)]
pub struct SglangWorkerInference {
    client: sglang::sglang_service_client::SglangServiceClient<Channel>,
}

impl SglangWorkerInference {
    pub async fn connect(endpoint: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let channel = crate::channel::connect_channel(endpoint).await?;
        Ok(Self {
            client: sglang::sglang_service_client::SglangServiceClient::new(channel),
        })
    }
}

type SglangAdapterStream =
    Pin<Box<dyn Stream<Item = Result<proto::GenerateResponse, Status>> + Send>>;

#[tonic::async_trait]
impl proto::worker_inference_server::WorkerInference for SglangWorkerInference {
    type GenerateStream = SglangAdapterStream;

    async fn generate(
        &self,
        request: Request<proto::GenerateRequest>,
    ) -> Result<Response<Self::GenerateStream>, Status> {
        let request = request.into_inner();
        let request_id = request.request_id.clone();
        let request = into_sglang_request(request)?;
        let stream = self
            .client
            .clone()
            .generate(Request::new(request))
            .await?
            .into_inner()
            .scan(HashMap::new(), move |emitted_by_index, item| {
                let result = item.and_then(|response| {
                    from_sglang_response(&request_id, response, emitted_by_index)
                });
                futures::future::ready(Some(result))
            });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn abort(
        &self,
        request: Request<proto::AbortRequest>,
    ) -> Result<Response<proto::AbortResponse>, Status> {
        let request = request.into_inner();
        let response = self
            .client
            .clone()
            .abort(Request::new(sglang::AbortRequest {
                rid: request.request_id,
                abort_all: false,
            }))
            .await?
            .into_inner();
        Ok(Response::new(proto::AbortResponse {
            success: response.success,
            message: if response.success {
                String::new()
            } else {
                "SGLang rejected the abort request".to_string()
            },
        }))
    }
}

fn into_sglang_request(request: proto::GenerateRequest) -> Result<sglang::GenerateRequest, Status> {
    if request.return_logprob
        || request.top_logprobs_num != 0
        || !request.token_ids_logprob.is_empty()
    {
        return Err(Status::unimplemented(
            "SGLang native gRPC does not expose token logprobs on GenerateResponse",
        ));
    }
    let input_ids = request
        .tokenized
        .map(|input| {
            input
                .input_ids
                .into_iter()
                .map(|id| i32::try_from(id).map_err(|_| numeric_range_error("input token id")))
                .collect()
        })
        .transpose()?
        .unwrap_or_default();
    let sampling_params = request
        .sampling_params
        .map(into_sglang_sampling)
        .transpose()?;
    Ok(sglang::GenerateRequest {
        input_ids,
        sampling_params,
        stream: Some(request.stream),
        return_logprob: Some(false),
        top_logprobs_num: None,
        logprob_start_len: request.logprob_start_len,
        rid: Some(request.request_id),
        routed_dp_rank: request.data_parallel_rank,
        priority: None,
        require_reasoning: None,
        max_thinking_tokens: None,
    })
}

fn into_sglang_sampling(params: proto::SamplingParams) -> Result<sglang::SamplingParams, Status> {
    if !params.logit_bias.is_empty() || params.engine_parameters.is_some() {
        return Err(Status::invalid_argument(
            "SGLang native gRPC does not support WorkerInference engine parameters or logit bias",
        ));
    }
    Ok(sglang::SamplingParams {
        temperature: params.temperature,
        top_p: params.top_p,
        top_k: params.top_k,
        min_p: params.min_p,
        frequency_penalty: params.frequency_penalty,
        presence_penalty: params.presence_penalty,
        repetition_penalty: params.repetition_penalty,
        max_new_tokens: params
            .max_new_tokens
            .map(|value| i32::try_from(value).map_err(|_| numeric_range_error("max_new_tokens")))
            .transpose()?,
        min_new_tokens: Some(
            i32::try_from(params.min_new_tokens)
                .map_err(|_| numeric_range_error("min_new_tokens"))?,
        ),
        stop: params.stop,
        stop_token_ids: params
            .stop_token_ids
            .into_iter()
            .map(|id| i32::try_from(id).map_err(|_| numeric_range_error("stop token id")))
            .collect::<Result<Vec<_>, _>>()?,
        ignore_eos: Some(params.ignore_eos),
        n: (params.n != 0)
            .then(|| i32::try_from(params.n).map_err(|_| numeric_range_error("n")))
            .transpose()?,
        seed: params
            .sampling_seed
            .map(|value| i64::try_from(value).map_err(|_| numeric_range_error("sampling_seed")))
            .transpose()?,
        guided_decoding: params.constraint.map(|constraint| sglang::GuidedDecoding {
            constraint: Some(match constraint {
                proto::sampling_params::Constraint::Regex(value) => {
                    sglang::guided_decoding::Constraint::Regex(value)
                }
                proto::sampling_params::Constraint::JsonSchema(value) => {
                    sglang::guided_decoding::Constraint::JsonSchema(value)
                }
                proto::sampling_params::Constraint::EbnfGrammar(value) => {
                    sglang::guided_decoding::Constraint::Ebnf(value)
                }
                proto::sampling_params::Constraint::StructuralTag(value) => {
                    sglang::guided_decoding::Constraint::StructuralTag(value)
                }
            }),
        }),
    })
}

fn numeric_range_error(field: &str) -> Status {
    Status::invalid_argument(format!("{field} is outside SGLang's signed 32-bit range"))
}

fn from_sglang_response(
    request_id: &str,
    response: sglang::GenerateResponse,
    emitted_by_index: &mut HashMap<u32, usize>,
) -> Result<proto::GenerateResponse, Status> {
    let mut output_ids = response
        .output_ids
        .into_iter()
        .map(|id| {
            u32::try_from(id).map_err(|_| Status::internal("SGLang returned a negative token id"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let prompt_tokens = meta_u32(&response.meta_info, "prompt_tokens");
    let completion_tokens = meta_u32(&response.meta_info, "completion_tokens");
    let cached_tokens = meta_u32(&response.meta_info, "cached_tokens");
    let index = meta_u32(&response.meta_info, "index");

    if !response.finished {
        let emitted = emitted_by_index.entry(index).or_default();
        if output_ids.len() < *emitted {
            return Err(Status::internal(
                "SGLang returned a shorter cumulative token sequence",
            ));
        }
        output_ids.drain(..*emitted);
        *emitted += output_ids.len();
    }

    let response = if response.finished {
        let (finish_reason, matched_stop) = finish_metadata(&response.meta_info);
        proto::generate_response::Response::Complete(proto::GenerateComplete {
            output_ids,
            finish_reason,
            prompt_tokens,
            completion_tokens,
            cached_tokens,
            output_logprobs: None,
            matched_stop,
            index,
        })
    } else {
        proto::generate_response::Response::Chunk(proto::GenerateStreamChunk {
            token_ids: output_ids,
            prompt_tokens,
            completion_tokens,
            cached_tokens,
            output_logprobs: None,
            index,
        })
    };

    Ok(proto::GenerateResponse {
        request_id: request_id.to_string(),
        response: Some(response),
    })
}

fn meta_u32(meta: &HashMap<String, String>, key: &str) -> u32 {
    meta.get(key)
        .and_then(|value| serde_json::from_str::<u32>(value).ok())
        .unwrap_or_default()
}

fn finish_metadata(
    meta: &HashMap<String, String>,
) -> (String, Option<proto::generate_complete::MatchedStop>) {
    let Some(value) = meta
        .get("finish_reason")
        .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
    else {
        return ("stop".to_string(), None);
    };

    match value {
        serde_json::Value::String(reason) => (reason, None),
        serde_json::Value::Object(object) => {
            let reason = object
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("stop")
                .to_string();
            let matched = object.get("matched").and_then(|value| {
                if let Some(id) = value.as_u64().and_then(|id| u32::try_from(id).ok()) {
                    Some(proto::generate_complete::MatchedStop::MatchedTokenId(id))
                } else {
                    value.as_str().map(|value| {
                        proto::generate_complete::MatchedStop::MatchedStopStr(value.to_string())
                    })
                }
            });
            (reason, matched)
        }
        _ => ("stop".to_string(), None),
    }
}

/// Convert the router's mature text-generation representation to the stable
/// Worker wire. Unsupported extension lanes fail explicitly rather than being
/// silently discarded.
pub fn from_tokenspeed_request(
    request: ts::GenerateRequest,
) -> Result<proto::GenerateRequest, Status> {
    if request.mm_inputs.is_some() {
        return Err(Status::unimplemented(
            "WorkerInference v1 does not support multimodal inputs",
        ));
    }
    if request.encode_bootstrap_info.is_some() || request.kv_bootstrap_info.is_some() {
        return Err(Status::unimplemented(
            "WorkerInference v1 does not support disaggregated execution",
        ));
    }

    Ok(proto::GenerateRequest {
        request_id: request.request_id,
        tokenized: request.tokenized.map(|input| proto::TokenizedInput {
            input_ids: input.input_ids,
            original_text: input.original_text,
        }),
        sampling_params: request.sampling_params.map(from_tokenspeed_sampling),
        return_logprob: request.return_logprob,
        logprob_start_len: request.logprob_start_len,
        top_logprobs_num: request.top_logprobs_num,
        token_ids_logprob: request.token_ids_logprob,
        stream: request.stream,
        data_parallel_rank: request.data_parallel_rank,
    })
}

/// Adapter-side conversion used by the mock worker and, later, each engine
/// binding. It is kept in the protocol crate so every engine receives exactly
/// the same argument semantics.
pub fn into_tokenspeed_request(request: proto::GenerateRequest) -> ts::GenerateRequest {
    ts::GenerateRequest {
        request_id: request.request_id,
        tokenized: request.tokenized.map(|input| ts::TokenizedInput {
            input_ids: input.input_ids,
            original_text: input.original_text,
        }),
        sampling_params: request.sampling_params.map(into_tokenspeed_sampling),
        return_logprob: request.return_logprob,
        logprob_start_len: request.logprob_start_len,
        top_logprobs_num: request.top_logprobs_num,
        token_ids_logprob: request.token_ids_logprob,
        stream: request.stream,
        data_parallel_rank: request.data_parallel_rank,
        ..Default::default()
    }
}

pub fn from_tokenspeed_response(response: ts::GenerateResponse) -> proto::GenerateResponse {
    use ts::generate_response::Response;
    proto::GenerateResponse {
        request_id: response.request_id,
        response: response.response.map(|response| match response {
            Response::Chunk(chunk) => {
                proto::generate_response::Response::Chunk(proto::GenerateStreamChunk {
                    token_ids: chunk.token_ids,
                    prompt_tokens: chunk.prompt_tokens,
                    completion_tokens: chunk.completion_tokens,
                    cached_tokens: chunk.cached_tokens,
                    output_logprobs: chunk.output_logprobs.map(from_tokenspeed_logprobs),
                    index: chunk.index,
                })
            }
            Response::Complete(complete) => {
                proto::generate_response::Response::Complete(proto::GenerateComplete {
                    output_ids: complete.output_ids,
                    finish_reason: complete.finish_reason,
                    prompt_tokens: complete.prompt_tokens,
                    completion_tokens: complete.completion_tokens,
                    cached_tokens: complete.cached_tokens,
                    output_logprobs: complete.output_logprobs.map(from_tokenspeed_logprobs),
                    matched_stop: complete.matched_stop.map(|matched| match matched {
                        ts::generate_complete::MatchedStop::MatchedTokenId(id) => {
                            proto::generate_complete::MatchedStop::MatchedTokenId(id)
                        }
                        ts::generate_complete::MatchedStop::MatchedStopStr(value) => {
                            proto::generate_complete::MatchedStop::MatchedStopStr(value)
                        }
                    }),
                    index: complete.index,
                })
            }
        }),
    }
}

pub fn into_tokenspeed_response(response: proto::GenerateResponse) -> ts::GenerateResponse {
    use proto::generate_response::Response;
    ts::GenerateResponse {
        request_id: response.request_id,
        response: response.response.map(|response| match response {
            Response::Chunk(chunk) => {
                ts::generate_response::Response::Chunk(ts::GenerateStreamChunk {
                    token_ids: chunk.token_ids,
                    prompt_tokens: chunk.prompt_tokens,
                    completion_tokens: chunk.completion_tokens,
                    cached_tokens: chunk.cached_tokens,
                    output_logprobs: chunk.output_logprobs.map(into_tokenspeed_logprobs),
                    index: chunk.index,
                })
            }
            Response::Complete(complete) => {
                ts::generate_response::Response::Complete(ts::GenerateComplete {
                    output_ids: complete.output_ids,
                    finish_reason: complete.finish_reason,
                    prompt_tokens: complete.prompt_tokens,
                    completion_tokens: complete.completion_tokens,
                    cached_tokens: complete.cached_tokens,
                    output_logprobs: complete.output_logprobs.map(into_tokenspeed_logprobs),
                    matched_stop: complete.matched_stop.map(|matched| match matched {
                        proto::generate_complete::MatchedStop::MatchedTokenId(id) => {
                            ts::generate_complete::MatchedStop::MatchedTokenId(id)
                        }
                        proto::generate_complete::MatchedStop::MatchedStopStr(value) => {
                            ts::generate_complete::MatchedStop::MatchedStopStr(value)
                        }
                    }),
                    index: complete.index,
                })
            }
        }),
    }
}

fn from_tokenspeed_sampling(params: ts::SamplingParams) -> proto::SamplingParams {
    proto::SamplingParams {
        temperature: params.temperature,
        top_p: params.top_p,
        top_k: params.top_k,
        min_p: params.min_p,
        frequency_penalty: params.frequency_penalty,
        presence_penalty: params.presence_penalty,
        repetition_penalty: params.repetition_penalty,
        max_new_tokens: params.max_new_tokens,
        min_new_tokens: params.min_new_tokens,
        stop: params.stop,
        stop_token_ids: params.stop_token_ids,
        ignore_eos: params.ignore_eos,
        skip_special_tokens: params.skip_special_tokens,
        spaces_between_special_tokens: params.spaces_between_special_tokens,
        n: params.n,
        logit_bias: params.logit_bias,
        constraint: params.constraint.map(|constraint| match constraint {
            ts::sampling_params::Constraint::Regex(value) => {
                proto::sampling_params::Constraint::Regex(value)
            }
            ts::sampling_params::Constraint::JsonSchema(value) => {
                proto::sampling_params::Constraint::JsonSchema(value)
            }
            ts::sampling_params::Constraint::EbnfGrammar(value) => {
                proto::sampling_params::Constraint::EbnfGrammar(value)
            }
            ts::sampling_params::Constraint::StructuralTag(value) => {
                proto::sampling_params::Constraint::StructuralTag(value)
            }
        }),
        engine_parameters: params.custom_params,
        no_stop_trim: params.no_stop_trim,
        sampling_seed: params.sampling_seed,
    }
}

fn into_tokenspeed_sampling(params: proto::SamplingParams) -> ts::SamplingParams {
    ts::SamplingParams {
        temperature: params.temperature,
        top_p: params.top_p,
        top_k: params.top_k,
        min_p: params.min_p,
        frequency_penalty: params.frequency_penalty,
        presence_penalty: params.presence_penalty,
        repetition_penalty: params.repetition_penalty,
        max_new_tokens: params.max_new_tokens,
        min_new_tokens: params.min_new_tokens,
        stop: params.stop,
        stop_token_ids: params.stop_token_ids,
        ignore_eos: params.ignore_eos,
        skip_special_tokens: params.skip_special_tokens,
        spaces_between_special_tokens: params.spaces_between_special_tokens,
        n: params.n,
        logit_bias: params.logit_bias,
        constraint: params.constraint.map(|constraint| match constraint {
            proto::sampling_params::Constraint::Regex(value) => {
                ts::sampling_params::Constraint::Regex(value)
            }
            proto::sampling_params::Constraint::JsonSchema(value) => {
                ts::sampling_params::Constraint::JsonSchema(value)
            }
            proto::sampling_params::Constraint::EbnfGrammar(value) => {
                ts::sampling_params::Constraint::EbnfGrammar(value)
            }
            proto::sampling_params::Constraint::StructuralTag(value) => {
                ts::sampling_params::Constraint::StructuralTag(value)
            }
        }),
        custom_params: params.engine_parameters,
        no_stop_trim: params.no_stop_trim,
        sampling_seed: params.sampling_seed,
    }
}

fn from_tokenspeed_logprobs(logprobs: ts::OutputLogProbs) -> proto::OutputLogProbs {
    proto::OutputLogProbs {
        token_logprobs: logprobs.token_logprobs,
        token_ids: logprobs.token_ids,
        top_logprobs: logprobs
            .top_logprobs
            .into_iter()
            .map(|item| proto::TopLogProbs {
                values: item.values,
                token_ids: item.token_ids,
            })
            .collect(),
    }
}

fn into_tokenspeed_logprobs(logprobs: proto::OutputLogProbs) -> ts::OutputLogProbs {
    ts::OutputLogProbs {
        token_logprobs: logprobs.token_logprobs,
        token_ids: logprobs.token_ids,
        top_logprobs: logprobs
            .top_logprobs
            .into_iter()
            .map(|item| ts::TopLogProbs {
                values: item.values,
                token_ids: item.token_ids,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use futures::{stream, Stream, StreamExt};
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{transport::Server, Response, Status};

    use super::*;

    #[derive(Clone, Default)]
    struct TestInference {
        aborts: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl proto::worker_inference_server::WorkerInference for TestInference {
        type GenerateStream =
            Pin<Box<dyn Stream<Item = Result<proto::GenerateResponse, Status>> + Send>>;

        async fn generate(
            &self,
            request: Request<proto::GenerateRequest>,
        ) -> Result<Response<Self::GenerateStream>, Status> {
            let request_id = request.into_inner().request_id;
            let first = proto::GenerateResponse {
                request_id,
                response: Some(proto::generate_response::Response::Chunk(
                    proto::GenerateStreamChunk {
                        token_ids: vec![42],
                        completion_tokens: 1,
                        ..Default::default()
                    },
                )),
            };
            Ok(Response::new(Box::pin(
                stream::once(async move { Ok(first) }).chain(stream::pending()),
            )))
        }

        async fn abort(
            &self,
            _request: Request<proto::AbortRequest>,
        ) -> Result<Response<proto::AbortResponse>, Status> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(proto::AbortResponse {
                success: true,
                message: String::new(),
            }))
        }
    }

    #[test]
    fn text_request_round_trips_without_engine_fields() {
        let request = ts::GenerateRequest {
            request_id: "req-1".to_string(),
            tokenized: Some(ts::TokenizedInput {
                input_ids: vec![1, 2, 3],
                original_text: "hello".to_string(),
            }),
            sampling_params: Some(ts::SamplingParams {
                temperature: Some(0.25),
                max_new_tokens: Some(8),
                stop: vec!["done".to_string()],
                ..Default::default()
            }),
            stream: true,
            ..Default::default()
        };

        let worker = from_tokenspeed_request(request.clone()).expect("portable request");
        assert_eq!(into_tokenspeed_request(worker), request);
    }

    #[test]
    fn disaggregated_request_is_rejected() {
        let request = ts::GenerateRequest {
            kv_bootstrap_info: Some(ts::KvBootstrapInfo::default()),
            ..Default::default()
        };
        let status = from_tokenspeed_request(request).expect_err("unsupported extension");
        assert_eq!(status.code(), tonic::Code::Unimplemented);
    }

    #[test]
    fn sglang_native_request_preserves_portable_arguments() {
        let request = proto::GenerateRequest {
            request_id: "native-1".to_string(),
            tokenized: Some(proto::TokenizedInput {
                input_ids: vec![10, 20],
                original_text: "ignored by tokenized native RPC".to_string(),
            }),
            sampling_params: Some(proto::SamplingParams {
                temperature: Some(0.2),
                max_new_tokens: Some(16),
                stop_token_ids: vec![99],
                sampling_seed: Some(7),
                ..Default::default()
            }),
            stream: true,
            data_parallel_rank: Some(2),
            ..Default::default()
        };

        let native = into_sglang_request(request).expect("native request");
        assert_eq!(native.input_ids, vec![10, 20]);
        assert_eq!(native.rid.as_deref(), Some("native-1"));
        assert_eq!(native.routed_dp_rank, Some(2));
        let sampling = native.sampling_params.expect("sampling params");
        assert_eq!(sampling.temperature, Some(0.2));
        assert_eq!(sampling.max_new_tokens, Some(16));
        assert_eq!(sampling.stop_token_ids, vec![99]);
        assert_eq!(sampling.seed, Some(7));
    }

    #[test]
    fn sglang_native_finish_metadata_maps_to_worker_contract() {
        let response = from_sglang_response(
            "native-2",
            sglang::GenerateResponse {
                output_ids: vec![42, 43],
                meta_info: HashMap::from([
                    ("prompt_tokens".to_string(), "3".to_string()),
                    ("completion_tokens".to_string(), "2".to_string()),
                    (
                        "finish_reason".to_string(),
                        r#"{"type":"stop","matched":43}"#.to_string(),
                    ),
                ]),
                finished: true,
            },
            &mut HashMap::new(),
        )
        .expect("worker response");

        let Some(proto::generate_response::Response::Complete(complete)) = response.response else {
            panic!("expected completion")
        };
        assert_eq!(complete.output_ids, vec![42, 43]);
        assert_eq!(complete.prompt_tokens, 3);
        assert_eq!(complete.completion_tokens, 2);
        assert_eq!(complete.finish_reason, "stop");
        assert_eq!(
            complete.matched_stop,
            Some(proto::generate_complete::MatchedStop::MatchedTokenId(43))
        );
    }

    #[test]
    fn sglang_native_stream_is_converted_from_cumulative_to_delta_tokens() {
        let mut emitted_by_index = HashMap::new();
        let first = from_sglang_response(
            "native-stream",
            sglang::GenerateResponse {
                output_ids: vec![10],
                meta_info: HashMap::new(),
                finished: false,
            },
            &mut emitted_by_index,
        )
        .expect("first chunk");
        let second = from_sglang_response(
            "native-stream",
            sglang::GenerateResponse {
                output_ids: vec![10, 20],
                meta_info: HashMap::new(),
                finished: false,
            },
            &mut emitted_by_index,
        )
        .expect("second chunk");

        let Some(proto::generate_response::Response::Chunk(first)) = first.response else {
            panic!("expected first chunk")
        };
        let Some(proto::generate_response::Response::Chunk(second)) = second.response else {
            panic!("expected second chunk")
        };
        assert_eq!(first.token_ids, vec![10]);
        assert_eq!(second.token_ids, vec![20]);
    }

    #[tokio::test]
    async fn stream_drop_sends_abort_over_worker_service() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener.local_addr().expect("listener address");
        let service = TestInference::default();
        let aborts = Arc::clone(&service.aborts);
        #[expect(
            clippy::disallowed_methods,
            reason = "test-only tonic server is explicitly aborted before return"
        )]
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(proto::worker_inference_server::WorkerInferenceServer::new(
                    service,
                ))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
        });

        let client = WorkerInferenceClient::connect(&format!("grpc://{address}"))
            .await
            .expect("connect WorkerInference");
        let mut response = client
            .generate(proto::GenerateRequest {
                request_id: "drop-me".to_string(),
                stream: true,
                ..Default::default()
            })
            .await
            .expect("generate");
        assert!(response.next().await.expect("first item").is_ok());
        drop(response);

        tokio::time::timeout(Duration::from_secs(2), async {
            while aborts.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("drop-triggered abort");
        server.abort();
    }
}

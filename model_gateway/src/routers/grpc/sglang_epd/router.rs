//! SGLang prepared-input protocol adapter for the shared EPD pipeline.

use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::response::Response;
use openai_protocol::chat::ChatCompletionRequest;
use prost::Message;
use serde::Deserialize;
#[cfg(test)]
use serde_json::Value;
use sha2::{Digest, Sha256};
use smg_grpc_client::{
    normalize_grpc_endpoint,
    sglang_managed::{proto, Limits, LAYOUT, VERSION},
    sglang_proto,
};
use tonic::Status;
use uuid::Uuid;

use super::{
    executor::{AttemptObserver, Executor},
    planner::Planner,
    registry::{wall_ms, Registry},
};
use crate::{
    app_context::AppContext,
    routers::{
        common::pd_admission::PdAdmissionGuard,
        grpc::{
            context::{ExecutionResult, RequestContext, RequestType, WorkerSelection},
            proto_wrapper::ProtoStream,
            utils::tonic_ext::TonicStatusExt,
        },
        http::pd_types::generate_room_id,
    },
};

/// Explicitly register native controls as gRPC in the shared worker workflow.
/// Tonic control URIs use HTTP schemes; discovery uses the scheme as a protocol hint.
pub(crate) fn worker_registration_url(url: &str) -> String {
    if let Some(address) = url.strip_prefix("http://") {
        format!("grpc://{address}")
    } else if let Some(address) = url.strip_prefix("https://") {
        format!("grpcs://{address}")
    } else {
        url.to_owned()
    }
}

pub(crate) struct PreparedInputAdapter {
    workers: Arc<crate::worker::WorkerRegistry>,
    config: Config,
    aliases: std::collections::HashMap<String, String>,
    registry: Arc<Registry>,
    executor: Executor,
}

/// Media references and the original deadline survive the shared ingress stages.
/// No HTTP body or duplicate response configuration is retained at dispatch.
pub(crate) struct PreparedInput {
    adapter: Arc<PreparedInputAdapter>,
    images: Vec<String>,
    tenant: String,
    digest: Vec<u8>,
    started: Instant,
    original_expiry: u64,
}

impl PreparedInput {
    pub(crate) fn has_media(&self) -> bool {
        !self.images.is_empty()
    }

    /// Transfer the common PD admission claim to the native resource owner.
    pub(crate) fn execute(
        self,
        generation: sglang_proto::GenerateRequest,
        workers: &WorkerSelection,
        admission: Option<PdAdmissionGuard>,
    ) -> Result<ExecutionResult, Response> {
        let adapter = Arc::clone(&self.adapter);
        adapter
            .execute_prepared(self, generation, workers, admission)
            .map_err(|status| {
                status.to_http_error("sglang_epd_failed", status.message().to_owned())
            })
    }
}

impl fmt::Debug for PreparedInputAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedInputAdapter")
            .field("model", &self.config.model)
            .finish_non_exhaustive()
    }
}

impl PreparedInputAdapter {
    /// Keep probe reads constant-time while managed workers use their own capability registry.
    pub(crate) fn maintain_readiness(
        &self,
        probe: Arc<crate::health::ProbeState>,
        tokenizers: Arc<llm_tokenizer::TokenizerRegistry>,
    ) -> tokio::task::JoinHandle<()> {
        let registry = Arc::downgrade(&self.registry);
        let workers = Arc::clone(&self.workers);
        let pairs = self.config.pairs.clone();
        let model = self.config.model.clone();
        #[expect(
            clippy::disallowed_methods,
            reason = "one weakly owned readiness monitor per managed router"
        )]
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let Some(registry) = registry.upgrade() else {
                    break;
                };
                let mut readiness = registry.readiness();
                let has_tokenizer = tokenizers.contains(&model)
                    || workers
                        .resolve_model_alias(&model)
                        .is_some_and(|canonical| tokenizers.contains(&canonical));
                readiness.workers_ready &= has_tokenizer
                    && pairs.iter().any(|pair| {
                        workers
                            .get_by_url(&worker_registration_url(&pair.prefill))
                            .is_some()
                            && workers
                                .get_by_url(&worker_registration_url(&pair.decode))
                                .is_some()
                    });
                probe.publish_managed(readiness);
            }
        })
    }

    /// Discover the protocol capability pool; standard worker registration owns
    /// tokenizer loading, native capacity and ordinary routing state.
    pub(crate) async fn new(ctx: &Arc<AppContext>) -> Result<Self, String> {
        let mut config = Config::from_router(&ctx.router_config).map_err(|e| e.to_string())?;
        config
            .discover_revision(&ctx.router_config)
            .await
            .map_err(|e| e.to_string())?;
        let config = config.resolve().map_err(|e| e.to_string())?;
        let registry = Registry::new(config.clone()).map_err(|e| e.to_string())?;
        let executor =
            Executor::new(Limits::default(), Duration::from_secs(10)).map_err(|e| e.to_string())?;
        Ok(Self {
            workers: ctx.worker_registry.clone(),
            config,
            aliases: ctx.router_config.model_aliases.clone(),
            registry,
            executor,
        })
    }

    /// Validate the supported request capability before any shared stage can
    /// fetch media, claim admission or dispatch native work.
    pub(crate) fn prepare(self: &Arc<Self>, ctx: &RequestContext) -> Result<PreparedInput, Status> {
        let RequestType::Chat(body) = &ctx.input.request_type else {
            return Err(Status::unimplemented(
                "SGLang EPD currently supports chat requests only",
            ));
        };
        if (body.model != self.config.model
            && self.aliases.get(&body.model) != Some(&self.config.model))
            || body.n.unwrap_or(1) != 1
            || body.session_params.is_some()
            || body.declares_tools()
            || body.return_hidden_states
        {
            return Err(Status::invalid_argument(
                "SGLang EPD requires configured model, n=1, no session/tools/hidden states",
            ));
        }
        let bytes =
            serde_json::to_vec(body).map_err(|e| Status::invalid_argument(e.to_string()))?;
        if bytes.len() > Limits::default().manifest_bytes {
            return Err(Status::resource_exhausted(
                "managed request exceeds control byte limit",
            ));
        }
        let images = image_references(body)?;
        if images.len() > self.config.max_images {
            return Err(Status::invalid_argument(
                "managed image count exceeds capability",
            ));
        }
        let tenant = ctx
            .input
            .tenant_request_meta
            .as_ref()
            .ok_or_else(|| Status::internal("missing tenant request metadata"))?;
        Ok(PreparedInput {
            adapter: Arc::clone(self),
            images,
            tenant: tenant.tenant_key().as_str().to_owned(),
            digest: Sha256::digest(bytes).to_vec(),
            started: Instant::now(),
            original_expiry: wall_ms()?.saturating_add(self.config.timeout_ms),
        })
    }

    /// Execute a prepared request without rendering, tokenizing, rate limiting,
    /// or formatting its response. The native request remains owned by this
    /// attempt until the immutable input binding has been stamped.
    fn execute_prepared(
        &self,
        request: PreparedInput,
        mut native_generation: sglang_proto::GenerateRequest,
        workers: &WorkerSelection,
        admission: Option<PdAdmissionGuard>,
    ) -> Result<ExecutionResult, Status> {
        let PreparedInput {
            images,
            tenant,
            digest: request_digest,
            started,
            original_expiry,
            ..
        } = request;
        let request_id = native_generation.request_id.clone();
        let prefill_url = workers
            .prefill_worker()
            .ok_or_else(|| Status::internal("missing selected P"))?
            .url();
        let decode_url = workers
            .decode_worker()
            .ok_or_else(|| Status::internal("missing selected D"))?
            .url();
        let prefill_url = normalize_grpc_endpoint(prefill_url);
        let decode_url = normalize_grpc_endpoint(decode_url);
        let attempt = Uuid::now_v7().to_string();
        let total = Duration::from_millis(self.config.timeout_ms);
        let (pairs, prefill_capabilities, generation) = self.registry.candidates_for(
            &request_id,
            !images.is_empty(),
            Some((&prefill_url, &decode_url)),
        )?;
        let remaining = total
            .checked_sub(started.elapsed())
            .ok_or_else(|| Status::deadline_exceeded("managed preparation expired"))?;
        let planner = Planner::new(
            self.config.max_pd_candidates,
            self.config.max_encoder_candidates,
        )?;
        let (pair_index, encoder_index, score) = planner.choose(&pairs, images.len(), remaining)?;
        let pair = &pairs[pair_index];
        let prefill_capability = &prefill_capabilities[pair_index];
        let identity = proto::Identity {
            logical_request_id: request_id.clone(),
            attempt_id: attempt.clone(),
            branch_id: 0,
        };
        let encoder = encoder_index.map(|index| &pair.encoders[index]);
        let prefill_rid = encoder.map_or_else(
            || format!("{attempt}-p"),
            |e| format!("{}.{attempt}-p", e.worker.instance.generation),
        );
        let mut binding = proto::InputBinding {
            decode: Some(proto::OperationRef {
                identity: Some(identity.clone()),
                target: Some(pair.decode.instance.clone()),
                operation_id: format!("{attempt}-d"),
                request_digest: request_digest.clone(),
                plan_digest: vec![0; 32],
            }),
            prefill: Some(pair.prefill.instance.clone()),
            prefill_rid,
            bootstrap: Some(proto::Bootstrap {
                host: prefill_capability.bootstrap_host.clone(),
                port: prefill_capability.bootstrap_port,
                room: generate_room_id() as i64,
            }),
            model_revision: self.config.model_revision.clone(),
            processor_contract: self.config.processor_contract.clone(),
            layout_contract: LAYOUT.into(),
            media_routing_labels: vec![],
        };
        let mut hash = Sha256::new();
        hash.update(binding.encode_to_vec());
        hash.update(pair.prefill.control_endpoint.as_bytes());
        hash.update(pair.decode.control_endpoint.as_bytes());
        if let Some(encoder) = encoder {
            hash.update(encoder.worker.instance.encode_to_vec());
            hash.update(encoder.worker.control_endpoint.as_bytes());
            hash.update(encoder.worker.receive_endpoint.as_bytes());
        }
        let plan_digest = hash.finalize().to_vec();
        binding
            .decode
            .as_mut()
            .ok_or_else(|| Status::internal("missing D operation"))?
            .plan_digest
            .clone_from(&plan_digest);
        let plan = Arc::new(planner.freeze(
            pair,
            encoder_index,
            images.len(),
            binding,
            score,
            generation,
        )?);
        let binding = plan.binding();
        let operation = |target: proto::Instance, suffix: &str| proto::OperationRef {
            identity: Some(identity.clone()),
            target: Some(target),
            operation_id: format!("{attempt}-{suffix}"),
            request_digest: request_digest.clone(),
            plan_digest: plan_digest.clone(),
        };
        // Work units are predicted milliseconds, so observed queue work and service priors share a unit.
        let work = encoder.map_or(1, |e| {
            e.cost.preprocess_ms.saturating_add(e.cost.encode_ms).max(1)
        });
        let budget = proto::Budget {
            original_expiry_unix_ms: original_expiry,
            remaining_timeout_ms: remaining.as_millis() as u64,
            estimated_work: work,
            context_bytes: (Limits::default().manifest_bytes * 2) as u64,
        };
        native_generation.request_id = format!("{attempt}-d");
        let mut d_generation = native_generation;
        let mut p_generation = d_generation.clone();
        p_generation.request_id.clone_from(&binding.prefill_rid);
        p_generation
            .sampling_params
            .as_mut()
            .ok_or_else(|| Status::invalid_argument("missing sampling parameters"))?
            .max_new_tokens = Some(1);
        if !images.is_empty() {
            let input = d_generation
                .tokenized
                .as_mut()
                .ok_or_else(|| Status::internal("missing D input"))?;
            input.input_ids.clear();
            input.original_text.clear();
        }
        let prefill = proto::PrefillStart {
            version: VERSION,
            operation: Some(operation(pair.prefill.instance.clone(), "p")),
            budget: Some(budget),
            binding: Some(binding.clone()),
            generation: Some(p_generation),
            encode_plan: Some(plan.external().clone()),
        };
        let decode = proto::DecodeStart {
            version: VERSION,
            binding: Some(binding.clone()),
            budget: Some(budget),
            generation: Some(d_generation),
            deferred_from_prefill: !images.is_empty(),
        };
        let encode = plan.encoder().map(|e| proto::EncodeRequest {
            version: VERSION,
            operation: Some(operation(e.instance.clone(), "e")),
            budget: Some(budget),
            receiver_binding: Some(binding.clone()),
            plan: Some(plan.external().clone()),
            image_references: images,
            transfer_backend: e.contract.transfer.clone(),
            tenant_id: tenant,
        });
        let mut operations = vec![
            (operation(pair.prefill.instance.clone(), "p"), false),
            (operation(pair.decode.instance.clone(), "d"), false),
        ];
        if let Some(encoder) = plan.encoder() {
            operations.push((operation(encoder.instance.clone(), "e"), true));
        }
        self.registry.reserve(
            &operations,
            work,
            plan.encoder()
                .map(|_| operations[1].0.operation_id.as_str()),
        )?;
        self.registry
            .retain_pd_admission(&operations[1].0, admission)?;
        metrics::histogram!("smg_managed_epd_predicted_ttft_seconds").record(score as f64 / 1000.0);
        tracing::info!(request_id, attempt, predicted_ttft_ms = score, registry_generation = generation,
            prefill = %pair.prefill.instance.worker_id, decode = %pair.decode.instance.worker_id,
            encoder = plan.encoder().map(|e| e.instance.worker_id.as_str()), "managed EPD plan frozen");
        let observer: Arc<dyn AttemptObserver> = self.registry.clone();
        let remaining = total.saturating_sub(started.elapsed());
        let response = match self
            .executor
            .execute(plan, prefill, decode, encode, remaining, observer)
        {
            Ok(response) => response,
            Err(error) => {
                for (operation, _) in &operations {
                    self.registry.termination(operation, true);
                }
                return Err(error);
            }
        };
        tracing::info!(
            request_id,
            attempt_id = attempt,
            predicted_ttft_ms = score,
            "managed EPD plan dispatched"
        );
        Ok(ExecutionResult::Single {
            stream: ProtoStream::Managed(Box::new(response)),
            dispatch_start: started,
        })
    }
}

/// Extract occurrence order without decoding, fetching, deduplicating, or examining image bytes.
fn image_references(body: &ChatCompletionRequest) -> Result<Vec<String>, Status> {
    use openai_protocol::{
        chat::{ChatMessage, MessageContent},
        common::ContentPart,
    };

    let mut images = Vec::new();
    for message in &body.messages {
        let content = match message {
            ChatMessage::System { content, .. }
            | ChatMessage::Root { content, .. }
            | ChatMessage::User { content, .. }
            | ChatMessage::Tool { content, .. }
            | ChatMessage::Developer { content, .. } => Some(content),
            ChatMessage::Assistant { content, .. } => content.as_ref(),
            ChatMessage::Function { .. } => None,
        };
        let Some(MessageContent::Parts(parts)) = content else {
            continue;
        };
        for part in parts {
            match part {
                ContentPart::Text { .. } | ContentPart::Refusal { .. } => {}
                ContentPart::ImageUrl { image_url } => {
                    if image_url.max_long_side_pixel.is_some()
                        || image_url
                            .detail
                            .as_deref()
                            .is_some_and(|detail| detail != "auto")
                    {
                        return Err(Status::invalid_argument(
                            "managed image resize/detail overrides are unsupported",
                        ));
                    }
                    if image_url.url.is_empty() {
                        return Err(Status::invalid_argument("managed image URL is missing"));
                    }
                    images.push(image_url.url.as_ref().clone());
                }
                _ => {
                    return Err(Status::invalid_argument(
                        "managed EPD supports text and images only",
                    ))
                }
            }
        }
    }
    Ok(images)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncoderEndpoint {
    pub endpoint: String,
}

#[derive(Debug, Clone)]
pub struct PdEndpoints {
    pub prefill: String,
    pub decode: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub model: String,
    pub model_path: String,
    pub model_revision: String,
    pub processor_contract: String,
    #[serde(skip)]
    pub pairs: Vec<PdEndpoints>,
    pub prefill_urls: Vec<String>,
    pub decode_urls: Vec<String>,
    pub encoders: Vec<EncoderEndpoint>,
    pub readiness: crate::config::PdReadinessConfig,
    pub max_records: usize,
    pub timeout_ms: u64,
    pub max_clock_error_ms: u64,
    pub probe_timeout_ms: u64,
    pub snapshot_max_age_ms: u64,
    pub max_images: usize,
    pub max_pd_candidates: usize,
    pub max_encoder_candidates: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            model: String::new(),
            model_path: String::new(),
            model_revision: String::new(),
            processor_contract: "smg-qwen3-vl-f32-v1".into(),
            pairs: Vec::new(),
            prefill_urls: Vec::new(),
            decode_urls: Vec::new(),
            encoders: Vec::new(),
            readiness: crate::config::PdReadinessConfig::default(),
            max_records: 1024,
            timeout_ms: 120_000,
            max_clock_error_ms: 1000,
            probe_timeout_ms: 3000,
            snapshot_max_age_ms: 3000,
            max_images: 32,
            max_pd_candidates: 4,
            max_encoder_candidates: 4,
        }
    }
}

impl Config {
    /// Use the existing model and E/P/D arguments; resource limits remain internal defaults.
    pub fn from_router(router: &crate::config::RouterConfig) -> Result<Self, Status> {
        let crate::config::RoutingMode::EncodePrefillDecode {
            encode_urls,
            prefill_urls,
            decode_urls,
            ..
        } = &router.mode
        else {
            return Err(Status::invalid_argument(
                "SGLang EPD requires E/P/D role pools",
            ));
        };
        if encode_urls.len() > 64
            || prefill_urls.is_empty()
            || decode_urls.is_empty()
            || prefill_urls.len().saturating_mul(decode_urls.len()) > 64
        {
            return Err(Status::invalid_argument("invalid managed E/P/D pools"));
        }
        let model_path = router
            .model_path
            .clone()
            .filter(|path| !path.is_empty())
            .ok_or_else(|| Status::invalid_argument("SGLang EPD requires --model-path"))?;
        let endpoint = |url: &str| -> Result<String, Status> {
            let url = normalize_grpc_endpoint(url);
            tonic::transport::Endpoint::from_shared(url.clone())
                .map_err(|_| Status::invalid_argument("invalid managed control endpoint"))?;
            Ok(url)
        };
        Ok(Self {
            readiness: router.pd_readiness.unwrap_or_default(),
            model: model_path.clone(),
            model_path,
            prefill_urls: prefill_urls
                .iter()
                .map(|(url, _)| endpoint(url))
                .collect::<Result<_, _>>()?,
            decode_urls: decode_urls
                .iter()
                .map(|url| endpoint(url))
                .collect::<Result<_, _>>()?,
            encoders: encode_urls
                .iter()
                .map(|(url, _)| {
                    Ok(EncoderEndpoint {
                        endpoint: endpoint(url)?,
                    })
                })
                .collect::<Result<_, Status>>()?,
            ..Self::default()
        })
    }

    /// Discover worker-advertised weight identity once; every pool observation must match it.
    async fn discover_revision(
        &mut self,
        router: &crate::config::RouterConfig,
    ) -> Result<(), Status> {
        use proto::managed_sglang_client::ManagedSglangClient;
        let deadline = Instant::now() + Duration::from_secs(router.worker_startup_timeout_secs);
        loop {
            let probes = self.prefill_urls.iter().map(|endpoint| async move {
                let channel = tonic::transport::Endpoint::from_shared(endpoint.clone())
                    .map_err(|e| Status::invalid_argument(e.to_string()))?
                    .connect_lazy();
                tokio::time::timeout(
                    Duration::from_secs(3),
                    ManagedSglangClient::new(channel).get_capabilities(proto::CapabilityQuery {}),
                )
                .await
                .map_err(|_| Status::unavailable("managed capability probe timed out"))?
                .map(|reply| reply.into_inner())
            });
            for result in futures::future::join_all(probes).await {
                match result {
                    Ok(capability)
                        if capability.version == VERSION
                            && capability.ready
                            && capability.role == "prefill"
                            && capability.layout_contract == LAYOUT
                            && capability.processor_contract == self.processor_contract
                            && !capability.model_revision.is_empty() =>
                    {
                        self.model_revision = capability.model_revision;
                        return Ok(());
                    }
                    Err(error) if error.code() == tonic::Code::Unimplemented => {
                        return Err(Status::failed_precondition(
                            "SGLang EPD requires managed worker capabilities; no legacy fallback",
                        ))
                    }
                    Ok(capability) if !capability.ready => {}
                    Ok(_) => {
                        return Err(Status::failed_precondition(
                            "incompatible managed prefill capability",
                        ))
                    }
                    Err(_) => {}
                }
            }
            if Instant::now() >= deadline {
                return Err(Status::unavailable(
                    "no compatible managed prefill became available before startup timeout",
                ));
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Resolve role pools once; candidate search remains bounded independently of pool size.
    pub fn resolve(mut self) -> Result<Self, Status> {
        if self.prefill_urls.is_empty()
            || self.decode_urls.is_empty()
            || self
                .prefill_urls
                .len()
                .saturating_mul(self.decode_urls.len())
                > 64
        {
            return Err(Status::invalid_argument("invalid managed P/D pools"));
        }
        self.pairs = self
            .prefill_urls
            .iter()
            .flat_map(|prefill| {
                self.decode_urls.iter().map(move |decode| PdEndpoints {
                    prefill: prefill.clone(),
                    decode: decode.clone(),
                })
            })
            .collect();
        self.validate()?;
        Ok(self)
    }

    /// Invalid internal bounds fail startup before any operation is admitted.
    pub fn validate(&self) -> Result<(), Status> {
        if self.model.is_empty()
            || self.model_path.is_empty()
            || self.model_revision.is_empty()
            || self.processor_contract.is_empty()
            || self.pairs.is_empty()
            || self.pairs.len() > 64
            || self.encoders.len() > 64
            || !(1..=64).contains(&self.max_pd_candidates)
            || !(1..=64).contains(&self.max_encoder_candidates)
            || self.max_records < 3
            || self.max_records > 65536
            || self.timeout_ms == 0
            || self.timeout_ms > 120_000
            || self.probe_timeout_ms == 0
            || self.max_clock_error_ms == 0
            || self.snapshot_max_age_ms == 0
            || self.max_images == 0
            || self.max_images > 32
        {
            return Err(Status::invalid_argument(
                "invalid managed gateway deployment bounds",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_uri_registers_as_grpc_and_matches_selected_worker() {
        use crate::worker::ConnectionMode;

        for (input, control) in [
            ("http://127.0.0.1:30000", "http://127.0.0.1:30000"),
            ("grpc://127.0.0.1:30000", "http://127.0.0.1:30000"),
            ("https://worker:30000", "https://worker:30000"),
            ("grpcs://worker:30000", "https://worker:30000"),
        ] {
            let registered = worker_registration_url(input);
            assert_eq!(
                ConnectionMode::from_url(&registered),
                Some(ConnectionMode::Grpc)
            );
            assert_eq!(normalize_grpc_endpoint(&registered), control);
        }
    }

    /// Typed extraction preserves duplicate occurrences and rejects unsupported media before dispatch.
    #[test]
    fn image_references_keep_order_and_capability_refusals() {
        let request = |parts: Value| {
            serde_json::from_value::<ChatCompletionRequest>(serde_json::json!({
                "model": "m", "messages": [{"role": "user", "content": parts}]
            }))
            .unwrap()
        };
        let image = |url: &str| serde_json::json!({"type": "image_url", "image_url": {"url": url}});
        assert_eq!(
            image_references(&request(serde_json::json!([
                image("first"), {"type": "text", "text": "middle"}, image("second"), image("first")
            ])))
            .unwrap(),
            ["first", "second", "first"]
        );
        for part in [
            image(""),
            serde_json::json!({"type": "video_url", "video_url": {"url": "clip"}}),
            serde_json::json!({"type": "image_url", "image_url": {"url": "image", "detail": "high"}}),
            serde_json::json!({"type": "image_url", "image_url": {"url": "image", "max_long_side_pixel": 224}}),
        ] {
            assert_eq!(
                image_references(&request(serde_json::json!([part])))
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument
            );
        }
    }

    /// The ordinary role arguments normalize gRPC endpoints without deployment JSON or limit overrides.
    #[test]
    fn role_arguments_keep_internal_defaults_and_require_discovered_identity() {
        let router = crate::config::RouterConfig {
            model_path: Some("/models/qwen3".into()),
            mode: crate::config::RoutingMode::EncodePrefillDecode {
                encode_urls: vec![("grpc://e:1".into(), None)],
                prefill_urls: vec![("grpc://p:1".into(), None)],
                decode_urls: vec!["grpc://d:1".into()],
                encode_policy: None,
                prefill_policy: None,
                decode_policy: None,
            },
            ..Default::default()
        };
        let mut config = Config::from_router(&router).unwrap();
        assert_eq!(config.model, "/models/qwen3");
        assert_eq!(config.encoders[0].endpoint, "http://e:1");
        assert_eq!(config.prefill_urls, ["http://p:1"]);
        assert_eq!(config.decode_urls, ["http://d:1"]);
        assert_eq!(config.timeout_ms, Config::default().timeout_ms);
        assert!(config.clone().resolve().is_err());
        config.model_revision = "worker-pinned-revision".into();
        assert_eq!(config.resolve().unwrap().pairs.len(), 1);
    }

    /// Ordinary deployments specify pools and model identity, without scheduler tuning fields.
    #[test]
    fn minimal_pools_derive_bounded_defaults() {
        let config: Config = serde_json::from_value(serde_json::json!({
            "model": "qwen3", "model_path": "/models/qwen3", "model_revision": "weights",
            "prefill_urls": ["http://p1:1", "http://p2:1"],
            "decode_urls": ["http://d1:1", "http://d2:1"],
            "encoders": [{"endpoint": "http://e:1"}]
        }))
        .unwrap();
        let config = config.resolve().unwrap();
        assert_eq!(config.pairs.len(), 4);
        assert_eq!(config.max_pd_candidates, 4);
        assert_eq!(config.max_encoder_candidates, 4);
        assert_eq!(config.processor_contract, "smg-qwen3-vl-f32-v1");
        assert!(config.max_records >= 3);
    }
}

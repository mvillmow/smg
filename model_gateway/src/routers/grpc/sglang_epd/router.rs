//! SGLang metadata adaptation; membership, placement and admission belong to SMG.
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use openai_protocol::chat::ChatCompletionRequest;
use prost::Message;
use sha2::{Digest, Sha256};
use smg_grpc_client::{
    sglang_managed::{proto, Limits, LAYOUT, VERSION},
    sglang_proto,
};
use tonic::Status;
use uuid::Uuid;

use crate::{
    config::types::PdPairingMode,
    routers::grpc::{
        common::stages::encode::EncodeDispatchPlan,
        context::{RequestContext, RequestType, WorkerSelection},
    },
    worker::{Worker, WorkerType},
};

pub const CAPABILITY: &str = "sglang_prepared";

/// Tonic URIs use HTTP schemes; SMG registration uses a protocol hint.
pub(crate) fn worker_registration_url(url: &str) -> String {
    if let Some(address) = url.strip_prefix("http://") {
        format!("grpc://{address}")
    } else if let Some(address) = url.strip_prefix("https://") {
        format!("grpcs://{address}")
    } else {
        url.to_owned()
    }
}

/// Read capabilities obtained by the ordinary worker registration workflow.
pub(crate) fn capability(worker: &dyn Worker) -> Option<proto::Capabilities> {
    serde_json::from_str(worker.metadata().spec.labels.get(CAPABILITY)?).ok()
}

/// Before placement, require a compatible E for this P, not merely the same engine.
pub(crate) fn compatible(encoder: &dyn Worker, worker: &dyn Worker) -> bool {
    let (Some(e), Some(p)) = (capability(encoder), capability(worker)) else {
        return false;
    };
    e.version == VERSION
        && p.version == VERSION
        && [&e, &p].iter().all(|c| {
            matches!(c.tp_size, 1 | 2 | 4 | 8)
                && c.topology_contract == format!("single-host-tp{}", c.tp_size)
        })
        && e.role == "encoder"
        && e.model_revision == p.model_revision
        && e.processor_contract == p.processor_contract
        && e.layout_contract == p.layout_contract
        && e.embedding_dtype == p.embedding_dtype
        && (p.role == "decode" || e.embedding_backend == p.embedding_backend)
}

/// Use the same contracts and pairing rules for readiness and request placement.
pub(crate) fn pipeline_ready(workers: &[Arc<dyn Worker>], mode: PdPairingMode) -> bool {
    let healthy = |role| {
        workers
            .iter()
            .filter(move |w| w.is_healthy() && *w.worker_type() == role)
    };
    healthy(WorkerType::Encode).any(|e| {
        healthy(WorkerType::Prefill).any(|p| {
            compatible(e.as_ref(), p.as_ref())
                && healthy(WorkerType::Decode).any(|d| {
                    compatible(e.as_ref(), d.as_ref())
                        && p.pd_pairing().mismatch(d.pd_pairing(), mode).is_none()
                })
        })
    })
}

#[derive(Debug)]
pub(crate) struct PreparedInputAdapter {
    timeout: Duration,
}

pub(crate) struct PreparedInput {
    images: Vec<String>,
    digest: Vec<u8>,
    deadline: Instant,
    expiry: u64,
}

impl PreparedInputAdapter {
    /// Registration discovers model names, tokenizer and capabilities asynchronously.
    pub(crate) fn new() -> Self {
        Self {
            timeout: Duration::from_secs(120),
        }
    }

    /// Only media enters the strict prepared-input path; text keeps ordinary Generate semantics.
    pub(crate) fn prepare(
        self: &Arc<Self>,
        ctx: &RequestContext,
    ) -> Result<Option<PreparedInput>, Status> {
        let RequestType::Chat(body) = &ctx.input.request_type else {
            return Ok(None);
        };
        let images = image_references(body)?;
        if images.is_empty() {
            return Ok(None);
        }
        if body.n.unwrap_or(1) != 1
            || body.session_params.is_some()
            || body.declares_tools()
            || body.return_hidden_states
            || images.len() > 32
        {
            return Err(Status::invalid_argument(
                "prepared-input v1 requires at most 32 images, n=1, no session/tools/hidden states",
            ));
        }
        let bytes =
            serde_json::to_vec(body).map_err(|e| Status::invalid_argument(e.to_string()))?;
        if bytes.len() > Limits::default().manifest_bytes {
            return Err(Status::resource_exhausted(
                "prepared request exceeds byte limit",
            ));
        }
        Ok(Some(PreparedInput {
            images,
            digest: Sha256::digest(bytes).to_vec(),
            deadline: Instant::now() + self.timeout,
            expiry: wall_ms()? + self.timeout.as_millis() as u64,
        }))
    }
}

impl PreparedInput {
    /// Whole-prompt assignment preserves occurrence order and native batching.
    pub(crate) fn hashes(&self) -> Vec<Vec<u8>> {
        vec![self.digest.clone()]
    }

    /// Freeze the workers already chosen by the common EPD policies, then adapt
    /// their protocol metadata without making another scheduling decision.
    pub(crate) fn build(
        self,
        generation: &mut sglang_proto::GenerateRequest,
        workers: &WorkerSelection,
    ) -> Result<EncodeDispatchPlan, Status> {
        let prefill = workers
            .prefill_worker()
            .ok_or_else(|| Status::internal("missing P"))?;
        let decode = workers
            .decode_worker()
            .ok_or_else(|| Status::internal("missing D"))?;
        let encoder = &workers
            .encode_assignments()
            .and_then(|a| a.first())
            .ok_or_else(|| Status::unavailable("missing E"))?
            .worker;
        let p = capability(prefill.as_ref())
            .ok_or_else(|| Status::unavailable("missing P capability"))?;
        let d = capability(decode.as_ref())
            .ok_or_else(|| Status::unavailable("missing D capability"))?;
        let e = capability(encoder.as_ref())
            .ok_or_else(|| Status::unavailable("missing E capability"))?;
        if !compatible(encoder.as_ref(), prefill.as_ref())
            || !compatible(encoder.as_ref(), decode.as_ref())
            || p.kv_backend != d.kv_backend
            || p.role != "prefill"
            || d.role != "decode"
        {
            return Err(Status::failed_precondition(
                "selected prepared-input contracts differ",
            ));
        }
        let remaining = self
            .deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| Status::deadline_exceeded("prepared request expired"))?;
        let attempt = Uuid::now_v7().to_string();
        let identity = proto::Identity {
            logical_request_id: generation.request_id.clone(),
            attempt_id: attempt.clone(),
            branch_id: 0,
        };
        let operation = |target, role: &str| proto::OperationRef {
            identity: Some(identity.clone()),
            target,
            operation_id: format!("{attempt}-{role}"),
            request_digest: self.digest.clone(),
            plan_digest: self.digest.clone(),
        };
        let params = generation
            .disaggregated_params
            .as_ref()
            .ok_or_else(|| Status::internal("missing shared PD rendezvous"))?;
        let mut binding = proto::InputBinding {
            decode: Some(operation(d.instance.clone(), "d")),
            prefill: p.instance.clone(),
            prefill_rid: format!(
                "{}.{attempt}-p",
                e.instance
                    .as_ref()
                    .ok_or_else(|| Status::unavailable("missing E instance"))?
                    .generation
            ),
            bootstrap: Some(proto::Bootstrap {
                host: params.bootstrap_host.clone(),
                port: params.bootstrap_port as u32,
                room: i64::from(params.bootstrap_room),
            }),
            model_revision: p.model_revision,
            processor_contract: p.processor_contract,
            layout_contract: LAYOUT.into(),
            media_routing_labels: vec![],
        };
        let plan = proto::ExternalEncodePlan {
            receiver_rid: binding.prefill_rid.clone(),
            encoder_receive_endpoints: vec![e.receive_endpoint],
            image_counts: vec![self.images.len() as u32],
            parts: vec![proto::EncodePart {
                native_part_rid: format!("{}_local_part_0", binding.prefill_rid),
                part_idx: 0,
                ordered_item_ids: (0..self.images.len() as u32).collect(),
            }],
        };
        let mut digest = Sha256::new();
        digest.update(binding.encode_to_vec());
        digest.update(plan.encode_to_vec());
        let plan_digest = digest.finalize().to_vec();
        binding
            .decode
            .as_mut()
            .ok_or_else(|| Status::internal("missing D operation"))?
            .plan_digest
            .clone_from(&plan_digest);
        let mut p_op = operation(p.instance, "p");
        p_op.plan_digest.clone_from(&plan_digest);

        let budget = proto::Budget {
            original_expiry_unix_ms: self.expiry,
            remaining_timeout_ms: remaining.as_millis() as u64,
            estimated_work: 1,
            context_bytes: (2 * Limits::default().manifest_bytes) as u64,
        };
        generation.prepared_control = proto::PrefillStart {
            version: VERSION,
            operation: Some(p_op),
            budget: Some(budget),
            binding: Some(binding.clone()),
            generation: None,
            encode_plan: Some(plan.clone()),
        }
        .encode_to_vec();
        generation.mm_inputs = None;
        generation.extra_mm_inputs.clear();
        let request = smg_grpc_client::sglang_encoder_proto::EncodeRequest {
            req_id: plan.parts[0].native_part_rid.clone(),
            mm_items: self.images,
            num_parts: 1,
            part_idx: 0,
            prefill_host: params.bootstrap_host.clone(),
            embedding_port: vec![],
        };
        Ok(EncodeDispatchPlan::sglang(
            Arc::clone(encoder),
            request,
            remaining,
        ))
    }
}

/// Wall time is used only to prevent a delayed RPC extending the original budget.
fn wall_ms() -> Result<u64, Status> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Status::unavailable("invalid clock"))?
        .as_millis() as u64)
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

/// Split one frozen control into P and D requests before either RPC starts.
pub(crate) fn split_requests(
    p: &mut sglang_proto::GenerateRequest,
    d: &mut sglang_proto::GenerateRequest,
) -> Result<Option<(proto::InputBinding, Duration)>, Status> {
    if p.prepared_control.is_empty() {
        return Ok(None);
    }
    let control = proto::PrefillStart::decode(p.prepared_control.as_slice())
        .map_err(|_| Status::invalid_argument("invalid prepared control"))?;
    let binding = control
        .binding
        .ok_or_else(|| Status::invalid_argument("missing prepared binding"))?;
    let budget = control
        .budget
        .ok_or_else(|| Status::invalid_argument("missing prepared deadline"))?;
    let remaining = budget
        .remaining_timeout_ms
        .min(budget.original_expiry_unix_ms.saturating_sub(wall_ms()?));
    if remaining == 0 {
        return Err(Status::deadline_exceeded("prepared request expired"));
    }
    p.request_id.clone_from(&binding.prefill_rid);
    if let Some(params) = &mut p.sampling_params {
        params.max_new_tokens = Some(1);
    }
    d.request_id.clone_from(
        &binding
            .decode
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing D binding"))?
            .operation_id,
    );
    if let Some(tokens) = &mut d.tokenized {
        tokens.input_ids.clear();
        tokens.original_text.clear();
    }
    Ok(Some((binding, Duration::from_millis(remaining))))
}

/// A malformed, stale or missing layout is fatal; raw-reference fallback is forbidden.
pub(crate) fn bind_input(
    request: &mut sglang_proto::GenerateRequest,
    binding: &proto::InputBinding,
    bytes: Vec<u8>,
) -> Result<(), Status> {
    if bytes.len() > Limits::default().manifest_bytes {
        return Err(Status::resource_exhausted("prepared input exceeds limit"));
    }
    let input = proto::PreparedDecodeInput::decode(bytes.as_slice())
        .map_err(|_| Status::data_loss("malformed P layout"))?;
    smg_grpc_client::sglang_managed::validate_manifest(&input, binding, Limits::default())?;
    request.prepared_input = bytes;
    Ok(())
}

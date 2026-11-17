//! Prepared-input handoff on the ordinary Generate RPC and request registry.
use std::{
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use prost::Message;
use smg_grpc_client::{
    sglang_managed::{
        budget::Deadline, clock, proto, validate_binding, validate_external_plan,
        validate_manifest, Limits, VERSION,
    },
    sglang_proto as sg,
};
use tonic::Status;

use super::State;
use crate::BoxStream;

/// Register before native submission. Only P may export layout, and D must bind
/// that exact layout; neither side may fall back to media preprocessing.
pub(super) fn generate(
    state: &Arc<State>,
    mut request: sg::GenerateRequest,
) -> Result<BoxStream<sg::GenerateResponse>, Status> {
    let config = state
        .managed_config
        .as_ref()
        .ok_or_else(|| Status::failed_precondition("worker does not support prepared inputs"))?;
    clock::check(config.max_clock_error_ms)?;
    let runtime = state
        .prepared
        .get()
        .ok_or_else(|| Status::unavailable("prepared runtime is not ready"))?;
    if !runtime.ready() {
        return Err(Status::unavailable("prepared runtime is unhealthy"));
    }
    let limits = Limits::default();
    if request.prepared_control.len() + request.prepared_input.len() > limits.manifest_bytes {
        return Err(Status::resource_exhausted(
            "prepared metadata exceeds byte limit",
        ));
    }
    let control = proto::PrefillStart::decode(request.prepared_control.as_slice())
        .map_err(|_| Status::invalid_argument("malformed prepared control"))?;
    let binding = control
        .binding
        .ok_or_else(|| Status::invalid_argument("missing prepared binding"))?;
    validate_binding(&binding)?;
    if control.generation.is_some()
        || control.version != VERSION
        || binding.model_revision != config.contract.model_revision
        || binding.processor_contract != config.contract.processor_contract
    {
        return Err(Status::failed_precondition("prepared contract mismatch"));
    }
    if request.mm_inputs.is_some()
        || !request.extra_mm_inputs.is_empty()
        || !request.input_embeds.is_empty()
        || request.media_identity.is_some()
        || request.sampling_params.as_ref().is_none_or(|p| p.n > 1)
        || !request.lora_id.is_empty()
        || request.return_hidden_states
    {
        return Err(Status::invalid_argument(
            "prepared inputs require n=1 without raw media, LoRA or hidden states",
        ));
    }
    let (operation, external, manifest) = if config.role == "prefill" {
        if !request.prepared_input.is_empty() || request.request_id != binding.prefill_rid {
            return Err(Status::invalid_argument("invalid prepared prefill request"));
        }
        let operation = control
            .operation
            .ok_or_else(|| Status::invalid_argument("missing prefill operation"))?;
        let decode = binding
            .decode
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing decode binding"))?;
        if operation.identity != decode.identity
            || operation.request_digest != decode.request_digest
            || operation.plan_digest != decode.plan_digest
            || operation.target != binding.prefill
        {
            return Err(Status::failed_precondition(
                "prefill operation differs from binding",
            ));
        }
        let plan = control
            .encode_plan
            .ok_or_else(|| Status::invalid_argument("missing encode plan"))?;
        let images = plan
            .image_counts
            .iter()
            .try_fold(0usize, |n, count| n.checked_add(*count as usize))
            .ok_or_else(|| Status::invalid_argument("image count overflow"))?;
        validate_external_plan(&plan, images)?;
        (operation, Some(plan), None)
    } else {
        let input = proto::PreparedDecodeInput::decode(request.prepared_input.as_slice())
            .map_err(|_| Status::invalid_argument("missing or malformed prepared input"))?;
        validate_manifest(&input, &binding, limits)?;
        let operation = binding
            .decode
            .clone()
            .ok_or_else(|| Status::invalid_argument("missing decode operation"))?;
        if request.request_id != operation.operation_id {
            return Err(Status::invalid_argument(
                "decode request id differs from binding",
            ));
        }
        (operation, None, Some(Box::new(input)))
    };
    if operation.target.as_ref().is_none_or(|target| {
        target.worker_id != config.worker_id || target.generation != config.generation
    }) {
        return Err(Status::failed_precondition(
            "stale prepared worker instance",
        ));
    }
    let budget = control
        .budget
        .ok_or_else(|| Status::invalid_argument("missing prepared deadline"))?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Status::unavailable("invalid clock"))?
        .as_millis() as u64;
    let deadline = Deadline::admit(&budget, now, Instant::now(), 0, 0, config.max_duration_ms)?;
    let (registration, cancel) = state.shared.registry.register(&request.request_id)?;
    request.prepared_control.clear();
    request.prepared_input.clear();
    runtime.submit(
        operation,
        binding,
        request,
        external,
        manifest,
        deadline,
        registration,
        cancel,
    )
}

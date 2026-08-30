//! Connection mode detection step.
//!
//! Determines whether a worker communicates via HTTP or gRPC.
//! This step only answers "HTTP or gRPC?" — backend runtime detection
//! (sglang vs vllm vs trtllm) is handled by the separate DetectBackendStep.

use async_trait::async_trait;
use tracing::debug;
use wfaas::{StepExecutor, StepId, StepResult, WorkflowContext, WorkflowError, WorkflowResult};

use crate::{
    worker::{ConnectionMode, WorkerMode},
    workflow::{
        data::{WorkerKind, WorkerWorkflowData},
        steps::util::{try_grpc_reachable, try_http_reachable, try_smg_worker_reachable},
    },
};

/// Step 1: Detect connection mode (HTTP vs gRPC).
///
/// Explicit URL schemes are honored. For bare host:port URLs, probes both
/// protocols in parallel and HTTP takes priority if both succeed.
/// Does NOT detect backend runtime — that's handled by DetectBackendStep.
pub struct DetectConnectionModeStep;

#[async_trait]
impl StepExecutor<WorkerWorkflowData> for DetectConnectionModeStep {
    async fn execute(
        &self,
        context: &mut WorkflowContext<WorkerWorkflowData>,
    ) -> WorkflowResult<StepResult> {
        if context.data.worker_kind != Some(WorkerKind::Local) {
            return Ok(StepResult::Skip);
        }

        let config = &context.data.config;
        let app_context = context
            .data
            .app_context
            .as_ref()
            .ok_or_else(|| WorkflowError::ContextValueNotFound("app_context".to_string()))?;

        debug!(
            "Detecting connection mode for {} (timeout: {:?}s, max_attempts: {})",
            config.url, config.health.timeout_secs, config.max_connection_attempts
        );

        let url = config.url.clone();
        let timeout = config
            .health
            .timeout_secs
            .unwrap_or(app_context.router_config.health_check.timeout_secs);
        let client = &app_context.client;

        // A two-tier worker exposes SMG's internal gRPC service rather than an
        // engine-specific health service. Its explicit identity is the
        // protocol discriminator; do not race engine probes to infer it.
        if config.worker_mode == WorkerMode::Smg {
            if let Some(mode) = ConnectionMode::from_url(&url) {
                if mode != ConnectionMode::Grpc {
                    return Err(WorkflowError::StepFailed {
                        step_id: StepId::new("detect_connection_mode"),
                        message: format!(
                            "SMG worker {} must use a grpc:// or grpcs:// URL, got {mode}",
                            config.url
                        ),
                    });
                }
            }
            let control_url = config.control_url.as_deref().unwrap_or(&url);
            if let Some(mode) = ConnectionMode::from_url(control_url) {
                if mode != ConnectionMode::Grpc {
                    return Err(WorkflowError::StepFailed {
                        step_id: StepId::new("detect_connection_mode"),
                        message: format!(
                            "SMG Worker control endpoint {control_url} must use grpc:// or grpcs://, got {mode}"
                        ),
                    });
                }
            }
            try_smg_worker_reachable(control_url, timeout)
                .await
                .map_err(|error| WorkflowError::StepFailed {
                    step_id: StepId::new("detect_connection_mode"),
                    message: format!(
                        "SMG Worker control-plane handshake failed for {control_url}: {error}"
                    ),
                })?;
            debug!(
                "{} identified as a ready SMG worker over gRPC (inference endpoint {})",
                control_url, config.url
            );
            context.data.connection_mode = Some(ConnectionMode::Grpc);
            return Ok(StepResult::Success);
        }

        if let Some(connection_mode) = ConnectionMode::from_url(&url) {
            let result = match connection_mode {
                ConnectionMode::Http => try_http_reachable(&url, timeout, client).await,
                ConnectionMode::Grpc => try_grpc_reachable(&url, timeout).await,
                // SMG binds the ZMQ sockets and the engine dials in, so there is
                // no endpoint to probe before binding; an explicit ipc:// URL is
                // taken as reachable.
                ConnectionMode::Zmq => Ok(()),
            };

            match result {
                Ok(()) => {
                    debug!(
                        "{} explicitly configured as {}",
                        config.url, connection_mode
                    );
                    context.data.connection_mode = Some(connection_mode);
                    return Ok(StepResult::Success);
                }
                Err(err) => {
                    return Err(WorkflowError::StepFailed {
                        step_id: StepId::new("detect_connection_mode"),
                        message: format!(
                            "{connection_mode} health check failed for explicitly configured worker URL {}: {}",
                            config.url, err
                        ),
                    });
                }
            }
        }

        let (http_result, grpc_result) = tokio::join!(
            try_http_reachable(&url, timeout, client),
            try_grpc_reachable(&url, timeout)
        );

        let connection_mode = match (http_result, grpc_result) {
            (Ok(()), _) => {
                debug!("{} detected as HTTP", config.url);
                ConnectionMode::Http
            }
            (_, Ok(())) => {
                debug!("{} detected as gRPC", config.url);
                ConnectionMode::Grpc
            }
            (Err(http_err), Err(grpc_err)) => {
                return Err(WorkflowError::StepFailed {
                    step_id: StepId::new("detect_connection_mode"),
                    message: format!(
                        "Both HTTP and gRPC health checks failed for {}: HTTP: {}, gRPC: {}",
                        config.url, http_err, grpc_err
                    ),
                });
            }
        };

        context.data.connection_mode = Some(connection_mode);
        Ok(StepResult::Success)
    }

    fn is_retryable(&self, _error: &WorkflowError) -> bool {
        true
    }
}

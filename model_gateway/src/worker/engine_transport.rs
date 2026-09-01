//! Worker-local engine transports.
//!
//! The public Router-to-Worker boundary remains `WorkerInference` gRPC. This
//! module adapts the existing same-host ZMQ engine client to that stable wire,
//! so colocated vLLM and TokenSpeed schedulers can avoid a Python/gRPC hop.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use smg_grpc_client::{
    worker_inference::{
        from_vllm_response, into_tokenspeed_request, into_vllm_request, EngineTransport,
        EngineTransportStream,
    },
    worker_inference_proto as proto,
};
use tokio::sync::oneshot;
use tonic::Status;

use crate::{
    routers::grpc::{
        proto_wrapper::ProtoGenerateRequest,
        zmq_client::{connect_for_worker, ZmqEngineClient, ZmqGenerateStream},
    },
    worker::RuntimeType,
};

type ActiveRequests = Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>;

/// Same-host ZMQ IPC implementation of the Worker-local engine boundary.
#[derive(Clone)]
pub struct ZmqWorkerTransport {
    client: ZmqEngineClient,
    runtime: RuntimeType,
    active: ActiveRequests,
}

impl ZmqWorkerTransport {
    /// Bind the Worker-side ZMQ sockets and await the engine handshake.
    pub async fn connect(
        base_url: &str,
        model_id: String,
        runtime: RuntimeType,
        handshake_override: Option<&str>,
        engine_count: usize,
    ) -> Result<Self, String> {
        if !matches!(runtime, RuntimeType::Vllm | RuntimeType::TokenSpeed) {
            return Err(format!(
                "Worker ZMQ transport supports only vllm and tokenspeed, not {runtime}"
            ));
        }
        if engine_count == 0 {
            return Err("Worker ZMQ engine_count must be positive".to_string());
        }
        let client = connect_for_worker(
            base_url,
            model_id,
            runtime,
            handshake_override,
            engine_count,
        )
        .await?;
        Ok(Self {
            client,
            runtime,
            active: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

struct ZmqStreamState {
    request_id: String,
    stream: ZmqGenerateStream,
    cancel: oneshot::Receiver<()>,
    active: ActiveRequests,
    done: bool,
}

impl Drop for ZmqStreamState {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&self.request_id);
        }
        // Dropping `stream` before its terminal frame triggers the existing
        // engine-zmq-client auto-abort path.
    }
}

#[tonic::async_trait]
impl EngineTransport for ZmqWorkerTransport {
    async fn generate(
        &self,
        request: proto::GenerateRequest,
    ) -> Result<EngineTransportStream, Status> {
        let request_id = request.request_id.clone();
        let engine_request = match self.runtime {
            RuntimeType::Vllm => ProtoGenerateRequest::Vllm(Box::new(into_vllm_request(request)?)),
            RuntimeType::TokenSpeed => {
                ProtoGenerateRequest::TokenSpeed(Box::new(into_tokenspeed_request(request)))
            }
            other => {
                return Err(Status::failed_precondition(format!(
                    "Worker ZMQ transport is unavailable for {other}"
                )))
            }
        };
        let stream = self.client.generate(engine_request).await?;
        let (cancel_tx, cancel_rx) = oneshot::channel();
        self.active
            .lock()
            .map_err(|_| Status::internal("Worker ZMQ request registry is poisoned"))?
            .insert(request_id.clone(), cancel_tx);

        let state = ZmqStreamState {
            request_id,
            stream,
            cancel: cancel_rx,
            active: Arc::clone(&self.active),
            done: false,
        };
        let stream = futures::stream::unfold(state, |mut state| async move {
            if state.done {
                return None;
            }
            tokio::select! {
                _ = &mut state.cancel => None,
                item = state.stream.next() => match item {
                    Some(Ok(response)) => {
                        let response = from_vllm_response(&state.request_id, response);
                        Some((Ok(response), state))
                    }
                    Some(Err(error)) => {
                        state.done = true;
                        Some((Err(error), state))
                    }
                    None => None,
                },
            }
        });
        Ok(Box::pin(stream))
    }

    async fn abort(&self, request: proto::AbortRequest) -> Result<proto::AbortResponse, Status> {
        let sender = self
            .active
            .lock()
            .map_err(|_| Status::internal("Worker ZMQ request registry is poisoned"))?
            .remove(&request.request_id);
        if let Some(sender) = sender {
            let _ = sender.send(());
        }
        // Abort is idempotent: an already-finished or already-cancelled
        // request is a successful no-op.
        Ok(proto::AbortResponse {
            success: true,
            message: String::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use engine_zmq_client::{
        mock_engine::{connect_to_frontend, default_ready_response, EngineInbound},
        EngineId,
    };
    use futures::StreamExt;

    use super::*;
    use crate::routers::grpc::zmq_client::zmq_handshake_address;

    #[tokio::test]
    async fn vllm_zmq_transport_maps_generate_and_propagates_abort() {
        let dir = tempfile::tempdir().expect("temporary socket directory");
        let base_url = format!("ipc://{}", dir.path().join("worker").display());
        let handshake = zmq_handshake_address(&base_url, None).expect("handshake address");

        let (transport, engine) = tokio::join!(
            ZmqWorkerTransport::connect(
                &base_url,
                "org/model".to_string(),
                RuntimeType::Vllm,
                None,
                1,
            ),
            connect_to_frontend(
                &handshake,
                EngineId::from_engine_index(0),
                default_ready_response(),
            ),
        );
        let transport = transport.expect("Worker ZMQ transport");
        let mut engine = engine.expect("mock vLLM engine");

        let request_id = "worker-zmq-1".to_string();
        let mut stream = transport
            .generate(proto::GenerateRequest {
                request_id: request_id.clone(),
                tokenized: Some(proto::TokenizedInput {
                    original_text: "hello".to_string(),
                    input_ids: vec![1, 2, 3],
                }),
                sampling_params: Some(proto::SamplingParams {
                    max_new_tokens: Some(4),
                    ..Default::default()
                }),
                stream: true,
                ..Default::default()
            })
            .await
            .expect("generate stream");

        let inbound = tokio::time::timeout(Duration::from_secs(2), engine.recv())
            .await
            .expect("engine request timeout")
            .expect("engine request");
        let EngineInbound::Add(request) = inbound else {
            panic!("expected add request, got {inbound:?}");
        };
        assert_eq!(request.request_id, request_id);
        assert_eq!(request.prompt_token_ids, Some(vec![1, 2, 3]));
        assert_eq!(request.sampling_params.expect("sampling").max_tokens, 4);

        transport
            .abort(proto::AbortRequest {
                request_id: request_id.clone(),
                reason: "test cancellation".to_string(),
            })
            .await
            .expect("abort");
        assert!(stream.next().await.is_none(), "cancelled stream must close");

        let inbound = tokio::time::timeout(Duration::from_secs(2), engine.recv())
            .await
            .expect("engine abort timeout")
            .expect("engine abort");
        let EngineInbound::Abort(request_ids) = inbound else {
            panic!("expected abort request, got {inbound:?}");
        };
        assert_eq!(request_ids, vec![request_id]);
    }
}

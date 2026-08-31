//! Python lifecycle binding for the Rust WorkerControl gRPC server.
//!
//! Rust owns transport and serves discovery and health from in-memory state.
//! Python only drives coarse lifecycle transitions; request-time health polls
//! never cross the GIL.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError},
        Arc, Mutex,
    },
    thread,
    time::{Duration, SystemTime},
};

use pyo3::{
    exceptions::{PyRuntimeError, PyTimeoutError, PyValueError},
    prelude::*,
};
use smg_grpc_client::worker_proto::{
    self as proto,
    worker_control_server::{WorkerControl, WorkerControlServer as TonicWorkerControlServer},
};
use smg_grpc_client::{
    worker_inference::SglangWorkerInference,
    worker_inference_proto::worker_inference_server::WorkerInferenceServer,
};
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Request, Response, Status};

#[derive(Clone)]
struct HealthSnapshot {
    state: proto::WorkerHealthState,
    message: String,
}

struct BridgeState {
    identity: proto::WorkerIdentity,
    capabilities: proto::WorkerCapabilities,
    topology: proto::WorkerTopology,
    health: Arc<Mutex<HealthSnapshot>>,
}

#[derive(Clone)]
struct PythonWorkerControl {
    state: Arc<BridgeState>,
}

impl PythonWorkerControl {
    fn new(config: BridgeConfig) -> Self {
        let engine = proto::EngineCapability {
            engine_type: config.engine_type.clone(),
            engine_version: config.engine_version,
            model_ids: config.model_ids.clone(),
            features: config.features.clone(),
        };
        Self {
            state: Arc::new(BridgeState {
                identity: proto::WorkerIdentity {
                    worker_id: config.worker_id.clone(),
                    instance_id: config.instance_id,
                    hostname: config.hostname,
                    zone: config.zone,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    started_at: Some(now()),
                    labels: [("role".to_string(), "smg-worker".to_string())].into(),
                },
                capabilities: proto::WorkerCapabilities {
                    api_major: 1,
                    api_minor: 0,
                    features: config.features,
                    engines: vec![engine],
                    max_concurrent_requests: config.max_concurrent_requests,
                    attributes: HashMap::new(),
                },
                topology: proto::WorkerTopology {
                    worker_id: config.worker_id,
                    topology_version: 1,
                    engines: vec![proto::EngineEndpoint {
                        engine_id: "python-engine-0".to_string(),
                        engine_type: config.engine_type,
                        endpoint: config.engine_endpoint,
                        model_ids: config.model_ids,
                        replica_group: String::new(),
                        data_parallel_rank: None,
                        tensor_parallel_rank: None,
                        pipeline_parallel_rank: None,
                        attributes: HashMap::new(),
                    }],
                    observed_at: Some(now()),
                },
                health: config.health,
            }),
        }
    }
}

#[tonic::async_trait]
impl WorkerControl for PythonWorkerControl {
    async fn get_identity(
        &self,
        _request: Request<proto::GetIdentityRequest>,
    ) -> Result<Response<proto::GetIdentityResponse>, Status> {
        Ok(Response::new(proto::GetIdentityResponse {
            identity: Some(self.state.identity.clone()),
        }))
    }

    async fn get_capabilities(
        &self,
        _request: Request<proto::GetCapabilitiesRequest>,
    ) -> Result<Response<proto::GetCapabilitiesResponse>, Status> {
        Ok(Response::new(proto::GetCapabilitiesResponse {
            capabilities: Some(self.state.capabilities.clone()),
        }))
    }

    async fn get_health(
        &self,
        request: Request<proto::GetHealthRequest>,
    ) -> Result<Response<proto::GetHealthResponse>, Status> {
        let health = self
            .state
            .health
            .lock()
            .map_err(|_| Status::internal("Worker health state is poisoned"))?
            .clone();
        let components = request
            .into_inner()
            .include_components
            .then(|| proto::ComponentHealth {
                component_id: "python-engine-0".to_string(),
                state: health.state.into(),
                message: health.message.clone(),
                checked_at: Some(now()),
            })
            .into_iter()
            .collect();
        Ok(Response::new(proto::GetHealthResponse {
            state: health.state.into(),
            message: health.message,
            checked_at: Some(now()),
            components,
        }))
    }

    async fn get_topology(
        &self,
        _request: Request<proto::GetTopologyRequest>,
    ) -> Result<Response<proto::GetTopologyResponse>, Status> {
        Ok(Response::new(proto::GetTopologyResponse {
            topology: Some(self.state.topology.clone()),
        }))
    }
}

fn now() -> prost_types::Timestamp {
    SystemTime::now().into()
}

fn parse_health_state(state: &str) -> PyResult<proto::WorkerHealthState> {
    match state.to_ascii_lowercase().as_str() {
        "starting" => Ok(proto::WorkerHealthState::Starting),
        "serving" => Ok(proto::WorkerHealthState::Serving),
        "degraded" => Ok(proto::WorkerHealthState::Degraded),
        "draining" => Ok(proto::WorkerHealthState::Draining),
        "not_serving" | "not-serving" => Ok(proto::WorkerHealthState::NotServing),
        _ => Err(PyValueError::new_err(format!(
            "unknown worker health state {state:?}"
        ))),
    }
}

struct BridgeConfig {
    worker_id: String,
    instance_id: String,
    hostname: String,
    zone: String,
    engine_type: String,
    engine_version: String,
    engine_endpoint: String,
    model_ids: Vec<String>,
    features: Vec<String>,
    max_concurrent_requests: u32,
    health: Arc<Mutex<HealthSnapshot>>,
}

/// Rust-owned WorkerControl server with lifecycle driven from Python.
#[pyclass(name = "WorkerControlServer")]
pub struct PyWorkerControlServer {
    address: String,
    health: Arc<Mutex<HealthSnapshot>>,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
    done: Mutex<Option<Receiver<()>>>,
    running: Arc<AtomicBool>,
    last_error: Arc<Mutex<Option<String>>>,
}

#[pymethods]
impl PyWorkerControlServer {
    #[new]
    #[pyo3(signature = (
        bind_address,
        worker_id,
        engine_type,
        model_ids,
        engine_endpoint,
        instance_id = None,
        hostname = None,
        zone = String::new(),
        engine_version = String::new(),
        features = None,
        max_concurrent_requests = 0,
        inference_enabled = false,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        bind_address: String,
        worker_id: String,
        engine_type: String,
        model_ids: Vec<String>,
        engine_endpoint: String,
        instance_id: Option<String>,
        hostname: Option<String>,
        zone: String,
        engine_version: String,
        features: Option<Vec<String>>,
        max_concurrent_requests: u32,
        inference_enabled: bool,
    ) -> PyResult<Self> {
        if worker_id.trim().is_empty() {
            return Err(PyValueError::new_err("worker_id must not be empty"));
        }
        if engine_type.trim().is_empty() {
            return Err(PyValueError::new_err("engine_type must not be empty"));
        }
        if engine_endpoint.trim().is_empty() {
            return Err(PyValueError::new_err("engine_endpoint must not be empty"));
        }
        let bind_address: SocketAddr = bind_address
            .parse()
            .map_err(|error| PyValueError::new_err(format!("invalid bind_address: {error}")))?;
        let health = Arc::new(Mutex::new(HealthSnapshot {
            state: proto::WorkerHealthState::Starting,
            message: "starting".to_string(),
        }));
        let inference = inference_enabled.then(|| InferenceConfig {
            engine_type: engine_type.clone(),
            engine_endpoint: engine_endpoint.clone(),
        });
        let config = BridgeConfig {
            worker_id: worker_id.clone(),
            instance_id: instance_id
                .unwrap_or_else(|| format!("{worker_id}-{:016x}", rand::random::<u64>())),
            hostname: hostname.unwrap_or_else(|| bind_address.ip().to_string()),
            zone,
            engine_type,
            engine_version,
            engine_endpoint,
            model_ids,
            features: features.unwrap_or_else(|| vec!["generate".to_string()]),
            max_concurrent_requests,
            health: Arc::clone(&health),
        };
        py.detach(|| {
            start_server(
                bind_address,
                PythonWorkerControl::new(config),
                health,
                inference,
            )
        })
    }

    #[getter]
    fn address(&self) -> &str {
        &self.address
    }

    #[getter]
    fn running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    #[getter]
    fn last_error(&self) -> PyResult<Option<String>> {
        Ok(lock(&self.last_error)?.clone())
    }

    #[pyo3(signature = (state, message = String::new()))]
    fn set_health(&self, state: &str, message: String) -> PyResult<()> {
        let state = parse_health_state(state)?;
        *lock(&self.health)? = HealthSnapshot { state, message };
        Ok(())
    }

    #[pyo3(signature = (timeout_secs = 5.0))]
    fn stop(&self, py: Python<'_>, timeout_secs: f64) -> PyResult<()> {
        if !timeout_secs.is_finite() || timeout_secs <= 0.0 {
            return Err(PyValueError::new_err(
                "timeout_secs must be finite and positive",
            ));
        }
        if let Some(shutdown) = lock(&self.shutdown)?.take() {
            let _ = shutdown.send(());
        }

        let receiver = lock(&self.done)?.take();
        if let Some(receiver) = receiver {
            let timeout = Duration::from_secs_f64(timeout_secs);
            let (result, receiver) = py.detach(move || {
                let result = receiver.recv_timeout(timeout);
                (result, receiver)
            });
            match result {
                Ok(()) | Err(RecvTimeoutError::Disconnected) => {}
                Err(RecvTimeoutError::Timeout) => {
                    *lock(&self.done)? = Some(receiver);
                    return Err(PyTimeoutError::new_err(
                        "WorkerControl server did not stop before timeout",
                    ));
                }
            }
        }
        if let Some(thread) = lock(&self.thread)?.take() {
            thread
                .join()
                .map_err(|_| PyRuntimeError::new_err("WorkerControl server thread panicked"))?;
        }
        Ok(())
    }
}

impl Drop for PyWorkerControlServer {
    fn drop(&mut self) {
        if let Ok(shutdown) = self.shutdown.get_mut() {
            if let Some(shutdown) = shutdown.take() {
                let _ = shutdown.send(());
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> PyResult<std::sync::MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| PyRuntimeError::new_err("WorkerControl server state is poisoned"))
}

fn start_server(
    bind_address: SocketAddr,
    service: PythonWorkerControl,
    health: Arc<Mutex<HealthSnapshot>>,
    inference: Option<InferenceConfig>,
) -> PyResult<PyWorkerControlServer> {
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let running = Arc::new(AtomicBool::new(false));
    let thread_running = Arc::clone(&running);
    let last_error = Arc::new(Mutex::new(None));
    let thread_last_error = Arc::clone(&last_error);
    let thread = thread::Builder::new()
        .name("smg-python-worker-control".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = started_tx.send(Err(format!("failed to create runtime: {error}")));
                    return;
                }
            };
            let runtime_running = Arc::clone(&thread_running);
            runtime.block_on(async move {
                let inference = match inference {
                    Some(config) if config.engine_type.eq_ignore_ascii_case("sglang") => {
                        match SglangWorkerInference::connect(&config.engine_endpoint).await {
                            Ok(service) => Some(WorkerInferenceServer::new(service)),
                            Err(error) => {
                                let _ = started_tx.send(Err(format!(
                                    "failed to connect WorkerInference adapter to {}: {error}",
                                    config.engine_endpoint
                                )));
                                return;
                            }
                        }
                    }
                    Some(config) => {
                        let _ = started_tx.send(Err(format!(
                            "WorkerInference adapter is not implemented for engine {}",
                            config.engine_type
                        )));
                        return;
                    }
                    None => None,
                };
                let listener = match tokio::net::TcpListener::bind(bind_address).await {
                    Ok(listener) => listener,
                    Err(error) => {
                        let _ =
                            started_tx.send(Err(format!("failed to bind {bind_address}: {error}")));
                        return;
                    }
                };
                let address = match listener.local_addr() {
                    Ok(address) => address,
                    Err(error) => {
                        let _ = started_tx
                            .send(Err(format!("failed to read listener address: {error}")));
                        return;
                    }
                };
                if started_tx.send(Ok(address)).is_err() {
                    return;
                }
                runtime_running.store(true, Ordering::Release);
                let incoming = TcpListenerStream::new(listener);
                if let Err(error) = Server::builder()
                    .add_service(TonicWorkerControlServer::new(service))
                    .add_optional_service(inference)
                    .serve_with_incoming_shutdown(incoming, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                {
                    if let Ok(mut last_error) = thread_last_error.lock() {
                        *last_error = Some(error.to_string());
                    }
                }
            });
            thread_running.store(false, Ordering::Release);
            let _ = done_tx.send(());
        })
        .map_err(|error| {
            PyRuntimeError::new_err(format!("failed to start server thread: {error}"))
        })?;

    let address = started_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| PyRuntimeError::new_err("WorkerControl server exited during startup"))?
        .map_err(PyRuntimeError::new_err)?;
    Ok(PyWorkerControlServer {
        address: address.to_string(),
        health,
        shutdown: Mutex::new(Some(shutdown_tx)),
        thread: Mutex::new(Some(thread)),
        done: Mutex::new(Some(done_rx)),
        running,
        last_error,
    })
}

struct InferenceConfig {
    engine_type: String,
    engine_endpoint: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_health_states() {
        assert_eq!(
            parse_health_state("serving").unwrap(),
            proto::WorkerHealthState::Serving
        );
        assert_eq!(
            parse_health_state("not_serving").unwrap(),
            proto::WorkerHealthState::NotServing
        );
        assert!(parse_health_state("unknown").is_err());
    }

    #[tokio::test]
    async fn lifecycle_health_is_served_from_rust_state() {
        let health = Arc::new(Mutex::new(HealthSnapshot {
            state: proto::WorkerHealthState::Starting,
            message: "warming up".to_string(),
        }));
        let control = PythonWorkerControl::new(BridgeConfig {
            worker_id: "worker-a".to_string(),
            instance_id: "instance-a".to_string(),
            hostname: "node-a".to_string(),
            zone: String::new(),
            engine_type: "sglang".to_string(),
            engine_version: String::new(),
            engine_endpoint: "grpc://worker-a:32000".to_string(),
            model_ids: vec!["model-a".to_string()],
            features: vec!["generate".to_string()],
            max_concurrent_requests: 32,
            health: Arc::clone(&health),
        });

        let starting = control
            .get_health(Request::new(proto::GetHealthRequest {
                include_components: true,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(starting.state(), proto::WorkerHealthState::Starting);
        assert_eq!(starting.components.len(), 1);

        *health.lock().unwrap() = HealthSnapshot {
            state: proto::WorkerHealthState::Serving,
            message: "ready".to_string(),
        };
        let serving = control
            .get_health(Request::new(proto::GetHealthRequest {
                include_components: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(serving.state(), proto::WorkerHealthState::Serving);
        assert!(serving.components.is_empty());
    }
}

//! Bounded managed worker discovery and local reservations, independent of legacy policies.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use sha2::{Digest, Sha256};
use smg_grpc_client::{
    sglang_managed::{
        clock,
        proto::{
            self, managed_encoder_client::ManagedEncoderClient,
            managed_sglang_client::ManagedSglangClient,
        },
        LAYOUT, VERSION,
    },
    sglang_proto::generate_response::Response as NativeResponse,
};
use tonic::{transport::Channel, Status};

use super::{
    executor::AttemptObserver,
    planner::{Contract, EncodeCost, EncoderCandidate, PdCandidate, PdCost, Worker},
    router::Config,
};
use crate::routers::common::pd_admission::PdAdmissionGuard;

struct Reservation {
    pd_admission: Option<PdAdmissionGuard>,
    operation: proto::OperationRef,
    encoder: bool,
    waiting_input: bool,
}

#[derive(Default)]
struct State {
    encoders: HashMap<String, LoadTracker>,
    capacities: HashMap<String, u64>,
    records: HashMap<String, Reservation>,
    generation: u64,
    watched: HashMap<String, (proto::Capabilities, Instant)>,
}

pub struct Registry {
    config: Config,
    state: Mutex<State>,
    channels: HashMap<String, Channel>,
}

impl Registry {
    /// This registry never mutates worker objects or any legacy policy during candidate scoring.
    pub fn new(config: Config) -> Result<Arc<Self>, Status> {
        let channels = config
            .encoders
            .iter()
            .map(|e| &e.endpoint)
            .chain(config.pairs.iter().flat_map(|p| [&p.prefill, &p.decode]))
            .map(|endpoint| {
                Channel::from_shared(endpoint.clone())
                    .map(|channel| (endpoint.clone(), channel.connect_lazy()))
                    .map_err(|error| Status::invalid_argument(error.to_string()))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;
        let registry = Arc::new(Self {
            config,
            state: Mutex::new(State::default()),
            channels,
        });
        let endpoints =
            registry
                .config
                .encoders
                .iter()
                .map(|e| (e.endpoint.clone(), "encoder"))
                .chain(
                    registry.config.pairs.iter().flat_map(|p| {
                        [(p.prefill.clone(), "prefill"), (p.decode.clone(), "decode")]
                    }),
                )
                .collect::<std::collections::HashSet<_>>();
        for (endpoint, role) in endpoints {
            let weak = Arc::downgrade(&registry);
            #[expect(
                clippy::disallowed_methods,
                reason = "one bounded monitor per configured endpoint; weak ownership stops it on router drop"
            )]
            tokio::spawn(async move {
                watch_worker(weak, endpoint, role).await;
            });
        }
        Ok(registry)
    }

    /// Readiness follows compatible live pools, independently of temporary admission saturation.
    pub(crate) fn readiness(&self) -> crate::health::ReadinessSnapshot {
        self.readiness_with_clock(clock::check(self.config.max_clock_error_ms).is_ok())
    }

    /// Clock health is sampled once for the entire pool snapshot.
    fn readiness_with_clock(&self, clock_healthy: bool) -> crate::health::ReadinessSnapshot {
        use crate::health::{ModelReadiness, ReadinessSnapshot};
        let now = Instant::now();
        let age = Duration::from_millis(self.config.snapshot_max_age_ms);
        let endpoints: std::collections::HashSet<_> = self
            .config
            .pairs
            .iter()
            .flat_map(|pair| [&pair.prefill, &pair.decode])
            .chain(self.config.encoders.iter().map(|encoder| &encoder.endpoint))
            .collect();
        let total_workers = endpoints.len();
        let Ok(state) = self.state.lock() else {
            return ReadinessSnapshot::default();
        };
        let live = |endpoint: &str| {
            state
                .watched
                .get(endpoint)
                .filter(|(_, observed)| now.saturating_duration_since(*observed) <= age)
                .map(|(capability, _)| capability)
        };
        let healthy_workers = endpoints
            .iter()
            .filter(|endpoint| live(endpoint).is_some())
            .count();
        let ready = clock_healthy
            && self.config.pairs.iter().any(|pair| {
                let Some(p) = live(&pair.prefill).and_then(|cap| worker(&pair.prefill, cap).ok())
                else {
                    return false;
                };
                let Some(d) = live(&pair.decode).and_then(|cap| worker(&pair.decode, cap).ok())
                else {
                    return false;
                };
                p.contract.accepts_decode(&d.contract)
                    && (self.config.encoders.is_empty()
                        || self.config.encoders.iter().any(|encoder| {
                            live(&encoder.endpoint)
                                .and_then(|cap| worker(&encoder.endpoint, cap).ok())
                                .is_some_and(|e| {
                                    p.contract.accepts_encoder(&e.contract)
                                        && state
                                            .encoders
                                            .get(&instance_key(&e.instance))
                                            .is_some_and(|tracker| tracker.is_available(now, age))
                                })
                        }))
            });
        let prefills: std::collections::HashSet<_> =
            self.config.pairs.iter().map(|p| &p.prefill).collect();
        let decodes: std::collections::HashSet<_> =
            self.config.pairs.iter().map(|p| &p.decode).collect();
        let ready = ready
            && prefills.iter().filter(|url| live(url).is_some()).count()
                >= self.config.readiness.prefill.required_workers()
            && decodes.iter().filter(|url| live(url).is_some()).count()
                >= self.config.readiness.decode.required_workers();
        ReadinessSnapshot {
            workers_ready: ready,
            tokenizers_ready: true,
            healthy_workers,
            total_workers,
            models: vec![ModelReadiness {
                model: self.config.model.clone(),
                ready,
                reason: (!ready).then_some("no_compatible_managed_pipeline"),
                healthy_workers,
                total_workers,
            }],
            ..ReadinessSnapshot::default()
        }
    }

    /// Read pool-level observations; requests never open per-encoder load streams.
    pub fn candidates(
        &self,
        request_id: &str,
        deferred_decode: bool,
    ) -> Result<(Vec<PdCandidate>, Vec<proto::Capabilities>, u64), Status> {
        self.candidates_for(request_id, deferred_decode, None)
    }

    /// Score encoders only for the P/D pair already selected by the shared pipeline.
    pub(crate) fn candidates_for(
        &self,
        request_id: &str,
        deferred_decode: bool,
        selected: Option<(&str, &str)>,
    ) -> Result<(Vec<PdCandidate>, Vec<proto::Capabilities>, u64), Status> {
        clock::check(self.config.max_clock_error_ms)?;
        let now = Instant::now();
        let age = Duration::from_millis(self.config.snapshot_max_age_ms);
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::internal("managed registry poisoned"))?;
        state.generation = state.generation.wrapping_add(1);
        let mut encoders = Vec::new();
        for configured in &self.config.encoders {
            let Some((capability, observed)) = state.watched.get(&configured.endpoint).cloned()
            else {
                continue;
            };
            if now.saturating_duration_since(observed) > age {
                continue;
            }
            let instance = capability
                .instance
                .clone()
                .ok_or_else(|| Status::data_loss("E instance missing"))?;
            let key = instance_key(&instance);
            if !state.encoders.contains_key(&key) && state.encoders.len() >= self.config.max_records
            {
                continue;
            }
            let tracker = state
                .encoders
                .entry(key)
                .or_insert_with(|| LoadTracker::new(instance, self.config.max_records));

            let mut cost = EncodeCost::cold();
            // Stages overlap: GPU occupancy must never become serial CPU waiting.
            let load = tracker.stage_work(now, age);
            let (cpu, gpu, send_bytes) = load.unwrap_or((u64::MAX, u64::MAX, u64::MAX));
            cost.cpu_wait_ms = cost.cpu_wait_ms.saturating_add(cpu);
            cost.gpu_available_ms = cost.gpu_available_ms.saturating_add(gpu);
            cost.send_available_ms = cost
                .send_available_ms
                .saturating_add(send_bytes.div_ceil(cost.bytes_per_ms));
            encoders.push(EncoderCandidate {
                worker: worker(&configured.endpoint, &capability)?,
                cost,
                fresh: load.is_some(),
                has_capacity: tracker.has_capacity(now, age),
                stable_tie_break: stable_tie(
                    request_id,
                    &capability
                        .instance
                        .as_ref()
                        .map(instance_key)
                        .unwrap_or_default(),
                ),
            });
        }
        let mut candidates = Vec::new();
        let mut prefills = Vec::new();
        for configured in &self.config.pairs {
            if selected.is_some_and(|(p, d)| configured.prefill != p || configured.decode != d) {
                continue;
            }
            let Some((prefill, p_observed)) = state.watched.get(&configured.prefill).cloned()
            else {
                continue;
            };
            let Some((decode, d_observed)) = state.watched.get(&configured.decode).cloned() else {
                continue;
            };
            if now.saturating_duration_since(p_observed) > age
                || now.saturating_duration_since(d_observed) > age
            {
                continue;
            }
            let p = worker(&configured.prefill, &prefill)?;
            let d = worker(&configured.decode, &decode)?;
            for (instance, capacity) in [
                (&p.instance, prefill.capacity),
                (&d.instance, decode.capacity),
            ] {
                let key = instance_key(instance);
                if !state.capacities.contains_key(&key)
                    && state.capacities.len() >= self.config.max_records
                {
                    return Err(Status::resource_exhausted("managed instance history full"));
                }
                state.capacities.insert(key, capacity);
            }
            let has_capacity = true;
            let prior = PdCost::default();
            candidates.push(PdCandidate {
                prefill: p,
                decode: d,
                encoders: Vec::new(),
                has_capacity,
                layout_ms: prior.layout_ms,
                manifest_delivery_ms: prior.manifest_delivery_ms,
                decode_registration_ms: prior.decode_registration_ms,
                bind_submit_ms: prior.bind_submit_ms,
                native_pd_ms: prior.native_pd_ms,
            });
            prefills.push(prefill);
        }
        let mut ranked: Vec<_> = candidates.into_iter().zip(prefills).collect();
        ranked.sort_by_cached_key(|(pair, _)| {
            (
                !pair.has_capacity,
                active(&state, &pair.prefill.instance).saturating_add(occupancy(
                    &state,
                    &pair.decode.instance,
                    deferred_decode,
                )),
                stable_tie(
                    request_id,
                    &format!(
                        "{}\0{}",
                        instance_key(&pair.prefill.instance),
                        instance_key(&pair.decode.instance)
                    ),
                ),
            )
        });
        ranked.truncate(self.config.max_pd_candidates);
        // Rank E candidates only for shortlisted P/D pairs, never the complete E×P×D product.
        for (pair, _) in &mut ranked {
            shortlist_encoders(pair, &encoders, self.config.max_encoder_candidates);
        }
        let (candidates, prefills) = ranked.into_iter().unzip();
        Ok((candidates, prefills, state.generation))
    }

    /// One ordered observer publishes a complete capability/load snapshot atomically.
    fn publish(
        &self,
        endpoint: &str,
        capability: proto::Capabilities,
        snapshot: Option<proto::LoadSnapshot>,
    ) -> Result<(), Status> {
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::internal("managed registry poisoned"))?;
        let instance = capability
            .instance
            .as_ref()
            .ok_or_else(|| Status::data_loss("managed instance missing"))?;
        let key = instance_key(instance);
        // Only discovery prunes history, never the request hot path. Replacing
        // an endpoint is not proof that its old work has safely terminated.
        let mut retained: std::collections::HashSet<_> = state
            .watched
            .iter()
            .filter(|(url, _)| url.as_str() != endpoint)
            .filter_map(|(_, (cap, _))| cap.instance.as_ref().map(instance_key))
            .chain(
                state
                    .records
                    .values()
                    .filter_map(|r| r.operation.target.as_ref().map(instance_key)),
            )
            .chain(
                state
                    .encoders
                    .iter()
                    .filter(|(_, tracker)| !tracker.pending.is_empty())
                    .map(|(key, _)| key.clone()),
            )
            .collect();
        retained.insert(key.clone());
        if let Some(snapshot) = snapshot {
            if let Some(tracker) = state.encoders.get_mut(&key) {
                tracker.observe(snapshot, now)?;
            } else {
                let mut tracker = LoadTracker::new(instance.clone(), self.config.max_records);
                tracker.observe(snapshot, now)?;
                if state
                    .encoders
                    .keys()
                    .filter(|key| retained.contains(*key))
                    .count()
                    >= self.config.max_records
                {
                    return Err(Status::resource_exhausted("managed instance history full"));
                }
                state.encoders.insert(key, tracker);
            }
        }
        state.encoders.retain(|key, _| retained.contains(key));
        state.capacities.retain(|key, _| retained.contains(key));
        state
            .watched
            .insert(endpoint.to_string(), (capability, now));
        Ok(())
    }

    /// Reserve all selected operations atomically before any control request can be sent.
    /// Preserve the standard PD claim through disconnect until native completion or cleanup.
    pub(crate) fn retain_pd_admission(
        &self,
        operation: &proto::OperationRef,
        admission: Option<PdAdmissionGuard>,
    ) -> Result<(), Status> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::internal("managed registry poisoned"))?;
        let record = state
            .records
            .get_mut(&operation.operation_id)
            .filter(|record| record.operation == *operation)
            .ok_or_else(|| Status::internal("missing decode reservation"))?;
        record.pd_admission = admission;
        Ok(())
    }

    pub fn reserve(
        &self,
        operations: &[(proto::OperationRef, bool)],
        work: u64,
        waiting_decode: Option<&str>,
    ) -> Result<(), Status> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::internal("managed registry poisoned"))?;
        if state.records.len().saturating_add(operations.len()) > self.config.max_records
            || operations
                .iter()
                .any(|(op, _)| state.records.contains_key(&op.operation_id))
        {
            return Err(Status::resource_exhausted(
                "managed local reservations full/conflicting",
            ));
        }
        for (operation, encoder) in operations {
            if *encoder {
                let target = operation
                    .target
                    .as_ref()
                    .ok_or_else(|| Status::invalid_argument("missing E target"))?;
                let tracker = state
                    .encoders
                    .get_mut(&instance_key(target))
                    .ok_or_else(|| Status::unavailable("E snapshot disappeared"))?;
                if !tracker.has_capacity(
                    Instant::now(),
                    Duration::from_millis(self.config.snapshot_max_age_ms),
                ) {
                    return Err(Status::resource_exhausted(
                        "E capacity changed during planning",
                    ));
                }
                tracker.reserve(operation.operation_id.clone(), work)?;
            }
        }
        for (operation, encoder) in operations {
            state.records.insert(
                operation.operation_id.clone(),
                Reservation {
                    pd_admission: None,
                    operation: operation.clone(),
                    encoder: *encoder,
                    waiting_input: waiting_decode == Some(operation.operation_id.as_str()),
                },
            );
        }
        Ok(())
    }

    /// Capabilities and load must come from the same current instance and a synchronized clock.
    async fn probe(
        &self,
        endpoint: &str,
        role: &str,
    ) -> Result<(proto::Capabilities, Option<proto::LoadSnapshot>), Status> {
        let started = wall_ms()?;
        let query = async {
            let channel = self.channel(endpoint)?;
            let (capability, snapshot) = if role == "encoder" {
                let mut client = ManagedEncoderClient::new(channel);
                let capability = client
                    .get_capabilities(proto::CapabilityQuery {})
                    .await?
                    .into_inner();
                let instance = capability
                    .instance
                    .clone()
                    .ok_or_else(|| Status::data_loss("E lacks instance"))?;
                let snapshot = client
                    .watch_load(instance)
                    .await?
                    .into_inner()
                    .message()
                    .await?
                    .ok_or_else(|| Status::unavailable("E load stream ended"))?;
                (capability, Some(snapshot))
            } else {
                (
                    ManagedSglangClient::new(channel)
                        .get_capabilities(proto::CapabilityQuery {})
                        .await?
                        .into_inner(),
                    None,
                )
            };
            let finished = wall_ms()?;
            if capability.version != VERSION
                || capability.role != role
                || !capability.ready
                || capability.layout_contract != LAYOUT
                || capability.model_revision != self.config.model_revision
                || capability.processor_contract != self.config.processor_contract
                || capability.capacity == 0
                || capability
                    .wall_time_unix_ms
                    .saturating_add(self.config.max_clock_error_ms)
                    < started
                || capability.wall_time_unix_ms
                    > finished.saturating_add(self.config.max_clock_error_ms)
            {
                return Err(Status::failed_precondition(
                    "managed capability/clock mismatch",
                ));
            }
            Ok((capability, snapshot))
        };
        tokio::time::timeout(Duration::from_millis(self.config.probe_timeout_ms), query)
            .await
            .map_err(|_| Status::unavailable("managed capability probe timed out"))?
    }
}

/// Reconnect bounded pool monitors; E load remains a long-lived ordered stream.
async fn watch_worker(registry: std::sync::Weak<Registry>, endpoint: String, role: &'static str) {
    loop {
        let Some(owner) = registry.upgrade() else {
            return;
        };
        let age = Duration::from_millis(owner.config.snapshot_max_age_ms);
        let interval = age / 3;
        let probe_timeout = Duration::from_millis(owner.config.probe_timeout_ms);
        let result = owner.probe(&endpoint, role).await;
        let capability = match result {
            Ok((capability, snapshot)) => {
                if let Err(error) = owner.publish(&endpoint, capability.clone(), snapshot) {
                    tracing::warn!(role, %error, "managed pool publication refused; retrying");
                    None
                } else {
                    Some(capability)
                }
            }
            Err(error) => {
                tracing::debug!(role, %error, "managed pool probe unavailable");
                None
            }
        };
        drop(owner);
        if let Some(capability) = capability.clone().filter(|_| role == "encoder") {
            let follow = async {
                let channel = registry
                    .upgrade()
                    .ok_or_else(|| Status::cancelled("managed registry dropped"))?
                    .channel(&endpoint)?;
                let mut client = ManagedEncoderClient::new(channel);
                let mut stream = tokio::time::timeout(
                    probe_timeout,
                    client.watch_load(
                        capability
                            .instance
                            .clone()
                            .ok_or_else(|| Status::data_loss("E instance missing"))?,
                    ),
                )
                .await
                .map_err(|_| Status::unavailable("E load stream setup expired"))??
                .into_inner();
                loop {
                    let snapshot = tokio::time::timeout(age, stream.message())
                        .await
                        .map_err(|_| Status::unavailable("E load snapshot expired"))??
                        .ok_or_else(|| Status::unavailable("E load stream ended"))?;
                    let Some(owner) = registry.upgrade() else {
                        return Ok::<(), Status>(());
                    };
                    owner.publish(&endpoint, capability.clone(), Some(snapshot))?;
                }
            };
            if let Err(error) = follow.await {
                tracing::debug!(%error, "managed E load stream reconnecting");
            }
        }
        let Some(owner) = registry.upgrade() else {
            return;
        };
        if role == "encoder" || capability.is_none() {
            if let Ok(mut state) = owner.state.lock() {
                state.watched.remove(&endpoint);
            }
        }
        drop(owner);
        tokio::time::sleep(interval.max(Duration::from_millis(50))).await;
    }
}

impl AttemptObserver for Registry {
    fn channel(&self, endpoint: &str) -> Result<Channel, Status> {
        self.channels
            .get(endpoint)
            .cloned()
            .ok_or_else(|| Status::failed_precondition("endpoint is outside managed deployment"))
    }

    /// Reconcile ordered acknowledgements against snapshot coverage without releasing unknown work.
    fn event(&self, event: &proto::ControlEvent) -> Result<(), Status> {
        let operation = event
            .operation
            .as_ref()
            .ok_or_else(|| Status::data_loss("event lacks operation"))?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| Status::internal("managed registry poisoned"))?;
        let Some(reservation) = state.records.get_mut(&operation.operation_id) else {
            return Ok(());
        };
        if &reservation.operation != operation {
            return Err(Status::data_loss("local operation fence mismatch"));
        }
        if matches!(&event.event, Some(proto::control_event::Event::Generation(response))
            if matches!(response.response, Some(NativeResponse::Complete(_))))
        {
            reservation.pd_admission.take();
        }
        // D emits InputBound when its native input is complete.
        // Late/replayed Accepted cannot move an execution owner back into WaitingInput.
        if matches!(event.event, Some(proto::control_event::Event::Stage(stage)) if stage == proto::Stage::InputBound as i32 || stage == proto::Stage::NativeSubmitted as i32)
        {
            reservation.waiting_input = false;
        }
        if reservation.encoder {
            let target = operation
                .target
                .as_ref()
                .ok_or_else(|| Status::data_loss("event lacks target"))?;
            if let Some(tracker) = state.encoders.get_mut(&instance_key(target)) {
                if matches!(event.event, Some(proto::control_event::Event::Stage(stage)) if stage == proto::Stage::Accepted as i32)
                {
                    tracker.accepted(&operation.operation_id, target, event.admission_seq)?;
                }
                if matches!(event.event, Some(proto::control_event::Event::Stage(stage)) if stage == proto::Stage::Quiesced as i32)
                {
                    tracker.rejected_or_quiesced(&operation.operation_id);
                    state.records.remove(&operation.operation_id);
                }
            }
        }
        Ok(())
    }

    /// Only a matching confirmed safe terminal receipt removes an outstanding reservation.
    fn termination(&self, operation: &proto::OperationRef, quiesced: bool) {
        if !quiesced {
            return;
        }
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state
            .records
            .get(&operation.operation_id)
            .is_some_and(|record| record.operation == *operation)
        {
            state.records.remove(&operation.operation_id);
            if let Some(tracker) = operation
                .target
                .as_ref()
                .and_then(|target| state.encoders.get_mut(&instance_key(target)))
            {
                tracker.rejected_or_quiesced(&operation.operation_id);
            }
        }
    }
}

/// Filter each P's representation/transport contract before applying its candidate bound.
fn shortlist_encoders(pair: &mut PdCandidate, encoders: &[EncoderCandidate], limit: usize) {
    pair.encoders = encoders
        .iter()
        .filter(|e| pair.prefill.contract.accepts_encoder(&e.worker.contract))
        .cloned()
        .collect();
    pair.encoders.sort_by_key(|e| {
        (
            !e.has_capacity,
            e.cost.media_ready_ms().unwrap_or(u64::MAX),
            e.stable_tie_break,
        )
    });
    pair.encoders.truncate(limit);
}

/// Instance generations are a separate namespace for occupancy and snapshot coverage.
fn instance_key(instance: &proto::Instance) -> String {
    format!("{}\0{}", instance.worker_id, instance.generation)
}

/// Unknown operations remain in this count until a real quiescence receipt arrives.
fn active(state: &State, instance: &proto::Instance) -> u64 {
    occupancy(state, instance, false)
}

/// Input owners are bounded independently and never masquerade as native execution.
fn occupancy(state: &State, instance: &proto::Instance, waiting_input: bool) -> u64 {
    state
        .records
        .values()
        .filter(|record| {
            record.operation.target.as_ref() == Some(instance)
                && record.waiting_input == waiting_input
        })
        .count() as u64
}

/// Convert the advertised complete contract into the planner's equality key.
fn worker(endpoint: &str, capability: &proto::Capabilities) -> Result<Worker, Status> {
    Ok(Worker {
        instance: capability
            .instance
            .clone()
            .ok_or_else(|| Status::data_loss("managed instance missing"))?,
        control_endpoint: endpoint.to_owned(),
        receive_endpoint: capability.receive_endpoint.clone(),
        contract: Contract {
            model_revision: capability.model_revision.clone(),
            processor: capability.processor_contract.clone(),
            layout: capability.layout_contract.clone(),
            embedding_dtype: capability.embedding_dtype.clone(),
            kv: capability.kv_backend.clone(),
            transfer: capability.embedding_backend.clone(),
            topology: capability.topology_contract.clone(),
        },
    })
}

/// Wall timestamps are for wire expiry only; execution retains a separate monotonic deadline.
pub fn wall_ms() -> Result<u64, Status> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Status::unavailable("invalid wall clock"))?
            .as_millis(),
    )
    .map_err(|_| Status::unavailable("wall timestamp overflow"))
}

/// Stable request/worker hashing is a tie breaker, never a latency estimate.
fn stable_tie(request: &str, worker: &str) -> u64 {
    let digest = Sha256::digest(format!("{request}\0{worker}"));
    let mut bytes = [0; 8];
    bytes.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(bytes)
}

struct Pending {
    work: u64,
    admission_seq: Option<u64>,
}

pub struct LoadTracker {
    instance: proto::Instance,
    snapshot: Option<(proto::LoadSnapshot, Instant)>,
    pending: HashMap<String, Pending>,
    max_pending: usize,
}

impl LoadTracker {
    /// A restarted worker gets a new tracker; old unknown work belongs to its old instance.
    pub fn new(instance: proto::Instance, max_pending: usize) -> Self {
        Self {
            instance,
            snapshot: None,
            pending: HashMap::new(),
            max_pending,
        }
    }

    /// Register in-flight work before sending Encode, including unknown acceptance outcomes.
    pub fn reserve(&mut self, operation_id: String, work: u64) -> Result<(), Status> {
        if self.pending.contains_key(&operation_id) {
            return Err(Status::already_exists("encode reservation exists"));
        }
        if self.pending.len() >= self.max_pending {
            return Err(Status::resource_exhausted("encode reservations full"));
        }
        self.pending.insert(
            operation_id,
            Pending {
                work,
                admission_seq: None,
            },
        );
        Ok(())
    }

    /// Accepted/GetOperation reconcile with an already-arrived covering snapshot too.
    pub fn accepted(
        &mut self,
        operation_id: &str,
        instance: &proto::Instance,
        seq: u64,
    ) -> Result<(), Status> {
        if instance != &self.instance || seq == 0 {
            return Err(Status::failed_precondition(
                "stale admission acknowledgement",
            ));
        }
        if self
            .snapshot
            .as_ref()
            .is_some_and(|(s, _)| seq <= s.covered_admission_seq)
        {
            self.pending.remove(operation_id);
            return Ok(());
        }
        let pending = self
            .pending
            .get_mut(operation_id)
            .ok_or_else(|| Status::not_found("unknown encode reservation"))?;
        if pending.admission_seq.is_some_and(|old| old != seq) {
            return Err(Status::failed_precondition("admission sequence changed"));
        }
        pending.admission_seq = Some(seq);
        Ok(())
    }

    /// Snapshot order and admission coverage cannot regress within an instance.
    pub fn observe(&mut self, snapshot: proto::LoadSnapshot, now: Instant) -> Result<bool, Status> {
        if snapshot.instance.as_ref() != Some(&self.instance) {
            return Err(Status::failed_precondition("stale load instance"));
        }
        if let Some((old, _)) = &self.snapshot {
            if snapshot.snapshot_seq <= old.snapshot_seq {
                return Ok(false);
            }
            if snapshot.covered_admission_seq < old.covered_admission_seq {
                return Err(Status::failed_precondition("load coverage regressed"));
            }
        }
        self.pending.retain(|_, p| {
            p.admission_seq
                .is_none_or(|seq| seq > snapshot.covered_admission_seq)
        });
        self.snapshot = Some((snapshot, now));
        Ok(true)
    }

    /// Unknown acceptance stays charged; cancellation alone must never call this.
    pub fn rejected_or_quiesced(&mut self, operation_id: &str) {
        self.pending.remove(operation_id);
    }

    /// Keep CPU/GPU availability separate; unknown admissions conservatively charge both.
    /// No fresh snapshot means no normal candidate, including a just-restarted instance.
    pub fn stage_work(&self, now: Instant, max_age: Duration) -> Option<(u64, u64, u64)> {
        let (snapshot, observed) = self.snapshot.as_ref()?;
        if now.saturating_duration_since(*observed) > max_age {
            return None;
        }
        let pending = self
            .pending
            .values()
            .try_fold(0u64, |total, p| total.checked_add(p.work))?;
        Some((
            snapshot.cpu_work.checked_add(pending)?,
            snapshot.gpu_work.checked_add(pending)?,
            snapshot.send_bytes,
        ))
    }

    /// A draining or clock-unhealthy encoder publishes zero admission capacity.
    pub fn is_available(&self, now: Instant, max_age: Duration) -> bool {
        self.snapshot.as_ref().is_some_and(|(snapshot, observed)| {
            snapshot.capacity > 0 && now.saturating_duration_since(*observed) <= max_age
        })
    }

    /// Count only local operations not covered by the same instance's snapshot.
    pub fn has_capacity(&self, now: Instant, max_age: Duration) -> bool {
        self.snapshot.as_ref().is_some_and(|(snapshot, observed)| {
            now.saturating_duration_since(*observed) <= max_age
                && snapshot.active.saturating_add(self.pending.len() as u64) < snapshot.capacity
        })
    }
}

#[cfg(test)]
mod readiness_tests {
    use super::*;

    /// An incompatible cheap encoder must not crowd compatible capacity out of a bounded list.
    #[test]
    fn encoder_shortlist_filters_contract_before_truncation() {
        let p = worker(
            "http://p:1",
            &proto::Capabilities {
                instance: Some(proto::Instance {
                    worker_id: "p".into(),
                    generation: "g".into(),
                }),
                embedding_backend: "mooncake".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let mut incompatible = p.clone();
        incompatible.contract.transfer = "zmq_to_scheduler".into();
        let mut pair = PdCandidate {
            prefill: p.clone(),
            decode: p.clone(),
            encoders: Vec::new(),
            has_capacity: true,
            layout_ms: 0,
            manifest_delivery_ms: 0,
            decode_registration_ms: 0,
            bind_submit_ms: 0,
            native_pd_ms: 0,
        };
        let mut cheap = EncodeCost::cold();
        cheap.cpu_wait_ms = 0;
        let mut expensive = cheap;
        expensive.cpu_wait_ms = 100;
        let encoders = vec![
            EncoderCandidate {
                worker: incompatible,
                cost: cheap,
                fresh: true,
                has_capacity: true,
                stable_tie_break: 0,
            },
            EncoderCandidate {
                worker: p,
                cost: expensive,
                fresh: true,
                has_capacity: true,
                stable_tie_break: 1,
            },
        ];
        shortlist_encoders(&mut pair, &encoders, 1);
        assert_eq!(pair.encoders.len(), 1);
        assert_eq!(pair.encoders[0].worker.contract.transfer, "mooncake");
        assert_eq!(pair.encoders[0].cost.cpu_wait_ms, 100);
    }

    /// Native admission acknowledgement promotes once; only quiescence releases it.
    #[test]
    fn decode_waiting_input_is_not_counted_as_execution() {
        let target = proto::Instance {
            worker_id: "d".into(),
            generation: "g".into(),
        };
        let operation = proto::OperationRef {
            operation_id: "d-op".into(),
            target: Some(target.clone()),
            ..Default::default()
        };
        let mut state = State::default();
        state.capacities.insert(instance_key(&target), 1);
        let registry = Registry {
            config: Config::default(),
            state: Mutex::new(state),
            channels: HashMap::new(),
        };
        registry
            .reserve(&[(operation.clone(), false)], 1, Some("d-op"))
            .unwrap();
        assert_eq!(active(&registry.state.lock().unwrap(), &target), 0);
        assert_eq!(occupancy(&registry.state.lock().unwrap(), &target, true), 1);
        for stage in [
            proto::Stage::InputBound,
            proto::Stage::Accepted,
            proto::Stage::NativeSubmitted,
        ] {
            registry
                .event(&proto::ControlEvent {
                    operation: Some(operation.clone()),
                    event: Some(proto::control_event::Event::Stage(stage as i32)),
                    ..Default::default()
                })
                .unwrap();
            assert_eq!(active(&registry.state.lock().unwrap(), &target), 1);
            assert_eq!(occupancy(&registry.state.lock().unwrap(), &target, true), 0);
        }
        registry.termination(&operation, false);
        assert_eq!(active(&registry.state.lock().unwrap(), &target), 1);
        registry.termination(&operation, true);
        assert_eq!(active(&registry.state.lock().unwrap(), &target), 0);
    }

    #[test]
    fn pd_resource_records_do_not_repeat_worker_concurrency_admission() {
        let target = proto::Instance {
            worker_id: "d".into(),
            generation: "g".into(),
        };
        let mut state = State::default();
        state.capacities.insert(instance_key(&target), 1);
        let registry = Registry {
            config: Config {
                max_records: 128,
                ..Default::default()
            },
            state: Mutex::new(state),
            channels: HashMap::new(),
        };
        let operation = |index| proto::OperationRef {
            operation_id: format!("d-{index}"),
            target: Some(target.clone()),
            ..Default::default()
        };
        for index in 0..128 {
            registry
                .reserve(&[(operation(index), false)], 1, None)
                .unwrap();
        }
        let reserve_next = || registry.reserve(&[(operation(128), false)], 1, None);
        assert_eq!(
            reserve_next().unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        registry.termination(&operation(0), false);
        assert!(reserve_next().is_err());
        registry.termination(&operation(0), true);
        reserve_next().unwrap();
        assert_eq!(active(&registry.state.lock().unwrap(), &target), 128);
    }

    #[tokio::test]
    async fn shared_pd_claim_survives_cancel_until_native_completion_or_quiescence() {
        use smg_grpc_client::sglang_proto::GenerateResponse;

        use crate::{
            routers::common::pd_admission::admit_decode,
            worker::{BasicWorkerBuilder, Worker as PoolWorker, WorkerType},
        };
        let worker: Arc<dyn PoolWorker> = Arc::new(
            BasicWorkerBuilder::new("http://d:1")
                .worker_type(WorkerType::Decode)
                .labels(HashMap::from([("max_running_requests".into(), "1".into())]))
                .build(),
        );
        let registry = Registry {
            config: Config::default(),
            state: Mutex::new(State::default()),
            channels: HashMap::new(),
        };
        let operation = proto::OperationRef {
            operation_id: "decode".into(),
            target: Some(proto::Instance {
                worker_id: "d".into(),
                generation: "g".into(),
            }),
            ..Default::default()
        };
        for completed in [false, true] {
            registry
                .reserve(&[(operation.clone(), false)], 1, None)
                .unwrap();
            registry
                .retain_pd_admission(&operation, admit_decode(&worker, "m", 1).await.unwrap())
                .unwrap();
            assert_eq!(worker.pd_admitted(), 1);
            registry.termination(&operation, false);
            assert_eq!(worker.pd_admitted(), 1);
            if completed {
                registry
                    .event(&proto::ControlEvent {
                        operation: Some(operation.clone()),
                        event: Some(proto::control_event::Event::Generation(GenerateResponse {
                            response: Some(NativeResponse::Complete(Default::default())),
                            ..Default::default()
                        })),
                        ..Default::default()
                    })
                    .unwrap();
                assert_eq!(worker.pd_admitted(), 0);
                assert!(registry
                    .state
                    .lock()
                    .unwrap()
                    .records
                    .contains_key("decode"));
            }
            registry.termination(&operation, true);
            assert_eq!(worker.pd_admitted(), 0);
        }
    }

    /// Replacement reclaims history only after every old operation has a safe receipt.
    #[test]
    fn instance_history_preserves_unknown_work_and_reclaims_released_generations() {
        let registry = Registry {
            config: Config {
                max_records: 1,
                ..Default::default()
            },
            state: Mutex::new(State::default()),
            channels: HashMap::new(),
        };
        let instance = |generation: &str| proto::Instance {
            worker_id: "e".into(),
            generation: generation.into(),
        };
        let publish = |generation: &str| {
            registry.publish(
                "http://e:1",
                proto::Capabilities {
                    instance: Some(instance(generation)),
                    ..Default::default()
                },
                Some(proto::LoadSnapshot {
                    instance: Some(instance(generation)),
                    snapshot_seq: 1,
                    capacity: 1,
                    ..Default::default()
                }),
            )
        };
        publish("old").unwrap();
        let operation = proto::OperationRef {
            operation_id: "op".into(),
            target: Some(instance("old")),
            ..Default::default()
        };
        {
            let mut state = registry.state.lock().unwrap();
            state.records.insert(
                "op".into(),
                Reservation {
                    pd_admission: None,
                    operation: operation.clone(),
                    encoder: true,
                    waiting_input: false,
                },
            );
            state.capacities.insert(instance_key(&instance("old")), 1);
        }
        assert_eq!(
            publish("new").unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        registry.termination(&operation, false);
        assert!(publish("new").is_err());
        registry.termination(&operation, true);
        publish("new").unwrap();
        let state = registry.state.lock().unwrap();
        assert_eq!(state.encoders.len(), 1);
        assert!(state.encoders.contains_key(&instance_key(&instance("new"))));
        assert!(state.capacities.is_empty());
        assert!(state.records.is_empty());
    }

    /// Saturation is an admission outcome; stale or incompatible workers remove readiness.
    #[test]
    fn readiness_requires_fresh_compatible_pools_but_not_free_slots() {
        let config: Config = serde_json::from_value(serde_json::json!({
            "model": "m", "model_path": "/model", "model_revision": "m", "processor_contract": "p",
            "prefill_urls": ["http://p:1"], "decode_urls": ["http://d:1"],
            "encoders": [{"endpoint": "http://e:1"}]
        }))
        .unwrap();
        let config = config.resolve().unwrap();
        let mut registry = Registry {
            config,
            state: Mutex::new(State::default()),
            channels: HashMap::new(),
        };
        assert!(!registry.readiness_with_clock(true).workers_ready);
        let now = Instant::now();
        {
            let mut state = registry.state.lock().unwrap();
            for (endpoint, role) in [
                ("http://e:1", "encoder"),
                ("http://p:1", "prefill"),
                ("http://d:1", "decode"),
            ] {
                let instance = proto::Instance {
                    worker_id: role.into(),
                    generation: "g".into(),
                };
                state.watched.insert(
                    endpoint.into(),
                    (
                        proto::Capabilities {
                            instance: Some(instance.clone()),
                            ready: true,
                            capacity: 1,
                            model_revision: "m".into(),
                            processor_contract: "p".into(),
                            layout_contract: LAYOUT.into(),
                            ..Default::default()
                        },
                        now,
                    ),
                );
                if role == "encoder" {
                    let mut load = LoadTracker::new(instance.clone(), 3);
                    load.observe(
                        proto::LoadSnapshot {
                            instance: Some(instance.clone()),
                            snapshot_seq: 1,
                            capacity: 1,
                            active: 1,
                            ..Default::default()
                        },
                        now,
                    )
                    .unwrap();
                    state.encoders.insert(instance_key(&instance), load);
                }
            }
        }
        assert!(registry.readiness_with_clock(true).workers_ready);
        assert_eq!(registry.readiness_with_clock(true).healthy_workers, 3);
        registry.config.readiness.prefill.min_healthy_workers = 2;
        assert!(!registry.readiness_with_clock(true).workers_ready);
        registry.config.readiness.prefill.min_healthy_workers = 1;
        registry.config.readiness.decode.expected_workers = Some(3);
        registry.config.readiness.decode.min_healthy_percent = Some(50);
        assert!(!registry.readiness_with_clock(true).workers_ready);
        registry.config.readiness.decode.min_healthy_percent = None;
        assert!(registry.readiness_with_clock(true).workers_ready);
        assert!(!registry.readiness_with_clock(false).workers_ready);
        registry
            .state
            .lock()
            .unwrap()
            .watched
            .get_mut("http://d:1")
            .unwrap()
            .1 = now - Duration::from_millis(registry.config.snapshot_max_age_ms + 1);
        assert!(!registry.readiness_with_clock(true).workers_ready);
        let mut state = registry.state.lock().unwrap();
        let (decode, observed) = state.watched.get_mut("http://d:1").unwrap();
        *observed = now;
        decode.model_revision = "different".into();
        drop(state);
        assert!(!registry.readiness_with_clock(true).workers_ready);
    }
}

//! Bounded instance-local admission and cancellation tombstones.

use std::collections::HashMap;

use smg_grpc_client::sglang_managed::{proto, validate_operation};
use tonic::Status;

#[derive(Debug, Clone)]
pub struct Record {
    pub operation: proto::OperationRef,
    pub admission_seq: u64,
    pub expiry: u64,
    pub work: u64,
    pub bytes: u64,
    pub waiting_input: bool,
    pub cancelled: bool,
    pub quiesced: bool,
}

/// Snapshot and admission share this owner; cancellation retains live resource charges.
pub struct Ledger {
    instance: proto::Instance,
    records: HashMap<String, Record>,
    max_records: usize,
    max_work: u64,
    max_bytes: u64,
    retention_ms: u64,
    max_lifetime_ms: u64,
    admission_seq: u64,
    snapshot_seq: u64,
    draining: bool,
}

impl Ledger {
    /// Require explicit finite limits; tombstone exhaustion also refuses new operations.
    pub fn new(
        instance: proto::Instance,
        max_records: usize,
        max_work: u64,
        max_bytes: u64,
        retention_ms: u64,
        max_lifetime_ms: u64,
    ) -> Result<Self, Status> {
        if max_records == 0 || max_work == 0 || max_bytes == 0 || max_lifetime_ms == 0 {
            return Err(Status::invalid_argument("invalid managed ledger limits"));
        }
        Ok(Self {
            instance,
            records: HashMap::new(),
            max_records,
            max_work,
            max_bytes,
            retention_ms,
            max_lifetime_ms,
            admission_seq: 0,
            snapshot_seq: 0,
            draining: false,
        })
    }

    /// Same operation/content returns its original admission; conflicting content is refused.
    pub fn admit(
        &mut self,
        operation: proto::OperationRef,
        budget: &proto::Budget,
        now_ms: u64,
    ) -> Result<(u64, bool), Status> {
        self.admit_phase(operation, budget, now_ms, false)
    }

    /// Reserve bounded D input memory without occupying native execution capacity.
    pub fn admit_waiting(
        &mut self,
        operation: proto::OperationRef,
        budget: &proto::Budget,
        now_ms: u64,
    ) -> Result<(u64, bool), Status> {
        self.admit_phase(operation, budget, now_ms, true)
    }

    /// Each phase has the existing active limit; bytes, work and history stay shared.
    fn admit_phase(
        &mut self,
        operation: proto::OperationRef,
        budget: &proto::Budget,
        now_ms: u64,
        waiting_input: bool,
    ) -> Result<(u64, bool), Status> {
        self.validate(&operation, budget.original_expiry_unix_ms, now_ms)?;
        self.collect(now_ms);
        if let Some(record) = self.records.get(&operation.operation_id) {
            if record.operation != operation || record.expiry != budget.original_expiry_unix_ms {
                return Err(Status::already_exists("conflicting managed operation"));
            }
            if record.cancelled || record.quiesced {
                return Err(Status::cancelled("managed operation already terminal"));
            }
            return Ok((record.admission_seq, false));
        }
        let (_, work, bytes) = self.usage();
        if self.draining
            || self.records.len() >= self.max_records
            || budget.estimated_work > self.max_work.saturating_sub(work)
            || budget.context_bytes > self.max_bytes.saturating_sub(bytes)
        {
            return Err(Status::resource_exhausted(
                "managed admission capacity exhausted",
            ));
        }
        self.admission_seq = self
            .admission_seq
            .checked_add(1)
            .ok_or_else(|| Status::unavailable("admission sequence exhausted"))?;
        self.records.insert(
            operation.operation_id.clone(),
            Record {
                operation,
                admission_seq: self.admission_seq,
                expiry: budget.original_expiry_unix_ms,
                work: budget.estimated_work,
                bytes: budget.context_bytes,
                waiting_input,
                cancelled: false,
                quiesced: false,
            },
        );
        Ok((self.admission_seq, true))
    }

    /// Mark complete input ready for native scheduling without a second execution queue.
    pub fn activate(&mut self, operation: &proto::OperationRef) -> Result<(), Status> {
        let record = self.get(operation)?;
        if record.cancelled || record.quiesced {
            return Err(Status::cancelled("managed operation already terminal"));
        }
        if !record.waiting_input {
            return Ok(());
        }
        if let Some(record) = self.records.get_mut(&operation.operation_id) {
            record.waiting_input = false;
        }
        Ok(())
    }

    /// A cancel arriving before Start installs a fence lasting past original expiry.
    pub fn cancel(
        &mut self,
        operation: proto::OperationRef,
        expiry: u64,
        now_ms: u64,
    ) -> Result<(), Status> {
        validate_operation(&operation)?;
        if operation.target.as_ref() != Some(&self.instance) {
            return Err(Status::failed_precondition("stale managed instance"));
        }
        if let Some(record) = self.records.get_mut(&operation.operation_id) {
            if record.operation != operation || record.expiry != expiry {
                return Err(Status::already_exists("conflicting managed termination"));
            }
            record.cancelled = true;
            return Ok(());
        }
        if expiry <= now_ms {
            return Ok(());
        }
        self.validate(&operation, expiry, now_ms)?;
        self.collect(now_ms);
        if self.records.len() >= self.max_records {
            return Err(Status::resource_exhausted(
                "managed cancellation ledger full",
            ));
        }
        self.records.insert(
            operation.operation_id.clone(),
            Record {
                operation,
                admission_seq: 0,
                expiry,
                work: 0,
                bytes: 0,
                waiting_input: false,
                cancelled: true,
                quiesced: true,
            },
        );
        Ok(())
    }

    /// Only the owner of CPU/batch/send completion may call this, not Cancel's caller.
    pub fn quiesce(&mut self, operation: &proto::OperationRef) -> Result<(), Status> {
        let record = self
            .records
            .get_mut(&operation.operation_id)
            .ok_or_else(|| Status::not_found("unknown managed operation"))?;
        if &record.operation != operation {
            return Err(Status::failed_precondition("managed operation mismatch"));
        }
        record.quiesced = true;
        Ok(())
    }

    /// The same consistency boundary produces coverage and current resource occupancy.
    pub fn snapshot(&mut self) -> Result<proto::LoadSnapshot, Status> {
        self.snapshot_seq = self
            .snapshot_seq
            .checked_add(1)
            .ok_or_else(|| Status::unavailable("snapshot sequence exhausted"))?;
        let (active, work, bytes) = self.usage();
        Ok(proto::LoadSnapshot {
            instance: Some(self.instance.clone()),
            snapshot_seq: self.snapshot_seq,
            covered_admission_seq: self.admission_seq,
            capacity: self.max_records as u64,
            active: active as u64,
            cpu_work: work,
            artifact_bytes: bytes,
            draining_work: self
                .records
                .values()
                .filter(|r| r.cancelled && !r.quiesced)
                .map(|r| r.work)
                .sum(),
            ..Default::default()
        })
    }

    /// Drain refuses new admissions while existing Bind and Cancel remain legal.
    pub fn drain(&mut self) {
        self.draining = true;
    }

    /// Capability probes must stop advertising new admission during drain.
    pub fn is_draining(&self) -> bool {
        self.draining
    }

    /// Safe terminal records expire; unresolved native work remains charged indefinitely.
    pub fn collect(&mut self, now_ms: u64) {
        let retention = self.retention_ms;
        self.records
            .retain(|_, r| !r.quiesced || now_ms <= r.expiry.saturating_add(retention));
    }

    /// Unknown state is not equivalent to a successfully cancelled operation.
    pub fn get(&self, operation: &proto::OperationRef) -> Result<&Record, Status> {
        let record = self
            .records
            .get(&operation.operation_id)
            .ok_or_else(|| Status::not_found("unknown managed operation"))?;
        if &record.operation != operation {
            return Err(Status::failed_precondition("managed operation mismatch"));
        }
        Ok(record)
    }

    /// A retained cancellation fence cannot outlive the maximum horizon accepted here.
    fn validate(&self, op: &proto::OperationRef, expiry: u64, now_ms: u64) -> Result<(), Status> {
        validate_operation(op)?;
        if op.target.as_ref() != Some(&self.instance) {
            return Err(Status::failed_precondition("stale managed instance"));
        }
        if expiry <= now_ms {
            return Err(Status::deadline_exceeded("managed operation expired"));
        }
        if expiry - now_ms > self.max_lifetime_ms {
            return Err(Status::invalid_argument(
                "managed expiry exceeds retention horizon",
            ));
        }
        Ok(())
    }

    /// Cancelled but unquiesced owners still consume every admission dimension.
    fn usage(&self) -> (usize, u64, u64) {
        self.records
            .values()
            .filter(|r| !r.quiesced)
            .fold((0, 0, 0), |(n, w, b), r| (n + 1, w + r.work, b + r.bytes))
    }
}

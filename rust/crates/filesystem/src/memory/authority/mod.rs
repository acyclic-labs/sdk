use super::*;

mod publication;
mod read;
mod reservation;
mod store;

struct AuthorityState {
    epoch: Epoch,
    commits: Vec<DurableCommit>,
    operations: HashMap<OperationId, (usize, KernelOwnedAllocation)>,
    publication_gate_tail: u64,
    publication_reservation: Option<crate::PublicationReservation>,
    publication_proofs: HashMap<u64, (crate::PublicationReservation, KernelOwnedAllocation)>,
    commits_custody: KernelOwnedAllocation,
    _entry_envelope: KernelOwnedAllocation,
}

impl Default for AuthorityState {
    fn default() -> Self {
        Self {
            epoch: Epoch::GENESIS,
            commits: Vec::new(),
            operations: HashMap::new(),
            publication_gate_tail: 0,
            publication_reservation: None,
            publication_proofs: HashMap::new(),
            commits_custody: KernelOwnedAllocation::Standalone,
            _entry_envelope: KernelOwnedAllocation::Standalone,
        }
    }
}

impl AuthorityState {
    fn head(&self) -> Head {
        self.commits.last().map_or_else(
            || Head::genesis(self.epoch),
            |commit| Head {
                epoch: self.epoch,
                sequence: commit.sequence,
                digest: commit.digest,
            },
        )
    }

    fn resolve_operation(
        &self,
        commit: &ProposedCommit,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<AppendOutcome>, FsError> {
        examine(accounting)?;
        let Some(&(index, _)) = self.operations.get(&commit.operation_id) else {
            return Ok(None);
        };
        let durable = &self.commits[index];
        accounting.charge(WorkCounters {
            authority_records_read: 1,
            authority_bytes_read: durable.payload.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        Ok(Some(if durable.fingerprint == commit.fingerprint {
            AppendOutcome::AlreadyCommitted(durable.clone())
        } else {
            AppendOutcome::IdempotencyConflict {
                committed_fingerprint: durable.fingerprint,
            }
        }))
    }
}

enum AppendPlan {
    Resolved(AppendOutcome),
    Publish {
        durable: DurableCommit,
        commits: Option<(Vec<DurableCommit>, KernelOwnedAllocation)>,
        operation: KernelOwnedAllocation,
    },
}

enum FirstRecordPlan {
    Resolved(bool),
    Publish(AuthorityState),
}
enum CreatePlan {
    Existing(Head),
    Publish(KernelOwnedAllocation),
}

/// Deterministic single-process authority store.
pub struct MemoryAuthorityStore {
    operation_claims: acyclic_stream::OperationClaims,
    maximum_payload_bytes: u64,
    // Lock order: authority map -> allocation ledger (native accounting only).
    // Never await, enter an object store, or call an owner guard while held.
    authorities: Mutex<HashMap<AuthorityId, AuthorityState>>,
}

impl MemoryAuthorityStore {
    /// Creates a store with a hard per-record payload bound.
    pub fn new(maximum_payload_bytes: u64) -> Result<Self, AuthorityStoreError> {
        if maximum_payload_bytes == 0 {
            return Err(AuthorityStoreError::PayloadTooLarge {
                observed: 0,
                maximum: 0,
            });
        }
        Ok(Self {
            maximum_payload_bytes,
            authorities: Mutex::new(HashMap::new()),
            operation_claims: acyclic_stream::OperationClaims::default(),
        })
    }

    fn validate_payload(
        &self,
        commit: &ProposedCommit,
        accounting: &KernelAccounting<'_>,
    ) -> Result<u64, FsError> {
        accounting.check()?;
        let bytes = commit.payload.len() as u64;
        if bytes > self.maximum_payload_bytes {
            return Err(AuthorityStoreError::PayloadTooLarge {
                observed: bytes,
                maximum: self.maximum_payload_bytes,
            }
            .into());
        }
        Ok(bytes)
    }

    fn durable(
        &self,
        authority: AuthorityId,
        actual: Head,
        commit: ProposedCommit,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<DurableCommit, FsError> {
        let sequence = actual.sequence.checked_next().map_err(|_| {
            AuthorityStoreError::RejectedStatic("authority sequence cannot advance")
        })?;
        let hashed = (commit.payload.len() as u64)
            .checked_add(AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES)
            .ok_or(WorkError::Overflow)?;
        accounting.charge(WorkCounters {
            bytes_hashed: hashed,
            ..WorkCounters::UNCHARGED
        })?;
        let digest = authority_commit_digest(
            authority,
            actual.epoch,
            sequence,
            commit.operation_id,
            commit.fingerprint,
            actual.digest,
            &commit.payload,
        );
        Ok(DurableCommit {
            epoch: actual.epoch,
            sequence,
            operation_id: commit.operation_id,
            fingerprint: commit.fingerprint,
            previous_digest: actual.digest,
            digest,
            payload: commit.payload,
            settled_at_micros: None,
        })
    }
}

impl Default for MemoryAuthorityStore {
    fn default() -> Self {
        Self {
            maximum_payload_bytes: 16 * 1024 * 1024,
            operation_claims: acyclic_stream::OperationClaims::default(),
            authorities: Mutex::new(HashMap::new()),
        }
    }
}

fn examine(accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if accounting.native_work().is_some() {
        return accounting.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        });
    }
    accounting.check()
}

fn reserve<K: Eq + std::hash::Hash, V>(
    map: &mut HashMap<K, V>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    if map.len() < map.capacity() {
        return Ok(());
    }
    accounting.charge(WorkCounters {
        allocation_operations: 1,
        ..WorkCounters::UNCHARGED
    })?;
    map.try_reserve(1)
        .map_err(|error| AuthorityStoreError::MemoryReplayAllocationFailed(error).into())
}

fn commits(
    capacity: usize,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<DurableCommit>, KernelOwnedAllocation), FsError> {
    allocate(capacity, accounting, |error| {
        AuthorityStoreError::MemoryReplayAllocationFailed(error).into()
    })
}

fn copy_work(count: usize, width: usize) -> Result<WorkCounters, FsError> {
    let bytes = count
        .checked_mul(width)
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(WorkError::Overflow)?;
    Ok(WorkCounters {
        bytes_copied: bytes,
        ..WorkCounters::UNCHARGED
    })
}

fn read_work() -> WorkCounters {
    WorkCounters {
        backend_read_operations: 1,
        ..WorkCounters::UNCHARGED
    }
}
fn write_work() -> WorkCounters {
    WorkCounters {
        backend_write_operations: 1,
        ..WorkCounters::UNCHARGED
    }
}
fn append_work(bytes: u64) -> WorkCounters {
    WorkCounters {
        authority_records_appended: 1,
        authority_bytes_written: bytes,
        ..write_work()
    }
}

//! Exact record identities and explicit authorization observations on Streams.
use super::*;
use state_record::StateRecord;

/// Native read custody, or no additional custody on the browser event loop.
#[cfg(not(target_arch = "wasm32"))]
pub type RecordAllocation = Option<acyclic_native_runtime::ReadAllocation>;
/// Browser provider bytes retain their own backing.
#[cfg(target_arch = "wasm32")]
pub type RecordAllocation = Option<()>;

/// Portable identity of permanent recorded data. This outlives the retry-key
/// lookup cache while the application's record retention keeps the data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordReceipt {
    /// Caller-chosen path inside its forkable subtree.
    pub path: StreamPath,
    /// Zero-based immutable record position.
    pub sequence: u64,
    /// Exact envelope that introduced this record, including inherited records.
    pub commit_id: acyclic_stream::CommitId,
}

impl RecordReceipt {
    /// Converts this last-record identity to an exact publication fence.
    pub fn condition(&self) -> Result<CommitCondition, AuthorityStoreError> {
        Ok(CommitCondition::Head {
            path: self.path.clone(),
            expected: self
                .sequence
                .checked_add(1)
                .ok_or(crate::WorkError::Overflow)?,
            commit_id: self.commit_id,
        })
    }

    /// Extracts the exact record receipt from a successful committed envelope.
    pub fn from_envelope(
        envelope: &acyclic_stream::CommittedEnvelope,
        path: &StreamPath,
        sequence: u64,
    ) -> Option<Self> {
        envelope.mutations.iter().find_map(|mutation| {
            let (destination, records) = match mutation {
                acyclic_stream::CommittedMutation::Append(append) => {
                    (&append.path, &append.records)
                }
                acyclic_stream::CommittedMutation::Fork(fork) => (&fork.destination, &fork.records),
            };
            if destination != path {
                return None;
            }
            records
                .iter()
                .find(|record| {
                    record.sequence == sequence && record.commit_id == envelope.commit_id
                })
                .map(|record| Self {
                    path: path.clone(),
                    sequence,
                    commit_id: record.commit_id,
                })
        })
    }
}

/// Facts used by an authorization decision. This is an observation, not a
/// capability: callers still authorize the resolved paths and immutable trees.
/// Its provider owner prevents accidental cross-provider atomic composition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizedObservation {
    owner: [u8; 16],
    condition: CommitCondition,
}

impl AuthorizedObservation {
    /// Exact condition to carry across an authorized service boundary.
    #[must_use]
    pub fn condition(&self) -> &CommitCondition {
        &self.condition
    }
}

/// A resolved state and the exact fact used to authorize it. Missing paths are
/// explicit; transport errors and missing historical records never become absence.
pub struct ObservedRecord {
    /// Present last record or an authoritative missing-path observation.
    pub record: Option<acyclic_stream::Record>,
    /// Include this fact in the mutation commit after authorization succeeds.
    pub observation: AuthorizedObservation,
    /// Keeps native provider backing funded for as long as the record is held.
    pub allocation: RecordAllocation,
}

impl<P: AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    /// Reads current state using existing Streams operations. Until G01 lands,
    /// present paths require tail plus one exact record read; no cache is a proof
    /// of absence and no coherent multi-path snapshot is claimed.
    pub async fn observe_record(
        &self,
        path: StreamPath,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObservedRecord, AuthorityStoreError> {
        let tail = self.tail(path.clone(), work).await;
        let (record, condition, allocation) = match tail {
            Err(error) if authority_missing(&error) => {
                (None, CommitCondition::Absent { path }, None)
            }
            Err(error) => return Err(error),
            Ok(0) => return Err(authority_corrupt("filesystem state stream is empty", work)),
            Ok(tail) => {
                let (record, allocation) = self.record(path.clone(), tail - 1, work).await?;
                let condition = CommitCondition::Head {
                    path,
                    expected: tail,
                    commit_id: record.commit_id,
                };
                (Some(record), condition, allocation)
            }
        };
        Ok(ObservedRecord {
            record,
            observation: AuthorizedObservation {
                owner: self.operation_claims().owner_identity(),
                condition,
            },
            allocation,
        })
    }

    /// One bounded historical read using a carried receipt. A missing/evicted
    /// record is an error, never permission to execute the operation again.
    pub async fn read_receipt(
        &self,
        receipt: &RecordReceipt,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObservedRecord, AuthorityStoreError> {
        let held = self
            .record(receipt.path.clone(), receipt.sequence, work)
            .await?;
        if held.0.sequence != receipt.sequence || held.0.commit_id != receipt.commit_id {
            return Err(authority_corrupt(
                "record receipt belongs to another stream history",
                work,
            ));
        }
        let condition = receipt.condition()?;
        Ok(ObservedRecord {
            record: Some(held.0),
            observation: AuthorizedObservation {
                owner: self.operation_claims().owner_identity(),
                condition,
            },
            allocation: held.1,
        })
    }

    /// Reconciles a lost reply at the predicted append position, even when the
    /// retry-key cache no longer has an answer. Exact operation key and encoded
    /// state must match. Missing data, replacement history or another operation
    /// is an error and never authorizes a replay at a later position.
    pub async fn reconcile_record(
        &self,
        path: StreamPath,
        sequence: u64,
        operation: &acyclic_stream::IdempotencyKey,
        expected: &StateRecord,
        work: &mut AuthorityWork<'_>,
    ) -> Result<RecordReceipt, AuthorityStoreError> {
        let held = self.record(path.clone(), sequence, work).await?;
        let encoded = work.encode_state(expected, operation)?;
        let comparison = WorkCounters {
            items_examined: encoded.len() as u64 + 1,
            ..WorkCounters::UNCHARGED
        };
        work.admit(comparison)?;
        let matches = held.0.sequence == sequence && held.0.value == encoded;
        work.charge(comparison)?;
        if !matches {
            return Err(acyclic_stream::StreamError::IdempotencyMismatch.into());
        }
        Ok(RecordReceipt {
            path,
            sequence,
            commit_id: held.0.commit_id,
        })
    }

    /// Captures an externally authorized canonical Streams condition explicitly.
    /// The caller must have obtained it from this provider under its authority.
    /// Immutable-root proofs may cover ancestor/symlink facts without one fence
    /// per object. A missing path uses Absent, never a fabricated zero head.
    pub fn authorized_observation(
        &self,
        condition: CommitCondition,
    ) -> Result<AuthorizedObservation, AuthorityStoreError> {
        if !matches!(
            condition,
            CommitCondition::Head { expected: 1.., .. } | CommitCondition::Absent { .. }
        ) {
            return Err(AuthorityStoreError::RejectedStatic(
                "authorized observations require an exact record identity or absence",
            ));
        }
        Ok(AuthorizedObservation {
            owner: self.operation_claims().owner_identity(),
            condition,
        })
    }

    /// Adds one root/companion record to the existing composed transaction.
    /// The caller stages and retains its immutable closure before submitting.
    /// No stream creation request or per-generation locator is needed.
    pub fn contribute_record(
        &self,
        commit: &mut FilesystemCommit,
        observed: &AuthorizedObservation,
        record: &StateRecord,
        hold: crate::PublicationHold,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        commit.check_owner(self.operation_claims().owner_identity())?;
        if commit.request_mut().is_none() {
            return Err(AuthorityStoreError::RejectedStatic(
                "sealed filesystem commit",
            ));
        }
        if observed.owner != self.operation_claims().owner_identity() {
            return Err(AuthorityStoreError::RejectedStatic(
                "observation and publication use different Streams owners",
            ));
        }
        let path = super::policy::condition_path(&observed.condition);
        if commit
            .request()
            .mutations
            .iter()
            .any(|mutation| match mutation {
                CommitMutation::Append {
                    path: destination, ..
                } => destination == path,
                CommitMutation::Fork { destination, .. } => destination == path,
            })
        {
            return Err(AuthorityStoreError::RejectedStatic(
                "duplicate filesystem state contribution",
            ));
        }
        let encoded = work.encode_state(record, &commit.request().idempotency_key)?;
        let mut records = work.vec(1)?;
        records.push(encoded.into());
        let condition = clone_condition(&observed.condition, work)?;
        let path = work.own_path(path.as_str())?;
        commit.reserve_hold(work)?;
        let request = commit
            .request_mut()
            .ok_or(AuthorityStoreError::RejectedStatic(
                "sealed filesystem commit",
            ))?;
        super::composition::grow(&mut request.conditions, 1, work)?;
        super::composition::grow(&mut request.mutations, 1, work)?;
        super::policy::merge_condition(&mut request.conditions, condition, work)?;
        request
            .mutations
            .push(CommitMutation::Append { path, records });
        commit.hold(hold);
        commit.bind_owner(observed.owner);
        Ok(())
    }

    /// Freezes a request with explicit authorized facts. This path never reads
    /// a task-local policy cut. Failed preparation leaves the request unchanged.
    pub fn seal_observed(
        &self,
        commit: &mut FilesystemCommit,
        observations: &[AuthorizedObservation],
        deadline: Option<u64>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        work.check()?;
        let owner = self.operation_claims().owner_identity();
        commit.check_owner(owner)?;
        if commit.request_mut().is_none() {
            return Err(AuthorityStoreError::RejectedStatic(
                "commit already sealed; resubmit its original request",
            ));
        }
        let mut request = work.copy_commit(commit.request())?;
        super::composition::grow(&mut request.conditions, observations.len(), work)?;
        for observed in observations {
            if observed.owner != self.operation_claims().owner_identity() {
                return Err(AuthorityStoreError::RejectedStatic(
                    "observation and publication use different Streams owners",
                ));
            }
            let condition = clone_condition(&observed.condition, work)?;
            super::policy::merge_condition(&mut request.conditions, condition, work)?;
        }
        // Use the primitive's one canonical shape/encoded-total admission.
        acyclic_stream::request::validate_commit(&mut request).map_err(map_stream_error)?;
        commit.seal_request(request, deadline);
        commit.bind_owner(owner);
        commit.retain_request_custody(work);
        Ok(())
    }
}

fn clone_condition(
    condition: &CommitCondition,
    work: &mut AuthorityWork<'_>,
) -> Result<CommitCondition, AuthorityStoreError> {
    let path = work.own_path(super::policy::condition_path(condition).as_str())?;
    Ok(match condition {
        CommitCondition::Head {
            expected,
            commit_id,
            ..
        } => CommitCondition::Head {
            path,
            expected: *expected,
            commit_id: *commit_id,
        },
        CommitCondition::Tail { expected, .. } => CommitCondition::Tail {
            path,
            expected: *expected,
        },
        CommitCondition::Absent { .. } => CommitCondition::Absent { path },
    })
}

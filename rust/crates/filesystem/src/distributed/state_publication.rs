//! Atomic state batches and read-only reconciliation of their permanent records.
use super::*;
use state_record::StateRecord;

/// A permanent multi-record publication decision, or its unchanged unresolved owner.
#[must_use]
pub enum StatePublicationOutcome {
    /// Every prepared state record was found in the same immutable commit.
    Recorded(acyclic_stream::CommitId),
    /// No terminal decision was established. Missing records are not absence
    /// proofs for the operation and never authorize replay at another position.
    Unknown {
        /// The exact original request, expiry, contributions and closure holds.
        commit: FilesystemCommit,
        /// Why permanent reconciliation could not establish the decision.
        error: AuthorityStoreError,
    },
}

impl<P: AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    /// Prepares and seals one all-or-none batch of portable volume snapshots
    /// and typed companion/ref records. Preparation performs no provider I/O.
    /// The iterator transfers each closure hold with its corresponding state.
    /// Any refusal leaves the caller's original request and holds unchanged.
    /// The canonical Streams validator admits the complete encoded command.
    pub fn prepare_records(
        &self,
        commit: &mut FilesystemCommit,
        records: impl IntoIterator<Item = (AuthorizedObservation, StateRecord, crate::PublicationHold)>,
        observations: &[AuthorizedObservation],
        deadline: Option<u64>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        // A sealed owner can already be uncertain: never change or roll it back.
        if commit.request_mut().is_none() {
            return Err(AuthorityStoreError::RejectedStatic(
                "sealed filesystem commit",
            ));
        }
        commit.check_owner(self.operation_claims().owner_identity())?;
        // Conditions can be strengthened while merging (Tail -> Head). A
        // length-only rollback cannot undo that. Stage the complete request
        // instead, leaving the caller's conditions and custody untouched.
        let mut prepared = FilesystemCommit::new(work.copy_commit(commit.request())?);
        for (observed, record, hold) in records {
            self.contribute_record(&mut prepared, &observed, &record, hold, work)?;
        }
        let holds = commit.record_holds(&prepared, work)?;
        self.seal_observed(&mut prepared, observations, deadline, work)?;
        // No fallible step follows the final seal/funding transfer.
        commit.install_records(prepared, holds);
        Ok(())
    }

    /// Resolves a lost reply without resubmission or retry-key lookup. This
    /// accepts only sealed portable state appends with exact Head/Absent fences.
    /// It reads each original predicted position and requires the exact key,
    /// original destination, bytes, headers and one commit identity across the
    /// entire batch. Callers must use unique operation keys for their intents.
    /// Retention loss, a different operation, cancellation or local admission
    /// refusal returns the same unresolved owner and keeps every closure hold.
    /// The immutable records, rather than a bounded retry cache, retain success.
    pub async fn reconcile_states(
        &self,
        commit: FilesystemCommit,
        work: &mut AuthorityWork<'_>,
    ) -> StatePublicationOutcome {
        let result = self.recorded_states(&commit, work).await;
        match result {
            Ok(commit_id) => {
                self.refresh_committed(commit, commit_id, work).await;
                StatePublicationOutcome::Recorded(commit_id)
            }
            Err(error) => StatePublicationOutcome::Unknown { commit, error },
        }
    }

    async fn recorded_states(
        &self,
        commit: &FilesystemCommit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<acyclic_stream::CommitId, AuthorityStoreError> {
        commit.check_owner(self.operation_claims().owner_identity())?;
        commit.ensure_sealed()?;
        work.check()?;
        if !commit.authorities().is_empty() {
            return Err(AuthorityStoreError::RejectedStatic(
                "state reconciliation cannot settle legacy authority contributions",
            ));
        }
        let request = commit.request();
        // seal_observed uses canonical path order. Walk conditions once across
        // all destinations, including extra policy/actor observations.
        let mut conditions = request.conditions.iter().peekable();
        let mut commit_id = None;
        for mutation in &request.mutations {
            let step = WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            };
            work.charge(step)?;
            let CommitMutation::Append { path, records } = mutation else {
                return Err(AuthorityStoreError::RejectedStatic(
                    "state reconciliation requires exact state appends",
                ));
            };
            let [expected] = records.as_slice() else {
                return Err(AuthorityStoreError::RejectedStatic(
                    "state reconciliation requires one snapshot per path",
                ));
            };
            let state = StateRecord::decode(expected.value.clone())?;
            let provenance = WorkCounters {
                items_examined: (path.as_str().len() + request.idempotency_key.as_bytes().len())
                    as u64,
                ..WorkCounters::UNCHARGED
            };
            work.admit(provenance)?;
            let destination_matches = state.published_at.as_ref() == path.as_str().as_bytes();
            work.charge(provenance)?;
            if state.operation != request.idempotency_key || !destination_matches {
                return Err(acyclic_stream::StreamError::IdempotencyMismatch.into());
            }
            while conditions
                .peek()
                .is_some_and(|condition| super::policy::condition_path(condition) < path)
            {
                work.charge(step)?;
                conditions.next();
            }
            let sequence = match conditions.next() {
                Some(CommitCondition::Head {
                    path: observed,
                    expected,
                    ..
                }) if observed == path => *expected,
                Some(CommitCondition::Absent { path: observed }) if observed == path => 0,
                _ => {
                    return Err(AuthorityStoreError::RejectedStatic(
                        "state reconciliation requires exact destination observations",
                    ));
                }
            };
            let held = self.record(path.clone(), sequence, work).await?;
            let bytes = held.0.value.len() as u64;
            let comparison = WorkCounters {
                items_examined: bytes.checked_add(1).ok_or(crate::WorkError::Overflow)?,
                ..WorkCounters::UNCHARGED
            };
            let matches = (|| {
                let headers = WorkCounters {
                    items_examined: (held.0.headers.keys().len()
                        + held.0.headers.attributes().len()
                        + expected.headers.keys().len()
                        + expected.headers.attributes().len())
                        as u64,
                    ..WorkCounters::UNCHARGED
                };
                work.charge(headers)?;
                let header_bytes = held
                    .0
                    .headers
                    .payload_bytes()
                    .checked_add(expected.headers.payload_bytes())
                    .ok_or(crate::WorkError::Overflow)? as u64;
                let comparison = comparison.checked_add(WorkCounters {
                    items_examined: header_bytes,
                    ..WorkCounters::UNCHARGED
                })?;
                work.admit(comparison)?;
                let matches = held.0.value == expected.value
                    && held.0.headers == expected.headers
                    && commit_id.is_none_or(|id| id == held.0.commit_id);
                work.charge(comparison)?;
                Ok::<_, AuthorityStoreError>(matches)
            })();
            let recorded_id = held.0.commit_id;
            // Physical provider values must retire before their scratch charge;
            // the original prepared request/holds are independent of this read.
            drop(held);
            work.release(bytes)?;
            if !matches? {
                return Err(acyclic_stream::StreamError::IdempotencyMismatch.into());
            }
            commit_id = Some(recorded_id);
        }
        commit_id.ok_or(AuthorityStoreError::RejectedStatic(
            "empty state publication",
        ))
    }
}

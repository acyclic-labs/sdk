use super::*;

impl MemoryAuthorityStore {
    pub(super) fn plan_create(
        &self,
        authority: AuthorityId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<CreatePlan, FsError> {
        accounting.check()?;
        let mut authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityStoreError::MemoryAuthorityPoisoned)?;
        if let Some(state) = authorities.get(&authority) {
            accounting.charge(read_work())?;
            return Ok(CreatePlan::Existing(state.head()));
        }
        accounting.admit(write_work())?;
        let envelope =
            accounting.claim_owned(size_of::<(AuthorityId, AuthorityState)>() as u64, 0)?;
        reserve(&mut authorities, accounting)?;
        Ok(CreatePlan::Publish(envelope))
    }

    pub(super) fn publish_create(
        &self,
        authority: AuthorityId,
        epoch: Epoch,
        plan: CreatePlan,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<CreateAuthorityOutcome>, FsError> {
        accounting.check()?;
        let envelope = match plan {
            CreatePlan::Existing(head) => return Ok(Some(CreateAuthorityOutcome::Existing(head))),
            CreatePlan::Publish(envelope) => envelope,
        };
        let mut authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityStoreError::MemoryAuthorityPoisoned)?;
        if let Some(state) = authorities.get(&authority) {
            accounting.charge(read_work())?;
            return Ok(Some(CreateAuthorityOutcome::Existing(state.head())));
        }
        if authorities.len() == authorities.capacity() {
            return Ok(None);
        }
        accounting.charge(write_work())?;
        authorities.insert(
            authority,
            AuthorityState {
                epoch,
                _entry_envelope: envelope,
                ..AuthorityState::default()
            },
        );
        Ok(Some(CreateAuthorityOutcome::Created(Head::genesis(epoch))))
    }

    fn first_record_matches(
        first: &DurableCommit,
        commit: &ProposedCommit,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, FsError> {
        accounting.charge(WorkCounters {
            authority_records_read: 1,
            authority_bytes_read: first.payload.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        Ok(first.sequence == Sequence::new(1)
            && first.operation_id == commit.operation_id
            && first.fingerprint == commit.fingerprint
            && first.payload == commit.payload)
    }

    pub(super) fn plan_first_record(
        &self,
        authority: AuthorityId,
        commit: &ProposedCommit,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<FirstRecordPlan, FsError> {
        let bytes = self.validate_payload(commit, accounting)?;
        accounting.charge(read_work())?;
        let mut authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityStoreError::MemoryAuthorityPoisoned)?;
        examine(accounting)?;
        if let Some(state) = authorities.get(&authority) {
            if let Some(first) = state.commits.first() {
                return Self::first_record_matches(first, commit, accounting)
                    .map(FirstRecordPlan::Resolved);
            }
            if state.publication_reservation.is_some() {
                return Ok(FirstRecordPlan::Resolved(false));
            }
        }
        let expected = authorities
            .get(&authority)
            .map_or(Head::genesis(Epoch::GENESIS), AuthorityState::head);
        accounting.admit(append_work(bytes))?;
        let envelope = if !authorities.contains_key(&authority) {
            let envelope =
                accounting.claim_owned(size_of::<(AuthorityId, AuthorityState)>() as u64, 0)?;
            reserve(&mut authorities, accounting)?;
            envelope
        } else {
            KernelOwnedAllocation::Standalone
        };
        let operation = accounting.claim_owned(
            size_of::<(OperationId, (usize, KernelOwnedAllocation))>() as u64,
            0,
        )?;
        let mut commits = commits(1, accounting)?;
        let mut operations = HashMap::new();
        reserve(&mut operations, accounting)?;
        let durable = self.durable(authority, expected, commit.clone(), accounting)?;
        operations.insert(durable.operation_id, (0, operation));
        accounting.charge(copy_work(1, size_of::<DurableCommit>())?)?;
        commits.0.push(durable);
        Ok(FirstRecordPlan::Publish(AuthorityState {
            epoch: expected.epoch,
            commits: commits.0,
            operations,
            commits_custody: commits.1,
            _entry_envelope: envelope,
            ..AuthorityState::default()
        }))
    }

    pub(super) fn publish_first_record(
        &self,
        authority: AuthorityId,
        commit: &ProposedCommit,
        plan: FirstRecordPlan,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<bool>, FsError> {
        accounting.check()?;
        let mut authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityStoreError::MemoryAuthorityPoisoned)?;
        if let Some(existing) = authorities.get(&authority) {
            if let Some(first) = existing.commits.first() {
                return Self::first_record_matches(first, commit, accounting).map(Some);
            }
            if existing.publication_reservation.is_some() {
                return Ok(Some(false));
            }
        }
        let FirstRecordPlan::Publish(mut state) = plan else {
            return Ok(Some(false));
        };
        let actual = authorities
            .get(&authority)
            .map_or(Head::genesis(Epoch::GENESIS), AuthorityState::head);
        if actual != Head::genesis(state.epoch) {
            return Ok(Some(false));
        }
        let write = append_work(commit.payload.len() as u64);
        accounting.admit(write)?;
        if !authorities.contains_key(&authority) && authorities.len() == authorities.capacity() {
            return Ok(None);
        }
        // Creation and the initial append publish under one lock.
        if let Some(existing) = authorities.get_mut(&authority) {
            state._entry_envelope = std::mem::replace(
                &mut existing._entry_envelope,
                KernelOwnedAllocation::Standalone,
            );
            state.publication_gate_tail = existing.publication_gate_tail;
            state.publication_proofs = std::mem::take(&mut existing.publication_proofs);
            *existing = state;
        } else {
            authorities.insert(authority, state);
        }
        accounting.record_completed(write)?;
        Ok(Some(true))
    }

    fn validate_permit(permit: crate::PublicationPermit) -> Result<(), FsError> {
        if matches!(permit, crate::PublicationPermit::Lease { .. }) {
            return Err(AuthorityStoreError::RejectedStatic(
                "authority backend does not support atomic publication leases",
            )
            .into());
        }
        Ok(())
    }

    fn permit_matches(state: &AuthorityState, permit: crate::PublicationPermit) -> bool {
        match (state.publication_reservation, permit) {
            (None, crate::PublicationPermit::Unrestricted) => true,
            (
                Some(proof),
                crate::PublicationPermit::Reservation {
                    operation_id,
                    gate_tail,
                    expected,
                },
            ) => {
                proof.operation_id == OperationId::from_bytes(operation_id)
                    && proof.gate_tail == gate_tail
                    && proof.expected == expected
            }
            _ => false,
        }
    }

    pub(super) fn plan_append(
        &self,
        request: &crate::GuardedAppend,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<AppendPlan, FsError> {
        self.validate_payload(&request.commit, accounting)?;
        Self::validate_permit(request.permit)?;
        accounting.charge(read_work())?;
        let mut authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityStoreError::MemoryAuthorityPoisoned)?;
        examine(accounting)?;
        let state = authorities
            .get_mut(&request.authority_id)
            .ok_or(AuthorityStoreError::Missing)?;
        let actual = state.head();
        if let Some(outcome) = state.resolve_operation(&request.commit, accounting)? {
            return Ok(AppendPlan::Resolved(outcome));
        }
        if request.epoch != actual.epoch {
            return Ok(AppendPlan::Resolved(AppendOutcome::Fenced {
                actual_epoch: actual.epoch,
            }));
        }
        if request.expected != actual || !Self::permit_matches(state, request.permit) {
            return Ok(AppendPlan::Resolved(AppendOutcome::Conflict { actual }));
        }
        accounting.admit(append_work(request.commit.payload.len() as u64))?;
        let replacement = if state.commits.len() == state.commits.capacity() {
            let capacity = state
                .commits
                .capacity()
                .checked_mul(2)
                .and_then(|double| {
                    state
                        .commits
                        .len()
                        .checked_add(1)
                        .map(|needed| needed.max(double))
                })
                .ok_or(WorkError::Overflow)?;
            Some(commits(capacity, accounting)?)
        } else {
            None
        };
        let operation = accounting.claim_owned(
            size_of::<(OperationId, (usize, KernelOwnedAllocation))>() as u64,
            0,
        )?;
        reserve(&mut state.operations, accounting)?;
        let copied = if replacement.is_some() {
            state
                .commits
                .len()
                .checked_add(1)
                .ok_or(WorkError::Overflow)?
        } else {
            1
        };
        accounting.admit(copy_work(copied, size_of::<DurableCommit>())?)?;
        let durable = self.durable(
            request.authority_id,
            actual,
            request.commit.clone(),
            accounting,
        )?;
        Ok(AppendPlan::Publish {
            durable,
            commits: replacement,
            operation,
        })
    }

    pub(super) fn publish_append(
        &self,
        request: &crate::GuardedAppend,
        plan: AppendPlan,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<AppendOutcome, FsError> {
        accounting.check()?;
        let (durable, commits, operation) = match plan {
            AppendPlan::Resolved(outcome) => return Ok(outcome),
            AppendPlan::Publish {
                durable,
                commits,
                operation,
            } => (durable, commits, operation),
        };
        let mut authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityStoreError::MemoryAuthorityPoisoned)?;
        Self::validate_permit(request.permit)?;
        let state = authorities
            .get_mut(&request.authority_id)
            .ok_or(AuthorityStoreError::Missing)?;
        let actual = state.head();
        if let Some(outcome) = state.resolve_operation(&request.commit, accounting)? {
            return Ok(outcome);
        }
        if request.epoch != actual.epoch {
            return Ok(AppendOutcome::Fenced {
                actual_epoch: actual.epoch,
            });
        }
        if request.expected != actual || !Self::permit_matches(state, request.permit) {
            return Ok(AppendOutcome::Conflict { actual });
        }
        let copied = if commits.is_some() {
            state
                .commits
                .len()
                .checked_add(1)
                .ok_or(WorkError::Overflow)?
        } else {
            1
        };
        let copied = copy_work(copied, size_of::<DurableCommit>())?;
        let write = append_work(durable.payload.len() as u64);
        accounting.admit(write.checked_add(copied)?)?;
        accounting.record_completed(write)?;
        if let Some(mut buffer) = commits {
            buffer.0.append(&mut state.commits);
            drop(std::mem::replace(&mut state.commits, buffer.0));
            state.commits_custody = buffer.1;
        }
        let index = state.commits.len();
        state
            .operations
            .insert(durable.operation_id, (index, operation));
        state.commits.push(durable.clone());
        accounting.record_completed(copied)?;
        Ok(AppendOutcome::Committed(durable))
    }
}

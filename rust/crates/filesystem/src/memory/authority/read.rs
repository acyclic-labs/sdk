use super::*;

impl MemoryAuthorityStore {
    pub(super) fn read_head(
        &self,
        authority: AuthorityId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Head, FsError> {
        accounting.charge(read_work())?;
        let authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityStoreError::MemoryAuthorityPoisoned)?;
        examine(accounting)?;
        Ok(authorities
            .get(&authority)
            .ok_or(AuthorityStoreError::Missing)?
            .head())
    }

    pub(super) fn read_page(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Vec<DurableCommit>, KernelOwnedAllocation), FsError> {
        accounting.check()?;
        if limit.records == 0 || limit.payload_bytes == 0 {
            return Err(AuthorityStoreError::InvalidReplayLimit.into());
        }
        accounting.charge(read_work())?;
        let authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityStoreError::MemoryAuthorityPoisoned)?;
        examine(accounting)?;
        let state = authorities
            .get(&authority)
            .ok_or(AuthorityStoreError::Missing)?;
        let head = state.head();
        if after > head.sequence {
            return Err(AuthorityStoreError::ReplayCursorAhead {
                after,
                head: head.sequence,
            }
            .into());
        }
        let start = usize::try_from(after.get())
            .map_err(|_| AuthorityStoreError::ReplayCursorProcessOverflow { after })?;
        let mut payload_bytes = 0_u64;
        let mut count = 0_usize;
        for commit in state
            .commits
            .iter()
            .skip(start)
            .take(usize::try_from(limit.records).unwrap_or(usize::MAX))
        {
            examine(accounting)?;
            let bytes = commit.payload.len() as u64;
            let next = payload_bytes
                .checked_add(bytes)
                .ok_or(WorkError::Overflow)?;
            if next > limit.payload_bytes {
                if count == 0 {
                    return Err(AuthorityStoreError::ReplayRecordTooLarge {
                        observed: bytes,
                        maximum: limit.payload_bytes,
                    }
                    .into());
                }
                break;
            }
            payload_bytes = next;
            count += 1;
        }
        let end = start.checked_add(count).ok_or(WorkError::Overflow)?;
        accounting.charge(WorkCounters {
            authority_records_read: count as u64,
            authority_bytes_read: payload_bytes,
            ..WorkCounters::UNCHARGED
        })?;
        let mut page = commits(count, accounting)?;
        for commit in &state.commits[start..end] {
            examine(accounting)?;
            #[cfg(not(target_arch = "wasm32"))]
            if accounting.native_work().is_some() {
                accounting.charge(copy_work(1, size_of::<DurableCommit>())?)?;
            }
            page.0.push(commit.clone());
        }
        Ok(page)
    }

    pub(super) fn advance_fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<crate::FenceOutcome, FsError> {
        accounting.charge(read_work())?;
        let mut authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityStoreError::MemoryAuthorityPoisoned)?;
        examine(accounting)?;
        let state = authorities
            .get_mut(&authority)
            .ok_or(AuthorityStoreError::Missing)?;
        let actual = state.head();
        if actual != expected || state.publication_reservation.is_some() {
            return Ok(crate::FenceOutcome::Conflict { actual });
        }
        let delta = WorkCounters {
            authority_records_appended: 1,
            ..write_work()
        };
        accounting.admit(delta)?;
        let epoch = actual
            .epoch
            .get()
            .checked_add(1)
            .and_then(|epoch| Epoch::new(epoch).ok())
            .ok_or(AuthorityStoreError::EpochExhausted)?;
        accounting.check()?;
        accounting.record_completed(delta)?;
        state.epoch = epoch;
        Ok(crate::FenceOutcome::Advanced(state.head()))
    }

    pub(super) fn lookup_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<DurableCommit>, FsError> {
        accounting.charge(read_work())?;
        let authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityStoreError::MemoryAuthorityPoisoned)?;
        examine(accounting)?;
        let state = authorities
            .get(&authority)
            .ok_or(AuthorityStoreError::Missing)?;
        let commit = state
            .operations
            .get(&operation)
            .map(|&(index, _)| &state.commits[index]);
        if let Some(commit) = commit {
            accounting.charge(WorkCounters {
                authority_records_read: 1,
                authority_bytes_read: commit.payload.len() as u64,
                ..WorkCounters::UNCHARGED
            })?;
        }
        accounting.check()?;
        Ok(commit.cloned())
    }
}

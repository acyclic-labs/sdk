use super::*;

fn select_authority_fault(
    state: &Mutex<State>,
    operation: SimulationOperation,
) -> Result<Option<SimulationFault>, AuthorityStoreError> {
    let mut state = state
        .lock()
        .map_err(|_| AuthorityStoreError::CorruptStatic("simulation state is poisoned"))?;
    state
        .intercept(operation)
        .map_err(AuthorityStoreError::RejectedStatic)
}

#[cfg(not(target_arch = "wasm32"))]
fn select_native_authority_fault(
    state: &Mutex<State>,
    operation: SimulationOperation,
    original: &crate::NativeMaterializationWork,
) -> Result<Option<SimulationFault>, AuthorityStoreError> {
    let mut state = state
        .lock()
        .map_err(|_| AuthorityStoreError::CorruptStatic("simulation state is poisoned"))?;
    let mut accounting = KernelAccounting::native(original, original.cancellation());
    state
        .prepare_trace(&mut accounting)
        .map_err(|error| match error {
            crate::FsError::Cancelled(_) => AuthorityStoreError::Cancelled,
            crate::FsError::Work(error) => AuthorityStoreError::Work(error),
            crate::FsError::NativeKernel(error) => AuthorityStoreError::NativeOriginal(error),
            crate::FsError::Object(ObjectStoreError::MemoryTableAllocationFailed(error)) => {
                AuthorityStoreError::NativeOriginal(
                    crate::NativeTreeMaterializationError::AllocationFailed(error),
                )
            }
            crate::FsError::Object(ObjectStoreError::Simulation(reason)) => {
                AuthorityStoreError::RejectedStatic(reason)
            }
            _ => AuthorityStoreError::CorruptStatic("simulation trace ownership is invalid"),
        })?;
    state
        .intercept(operation)
        .map_err(AuthorityStoreError::RejectedStatic)
}

fn lost_authority_acknowledgement(operation: &'static str) -> AuthorityStoreError {
    AuthorityStoreError::Indeterminate {
        operation,
        source: std::io::ErrorKind::ConnectionReset.into(),
    }
}

fn check_authority_fault(fault: Option<SimulationFault>) -> Result<(), AuthorityStoreError> {
    match fault {
        Some(SimulationFault::RejectBefore) => {
            Err(AuthorityStoreError::RejectedStatic("scheduled rejection"))
        }
        Some(SimulationFault::PartitionBefore) => {
            Err(AuthorityStoreError::RejectedStatic("scheduled partition"))
        }
        _ => Ok(()),
    }
}

impl<A: AsyncAuthorityStore> AsyncAuthorityStore for SimulatedStore<A> {
    async fn create_first_record<G: crate::AuthorityGuard>(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<bool, crate::AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() && guard.is_some() {
            return Err(AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        #[cfg(not(target_arch = "wasm32"))]
        let fault = if let Some(original) = work.original {
            original
                .admit(WorkCounters::UNCHARGED)
                .map_err(AuthorityStoreError::from)?;
            select_native_authority_fault(
                &self.state,
                SimulationOperation::AuthorityCreate,
                original,
            )?
        } else {
            select_authority_fault(&self.state, SimulationOperation::AuthorityCreate)?
        };
        #[cfg(target_arch = "wasm32")]
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityCreate)?;
        check_authority_fault(fault)?;
        let value = self
            .inner
            .create_first_record(authority, commit, work, guard.as_deref_mut())
            .await?;
        if value && matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend)) {
            return Err(
                lost_authority_acknowledgement("create-authority-with-first-record").into(),
            );
        }
        Ok(value)
    }
    async fn compare_and_append<G: crate::AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<AppendOutcome, crate::AuthorityError<G::Error>> {
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        #[cfg(not(target_arch = "wasm32"))]
        let fault = if let Some(original) = work.original {
            original
                .admit(WorkCounters::UNCHARGED)
                .map_err(AuthorityStoreError::from)?;
            select_native_authority_fault(
                &self.state,
                SimulationOperation::AuthorityAppend,
                original,
            )?
        } else {
            select_authority_fault(&self.state, SimulationOperation::AuthorityAppend)?
        };
        #[cfg(target_arch = "wasm32")]
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityAppend)?;
        check_authority_fault(fault)?;
        if matches!(fault, Some(SimulationFault::FenceBeforeAuthorityAppend)) {
            self.inner
                .fence(request.authority_id, request.expected, work)
                .await?;
        }
        let value = self
            .inner
            .compare_and_append(request, work, guard.as_deref_mut())
            .await?;
        if matches!(
            &value,
            AppendOutcome::Committed(_) | AppendOutcome::AlreadyCommitted(_)
        ) && matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend))
        {
            return Err(lost_authority_acknowledgement("compare-and-append").into());
        }
        Ok(value)
    }
    async fn create_authority<G: crate::AuthorityGuard>(
        &self,
        authority: AuthorityId,
        epoch: Epoch,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityCreate)?;
        check_authority_fault(fault)?;
        let value = self
            .inner
            .create_authority(authority, epoch, work, None::<&mut ()>)
            .await
            .map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })?;
        if matches!(value, CreateAuthorityOutcome::Existing(_)) {
            crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        }
        Ok(value)
    }
    async fn fork_authority<G: crate::AuthorityGuard>(
        &self,
        source: GenerationForkSource,
        destination: AuthorityId,
        operation: OperationId,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityCreate)?;
        check_authority_fault(fault)?;
        let value = self
            .inner
            .fork_authority(source, destination, operation, work, None::<&mut ()>)
            .await
            .map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })?;
        if matches!(value, CreateAuthorityOutcome::Existing(_)) {
            crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        }
        Ok(value)
    }
    async fn publication_reservation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Option<PublicationReservation>, AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            work.check()?;
            return Err(AuthorityStoreError::NativeOriginalUnsupported);
        }
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityReservation)?;
        check_authority_fault(fault)?;
        let value = self
            .inner
            .publication_reservation(authority, operation, work)
            .await?;
        Ok(value)
    }
    async fn reserve_publication(
        &self,
        authority: AuthorityId,
        expected: Head,
        operation: OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<ReservationOutcome, AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            work.check()?;
            return Err(AuthorityStoreError::NativeOriginalUnsupported);
        }
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityReserve)?;
        check_authority_fault(fault)?;
        let value = self
            .inner
            .reserve_publication(authority, expected, operation, work)
            .await?;
        if matches!(
            value,
            ReservationOutcome::Reserved(_) | ReservationOutcome::AlreadyReserved(_)
        ) && matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend))
        {
            return Err(AuthorityStoreError::Indeterminate {
                operation: "reserve-publication",
                source: std::io::Error::other("scheduled acknowledgement loss"),
            });
        }
        Ok(value)
    }
    async fn release_publication(
        &self,
        reservation: PublicationReservation,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            work.check()?;
            return Err(AuthorityStoreError::NativeOriginalUnsupported);
        }
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityRelease)?;
        check_authority_fault(fault)?;
        let value = self.inner.release_publication(reservation, work).await?;
        if matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend)) {
            return Err(AuthorityStoreError::Indeterminate {
                operation: "release-publication",
                source: std::io::Error::other("scheduled acknowledgement loss"),
            });
        }
        Ok(value)
    }
    async fn head(
        &self,
        authority: AuthorityId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = work.original {
            original.admit(WorkCounters::UNCHARGED)?;
        }
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityHead)?;
        check_authority_fault(fault)?;
        if let Some(SimulationFault::StaleAuthorityHead(head)) = fault {
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() {
                return Ok(head);
            }
            let delta = WorkCounters {
                authority_records_read: 1,
                ..WorkCounters::UNCHARGED
            };
            delta
                .verify(work.remaining()?)
                .map_err(AuthorityStoreError::from)?;
            work.record(delta)?;
            return Ok(head);
        }
        self.inner.head(authority, work).await
    }
    async fn fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::FenceOutcome, AuthorityStoreError> {
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = work.original {
            original.admit(WorkCounters::UNCHARGED)?;
        }
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityFence)?;
        check_authority_fault(fault)?;
        self.inner.fence(authority, expected, work).await
    }
    async fn find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = work.original {
            original.admit(WorkCounters::UNCHARGED)?;
        }
        let fault =
            select_authority_fault(&self.state, SimulationOperation::AuthorityFindOperation)?;
        check_authority_fault(fault)?;
        self.inner.find_operation(authority, operation, work).await
    }
    async fn replay(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, crate::AuthorityAllocation), AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = context.original {
            let cancellation = context.cancellation;

            use crate::materializer::NativeTreeMaterializationError;
            cancellation
                .check()
                .map_err(|_| AuthorityStoreError::Cancelled)?;
            work.admit(WorkCounters::UNCHARGED)?;
            let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityReplay)?;
            check_authority_fault(fault)?;
            let (mut records, custody) = self
                .inner
                .replay(authority_id, after, limit, context)
                .await?;
            let mut custody = custody.expect("native replay allocation");
            if !matches!(fault, Some(SimulationFault::DuplicateAuthorityReplay))
                || records.is_empty()
            {
                return Ok((records, Some(custody)));
            }

            let needs_growth = records.len() == records.capacity();
            let copied_records = if needs_growth {
                records.len().checked_add(1)
            } else {
                Some(1)
            }
            .ok_or(NativeTreeMaterializationError::Work(
                crate::WorkError::Overflow,
            ))?;
            let extra = WorkCounters {
                bytes_copied: copied_records
                    .checked_mul(std::mem::size_of::<DurableCommit>())
                    .ok_or(NativeTreeMaterializationError::Work(
                        crate::WorkError::Overflow,
                    ))? as u64,
                items_returned: 1,
                ..WorkCounters::UNCHARGED
            };
            work.admit(extra)?;
            if needs_growth {
                let count =
                    records
                        .len()
                        .checked_add(1)
                        .ok_or(NativeTreeMaterializationError::Work(
                            crate::WorkError::Overflow,
                        ))?;
                let old_bytes = records
                    .capacity()
                    .checked_mul(std::mem::size_of::<DurableCommit>())
                    .ok_or(NativeTreeMaterializationError::Work(
                        crate::WorkError::Overflow,
                    ))? as u64;
                let (mut replacement, mut replacement_custody) =
                    work.allocations(|ledger, counters, budget, cancellation| {
                        cancellation
                            .check()
                            .map_err(|_| AuthorityStoreError::Cancelled)?;
                        counters
                            .checked_add(extra)
                            .and_then(|projected| projected.verify(budget))
                            .map_err(NativeTreeMaterializationError::Work)?;
                        ledger
                            .claim_elements::<DurableCommit>(count, counters, budget)
                            .map_err(|error| {
                                error.into_work_or(NativeTreeMaterializationError::Work(
                                    crate::WorkError::Overflow,
                                ))
                            })?;
                        let mut replacement = Vec::new();
                        replacement
                            .try_reserve_exact(count)
                            .map_err(NativeTreeMaterializationError::AllocationFailed)?;
                        Ok::<_, AuthorityStoreError>(replacement)
                    })?;
                replacement.append(&mut records);
                drop(records);
                let moved = WorkCounters {
                    bytes_copied: extra.bytes_copied - std::mem::size_of::<DurableCommit>() as u64,
                    ..WorkCounters::UNCHARGED
                };
                let transferred = work
                    .record_admitted(moved)
                    .and_then(|()| custody.release(old_bytes))
                    .and_then(|()| custody.absorb(&mut replacement_custody));
                if let Err(error) = transferred {
                    // The buffer must retire before either unchanged failure guard.
                    drop(replacement);
                    return Err(error.into());
                }
                records = replacement;
            }
            let last = records.last().expect("nonempty replay page").clone();
            records.push(last);
            work.record_admitted(WorkCounters {
                bytes_copied: std::mem::size_of::<DurableCommit>() as u64,
                items_returned: 1,
                ..WorkCounters::UNCHARGED
            })?;
            return Ok((records, Some(custody)));
        }
        context
            .cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityReplay)?;
        check_authority_fault(fault)?;
        let (mut records, allocation) = self
            .inner
            .replay(authority_id, after, limit, context)
            .await?;
        if matches!(fault, Some(SimulationFault::DuplicateAuthorityReplay))
            && let Some(last) = records.last().cloned()
        {
            let copied = u64::try_from(last.payload.len()).unwrap_or(u64::MAX);
            let extra = WorkCounters {
                allocation_operations: 1,
                bytes_copied: copied,
                peak_allocation_bytes: copied,
                items_returned: 1,
                ..WorkCounters::UNCHARGED
            };
            extra
                .verify(context.remaining()?)
                .map_err(AuthorityStoreError::from)?;
            records.push(last);
            context.record(extra)?;
        }
        Ok((records, allocation))
    }
}

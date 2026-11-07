use super::*;

// One task borrow names the erased future lifetime; the work context remains
// independent. Construct the task synchronously at the authority boundary.

impl<P: crate::distributed::AuthorityProvider + ?Sized> AsyncAuthorityStore
    for StreamAuthorityStore<P>
{
    fn own_group<G: crate::OwnedAuthorityGuard>(
        &self,
        group: crate::OwnedAuthorityGroup<G>,
    ) -> Option<crate::OwnedAuthorityTask> {
        group.bind(self.clone())
    }

    fn supports_atomic_group_commit(&self) -> bool {
        true
    }

    fn quote_group_append<'task, 'context>(
        &'task self,
        request: &'task crate::GuardedAppend,
        growth: usize,
        work: &'task mut AuthorityWork<'context>,
    ) -> impl std::future::Future<
        Output = Result<Option<crate::AuthorityGroupCost>, AuthorityStoreError>,
    > + acyclic_stream::ProviderTask
    + use<'task, 'context, P> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            StreamAuthorityStore::quote_group_append(self, request, growth, work).await
        })
    }

    fn compare_and_append_group<'task, 'context, G: AuthorityGuard>(
        &'task self,
        requests: &'task [crate::GuardedAppend],
        cuts: &'task [crate::PolicyCut],
        work: &'task mut AuthorityWork<'context>,
        guard: Option<&'task mut G>,
    ) -> impl std::future::Future<
        Output = Result<super::AppendGroupOutcome, AuthorityError<G::Error>>,
    > + acyclic_stream::ProviderTask
    + use<'task, 'context, P, G> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            StreamAuthorityStore::compare_and_append_group(self, requests, cuts, work, guard).await
        })
    }

    fn policy_owner(&self) -> crate::PolicyOwner {
        if self.provider.context().process_exclusion() {
            crate::PolicyOwner::ExclusiveProcess
        } else {
            crate::PolicyOwner::AtomicPublication
        }
    }

    fn policy_cut<'task, 'context>(
        &'task self,
        authority: AuthorityId,
        work: &'task mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<crate::PolicyCut, AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, P> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            let snapshot = self.fork_snapshot(authority, work).await?;
            Ok(crate::PolicyCut::new(
                self.operation_claims().owner_identity(),
                authority,
                snapshot.head,
                snapshot.epoch_tail,
            ))
        })
    }
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.provider.context().operation_claims()
    }
    fn actor_scope(&self) -> [u8; 16] {
        self.placement.actor_scope()
    }

    fn supports_deferred_durability(&self) -> bool {
        self.provider.context().deferred_durability()
    }

    fn durability_barrier<'task, 'context>(
        &'task self,
        work: &'task mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<(), AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, P> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            work.check()?;
            #[cfg(not(target_arch = "wasm32"))]
            if work.is_native() {
                let meter = work.native_meter()?;
                return work
                    .result(self.provider.native_durability_barrier(meter).await)
                    .map(|((), _allocation)| ());
            }
            work.charge(WorkCounters {
                durability_operations: 1,
                ..WorkCounters::UNCHARGED
            })?;
            self.provider
                .durability_barrier()
                .await
                .map_err(map_stream_error)
        })
    }

    fn prepare_durability_barrier<'a>(
        &'a self,
        work: &mut AuthorityWork<'_>,
    ) -> Result<
        impl std::future::Future<Output = Result<(), AuthorityStoreError>>
        + acyclic_stream::ProviderTask
        + use<'a, P>,
        AuthorityStoreError,
    > {
        work.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        let native = if let Some(original) = work.original {
            let future = self
                .provider
                .prepare_native_durability_barrier(original.runtime_meter())
                .map_err(|error| work.native_io_failure(error))?;
            Some((future, original.clone()))
        } else {
            None
        };
        if !work.is_native() {
            work.charge(WorkCounters {
                durability_operations: 1,
                ..WorkCounters::UNCHARGED
            })?;
        }
        Ok(acyclic_stream::erase_provider_future(|| async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some((future, original)) = native {
                future
                    .await
                    .map_err(|error| {
                        AuthorityStoreError::NativeOriginal(original.runtime_error(error))
                    })?
                    .map_err(AuthorityStoreError::from)?;
                return Ok(());
            }
            self.provider
                .durability_barrier()
                .await
                .map_err(map_stream_error)
        }))
    }

    fn supports_generation_lineage_prefix(&self) -> bool {
        true
    }

    fn retains_generation<'task, 'context>(
        &'task self,
        authority: AuthorityId,
        generation: GenerationId,
        work: &'task mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<bool, AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, P> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.retained_generation(authority, generation, work).await
        })
    }

    fn create_authority<'task, 'context, G: AuthorityGuard>(
        &'task self,
        authority: AuthorityId,
        epoch: Epoch,
        work: &'task mut AuthorityWork<'context>,
        guard: Option<&'task mut G>,
    ) -> impl std::future::Future<Output = Result<CreateAuthorityOutcome, AuthorityError<G::Error>>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, P, G> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.create(authority, epoch, work, guard).await
        })
    }

    fn create_first_record<'task, 'context, G: AuthorityGuard>(
        &'task self,
        authority: AuthorityId,
        commit: ProposedCommit,
        work: &'task mut AuthorityWork<'context>,
        guard: Option<&'task mut G>,
    ) -> impl std::future::Future<Output = Result<bool, AuthorityError<G::Error>>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, P, G> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.first(authority, commit, work, guard).await
        })
    }

    fn fork_workspace<'task, 'context, G: AuthorityGuard>(
        &'task self,
        fork: WorkspaceForkCommit,
        permit: PublicationPermit,
        work: &'task mut AuthorityWork<'context>,
        guard: Option<&'task mut G>,
    ) -> impl std::future::Future<Output = Result<WorkspaceForkOutcome, AuthorityError<G::Error>>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, P, G> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.fork_records(fork, permit, work, guard).await
        })
    }

    fn fork_authority<'task, 'context, G: AuthorityGuard>(
        &'task self,
        source: crate::GenerationForkSource,
        destination: AuthorityId,
        operation: OperationId,
        work: &'task mut AuthorityWork<'context>,
        guard: Option<&'task mut G>,
    ) -> impl std::future::Future<Output = Result<CreateAuthorityOutcome, AuthorityError<G::Error>>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, P, G> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            if source.lineage == crate::GenerationFork::Independent {
                return self
                    .create_authority(destination, Epoch::GENESIS, work, guard)
                    .await;
            }
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.fork_generation(source, destination, operation, work, guard)
                .await
        })
    }

    fn compare_and_append<'task, 'context, G: AuthorityGuard>(
        &'task self,
        request: crate::GuardedAppend,
        work: &'task mut AuthorityWork<'context>,
        guard: Option<&'task mut G>,
    ) -> impl std::future::Future<Output = Result<FsAppendOutcome, AuthorityError<G::Error>>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, P, G> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.append(request, work, guard)
                .await
                .and_then(|value| work.finish(value).map_err(Into::into))
        })
    }

    fn head<'task, 'context>(
        &'task self,
        authority: AuthorityId,
        work: &'task mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<Head, AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, P> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.fork_snapshot(authority, work)
                .await
                .and_then(|snapshot| work.finish(snapshot.head))
        })
    }

    fn head_payload<'task, 'context>(
        &'task self,
        authority: AuthorityId,
        maximum_payload_bytes: u64,
        work: &'task mut AuthorityWork<'context>,
    ) -> impl std::future::Future<
        Output = Result<((Head, Option<Bytes>), AuthorityAllocation), AuthorityStoreError>,
    > + acyclic_stream::ProviderTask
    + use<'task, 'context, P> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            self.head_payload_checked::<true>(authority, maximum_payload_bytes, work)
                .await
        })
    }

    fn first_generation<'task, 'context>(
        &'task self,
        authority: AuthorityId,
        generation: GenerationId,
        through: Sequence,
        work: &'task mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<Option<Sequence>, AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, P> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            self.seek_generation(authority, generation, through, work)
                .await
        })
    }

    fn replay<'task, 'context>(
        &'task self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &'task mut AuthorityWork<'context>,
    ) -> impl std::future::Future<
        Output = Result<(Vec<DurableCommit>, AuthorityAllocation), AuthorityStoreError>,
    > + acyclic_stream::ProviderTask
    + use<'task, 'context, P> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            if limit.records == 0 || limit.payload_bytes == 0 {
                return Err(AuthorityStoreError::InvalidReplayLimit);
            }
            let mut scope = work.scope()?;
            self.authority_require_live(authority, &mut scope).await?;
            let value = self
                .read_records(authority, after, limit, &mut scope)
                .await?;
            Ok(crate::async_storage::authority_parts(
                scope.finish_owned(value).map_err(authority_error)?,
            ))
        })
    }

    fn fence<'task, 'context>(
        &'task self,
        authority: AuthorityId,
        expected: Head,
        work: &'task mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<FenceOutcome, AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, P> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.authority_fence(authority, expected, work)
                .await
                .and_then(|value| work.finish(value))
        })
    }

    fn find_operation<'task, 'context>(
        &'task self,
        authority: AuthorityId,
        operation: OperationId,
        work: &'task mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<Option<DurableCommit>, AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, P> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.authority_find_operation(authority, operation, work)
                .await
                .and_then(|value| work.finish(value))
        })
    }

    fn publication_reservation<'task, 'context>(
        &'task self,
        authority: AuthorityId,
        operation: OperationId,
        work: &'task mut AuthorityWork<'context>,
    ) -> impl std::future::Future<
        Output = Result<Option<PublicationReservation>, AuthorityStoreError>,
    > + acyclic_stream::ProviderTask
    + use<'task, 'context, P> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.reservation(authority, operation, work).await
        })
    }

    fn reserve_publication<'task, 'context>(
        &'task self,
        authority: AuthorityId,
        expected: Head,
        operation: OperationId,
        work: &'task mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<ReservationOutcome, AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, P> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.reserve(authority, expected, operation, work).await
        })
    }

    fn release_publication<'task, 'context>(
        &'task self,
        reservation: PublicationReservation,
        work: &'task mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<(), AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, P> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.release(reservation, work).await
        })
    }

    fn retire_authority<'task, 'context>(
        &'task self,
        authority: AuthorityId,
        work: &'task mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<(), AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'task, 'context, P> {
        acyclic_stream::erase_provider_future::<'task, _>(move || async move {
            let actor = self.actor(authority, work)?;
            let journal = actor.lease(work).await?;
            let _revision = actor.begin(journal)?;
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() {
                return Err(AuthorityStoreError::NativeOriginalUnsupported);
            }
            work.cancellation
                .check()
                .map_err(|_| AuthorityStoreError::Cancelled)?;
            let root = self.placement.stream_path(authority, "")?;
            let path = self.placement.stream_path(authority, "/retirement")?;
            work.record(authority_read_work(1))?;
            work.remaining()?;
            match self.provider.tail(root).await {
                Err(acyclic_stream::StreamError::NotFound) => return Ok(()),
                Ok(_) => {}
                Err(error) => return Err(map_stream_error(error)),
            }
            work.record(WorkCounters {
                authority_records_appended: 1,
                authority_bytes_written: AUTHORITY_RETIRED.len() as u64,
                backend_write_operations: 1,
                durability_operations: 1,
                ..WorkCounters::UNCHARGED
            })?;
            work.remaining()?;
            let request = acyclic_stream::AppendRequest {
                path,
                records: vec![acyclic_stream::OwnedBytes::from_static(AUTHORITY_RETIRED).into()],
                if_tail: Some(0),
                idempotency_key: placed_stream_key(
                    self.placement.key_scope(),
                    b"retire-authority",
                    &authority.into_bytes(),
                )?,
            };
            match self.provider.append(request).await {
                Ok(acyclic_stream::AppendOutcome::Committed(_)) => Ok(()),
                Ok(acyclic_stream::AppendOutcome::TailConflict { .. }) => {
                    work.record(authority_read_work(2))?;
                    work.remaining()?;
                    if self.is_retired(authority).await? {
                        Ok(())
                    } else {
                        Err(AuthorityStoreError::CorruptStatic(
                            "filesystem retirement marker conflicted without retirement",
                        ))
                    }
                }
                Err(error) => Err(map_stream_error(error)),
            }
        })
    }
}

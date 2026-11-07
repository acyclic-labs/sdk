use super::*;

// Construct the erased future before returning so the outer future holds only
// that erased task. Keep each original input borrow independent.

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

    fn quote_group_append<'store, 'request, 'work, 'context>(
        &'store self,
        request: &'request crate::GuardedAppend,
        growth: usize,
        work: &'work mut AuthorityWork<'context>,
    ) -> impl std::future::Future<
        Output = Result<Option<crate::AuthorityGroupCost>, AuthorityStoreError>,
    > + acyclic_stream::ProviderTask
    + use<'store, 'request, 'work, 'context, P> {
        let future = acyclic_stream::erase_provider_future(|| async move {
            StreamAuthorityStore::quote_group_append(self, request, growth, work).await
        });
        async move { future.await }
    }

    fn compare_and_append_group<
        'store,
        'requests,
        'cuts,
        'work,
        'guard,
        'context,
        G: AuthorityGuard,
    >(
        &'store self,
        requests: &'requests [crate::GuardedAppend],
        cuts: &'cuts [crate::PolicyCut],
        work: &'work mut AuthorityWork<'context>,
        guard: Option<&'guard mut G>,
    ) -> impl std::future::Future<
        Output = Result<super::AppendGroupOutcome, AuthorityError<G::Error>>,
    > + acyclic_stream::ProviderTask
    + use<'store, 'requests, 'cuts, 'work, 'guard, 'context, P, G> {
        let future = acyclic_stream::erase_provider_future(|| async move {
            StreamAuthorityStore::compare_and_append_group(self, requests, cuts, work, guard).await
        });
        async move { future.await }
    }

    fn policy_owner(&self) -> crate::PolicyOwner {
        if self.provider.context().process_exclusion() {
            crate::PolicyOwner::ExclusiveProcess
        } else {
            crate::PolicyOwner::AtomicPublication
        }
    }

    fn policy_cut<'store, 'work, 'context>(
        &'store self,
        authority: AuthorityId,
        work: &'work mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<crate::PolicyCut, AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'store, 'work, 'context, P> {
        let future = acyclic_stream::erase_provider_future(|| async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            let snapshot = self.fork_snapshot(authority, work).await?;
            Ok(crate::PolicyCut::new(
                self.operation_claims().owner_identity(),
                authority,
                snapshot.head,
                snapshot.epoch_tail,
            ))
        });
        async move { future.await }
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

    fn durability_barrier<'store, 'work, 'context>(
        &'store self,
        work: &'work mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<(), AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'store, 'work, 'context, P> {
        let future = acyclic_stream::erase_provider_future(|| async move {
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
        });
        async move { future.await }
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

    fn retains_generation<'store, 'work, 'context>(
        &'store self,
        authority: AuthorityId,
        generation: GenerationId,
        work: &'work mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<bool, AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'store, 'work, 'context, P> {
        let future = acyclic_stream::erase_provider_future(|| async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.retained_generation(authority, generation, work).await
        });
        async move { future.await }
    }

    fn create_authority<'store, 'work, 'guard, 'context, G: AuthorityGuard>(
        &'store self,
        authority: AuthorityId,
        epoch: Epoch,
        work: &'work mut AuthorityWork<'context>,
        guard: Option<&'guard mut G>,
    ) -> impl std::future::Future<Output = Result<CreateAuthorityOutcome, AuthorityError<G::Error>>>
    + acyclic_stream::ProviderTask
    + use<'store, 'work, 'guard, 'context, P, G> {
        let future = acyclic_stream::erase_provider_future(|| async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.create(authority, epoch, work, guard).await
        });
        async move { future.await }
    }

    fn create_first_record<'store, 'work, 'guard, 'context, G: AuthorityGuard>(
        &'store self,
        authority: AuthorityId,
        commit: ProposedCommit,
        work: &'work mut AuthorityWork<'context>,
        guard: Option<&'guard mut G>,
    ) -> impl std::future::Future<Output = Result<bool, AuthorityError<G::Error>>>
    + acyclic_stream::ProviderTask
    + use<'store, 'work, 'guard, 'context, P, G> {
        let future = acyclic_stream::erase_provider_future(|| async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.first(authority, commit, work, guard).await
        });
        async move { future.await }
    }

    fn fork_workspace<'store, 'work, 'guard, 'context, G: AuthorityGuard>(
        &'store self,
        fork: WorkspaceForkCommit,
        permit: PublicationPermit,
        work: &'work mut AuthorityWork<'context>,
        guard: Option<&'guard mut G>,
    ) -> impl std::future::Future<Output = Result<WorkspaceForkOutcome, AuthorityError<G::Error>>>
    + acyclic_stream::ProviderTask
    + use<'store, 'work, 'guard, 'context, P, G> {
        let future = acyclic_stream::erase_provider_future(|| async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.fork_records(fork, permit, work, guard).await
        });
        async move { future.await }
    }

    fn fork_authority<'store, 'work, 'guard, 'context, G: AuthorityGuard>(
        &'store self,
        source: crate::GenerationForkSource,
        destination: AuthorityId,
        operation: OperationId,
        work: &'work mut AuthorityWork<'context>,
        guard: Option<&'guard mut G>,
    ) -> impl std::future::Future<Output = Result<CreateAuthorityOutcome, AuthorityError<G::Error>>>
    + acyclic_stream::ProviderTask
    + use<'store, 'work, 'guard, 'context, P, G> {
        let future = acyclic_stream::erase_provider_future(|| async move {
            if source.lineage == crate::GenerationFork::Independent {
                return self
                    .create_authority(destination, Epoch::GENESIS, work, guard)
                    .await;
            }
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.fork_generation(source, destination, operation, work, guard)
                .await
        });
        async move { future.await }
    }

    fn compare_and_append<'store, 'work, 'guard, 'context, G: AuthorityGuard>(
        &'store self,
        request: crate::GuardedAppend,
        work: &'work mut AuthorityWork<'context>,
        guard: Option<&'guard mut G>,
    ) -> impl std::future::Future<Output = Result<FsAppendOutcome, AuthorityError<G::Error>>>
    + acyclic_stream::ProviderTask
    + use<'store, 'work, 'guard, 'context, P, G> {
        let future = acyclic_stream::erase_provider_future(|| async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.append(request, work, guard)
                .await
                .and_then(|value| work.finish(value).map_err(Into::into))
        });
        async move { future.await }
    }

    fn head<'store, 'work, 'context>(
        &'store self,
        authority: AuthorityId,
        work: &'work mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<Head, AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'store, 'work, 'context, P> {
        let future = acyclic_stream::erase_provider_future(|| async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.fork_snapshot(authority, work)
                .await
                .and_then(|snapshot| work.finish(snapshot.head))
        });
        async move { future.await }
    }

    fn head_payload<'store, 'work, 'context>(
        &'store self,
        authority: AuthorityId,
        maximum_payload_bytes: u64,
        work: &'work mut AuthorityWork<'context>,
    ) -> impl std::future::Future<
        Output = Result<((Head, Option<Bytes>), AuthorityAllocation), AuthorityStoreError>,
    > + acyclic_stream::ProviderTask
    + use<'store, 'work, 'context, P> {
        let future = acyclic_stream::erase_provider_future(|| async move {
            self.head_payload_checked::<true>(authority, maximum_payload_bytes, work)
                .await
        });
        async move { future.await }
    }

    fn first_generation<'store, 'work, 'context>(
        &'store self,
        authority: AuthorityId,
        generation: GenerationId,
        through: Sequence,
        work: &'work mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<Option<Sequence>, AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'store, 'work, 'context, P> {
        let future = acyclic_stream::erase_provider_future(|| async move {
            self.seek_generation(authority, generation, through, work)
                .await
        });
        async move { future.await }
    }

    fn replay<'store, 'work, 'context>(
        &'store self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &'work mut AuthorityWork<'context>,
    ) -> impl std::future::Future<
        Output = Result<(Vec<DurableCommit>, AuthorityAllocation), AuthorityStoreError>,
    > + acyclic_stream::ProviderTask
    + use<'store, 'work, 'context, P> {
        let future = acyclic_stream::erase_provider_future(|| async move {
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
        });
        async move { future.await }
    }

    fn fence<'store, 'work, 'context>(
        &'store self,
        authority: AuthorityId,
        expected: Head,
        work: &'work mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<FenceOutcome, AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'store, 'work, 'context, P> {
        let future = acyclic_stream::erase_provider_future(|| async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.authority_fence(authority, expected, work)
                .await
                .and_then(|value| work.finish(value))
        });
        async move { future.await }
    }

    fn find_operation<'store, 'work, 'context>(
        &'store self,
        authority: AuthorityId,
        operation: OperationId,
        work: &'work mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<Option<DurableCommit>, AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'store, 'work, 'context, P> {
        let future = acyclic_stream::erase_provider_future(|| async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.authority_find_operation(authority, operation, work)
                .await
                .and_then(|value| work.finish(value))
        });
        async move { future.await }
    }

    fn publication_reservation<'store, 'work, 'context>(
        &'store self,
        authority: AuthorityId,
        operation: OperationId,
        work: &'work mut AuthorityWork<'context>,
    ) -> impl std::future::Future<
        Output = Result<Option<PublicationReservation>, AuthorityStoreError>,
    > + acyclic_stream::ProviderTask
    + use<'store, 'work, 'context, P> {
        let future = acyclic_stream::erase_provider_future(|| async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.reservation(authority, operation, work).await
        });
        async move { future.await }
    }

    fn reserve_publication<'store, 'work, 'context>(
        &'store self,
        authority: AuthorityId,
        expected: Head,
        operation: OperationId,
        work: &'work mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<ReservationOutcome, AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'store, 'work, 'context, P> {
        let future = acyclic_stream::erase_provider_future(|| async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.reserve(authority, expected, operation, work).await
        });
        async move { future.await }
    }

    fn release_publication<'store, 'work, 'context>(
        &'store self,
        reservation: PublicationReservation,
        work: &'work mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<(), AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'store, 'work, 'context, P> {
        let future = acyclic_stream::erase_provider_future(|| async move {
            let mut scope = work.scope()?;
            let work = &mut *scope;
            self.release(reservation, work).await
        });
        async move { future.await }
    }

    fn retire_authority<'store, 'work, 'context>(
        &'store self,
        authority: AuthorityId,
        work: &'work mut AuthorityWork<'context>,
    ) -> impl std::future::Future<Output = Result<(), AuthorityStoreError>>
    + acyclic_stream::ProviderTask
    + use<'store, 'work, 'context, P> {
        let future = acyclic_stream::erase_provider_future(|| async move {
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
        });
        async move { future.await }
    }
}

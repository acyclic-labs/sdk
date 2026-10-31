// The ordinary append builder contributes at most nine transaction items;
// every member can additionally carry the three source-policy fences.
pub(crate) const MAX_AUTHORITY_GROUP_MEMBERS: usize = acyclic_stream::MAX_ITEMS / 12;

use super::*;
mod cost;
pub use cost::AuthorityGroupCost;

/// Atomic submission of an already composed, dense authority history.
pub enum AppendGroupOutcome {
    /// Every member was admitted by one provider transaction.
    Published {
        /// One outcome per member in authority order.
        receipts: Vec<AppendOutcome>,
        /// Retain this allocation until the result vector is destroyed.
        allocation: AuthorityAllocation,
    },
    /// No prefix was published; this member needs recovery or rebase.
    Rejected {
        /// Member whose precondition failed.
        index: usize,
        /// Canonical single-append result for that member.
        outcome: AppendOutcome,
        /// Authenticated durable head observed when admitting this group.
        /// This excludes its uncommitted prefix and may precede concurrent
        /// appends; callers must not replace it with a later head probe.
        observed: Head,
    },
}

contract! {
/// Nonblocking authority-store contract.
pub trait AsyncAuthorityStore: ProviderPlatform {
    /// Transfers a group to its actual owner. An absent task means its terminal
    /// reply has already been delivered, including unsupported dispatch.
    fn own_group<G: OwnedAuthorityGuard>(&self, group: OwnedAuthorityGroup<G>) -> Option<OwnedAuthorityTask> {
        drop(group);
        None
    }
    /// Live ownership of this backend owner, preserved by forwarding adapters.
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims;
    /// Separates actor registries of distinct stores sharing one owner, such
    /// as two Streams placements on one provider. Forwarding adapters forward it.
    fn actor_scope(&self) -> [u8; 16];
    /// Backend guarantee used to keep authorization and publication on one cut.
    fn policy_owner(&self) -> PolicyOwner {
        PolicyOwner::Unsupported
    }
    /// Captures a cut which this backend checks atomically at publication.
    fn policy_cut(
        &self,
        _authority: AuthorityId,
        _work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<PolicyCut, crate::AuthorityStoreError>> + ProviderTask {
        std::future::ready(Err(crate::AuthorityStoreError::RejectedStatic(
            "authority backend does not support atomic policy cuts",
        )))
    }
    /// Whether a phase may defer authority flushes until an explicit barrier.
    fn supports_deferred_durability(&self) -> bool {
        false
    }
    /// Seals all authority writes accepted by this store before the call.
    fn durability_barrier(
        &self,
        _work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), crate::AuthorityStoreError>> + ProviderTask {
        std::future::ready(Err(crate::AuthorityStoreError::Retired))
    }
    /// Funds a phase's final flush before accepting deferred writes. The
    /// returned future must complete without consulting caller cancellation.
    fn prepare_durability_barrier<'a>(
        &'a self,
        _work: &mut AuthorityWork<'_>,
    ) -> Result<impl Future<Output = Result<(), crate::AuthorityStoreError>> + ProviderTask + use<'a, Self>, crate::AuthorityStoreError> {
        Err::<std::future::Ready<Result<(), crate::AuthorityStoreError>>, _>(crate::AuthorityStoreError::Retired)
    }
    /// Whether creation can retain a published-generation prefix. Such a
    /// store's `fork_workspace` also forks the source's changes stream in the
    /// same commit when the creation seal inherits them.
    fn supports_generation_lineage_prefix(&self) -> bool {
        false
    }

    /// Whether durable published lineage already retains this exact generation.
    /// Unsupported backends conservatively require an independent retention pin.
    fn retains_generation(
        &self,
        _authority: AuthorityId,
        _generation: GenerationId,
        _work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<bool, crate::AuthorityStoreError>> + ProviderTask {
        std::future::ready(Ok(false))
    }

    /// Creates an authority, sealing publication and recovery.
    fn create_authority<G: AuthorityGuard>(
        &self,
        _authority: AuthorityId,
        _epoch: Epoch,
        _work: &mut AuthorityWork<'_>,
        _guard: Option<&mut G>,
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, AuthorityError<G::Error>>> + ProviderTask {
        std::future::ready(Err(crate::AuthorityStoreError::Retired.into()))
    }

    /// Creates an authority rooted at one source generation.
    fn fork_authority<G: AuthorityGuard>(
        &self,
        source: GenerationForkSource,
        destination: AuthorityId,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, AuthorityError<G::Error>>> + ProviderTask
    {
        async move {
            let mut guard = guard;
            if source.lineage == GenerationFork::PublishedPrefix {
                #[cfg(not(target_arch = "wasm32"))]
                if work.original.is_some() {
                    return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
                }
                seal(guard.as_deref_mut(), work).await?;
                return Err(crate::AuthorityStoreError::RejectedStatic(
                    "authority store does not retain generation lineage prefixes",
                )
                .into());
            }
            let _ = operation;
            self.create_authority(destination, Epoch::GENESIS, work, guard)
                .await
        }
    }

    /// Creates or authenticates an authority's exact first record.
    fn create_first_record<G: AuthorityGuard>(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> impl Future<Output = Result<bool, AuthorityError<G::Error>>> + ProviderTask {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() {
                return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
            }
            first_record(self, authority, commit, work, guard).await
        }
    }

    /// Publishes retention before a forked workspace's creation.
    fn fork_workspace<G: AuthorityGuard>(
        &self,
        fork: WorkspaceForkCommit,
        permit: PublicationPermit,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> impl Future<Output = Result<WorkspaceForkOutcome, AuthorityError<G::Error>>> + ProviderTask
    {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() && guard.is_some() {
                return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
            }
            workspace_fork(self, fork, permit, work, guard).await
        }
    }

    /// Retires authority metadata after workspace deletion is durable.
    fn retire_authority(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), crate::AuthorityStoreError>> + ProviderTask {
        let _ = (authority, work);
        std::future::ready(Ok(()))
    }

    /// Reads the linearizable authority head.
    fn head(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<Head, crate::AuthorityStoreError>> + ProviderTask;

    /// Reads the head and its bounded, authenticated last payload together.
    /// Genesis has no durable record. Backends may reuse the head's exact read.
    /// Keep the returned allocation alive until the payload is destroyed.
    fn head_payload(
        &self,
        authority: AuthorityId,
        maximum_payload_bytes: u64,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<((Head, Option<bytes::Bytes>), crate::AuthorityAllocation), crate::AuthorityStoreError>>
    + ProviderTask {
        async move {
            let head = self.head(authority, work).await?;
            if head.sequence == Sequence::GENESIS {
                return Ok(((head, None), None));
            }
            let (records, allocation) = self.replay(
                authority,
                Sequence::new(head.sequence.get().saturating_sub(1)),
                ReplayLimit { records: 1, payload_bytes: maximum_payload_bytes },
                work,
            ).await?;
            let payload = records.into_iter().next().map(|record| record.payload);
            Ok(((head, payload), allocation))
        }
    }

    /// Compares and appends under the caller's publication permit and guard.
    fn compare_and_append<G: AuthorityGuard>(
        &self,
        _request: GuardedAppend,
        _work: &mut AuthorityWork<'_>,
        _guard: Option<&mut G>,
    ) -> impl Future<Output = Result<AppendOutcome, AuthorityError<G::Error>>> + ProviderTask {
        std::future::ready(Err(crate::AuthorityStoreError::Retired.into()))
    }

    /// Whether this adapter exposes the canonical atomic authority-group path.
    fn supports_atomic_group_commit(&self) -> bool {
        false
    }

    /// Quotes this proposal using the canonical append builder, without publication.
    /// The cost includes the caller's maximum payload growth during composition.
    fn quote_group_append(&self, _request: &GuardedAppend, _payload_growth: usize, _work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<Option<AuthorityGroupCost>, crate::AuthorityStoreError>> + ProviderTask {
        std::future::ready(Ok(None))
    }

    /// Submits a precomposed serial authority history as one atomic transaction.
    /// Unsupported adapters fail closed; they must never emulate atomicity by
    /// appending a prefix and then discovering that a later member conflicts.
    fn compare_and_append_group<G: AuthorityGuard>(
        &self,
        _requests: &[GuardedAppend],
        _cuts: &[crate::PolicyCut],
        _work: &mut AuthorityWork<'_>,
        _guard: Option<&mut G>,
    ) -> impl Future<Output = Result<AppendGroupOutcome, AuthorityError<G::Error>>> + ProviderTask {
        std::future::ready(Err(crate::AuthorityStoreError::RejectedStatic(
            "authority adapter does not support atomic groups",
        ).into()))
    }

    /// Reads an existing reservation for the exact operation identity.
    fn publication_reservation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<Option<PublicationReservation>, crate::AuthorityStoreError>>
    + ProviderTask {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = work.original {
                original.admit(WorkCounters::UNCHARGED)?;
            }
            work.cancellation
                .check()
                .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() {
                return Err(crate::AuthorityStoreError::NativeOriginalUnsupported);
            }
            let _ = (authority, operation);
            Err(crate::AuthorityStoreError::RejectedStatic(
                "authority backend cannot query original publication reservation evidence",
            ))
        }
    }

    /// Acquires the exclusive publication gate at an exact head.
    fn reserve_publication(
        &self,
        authority: AuthorityId,
        expected: Head,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<ReservationOutcome, crate::AuthorityStoreError>> + ProviderTask
    {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() {
                work.check()?;
                return Err(crate::AuthorityStoreError::NativeOriginalUnsupported);
            }
            let _ = (authority, expected, operation, work);
            Err(crate::AuthorityStoreError::RejectedStatic(
                "authority backend does not support durable publication reservations",
            ))
        }
    }

    /// Releases the exact publication reservation.
    fn release_publication(
        &self,
        reservation: PublicationReservation,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<(), crate::AuthorityStoreError>> + ProviderTask {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() {
                work.check()?;
                return Err(crate::AuthorityStoreError::NativeOriginalUnsupported);
            }
            let _ = (reservation, work);
            Err(crate::AuthorityStoreError::RejectedStatic(
                "authority backend does not support durable publication reservations",
            ))
        }
    }

    /// Replays one bounded page while retaining its native storage.
    fn replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<
        Output = Result<
            (Vec<crate::DurableCommit>, AuthorityAllocation),
            crate::AuthorityStoreError,
        >,
    > + ProviderTask;

    /// Advances a writer fence.
    fn fence(
        &self,
        _authority: AuthorityId,
        _expected: Head,
        _work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<FenceOutcome, crate::AuthorityStoreError>> + ProviderTask {
        std::future::ready(Err(crate::AuthorityStoreError::Retired))
    }

    /// Resolves an idempotent operation.
    fn find_operation(
        &self,
        _authority: AuthorityId,
        _operation: OperationId,
        _work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<Option<crate::DurableCommit>, crate::AuthorityStoreError>>
    + ProviderTask {
        std::future::ready(Err(crate::AuthorityStoreError::Retired))
    }
}

}

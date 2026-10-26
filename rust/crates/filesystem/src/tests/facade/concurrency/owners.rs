//! Distinct process owners share durable state, never the combiner registry.
use super::*;

#[derive(Clone)]
struct ForeignOwner {
    inner: Arc<crate::StreamAuthorityStore<acyclic_stream::MemoryStream>>,
    claims: Arc<acyclic_stream::OperationClaims>,
    forbid_probe: Arc<std::sync::atomic::AtomicBool>,
    racing_operation: Option<OperationId>,
    forbid_replay: Arc<std::sync::atomic::AtomicBool>,
    dispatch_gate: Option<Arc<DispatchGate>>,
}

impl AsyncAuthorityStore for ForeignOwner {
    fn own_group<G: crate::OwnedAuthorityGuard>(
        &self,
        group: crate::OwnedAuthorityGroup<G>,
    ) -> Option<crate::OwnedAuthorityTask> {
        group.bind(self.clone())
    }

    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        &self.claims
    }
    fn actor_scope(&self) -> [u8; 16] {
        [0; 16]
    }
    fn supports_atomic_group_commit(&self) -> bool {
        self.inner.supports_atomic_group_commit()
    }
    async fn head(
        &self,
        authority: AuthorityId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Head, crate::AuthorityStoreError> {
        if self.forbid_probe.load(Ordering::SeqCst) {
            return Err(crate::AuthorityStoreError::RejectedStatic(
                "head probe after a rejected group",
            ));
        }
        self.inner.head(authority, work).await
    }
    async fn head_payload(
        &self,
        authority: AuthorityId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<
        ((Head, Option<bytes::Bytes>), crate::AuthorityAllocation),
        crate::AuthorityStoreError,
    > {
        if let Some(gate) = &self.dispatch_gate {
            gate.observe_preflight();
        }
        self.inner.head_payload(authority, maximum, work).await
    }
    async fn replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: crate::ReplayLimit,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::DurableCommit>, crate::AuthorityAllocation), crate::AuthorityStoreError>
    {
        if self.forbid_replay.load(Ordering::SeqCst) {
            return Err(crate::AuthorityStoreError::RejectedStatic(
                "recent actor witness required",
            ));
        }
        self.inner.replay(authority, after, limit, work).await
    }
    async fn find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Option<crate::DurableCommit>, crate::AuthorityStoreError> {
        self.inner.find_operation(authority, operation, work).await
    }
    async fn quote_group_append(
        &self,
        request: &crate::GuardedAppend,
        growth: usize,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Option<crate::AuthorityGroupCost>, crate::AuthorityStoreError> {
        if let Some(gate) = &self.dispatch_gate
            && gate.active.load(Ordering::SeqCst)
        {
            gate.quotes.fetch_add(1, Ordering::SeqCst);
        }
        // A quote is not an idempotency reservation. Model a quote obtained
        // before this operation became durable: another fresh UUID has the
        // same encoded cost, while admission still sees the real operation.
        if self.racing_operation == Some(request.commit.operation_id) {
            let mut quote = request.clone();
            quote.commit.operation_id = OperationId::new();
            self.inner.quote_group_append(&quote, growth, work).await
        } else {
            self.inner.quote_group_append(request, growth, work).await
        }
    }
    async fn compare_and_append<G: crate::AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut crate::AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<crate::AppendOutcome, crate::AuthorityError<G::Error>> {
        self.inner.compare_and_append(request, work, guard).await
    }
    async fn compare_and_append_group<G: crate::AuthorityGuard>(
        &self,
        requests: &[crate::GuardedAppend],
        cuts: &[crate::PolicyCut],
        work: &mut crate::AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<crate::AppendGroupOutcome, crate::AuthorityError<G::Error>> {
        if let Some(gate) = &self.dispatch_gate {
            gate.appends.fetch_add(1, Ordering::SeqCst);
            gate.active.store(true, Ordering::SeqCst);
            gate.started.notify_one();
            gate.release.notified().await;
            gate.active.store(false, Ordering::SeqCst);
        }
        if let Some(first) = requests.first()
            && self.racing_operation == Some(first.commit.operation_id)
        {
            // A takeover after the quote makes admission resolve the existing
            // semantic operation before rejecting its stale writer epoch.
            // Exact-head duplicates instead settle through the atomic locator.
            self.inner
                .fence(first.authority_id, first.expected, work)
                .await?;
        }
        let result = self
            .inner
            .compare_and_append_group(requests, cuts, work, guard)
            .await;
        if matches!(result, Ok(crate::AppendGroupOutcome::Rejected { .. })) {
            self.forbid_probe.store(true, Ordering::SeqCst);
        }
        result
    }
}

async fn edit<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    volume: &Volume<A, O>,
    name: &str,
    cancel: &CancellationToken,
) -> TestResult<Checkout<A, O>> {
    let mut checkout = open_checkout(volume, writable_manual(), cancel).await?;
    checkout
        .change_content(
            path(name)?,
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::from_static(b"new"),
            },
            ContentTimes::Preserve,
            B,
            cancel,
        )
        .await?;
    Ok(checkout)
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) async fn submit_proposal<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
>(
    rejected: &mut Checkout<A, O>,
    operation: OperationId,
    native: bool,
    cancel: &CancellationToken,
) -> TestResult<LivePublicationObservation> {
    let expected = rejected.authority_head.ok_or("missing head")?;
    // Bypass the facade's optional pre-resolution, exercising the shared
    // group path's own idempotency admission and rejected-group reply.
    let outcome = if native {
        let original =
            crate::NativeMaterializationWork::new(B, cancel).map_err(|(error, _)| error)?;
        *rejected =
            rejected.private_candidate(&mut KernelAccounting::native(&original, cancel), cancel)?;
        rejected.validate_native_work(&original)?;
        let mut seal =
            |_: &crate::NativeMaterializationWork| std::future::ready(Ok::<_, FsError>(()));
        rejected
            .publish_pending_against_with_native_work(
                operation,
                None,
                None,
                None,
                PublicationPermit::Unrestricted,
                expected,
                &original,
                cancel,
                &mut seal,
                None,
                None,
            )
            .await?
    } else {
        let mut seal = allow::<FsError>;
        rejected
            .publish_pending_against(
                operation,
                None,
                None,
                None,
                PublicationPermit::Unrestricted,
                expected,
                B,
                cancel,
                &mut seal,
                None,
                None,
            )
            .await?
            .value
    };
    Ok(outcome)
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn ordinary_rejected_groups_reply_without_a_head_probe() -> TestResult {
    check_rejected_group_reply(false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_rejected_groups_reply_without_a_head_probe() -> TestResult {
    check_rejected_group_reply(true).await
}

#[cfg(not(target_arch = "wasm32"))]
async fn check_rejected_group_reply(native: bool) -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    for name in ["a", "b"] {
        create(&mut seed, name, OwnedBytes::from_static(b"old"), &cancel).await?;
    }
    commit(&mut seed, &cancel).await?;
    let operation = OperationId::new();
    let mut winner = edit(&volume, "a", &cancel).await?;
    winner
        .commit(
            operation,
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        )
        .await?;
    let observer = Fs::new(
        ForeignOwner {
            inner: Arc::new(crate::StreamAuthorityStore::new(
                volume.fs.inner.authority.provider(),
            )),
            claims: Default::default(),
            forbid_probe: Default::default(),
            racing_operation: Some(operation),
            forbid_replay: Default::default(),
            dispatch_gate: None,
        },
        crate::LogicalObjectStore::new(
            Arc::clone(volume.fs.inner.objects.provider()),
            volume.fs.inner.objects.bucket().clone(),
        ),
        EmbeddedCapabilities::MEMORY,
    );
    let observed = observer.open_volume(volume.id(), B, &cancel).await?.value;
    let mut rejected = edit(&observed, "b", &cancel).await?;
    let outcome = submit_proposal(&mut rejected, operation, native, &cancel).await?;
    assert!(matches!(
        outcome,
        LivePublicationObservation::IdempotencyConflict { .. }
    ));
    assert!(
        observer.inner.authority.forbid_probe.load(Ordering::SeqCst),
        "the control did not reach a rejected group"
    );
    assert!(rejected.has_pending_mutations());
    Ok(())
}

#[tokio::test]
async fn independent_owners_use_durable_frontiers_and_recover_original_operations() -> TestResult {
    let cancel = CancellationToken::new();
    let authority = Arc::new(crate::StreamAuthorityStore::new(Arc::new(
        acyclic_stream::MemoryStream::default(),
    )));
    let objects = Arc::new(crate::MemoryObjectStore::default());
    let first = Fs::new(
        Arc::clone(&authority),
        Arc::clone(&objects),
        EmbeddedCapabilities::MEMORY,
    );
    let volume = first.create_volume(config(), B, &cancel).await?.value;
    let mut seed = open_checkout(&volume, writable_manual(), &cancel).await?;
    for name in ["a", "b"] {
        seed.create(
            AuthoredMutation::CreateFile {
                path: path(name)?,
                bytes: OwnedBytes::from_static(b"old"),
                metadata: FileMetadata::default(),
            },
            B,
            &cancel,
        )
        .await?;
    }
    commit(&mut seed, &cancel).await?;
    let second = Fs::new(
        ForeignOwner {
            inner: authority,
            claims: Default::default(),
            forbid_probe: Default::default(),
            racing_operation: None,
            forbid_replay: Default::default(),
            dispatch_gate: None,
        },
        objects,
        EmbeddedCapabilities::MEMORY,
    );
    assert_ne!(
        first.operation_claims().owner_identity(),
        second.operation_claims().owner_identity()
    );
    let remote_volume = second.open_volume(volume.id(), B, &cancel).await?.value;
    let mut stale = edit(&volume, "a", &cancel).await?;
    let mut retry = edit(&volume, "a", &cancel).await?;
    let mut remote = edit(&remote_volume, "b", &cancel).await?;
    assert!(matches!(
        remote
            .commit(
                OperationId::new(),
                None,
                None,
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<FsError>
            )
            .await?
            .value,
        LivePublicationObservation::Committed { .. }
    ));
    let operation = OperationId::new();
    assert!(
        matches!(
            stale
                .commit(
                    operation,
                    None,
                    None,
                    None,
                    PublicationPermit::Unrestricted,
                    B,
                    &cancel,
                    allow::<FsError>
                )
                .await?
                .value,
            LivePublicationObservation::Committed { .. }
        ),
        "another owner forced a caller replay"
    );
    assert!(matches!(
        retry
            .commit(
                operation,
                None,
                None,
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<FsError>
            )
            .await?
            .value,
        LivePublicationObservation::AlreadyCommitted { .. }
    ));
    let mut current = open_checkout(&volume, pinned(), &cancel).await?;
    for name in ["a", "b"] {
        assert_eq!(
            current
                .read_file_range(
                    &path(name)?,
                    ByteRange {
                        offset: 0,
                        length: 3
                    },
                    B,
                    &cancel
                )
                .await?
                .value
                .bytes
                .as_ref(),
            b"new"
        );
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn ordinary_committed_volume_witness_survives_round_delivery_and_skips_replay() -> TestResult
{
    check_committed_volume_witness(false, false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_committed_volume_witness_survives_round_delivery_and_skips_replay() -> TestResult {
    check_committed_volume_witness(true, false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn ordinary_committed_volume_witness_never_certifies_an_overlapping_read() -> TestResult {
    check_committed_volume_witness(false, true).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_committed_volume_witness_never_certifies_an_overlapping_read() -> TestResult {
    check_committed_volume_witness(true, true).await
}

#[cfg(not(target_arch = "wasm32"))]
async fn check_committed_volume_witness(native: bool, overlapping_read: bool) -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    for name in ["a", "b", "c"] {
        create(&mut seed, name, OwnedBytes::from_static(b"old"), &cancel).await?;
    }
    commit(&mut seed, &cancel).await?;
    let observer = Fs::new(
        ForeignOwner {
            inner: Arc::new(crate::StreamAuthorityStore::new(
                volume.fs.inner.authority.provider(),
            )),
            claims: Default::default(),
            forbid_probe: Default::default(),
            racing_operation: None,
            forbid_replay: Default::default(),
            dispatch_gate: None,
        },
        crate::LogicalObjectStore::new(
            Arc::clone(volume.fs.inner.objects.provider()),
            volume.fs.inner.objects.bucket().clone(),
        ),
        EmbeddedCapabilities::MEMORY,
    );
    let observed = observer.open_volume(volume.id(), B, &cancel).await?.value;
    let mut a = edit(&observed, "a", &cancel).await?;
    let mut b = edit(&observed, "b", &cancel).await?;
    let mut stale = edit(&observed, "c", &cancel).await?;
    if overlapping_read {
        stale
            .read_file_range(
                &path("a")?,
                ByteRange {
                    offset: 0,
                    length: 3,
                },
                B,
                &cancel,
            )
            .await?;
    }
    let outcomes = futures::future::join(
        a.commit(
            OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        ),
        b.commit(
            OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        ),
    )
    .await;
    for outcome in [outcomes.0, outcomes.1] {
        assert!(matches!(
            outcome?.value,
            LivePublicationObservation::Committed { .. }
        ));
    }
    // Both deliveries and the associated transient group have been destroyed.
    drop(a);
    drop(b);
    observer
        .inner
        .authority
        .forbid_replay
        .store(true, Ordering::SeqCst);
    let outcome = submit_proposal(&mut stale, OperationId::new(), native, &cancel).await;
    if overlapping_read {
        assert!(
            outcome.is_err(),
            "a recent witness certified a changed read precondition"
        );
        assert!(stale.has_pending_mutations());
    } else {
        assert!(matches!(
            outcome?,
            LivePublicationObservation::Committed { .. }
        ));
    }
    Ok(())
}

#[derive(Default)]
struct DispatchGate {
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
    active: std::sync::atomic::AtomicBool,
    preflights: std::sync::atomic::AtomicUsize,
    quotes: std::sync::atomic::AtomicUsize,
    appends: std::sync::atomic::AtomicUsize,
}

impl DispatchGate {
    fn observe_preflight(&self) {
        if self.active.load(Ordering::SeqCst) {
            self.preflights.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn ordinary_successor_enqueues_before_the_predecessors_receipt() -> TestResult {
    check_successor_intake(false, false, false, false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_successor_enqueues_before_the_predecessors_receipt() -> TestResult {
    check_successor_intake(true, false, false, false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn ordinary_vacated_successor_retains_its_predecessors_append() -> TestResult {
    check_successor_intake(false, true, false, false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_vacated_successor_retains_its_predecessors_append() -> TestResult {
    check_successor_intake(true, true, false, false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn ordinary_successor_composes_during_the_predecessors_append() -> TestResult {
    check_successor_intake(false, false, true, false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_successor_composes_during_the_predecessors_append() -> TestResult {
    check_successor_intake(true, false, true, false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn ordinary_successor_discards_composition_after_a_fenced_predecessor() -> TestResult {
    check_successor_intake(false, false, true, true).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_successor_discards_composition_after_a_fenced_predecessor() -> TestResult {
    check_successor_intake(true, false, true, true).await
}

#[cfg(not(target_arch = "wasm32"))]
async fn check_successor_intake(
    native: bool,
    vacate: bool,
    prepare: bool,
    reject: bool,
) -> TestResult {
    use futures::future::{Either, select};
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    for name in ["a", "b", "c", "abandoned"] {
        create(&mut seed, name, OwnedBytes::from_static(b"old"), &cancel).await?;
    }
    commit(&mut seed, &cancel).await?;
    let gate = Arc::new(DispatchGate::default());
    let operations = [OperationId::new(), OperationId::new(), OperationId::new()];
    let observer = Fs::new(
        ForeignOwner {
            inner: Arc::new(crate::StreamAuthorityStore::new(
                volume.fs.inner.authority.provider(),
            )),
            claims: Default::default(),
            forbid_probe: Default::default(),
            racing_operation: reject.then_some(operations[0]),
            forbid_replay: Default::default(),
            dispatch_gate: Some(Arc::clone(&gate)),
        },
        crate::LogicalObjectStore::new(
            Arc::clone(volume.fs.inner.objects.provider()),
            volume.fs.inner.objects.bucket().clone(),
        ),
        EmbeddedCapabilities::MEMORY,
    );
    let observed = observer.open_volume(volume.id(), B, &cancel).await?.value;
    let mut a = edit(&observed, "a", &cancel).await?;
    let mut b = edit(&observed, "b", &cancel).await?;
    let mut c = edit(&observed, "c", &cancel).await?;
    let mut abandoned = edit(&observed, "abandoned", &cancel).await?;
    let abandoned_operation = OperationId::new();
    let mut first = Box::pin(submit_proposal(&mut a, operations[0], native, &cancel));
    assert!(
        matches!(
            select(Box::pin(gate.started.notified()), first.as_mut()).await,
            Either::Left(_)
        ),
        "first publication did not reach its durability owner"
    );
    if vacate {
        let mut withdrawn = Box::pin(submit_proposal(
            &mut abandoned,
            abandoned_operation,
            native,
            &cancel,
        ));
        assert!(futures::poll!(withdrawn.as_mut()).is_pending());
        drop(withdrawn);
    }
    let mut next = Box::pin(futures::future::join(
        submit_proposal(&mut b, operations[1], native, &cancel),
        submit_proposal(&mut c, operations[2], native, &cancel),
    ));
    assert!(futures::poll!(next.as_mut()).is_pending());
    if prepare {
        // Drive the existing bounded collection window, without releasing the
        // durability owner or sleeping for a test-dependent timeout.
        for _ in 0..=crate::async_storage::MAX_AUTHORITY_GROUP_MEMBERS {
            acyclic_stream::runtime::yield_now().await?;
            assert!(futures::poll!(next.as_mut()).is_pending());
        }
        let mut inspection = KernelAccounting::standalone(B, &cancel);
        let actor = crate::kernel::authority_actor(
            observer.inner.authority.operation_claims(),
            observer.inner.authority.actor_scope(),
            volume_authority_id(volume.id()),
            &mut inspection,
        )?;
        assert_eq!(
            actor.provisional_successors(),
            2,
            "disjoint metadata did not prepare while its predecessor append was held"
        );
        assert_eq!(
            gate.quotes.load(Ordering::SeqCst),
            2,
            "successor capacity was not admitted against the real durable projection"
        );
    }
    assert_eq!(
        gate.preflights.load(Ordering::SeqCst),
        0,
        "successor probed an in-flight authority head"
    );
    assert_eq!(
        gate.appends.load(Ordering::SeqCst),
        1,
        "successor dispatched before its predecessor settled"
    );
    gate.release.notify_one();
    if reject {
        let (first, successors) = futures::future::join(first, next).await;
        assert!(!matches!(
            first?,
            LivePublicationObservation::Committed { .. }
        ));
        for result in [successors.0, successors.1] {
            assert!(
                !matches!(result, Ok(LivePublicationObservation::Committed { .. })),
                "a fenced predecessor promoted its provisional successor"
            );
        }
        assert_eq!(gate.appends.load(Ordering::SeqCst), 1);
        for operation in operations {
            assert!(
                volume
                    .fs
                    .inner
                    .authority
                    .find_operation(
                        volume_authority_id(volume.id()),
                        operation,
                        &mut crate::AuthorityWork::new(B, &cancel),
                    )
                    .await?
                    .is_none()
            );
        }
        return Ok(());
    }
    assert!(
        matches!(
            select(Box::pin(gate.started.notified()), next.as_mut()).await,
            Either::Left(_)
        ),
        "successor was acknowledged before its durability receipt"
    );
    assert!(matches!(
        first.await?,
        LivePublicationObservation::Committed { .. }
    ));
    assert!(futures::poll!(next.as_mut()).is_pending());
    gate.release.notify_one();
    let (b, c) = next.await;
    for outcome in [b?, c?] {
        assert!(matches!(
            outcome,
            LivePublicationObservation::Committed { .. }
        ));
    }
    assert_eq!(
        gate.appends.load(Ordering::SeqCst),
        2,
        "successor proposals lost batching or repeated a stale append"
    );
    verify_owned_operations(&volume, operations, &cancel).await?;
    assert!(
        volume
            .fs
            .inner
            .authority
            .find_operation(
                volume_authority_id(volume.id()),
                abandoned_operation,
                &mut crate::AuthorityWork::new(B, &cancel)
            )
            .await?
            .is_none(),
        "vacated successor published a withdrawn operation"
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn ordinary_owned_append_survives_either_client_dropping_its_reply() -> TestResult {
    check_owned_append_client_drop(false, false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_owned_append_survives_either_client_dropping_its_reply() -> TestResult {
    check_owned_append_client_drop(true, false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn ordinary_owned_append_survives_either_client_cancelling_its_token() -> TestResult {
    check_owned_append_client_drop(false, true).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_owned_append_survives_either_client_cancelling_its_token() -> TestResult {
    check_owned_append_client_drop(true, true).await
}

#[cfg(not(target_arch = "wasm32"))]
async fn check_owned_append_client_drop(native: bool, explicit_cancellation: bool) -> TestResult {
    use futures::future::{AbortHandle, Abortable, Either, select};
    for dropped in 0..2 {
        let cancel = CancellationToken::new();
        let (volume, mut seed) = fixture(config(), &cancel).await?;
        for name in ["a", "b"] {
            create(&mut seed, name, OwnedBytes::from_static(b"old"), &cancel).await?;
        }
        commit(&mut seed, &cancel).await?;
        let gate = Arc::new(DispatchGate::default());
        let observer = Fs::new(
            ForeignOwner {
                inner: Arc::new(crate::StreamAuthorityStore::new(
                    volume.fs.inner.authority.provider(),
                )),
                claims: Default::default(),
                forbid_probe: Default::default(),
                racing_operation: None,
                forbid_replay: Default::default(),
                dispatch_gate: Some(Arc::clone(&gate)),
            },
            crate::LogicalObjectStore::new(
                Arc::clone(volume.fs.inner.objects.provider()),
                volume.fs.inner.objects.bucket().clone(),
            ),
            EmbeddedCapabilities::MEMORY,
        );
        let observed = observer.open_volume(volume.id(), B, &cancel).await?.value;
        let mut a = edit(&observed, "a", &cancel).await?;
        let mut b = edit(&observed, "b", &cancel).await?;
        let operations = [OperationId::new(), OperationId::new()];
        let callers = [CancellationToken::new(), CancellationToken::new()];
        let (abort_a, registration_a) = AbortHandle::new_pair();
        let (abort_b, registration_b) = AbortHandle::new_pair();
        let mut joined = Box::pin(futures::future::join(
            Abortable::new(
                submit_proposal(&mut a, operations[0], native, &callers[0]),
                registration_a,
            ),
            Abortable::new(
                submit_proposal(&mut b, operations[1], native, &callers[1]),
                registration_b,
            ),
        ));
        let started = Box::pin(gate.started.notified());
        assert!(
            matches!(select(started, joined.as_mut()).await, Either::Left(_)),
            "batch finished before its durability step"
        );
        if explicit_cancellation {
            callers[dropped].cancel();
        } else if dropped == 0 {
            abort_a.abort();
        } else {
            abort_b.abort();
        }
        assert!(
            matches!(futures::poll!(joined.as_mut()), std::task::Poll::Pending),
            "a dropped reply aborted another admitted writer"
        );
        gate.release.notify_one();
        let (first, second) = joined.await;
        let (withdrawn, delivered) = if dropped == 0 {
            (first, second)
        } else {
            (second, first)
        };
        if explicit_cancellation {
            assert!(
                withdrawn?.is_err(),
                "cancelled caller retained reply delivery"
            );
        } else {
            assert!(withdrawn.is_err());
        }
        assert!(matches!(
            delivered??,
            LivePublicationObservation::Committed { .. }
        ));
        verify_owned_operations(&volume, operations, &cancel).await?;
    }
    Ok(())
}

async fn verify_owned_operations<const N: usize>(
    volume: &TestVolume,
    operations: [OperationId; N],
    cancel: &CancellationToken,
) -> TestResult {
    let mut work = crate::AuthorityWork::new(B, cancel);
    for operation in operations {
        assert!(
            volume
                .fs
                .inner
                .authority
                .find_operation(
                    crate::kernel::volume_authority_id(volume.id()),
                    operation,
                    &mut work,
                )
                .await?
                .is_some(),
            "owned append lost a member's durable operation"
        );
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn ordinary_owned_append_retains_the_actual_composition_after_every_client_leaves()
-> TestResult {
    check_composition_custody(false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_owned_append_retains_the_actual_composition_after_every_client_leaves() -> TestResult
{
    check_composition_custody(true).await
}

#[cfg(not(target_arch = "wasm32"))]
async fn check_composition_custody(native: bool) -> TestResult {
    use futures::future::{Either, select};
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    for name in ["a", "b"] {
        create(&mut seed, name, OwnedBytes::from_static(b"old"), &cancel).await?;
    }
    commit(&mut seed, &cancel).await?;
    let gate = Arc::new(DispatchGate::default());
    let observer = Fs::new(
        ForeignOwner {
            inner: Arc::new(crate::StreamAuthorityStore::new(
                volume.fs.inner.authority.provider(),
            )),
            claims: Default::default(),
            forbid_probe: Default::default(),
            racing_operation: None,
            forbid_replay: Default::default(),
            dispatch_gate: Some(Arc::clone(&gate)),
        },
        crate::LogicalObjectStore::new(
            Arc::clone(volume.fs.inner.objects.provider()),
            volume.fs.inner.objects.bucket().clone(),
        ),
        EmbeddedCapabilities::MEMORY,
    );
    let actual = Arc::downgrade(&observer.inner);
    let observed = observer.open_volume(volume.id(), B, &cancel).await?.value;
    let mut a = edit(&observed, "a", &cancel).await?;
    let mut b = edit(&observed, "b", &cancel).await?;
    let operations = [OperationId::new(), OperationId::new()];
    let mut replies = Box::pin(futures::future::join(
        submit_proposal(&mut a, operations[0], native, &cancel),
        submit_proposal(&mut b, operations[1], native, &cancel),
    ));
    assert!(matches!(
        select(Box::pin(gate.started.notified()), replies.as_mut()).await,
        Either::Left(_)
    ));
    drop(replies);
    drop(a);
    drop(b);
    drop(observed);
    drop(observer);
    assert!(
        actual.upgrade().is_some(),
        "durability I/O outlived the actual filesystem composition"
    );
    gate.release.notify_one();
    while actual.strong_count() != 0 {
        tokio::task::yield_now().await;
    }
    verify_owned_operations(&volume, operations, &cancel).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn ready_memory_batches_need_no_detached_executor() -> TestResult {
    futures::executor::block_on(async {
        let cancel = CancellationToken::new();
        let (volume, mut seed) = fixture(config(), &cancel).await?;
        for name in ["a", "b"] {
            create(&mut seed, name, OwnedBytes::from_static(b"old"), &cancel).await?;
        }
        commit(&mut seed, &cancel).await?;
        let mut a = edit(&volume, "a", &cancel).await?;
        let mut b = edit(&volume, "b", &cancel).await?;
        let operations = [OperationId::new(), OperationId::new()];
        let (a, b) = futures::try_join!(
            submit_proposal(&mut a, operations[0], false, &cancel),
            submit_proposal(&mut b, operations[1], false, &cancel),
        )?;
        for outcome in [a, b] {
            assert!(matches!(
                outcome,
                LivePublicationObservation::Committed { .. }
            ));
        }
        verify_owned_operations(&volume, operations, &cancel).await
    })
}

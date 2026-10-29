//! Exercise receipt-owned locator coverage through the actual authority APIs.
use super::*;
use crate::{AuthorityWork, PublicationChanges, StreamAuthorityStore};
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitOutcome, CommitRequest, StreamProviderExt,
};
use std::sync::Arc;

// Count requests at the provider boundary. Native Commit's internal Head
// identity inspections belong to that durability request, not client preflight.
mod probe {
    use acyclic_stream::*;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    };

    #[derive(Default)]
    pub(super) struct Provider {
        inner: Arc<MemoryStream>,
        claims: OperationClaims,
        reads: AtomicU64,
        appends: AtomicU64,
        next_commit: Mutex<Option<Arc<CommitGate>>>,
    }

    #[derive(Default)]
    pub(super) struct CommitGate {
        pub(super) entered: tokio::sync::Notify,
        pub(super) release: tokio::sync::Notify,
    }

    impl Provider {
        pub(super) fn peer(&self) -> Self {
            Self {
                inner: Arc::clone(&self.inner),
                claims: OperationClaims::default(),
                reads: AtomicU64::new(0),
                appends: AtomicU64::new(0),
                next_commit: Mutex::new(None),
            }
        }

        pub(super) fn requests(&self) -> (u64, u64) {
            (
                self.reads.load(Ordering::Relaxed),
                self.appends.load(Ordering::Relaxed),
            )
        }

        pub(super) fn hold_next_commit(&self) -> Arc<CommitGate> {
            let gate = Arc::new(CommitGate::default());
            let previous = self
                .next_commit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .replace(Arc::clone(&gate));
            assert!(previous.is_none(), "a test already owns the pending commit");
            gate
        }

        async fn await_commit_gate(&self) {
            let gate = self
                .next_commit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
        }

        fn read_request(&self) {
            self.reads.fetch_add(1, Ordering::Relaxed);
        }
        fn append_request(&self) {
            self.appends.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[async_trait::async_trait]
    impl StreamProvider for Provider {
        fn context(&self) -> acyclic_stream::ProviderContext<'_> {
            acyclic_stream::ProviderContext::new(&self.claims)
        }
        async fn inspect_idempotency(
            &self,
            key: IdempotencyKey,
        ) -> Result<Option<IdempotencyObservation>, StreamError> {
            self.read_request();
            self.inner.inspect_idempotency(key).await
        }

        async fn heads(
            &self,
            request: acyclic_stream::HeadsRequest,
        ) -> std::result::Result<acyclic_stream::Heads, acyclic_stream::StreamError> {
            self.read_request();
            self.inner.heads(request).await
        }
        fn write(
            &self,
            request: acyclic_stream::WriteRequest,
        ) -> acyclic_stream::BoxProviderFuture<
            '_,
            std::result::Result<acyclic_stream::WriteOutcome, acyclic_stream::StreamError>,
        > {
            match request {
                acyclic_stream::WriteRequest::Append(request) => {
                    Box::pin(futures::TryFutureExt::map_ok(
                        async move {
                            self.append_request();
                            self.inner.append(request).await
                        },
                        acyclic_stream::WriteOutcome::Append,
                    ))
                }
                acyclic_stream::WriteRequest::Fork(request) => {
                    Box::pin(futures::TryFutureExt::map_ok(
                        async move {
                            self.append_request();
                            self.inner.fork(request).await
                        },
                        acyclic_stream::WriteOutcome::Fork,
                    ))
                }
                acyclic_stream::WriteRequest::Commit(request) => {
                    Box::pin(futures::TryFutureExt::map_ok(
                        async move {
                            self.append_request();
                            self.await_commit_gate().await;
                            self.inner.commit(request).await
                        },
                        acyclic_stream::WriteOutcome::Commit,
                    ))
                }
            }
        }

        async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
            self.read_request();
            self.inner.read(request).await
        }
        async fn follow(
            &self,
            request: acyclic_stream::FollowRequest,
        ) -> Result<acyclic_stream::FollowStream, StreamError> {
            self.read_request();
            self.inner.follow(request).await
        }

        async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
            self.read_request();
            self.inner.read_commit(id).await
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl acyclic_stream::NativeStreamProvider for Provider {
        fn native_inspect_idempotency(
            &self,
            key: IdempotencyKey,
            meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        ) -> NativeProviderFuture<'_, Option<IdempotencyObservation>> {
            self.read_request();
            self.inner.native_inspect_idempotency(key, meter)
        }
        fn native_tail(
            &self,
            path: StreamPath,
            meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        ) -> NativeProviderFuture<'_, u64> {
            self.read_request();
            self.inner.native_tail(path, meter)
        }
        fn native_read(
            &self,
            request: ReadRequest,
            meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        ) -> NativeProviderFuture<'_, NativeRecordStream> {
            self.read_request();
            self.inner.native_read(request, meter)
        }
        fn native_write(
            &self,
            request: acyclic_stream::WriteRequest,
            deadline: Option<u64>,
            meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        ) -> NativeProviderFuture<'_, acyclic_stream::WriteOutcome> {
            let acyclic_stream::WriteRequest::Commit(_) = &request else {
                return NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)));
            };
            self.append_request();
            let funding = Arc::clone(&meter);
            NativeProviderFuture::new(&funding, async move {
                self.await_commit_gate().await;
                self.inner.native_write(request, deadline, meter).await
            })
        }
    }
}

async fn fixture(
    count: u8,
    repeat: bool,
) -> TestResult<(StreamAuthorityStore<probe::Provider>, Vec<GuardedAppend>)> {
    fixture_with_fence(count, repeat, None).await
}

async fn fixture_with_fence(
    count: u8,
    repeat: bool,
    fence: Option<bool>,
) -> TestResult<(StreamAuthorityStore<probe::Provider>, Vec<GuardedAppend>)> {
    let store = StreamAuthorityStore::new(Arc::new(probe::Provider::default()));
    let objects = MemoryObjectStore::default();
    let fs = crate::Fs::new(store.clone(), objects, crate::EmbeddedCapabilities::MEMORY);
    let objects = fs.objects();
    let cancel = CancellationToken::new();
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let volume = fs
        .create_volume(config, WorkBudget::UNBOUNDED, &cancel)
        .await
        .map_err(|failure| failure.error)?
        .value;
    let authority = volume_authority_id(volume.id());
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
    let ((mut expected, payload), _) = store.head_payload(authority, u64::MAX, &mut work).await?;
    let creation = crate::kernel::decode_volume_created(
        payload.as_ref().ok_or("creation missing")?,
        u64::MAX,
        &mut |_| Ok::<_, crate::FsError>(()),
    )?;
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancel);
    let (mut base, _guard) = crate::kernel::read_generation_root_with_limits_and_accounting(
        objects,
        creation.initial_generation_root,
        DecodeLimits::for_volume(config),
        &mut accounting,
        &cancel,
    )
    .await?;
    if let Some(native) = fence {
        let original =
            crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
                .map_err(|(error, _)| error)?;
        let mut work = measured(native, &original, &cancel);
        let crate::FenceOutcome::Advanced(head) =
            store.fence(authority, expected, &mut work).await?
        else {
            return Err("fixture epoch fence conflicted".into());
        };
        expected = head;
    }
    let mut root = creation.initial_generation_root;
    let mut members = Vec::new();
    for id in 1..=count {
        let mut candidate = base.clone();
        if !repeat {
            candidate.parents = vec![crate::GenerationId::new(root.digest)];
            let bytes = encode_generation_root(&candidate, accept, allocation_failed)?;
            root = ObjectId {
                kind: ObjectKind::GenerationRoot,
                digest: object_digest(&bytes),
            };
            objects
                .put(root, acyclic_stream::OwnedBytes::from(bytes), &mut work)
                .await?;
        }
        let request = PublishGenerationRequest {
            authority_id: authority,
            volume_id: volume.id(),
            epoch: expected.epoch,
            expected,
            operation_id: OperationId::from_bytes([id; 16]),
            generation_root: root,
        };
        let mut commit =
            prepare_publication_commit(request, None, None, None, &mut accounting, &cancel)?;
        commit.publication = Some(
            PublicationChanges::proposed(
                objects,
                &base,
                &candidate,
                request,
                config,
                &mut accounting,
                &cancel,
            )
            .await?,
        );
        let sequence = expected.sequence.checked_next()?;
        let digest = crate::authority_commit_digest(
            authority,
            expected.epoch,
            sequence,
            commit.operation_id,
            commit.fingerprint,
            expected.digest,
            &commit.payload,
        );
        members.push(GuardedAppend {
            authority_id: authority,
            epoch: expected.epoch,
            expected,
            commit,
            permit: PublicationPermit::Unrestricted,
        });
        expected = Head {
            epoch: expected.epoch,
            sequence,
            digest,
        };
        base = candidate;
    }
    Ok((store, members))
}

fn measured<'a>(
    native: bool,
    original: &'a crate::NativeMaterializationWork,
    cancel: &'a CancellationToken,
) -> AuthorityWork<'a> {
    if native {
        AuthorityWork::native(original, cancel)
    } else {
        AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, cancel)
    }
}

#[tokio::test]
async fn generation_catalog_single_group_and_quote_use_receipts_without_preflight_reads()
-> TestResult {
    crate::initialize_test_native_io();
    for native in [false, true] {
        for count in [1, 4] {
            for repeat in [false, true] {
                let (store, members) = fixture(count, repeat).await?;
                let provider = store.provider();
                let (reads, appends) = provider.requests();
                let cancel = CancellationToken::new();
                let original = crate::NativeMaterializationWork::new(
                    crate::performance::BOUNDARY_BUDGET,
                    &cancel,
                )
                .map_err(|(error, _)| error)?;
                let mut quote = measured(native, &original, &cancel);
                assert!(
                    store
                        .quote_group_append(members.first().ok_or("no member")?, 0, &mut quote)
                        .await?
                        .is_some()
                );
                assert_eq!(
                    provider.requests(),
                    (reads, appends),
                    "quote issued a provider request: native={native}, count={count}, repeat={repeat}"
                );
                let mut work = measured(native, &original, &cancel);
                if count == 1 {
                    assert!(matches!(
                        store
                            .compare_and_append(members[0].clone(), &mut work, None::<&mut ()>)
                            .await?,
                        crate::AppendOutcome::Committed(_)
                    ));
                } else {
                    let crate::AppendGroupOutcome::Published {
                        receipts,
                        allocation,
                    } = store
                        .compare_and_append_group(&members, &[], &mut work, None::<&mut ()>)
                        .await?
                    else {
                        return Err("generation group rejected".into());
                    };
                    assert_eq!(receipts.len(), usize::from(count));
                    assert!(
                        receipts
                            .iter()
                            .all(|receipt| matches!(receipt, crate::AppendOutcome::Committed(_)))
                    );
                    drop(receipts);
                    drop(allocation);
                }
                assert_eq!(
                    provider.requests(),
                    (reads, appends + 1),
                    "covered publication issued extra provider requests: native={native}, count={count}, repeat={repeat}"
                );
                assert_eq!(work.spent().backend_write_operations, 1);
                let last = members.last().ok_or("last member missing")?;
                let sequence = last.expected.sequence.checked_next()?;
                let head = Head {
                    epoch: last.epoch,
                    sequence,
                    digest: crate::authority_commit_digest(
                        last.authority_id,
                        last.epoch,
                        sequence,
                        last.commit.operation_id,
                        last.commit.fingerprint,
                        last.expected.digest,
                        &last.commit.payload,
                    ),
                };
                let publication =
                    decode_published_generation(&last.commit.payload, u64::MAX, accept)?;
                let request = PublishGenerationRequest {
                    authority_id: last.authority_id,
                    volume_id: publication.volume_id,
                    epoch: head.epoch,
                    expected: head,
                    operation_id: OperationId::from_bytes([87; 16]),
                    generation_root: publication.generation_root,
                };
                let mut commit = prepare_publication_commit(
                    request,
                    None,
                    None,
                    None,
                    &mut KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel),
                    &cancel,
                )?;
                commit.publication = Some(
                    last.commit
                        .publication
                        .as_ref()
                        .ok_or("seal missing")?
                        .composed(last.expected, head, publication.generation_root)?,
                );
                let next = GuardedAppend {
                    authority_id: request.authority_id,
                    epoch: head.epoch,
                    expected: head,
                    commit,
                    permit: PublicationPermit::Unrestricted,
                };
                let mut successor = measured(native, &original, &cancel);
                assert!(
                    store
                        .quote_group_append(&next, 0, &mut successor)
                        .await?
                        .is_some()
                );
                assert!(matches!(
                    store
                        .compare_and_append(next, &mut successor, None::<&mut ()>)
                        .await?,
                    crate::AppendOutcome::Committed(_)
                ));
                assert_eq!(
                    provider.requests(),
                    (reads, appends + 2),
                    "the next batch did not consume its predecessor's sealed generation fact"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn epoch_fence_uses_one_provider_request_without_a_head_probe() -> TestResult {
    crate::initialize_test_native_io();
    for native in [false, true] {
        let (store, members) = fixture(1, false).await?;
        let first = members.first().ok_or("no member")?;
        let provider = store.provider();
        let (reads, appends) = provider.requests();
        let cancel = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
                .map_err(|(error, _)| error)?;
        let mut work = measured(native, &original, &cancel);
        let crate::FenceOutcome::Advanced(head) = store
            .fence(first.authority_id, first.expected, &mut work)
            .await?
        else {
            return Err("epoch fence conflicted".into());
        };
        assert_eq!(head.sequence, first.expected.sequence);
        assert_eq!(head.digest, first.expected.digest);
        assert_eq!(head.epoch.get(), first.epoch.get() + 1);
        assert_eq!(
            provider.requests(),
            (reads, appends + 1),
            "fencing probed an owned head: native={native}"
        );
        let mut stale = measured(native, &original, &cancel);
        assert!(
            matches!(store.fence(first.authority_id, first.expected, &mut stale).await?, crate::FenceOutcome::Conflict { actual } if actual == head)
        );
        assert_eq!(
            provider.requests().1,
            appends + 1,
            "stale fence advanced the epoch"
        );
    }
    Ok(())
}

#[tokio::test]
async fn epoch_fence_recovers_only_the_actual_conflict_tail_across_writer_owners() -> TestResult {
    crate::initialize_test_native_io();
    for native in [false, true] {
        let (store, members) = fixture(1, false).await?;
        let first = members.first().ok_or("no member")?;
        let provider = store.provider();
        // Separate process-local claim registries share the actual durable
        // provider; the second actor cannot update the first actor's journal.
        let cancel = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
                .map_err(|(error, _)| error)?;
        let mut setup = measured(native, &original, &cancel);
        let mut actual = first.expected;
        for _ in 0..2 {
            let peer = StreamAuthorityStore::new(Arc::new(provider.peer()));
            let crate::FenceOutcome::Advanced(head) =
                peer.fence(first.authority_id, actual, &mut setup).await?
            else {
                return Err("peer epoch fence conflicted".into());
            };
            actual = head;
        }
        let (reads, appends) = provider.requests();
        let mut work = measured(native, &original, &cancel);
        assert!(
            matches!(store.fence(first.authority_id, first.expected, &mut work).await?, crate::FenceOutcome::Conflict { actual: head } if head == actual),
            "a new writer reused another actor's historical successful fence"
        );
        assert_eq!(
            provider.requests(),
            (reads + 1, appends + 1),
            "takeover recovery probed a newer head instead of reading the conflict's exact epoch tail"
        );
    }
    Ok(())
}

#[tokio::test]
async fn epoch_fence_revalidates_cached_mismatches_before_conflict_or_takeover() -> TestResult {
    crate::initialize_test_native_io();
    for native in [false, true] {
        for matches_current in [false, true] {
            let (store, members) = fixture(1, false).await?;
            let first = members.first().ok_or("no member")?;
            let cancel = CancellationToken::new();
            let original =
                crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
                    .map_err(|(error, _)| error)?;
            let mut work = measured(native, &original, &cancel);
            let crate::FenceOutcome::Advanced(owned) = store
                .fence(first.authority_id, first.expected, &mut work)
                .await?
            else {
                return Err("initial fence conflicted".into());
            };
            let peer = StreamAuthorityStore::new(Arc::new(store.provider().peer()));
            let crate::FenceOutcome::Advanced(actual) =
                peer.fence(first.authority_id, owned, &mut work).await?
            else {
                return Err("peer fence conflicted".into());
            };
            let requested = if matches_current {
                actual
            } else {
                first.expected
            };
            let result = store
                .fence(first.authority_id, requested, &mut work)
                .await?;
            if matches_current {
                assert!(
                    matches!(result, crate::FenceOutcome::Advanced(head) if head.epoch.get() == actual.epoch.get() + 1 && head.sequence == actual.sequence && head.digest == actual.digest),
                    "a stale projection rejected the current owner transition"
                );
            } else {
                assert!(
                    matches!(result, crate::FenceOutcome::Conflict { actual: head } if head == actual),
                    "Conflict.actual was an old cached observation"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn epoch_fence_preserves_generation_facts_and_the_authority_record_identity() -> TestResult {
    crate::initialize_test_native_io();
    for native in [false, true] {
        for count in [1, 4] {
            for repeat in [false, true] {
                let (store, members) = fixture_with_fence(count, repeat, Some(native)).await?;
                let first = members.first().ok_or("no member")?;
                let provider = store.provider();
                let cancel = CancellationToken::new();
                let original = crate::NativeMaterializationWork::new(
                    crate::performance::BOUNDARY_BUDGET,
                    &cancel,
                )
                .map_err(|(error, _)| error)?;
                let mut stale = measured(native, &original, &cancel);
                let before_epoch = Head {
                    epoch: Epoch::GENESIS,
                    ..first.expected
                };
                assert!(
                    matches!(store.fence(first.authority_id, before_epoch, &mut stale).await?, crate::FenceOutcome::Conflict { actual } if actual == first.expected)
                );
                let (reads, appends) = provider.requests();
                let mut work = measured(native, &original, &cancel);
                assert!(
                    store
                        .quote_group_append(first, 0, &mut work)
                        .await?
                        .is_some()
                );
                assert_eq!(
                    provider.requests(),
                    (reads, appends),
                    "fence lost receipt-owned generation facts"
                );
                if count == 1 {
                    assert!(matches!(
                        store
                            .compare_and_append(first.clone(), &mut work, None::<&mut ()>)
                            .await?,
                        crate::AppendOutcome::Committed(_)
                    ));
                } else {
                    let crate::AppendGroupOutcome::Published {
                        receipts,
                        allocation,
                    } = store
                        .compare_and_append_group(&members, &[], &mut work, None::<&mut ()>)
                        .await?
                    else {
                        return Err("publication after fencing rejected".into());
                    };
                    assert_eq!(receipts.len(), usize::from(count));
                    assert!(
                        receipts
                            .iter()
                            .all(|receipt| matches!(receipt, crate::AppendOutcome::Committed(_)))
                    );
                    drop(receipts);
                    drop(allocation);
                }
                assert_eq!(
                    provider.requests(),
                    (reads, appends + 1),
                    "fence installed the epoch receipt as the authority record identity"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn in_flight_publication_quotes_reuse_only_the_last_durable_projection() -> TestResult {
    crate::initialize_test_native_io();
    for native in [false, true] {
        for count in [1, 4] {
            let (store, members) = fixture(count, false).await?;
            let first = members.first().ok_or("no member")?;
            let provider = store.provider();
            let gate = provider.hold_next_commit();
            let cancel = CancellationToken::new();
            let original =
                crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
                    .map_err(|(error, _)| error)?;
            let quote_owner =
                crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
                    .map_err(|(error, _)| error)?;
            let mut pending_work = measured(native, &original, &cancel);
            let publication = async {
                if count == 1 {
                    assert!(matches!(
                        store
                            .compare_and_append(first.clone(), &mut pending_work, None::<&mut ()>)
                            .await?,
                        crate::AppendOutcome::Committed(_)
                    ));
                } else {
                    let crate::AppendGroupOutcome::Published {
                        receipts,
                        allocation,
                    } = store
                        .compare_and_append_group(&members, &[], &mut pending_work, None::<&mut ()>)
                        .await?
                    else {
                        return Err("held group publication rejected".into());
                    };
                    assert_eq!(receipts.len(), usize::from(count));
                    assert!(
                        receipts
                            .iter()
                            .all(|receipt| matches!(receipt, crate::AppendOutcome::Committed(_)))
                    );
                    drop(receipts);
                    drop(allocation);
                }
                Ok::<(), Box<dyn std::error::Error>>(())
            };
            let mut publication = std::pin::pin!(publication);
            match futures::future::select(
                publication.as_mut(),
                std::pin::pin!(gate.entered.notified()),
            )
            .await
            {
                futures::future::Either::Left(_) => {
                    return Err(
                        "publication settled before its real provider append was released".into(),
                    );
                }
                futures::future::Either::Right(_) => {}
            }
            let before = provider.requests();
            let mut quote_work = measured(native, &quote_owner, &cancel);
            assert!(
                store
                    .quote_group_append(first, 0, &mut quote_work)
                    .await?
                    .is_some()
            );
            assert_eq!(
                provider.requests(),
                before,
                "in-flight quotation reactivated the authority: native={native}, count={count}"
            );
            assert!(
                matches!(
                    futures::poll!(publication.as_mut()),
                    std::task::Poll::Pending
                ),
                "quotation acknowledged an unsealed publication"
            );
            gate.release.notify_one();
            publication.await?;
            assert_eq!(
                provider.requests(),
                before,
                "receipt promotion issued a separate provider request"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn rejected_preparation_preserves_the_receipt_owned_frontier() -> TestResult {
    crate::initialize_test_native_io();
    for native in [false, true] {
        for count in [1, 4] {
            for rejected_index in 0..usize::from(count) {
                let (store, members) = fixture(count, false).await?;
                let provider = store.provider();
                let (_, appends) = provider.requests();
                let first = members.first().ok_or("no member")?;
                let mut rejected = members.clone();
                rejected[rejected_index].expected = Head::genesis(first.epoch);
                let cancel = CancellationToken::new();
                let original = crate::NativeMaterializationWork::new(
                    crate::performance::BOUNDARY_BUDGET,
                    &cancel,
                )
                .map_err(|(error, _)| error)?;
                let mut work = measured(native, &original, &cancel);
                if count == 1 {
                    assert!(matches!(
                        store
                            .compare_and_append(rejected[0].clone(), &mut work, None::<&mut ()>)
                            .await?,
                        crate::AppendOutcome::Conflict { actual } if actual == first.expected
                    ));
                } else {
                    assert!(matches!(
                        store
                            .compare_and_append_group(&rejected, &[], &mut work, None::<&mut ()>)
                            .await?,
                        crate::AppendGroupOutcome::Rejected {
                            index,
                            outcome: crate::AppendOutcome::Conflict { actual },
                            observed,
                        } if index == rejected_index && actual == first.expected && observed == first.expected
                    ));
                }
                let (reads, after_rejection) = provider.requests();
                assert_eq!(after_rejection, appends, "rejected prefix became durable");
                let mut quote = measured(native, &original, &cancel);
                assert!(
                    store
                        .quote_group_append(first, 0, &mut quote)
                        .await?
                        .is_some()
                );
                assert_eq!(
                    provider.requests(),
                    (reads, appends),
                    "rejected preparation discarded the verified frontier: native={native}, count={count}, index={rejected_index}"
                );
                let mut retry = measured(native, &original, &cancel);
                let crate::AppendGroupOutcome::Published {
                    receipts,
                    allocation,
                } = store
                    .compare_and_append_group(&members, &[], &mut retry, None::<&mut ()>)
                    .await?
                else {
                    return Err("valid proposal rejected after preparation rollback".into());
                };
                assert_eq!(receipts.len(), usize::from(count));
                assert!(
                    receipts
                        .iter()
                        .all(|receipt| matches!(receipt, crate::AppendOutcome::Committed(_)))
                );
                drop(receipts);
                drop(allocation);
                assert_eq!(provider.requests(), (reads, appends + 1));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn generation_catalog_cached_locators_remain_atomically_fenced() -> TestResult {
    crate::initialize_test_native_io();
    for native in [false, true] {
        for count in [1, 4] {
            let (store, members) = fixture(count, true).await?;
            let first = members.first().ok_or("no member")?;
            let cancel = CancellationToken::new();
            let original =
                crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
                    .map_err(|(error, _)| error)?;
            let mut setup = measured(native, &original, &cancel);
            let generation = decode_published_generation(&first.commit.payload, u64::MAX, accept)?
                .generation_root
                .digest;
            let locator = acyclic_stream::StreamPath::new(format!(
                "fs/authorities/{}/generations/{}",
                hex::encode(first.authority_id.into_bytes()),
                hex::encode(generation.as_bytes())
            ))?;
            let provider = store.provider();
            let outcome = provider
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    conditions: vec![CommitCondition::Tail {
                        path: locator.clone(),
                        expected: 1,
                    }],
                    mutations: vec![CommitMutation::Append {
                        path: locator,
                        records: vec![
                            acyclic_stream::OwnedBytes::from_static(
                                b"corrupt extra locator record",
                            )
                            .into(),
                        ],
                    }],
                    idempotency_key: acyclic_stream::IdempotencyKey::new(Bytes::from_static(
                        b"catalog-corruption-control",
                    ))?,
                })
                .await?;
            assert!(matches!(outcome, CommitOutcome::Committed(_)));
            let mut work = measured(native, &original, &cancel);
            if count == 1 {
                assert!(
                    matches!(store.compare_and_append(first.clone(), &mut work, None::<&mut ()>).await?,
                    crate::AppendOutcome::Conflict { actual } if actual == first.expected),
                    "cached locator bypassed its atomic identity fence"
                );
            } else {
                let crate::AppendGroupOutcome::Published {
                    receipts,
                    allocation,
                } = store
                    .compare_and_append_group(&members, &[], &mut work, None::<&mut ()>)
                    .await?
                else {
                    return Err("group did not reach its atomic fence".into());
                };
                assert_eq!(receipts.len(), usize::from(count));
                assert!(
                    receipts.iter().all(|receipt| matches!(receipt,
                    crate::AppendOutcome::Conflict { actual } if *actual == first.expected)),
                    "cached locator bypassed its atomic identity fence"
                );
                drop(receipts);
                drop(allocation);
            }
            assert_eq!(
                store.head(first.authority_id, &mut setup).await?,
                first.expected
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn group_capacity_quote_covers_reused_to_composed_generation_shape() -> TestResult {
    crate::initialize_test_native_io();
    for native in [false, true] {
        let (store, members) = fixture(1, false).await?;
        let fresh = members.first().ok_or("missing fresh proposal")?;
        let cancel = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
                .map_err(|(error, _)| error)?;
        let mut work = measured(native, &original, &cancel);
        let ((head, payload), _) = store
            .head_payload(fresh.authority_id, u64::MAX, &mut work)
            .await?;
        let creation = crate::kernel::decode_volume_created(
            payload.as_ref().ok_or("creation missing")?,
            u64::MAX,
            &mut |_| Ok::<_, crate::FsError>(()),
        )?;
        let changes = fresh
            .commit
            .publication
            .as_ref()
            .ok_or("missing change seal")?;
        let request = PublishGenerationRequest {
            authority_id: fresh.authority_id,
            volume_id: changes.volume(),
            epoch: head.epoch,
            expected: head,
            operation_id: OperationId::new(),
            generation_root: creation.initial_generation_root,
        };
        let mut accounting =
            KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
        let mut commit =
            prepare_publication_commit(request, None, None, None, &mut accounting, &cancel)?;
        commit.publication = Some(changes.composed(head, head, request.generation_root)?);
        let reused = GuardedAppend {
            authority_id: request.authority_id,
            epoch: head.epoch,
            expected: head,
            commit,
            permit: PublicationPermit::Unrestricted,
        };
        let before = store.provider().requests();
        let quoted = store
            .quote_group_append(&reused, 0, &mut work)
            .await?
            .ok_or("missing reused quote")?;
        let needed = store
            .quote_group_append(fresh, 0, &mut work)
            .await?
            .ok_or("missing fresh quote")?;
        assert_eq!(
            before,
            store.provider().requests(),
            "capacity quotation issued a provider request"
        );
        assert!(
            quoted.records >= needed.records,
            "reused-generation quote cannot admit its composed successor's records"
        );
        assert!(
            quoted.bytes >= needed.bytes,
            "reused-generation quote cannot admit its composed successor's command"
        );
    }
    Ok(())
}

use super::*;
use acyclic_stream::StreamProvider;
use futures::TryStreamExt;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_group_seals_each_member_with_its_original_ledger() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    for name in ["a", "b"] {
        create(&mut seed, name, OwnedBytes::from_static(b"old"), &cancel).await?;
    }
    commit(&mut seed, &cancel).await?;
    let mut proposals = Vec::new();
    for name in ["a", "b"] {
        let original =
            crate::NativeMaterializationWork::new(B, &cancel).map_err(|(error, _)| error)?;
        let mut checkout = volume
            .checkout(
                GenerationSelector::Head,
                writable_manual(),
                &original,
                &cancel,
            )
            .await?;
        checkout
            .change_content(
                path(name)?,
                ContentChange::Write {
                    offset: 0,
                    bytes: OwnedBytes::from_static(b"new"),
                },
                ContentTimes::Preserve,
                &original,
                &cancel,
            )
            .await?;
        proposals.push((checkout, original, Arc::new(AtomicUsize::new(0))));
    }
    let results =
        futures::future::join_all(proposals.iter_mut().map(|(checkout, original, calls)| {
            let cancel = &cancel;
            async move {
                let owner = original.clone();
                let calls = Arc::clone(calls);
                let mut guard = move |work: &crate::NativeMaterializationWork| {
                    assert!(
                        owner.shares_ledger(work),
                        "member seal ran on another producer's ledger"
                    );
                    calls.fetch_add(1, Ordering::SeqCst);
                    std::future::ready(Ok::<_, FsError>(()))
                };
                checkout
                    .publish_pending_against_with_native_work(
                        OperationId::new(),
                        None,
                        None,
                        None,
                        PublicationPermit::Unrestricted,
                        checkout
                            .authority_head
                            .ok_or(FsError::WritableCheckoutRequiresHead)?,
                        original,
                        cancel,
                        &mut guard,
                        None,
                        None,
                    )
                    .await
            }
        }))
        .await;
    for result in results {
        assert!(matches!(
            result?,
            LivePublicationObservation::Committed { .. }
        ));
    }
    for (checkout, _, calls) in &proposals {
        assert!(!checkout.has_pending_mutations());
        assert!(
            calls.load(Ordering::SeqCst) != 0,
            "member's original seal was omitted"
        );
    }
    Ok(())
}

#[tokio::test]
async fn public_checkouts_batch_disjoint_edits_and_recover_each_original_operation() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    for name in ["a", "b", "c", "d"] {
        create(&mut seed, name, OwnedBytes::from_static(b"old"), &cancel).await?;
    }
    commit(&mut seed, &cancel).await?;
    let base = seed.authority_head.ok_or("missing base")?;
    let independent_fs = Fs::new(
        crate::StreamAuthorityStore::new(volume.fs.inner.authority.provider()),
        crate::LogicalObjectStore::new(Arc::clone(volume.fs.inner.objects.provider())),
        EmbeddedCapabilities::MEMORY,
    );
    let independent = independent_fs
        .open_volume(volume.id(), B, &cancel)
        .await?
        .value;
    let mut proposals = Vec::new();
    let mut retries = Vec::new();
    for (name, owner) in [
        ("a", &volume),
        ("b", &independent),
        ("c", &volume),
        ("d", &independent),
    ] {
        for destination in [&mut proposals, &mut retries] {
            let mut checkout = open_checkout(owner, writable_manual(), &cancel).await?;
            checkout
                .change_content(
                    path(name)?,
                    ContentChange::Write {
                        offset: 0,
                        bytes: OwnedBytes::from_static(b"new"),
                    },
                    ContentTimes::Preserve,
                    B,
                    &cancel,
                )
                .await?;
            destination.push(checkout);
        }
    }
    let operations: Vec<_> = (0..4).map(|_| OperationId::new()).collect();
    let results = futures::future::join_all(proposals.iter_mut().zip(&operations).map(
        |(checkout, operation)| {
            checkout.commit(
                *operation,
                None,
                None,
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<FsError>,
            )
        },
    ))
    .await;
    for result in results {
        assert!(
            matches!(result?.value, LivePublicationObservation::Committed { .. }),
            "disjoint proposal lost its batch"
        );
    }
    let provider = volume.fs.inner.authority.provider();
    let records: Vec<_> = provider
        .read(acyclic_stream::ReadRequest {
            path: acyclic_stream::StreamPath::new(format!(
                "fs/authorities/{}/records",
                hex::encode(volume_authority_id(volume.id).into_bytes())
            ))?,
            from: base.sequence.get() + 1,
            limit: 4,
        })
        .await?
        .try_collect()
        .await?;
    assert_eq!(records.len(), 4);
    assert!(
        records
            .iter()
            .all(|record| record.commit_id == records[0].commit_id),
        "independent proposals used separate stream appends"
    );
    for (retry, operation) in retries.iter_mut().zip(operations) {
        let retried = retry
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
        assert!(
            matches!(
                retried.value,
                LivePublicationObservation::AlreadyCommitted { .. }
            ),
            "original operation did not recover: {:?}",
            retried.value
        );
    }
    let mut final_tree = open_checkout(&volume, pinned(), &cancel).await?;
    for name in ["a", "b", "c", "d"] {
        assert_eq!(
            final_tree
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

#[tokio::test]
async fn publication_snapshot_excludes_late_observations_and_releases_on_drop() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut checkout) = fixture(config(), &cancel).await?;
    create(
        &mut checkout,
        "file",
        OwnedBytes::from_static(b"value"),
        &cancel,
    )
    .await?;
    commit(&mut checkout, &cancel).await?;
    let mut checkout = open_checkout(&volume, writable_manual(), &cancel).await?;
    let frozen = checkout.dependencies.freeze_publication()?;
    let before = checkout.dependencies.proof().dependencies.clone();
    let rejected = checkout
        .read_file_range(
            &path("file")?,
            ByteRange {
                offset: 0,
                length: 5,
            },
            B,
            &cancel,
        )
        .await;
    assert!(
        matches!(
            rejected,
            Err(OperationFailure {
                error: FsError::Rebase(RebaseError::DependenciesChanged),
                ..
            })
        ),
        "publication admitted a late read"
    );
    assert_eq!(before, checkout.dependencies.proof().dependencies);
    drop(frozen);
    checkout
        .read_file_range(
            &path("file")?,
            ByteRange {
                offset: 0,
                length: 5,
            },
            B,
            &cancel,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn ordinary_pending_member_withdrawal_preserves_the_other_commit() -> TestResult {
    check_pending_withdrawal(false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_pending_member_withdrawal_preserves_the_other_commit() -> TestResult {
    check_pending_withdrawal(true).await
}

async fn withdrawal_proposal(
    checkout: &mut TestCheckout,
    operation: OperationId,
    native: bool,
    cancel: &CancellationToken,
) -> TestResult<LivePublicationObservation> {
    #[cfg(not(target_arch = "wasm32"))]
    if native {
        return super::owners::submit_proposal(checkout, operation, native, cancel).await;
    }
    #[cfg(target_arch = "wasm32")]
    assert!(!native, "native publication requires a native host");
    Ok(checkout
        .commit(
            operation,
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            cancel,
            allow::<FsError>,
        )
        .await?
        .value)
}

async fn verify_withdrawn_member(
    volume: &TestVolume,
    operations: [OperationId; 2],
    survivor: usize,
    cancel: &CancellationToken,
) -> TestResult {
    let mut work = crate::AuthorityWork::new(B, cancel);
    for (index, operation) in operations.into_iter().enumerate() {
        assert_eq!(
            volume
                .fs
                .inner
                .authority
                .find_operation(volume_authority_id(volume.id()), operation, &mut work)
                .await?
                .is_some(),
            index == survivor,
            "withdrawal changed another proposal's durable operation"
        );
    }
    let mut tree = open_checkout(volume, pinned(), cancel).await?;
    for (index, name) in ["a", "b"].into_iter().enumerate() {
        let read = tree
            .read_file_range(
                &path(name)?,
                ByteRange {
                    offset: 0,
                    length: 3,
                },
                B,
                cancel,
            )
            .await?;
        assert_eq!(
            read.value.bytes.as_ref(),
            if index == survivor { b"new" } else { b"old" },
            "withdrawal published the absent member or lost the survivor"
        );
    }
    Ok(())
}

async fn check_pending_withdrawal(native: bool) -> TestResult {
    use std::future::Future;
    use std::task::Poll;
    for cancel_member in [false, true] {
        let cancel = CancellationToken::new();
        let second_cancel = CancellationToken::new();
        let (volume, mut seed) = fixture(config(), &cancel).await?;
        for name in ["a", "b"] {
            create(&mut seed, name, OwnedBytes::from_static(b"old"), &cancel).await?;
        }
        commit(&mut seed, &cancel).await?;
        let base = seed.authority_head.ok_or("missing base")?;
        let mut a = open_checkout(&volume, writable_manual(), &cancel).await?;
        let mut b = open_checkout(&volume, writable_manual(), &cancel).await?;
        for (checkout, name) in [(&mut a, "a"), (&mut b, "b")] {
            checkout
                .change_content(
                    path(name)?,
                    ContentChange::Write {
                        offset: 0,
                        bytes: OwnedBytes::from_static(b"new"),
                    },
                    ContentTimes::Preserve,
                    B,
                    &cancel,
                )
                .await?;
        }
        let operations = [OperationId::new(), OperationId::new()];
        let mut first = Box::pin(withdrawal_proposal(&mut a, operations[0], native, &cancel));
        let mut second = Box::pin(withdrawal_proposal(
            &mut b,
            operations[1],
            native,
            &second_cancel,
        ));
        futures::future::poll_fn(|context| {
            assert!(first.as_mut().poll(context).is_pending());
            assert!(second.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        if cancel_member {
            second_cancel.cancel();
            futures::future::poll_fn(|context| {
                assert!(
                    matches!(second.as_mut().poll(context), Poll::Ready(Err(_))),
                    "cancelled member remained asleep in the combiner"
                );
                Poll::Ready(())
            })
            .await;
            drop(second);
            assert!(
                matches!(
                    first.await,
                    Ok(LivePublicationObservation::Committed { .. })
                ),
                "pending withdrawal revoked a surviving proposal"
            );
        } else {
            drop(first);
            assert!(
                matches!(
                    second.await,
                    Ok(LivePublicationObservation::Committed { .. })
                ),
                "pending withdrawal revoked a surviving proposal"
            );
        }
        let mut accounting = KernelAccounting::standalone(B, &cancel);
        let (_, head) = volume
            .resolve_head_generation(&mut accounting, &cancel)
            .await?;
        assert_eq!(
            head.sequence,
            base.sequence.checked_next()?,
            "withdrawal changed the survivor count"
        );
        verify_withdrawn_member(&volume, operations, usize::from(!cancel_member), &cancel).await?;
        if cancel_member {
            commit(&mut b, &cancel).await?;
        } else {
            commit(&mut a, &cancel).await?;
        }
        assert!(
            !a.has_pending_mutations() && !b.has_pending_mutations(),
            "fresh round did not recover aborted members"
        );
    }
    Ok(())
}

mod property {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(16))]
        #[test]
        fn actual_batches_have_a_valid_serial_order(keys in proptest::collection::vec(0usize..4, 2..9)) {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            runtime.block_on(async {
                let cancel = CancellationToken::new();
                let (volume, mut seed) = fixture(config(), &cancel).await?;
                for name in ["a", "b", "c", "d"] { create(&mut seed, name, OwnedBytes::from_static(&[0]), &cancel).await?; }
                commit(&mut seed, &cancel).await?;
                let mut serial = open_checkout(&volume, writable_manual(), &cancel).await?;
                let mut proposals = Vec::new();
                for (index, key) in keys.iter().enumerate() {
                    let mut checkout = open_checkout(&volume, writable_manual(), &cancel).await?;
                    checkout.change_content(path(["a", "b", "c", "d"][*key])?, ContentChange::Write {
                        offset: 0, bytes: OwnedBytes::from(vec![u8::try_from(index + 1)?]),
                    }, ContentTimes::Preserve, B, &cancel).await?;
                    proposals.push(checkout);
                }
                let outcomes = futures::future::join_all(proposals.iter_mut().map(|checkout| {
                    checkout.commit(OperationId::new(), None, None, None, PublicationPermit::Unrestricted, B, &cancel, allow::<FsError>)
                })).await;
                let mut committed = Vec::new();
                for (index, outcome) in outcomes.into_iter().enumerate() {
                    match outcome?.value {
                        LivePublicationObservation::Committed { head, .. } => committed.push((head.sequence, index)),
                        LivePublicationObservation::Conflict { .. } => {},
                        outcome => return Err(format!("unexpected outcome: {outcome:?}").into()),
                    }
                }
                committed.sort_unstable();
                let mut written = std::collections::BTreeSet::new();
                for (_, index) in committed {
                    assert!(written.insert(keys[index]), "batch admitted two writes to the same before-record");
                    serial.change_content(path(["a", "b", "c", "d"][keys[index]])?, ContentChange::Write {
                        offset: 0, bytes: OwnedBytes::from(vec![u8::try_from(index + 1)?]),
                    }, ContentTimes::Preserve, B, &cancel).await?;
                }
                let actual = open_checkout(&volume, pinned(), &cancel).await?;
                assert_eq!(actual.root.file_table, serial.root.file_table, "actual batch differs from its serial history");
                Ok::<_, Box<dyn std::error::Error>>(())
            }).map_err(|error| TestCaseError::fail(error.to_string()))?;
        }
    }
}

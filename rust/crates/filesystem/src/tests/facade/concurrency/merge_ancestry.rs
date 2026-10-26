//! Explicit merge ancestry survives the shared tree-composition path.
use super::*;
use acyclic_stream::StreamProvider;
use futures::TryStreamExt;

#[tokio::test]
async fn prepared_merge_ancestry_survives_batch_followers() -> TestResult {
    composed_merge(false, false).await
}

#[tokio::test]
async fn prepared_merge_ancestry_survives_stale_head_normalization() -> TestResult {
    composed_merge(true, false).await
}

#[tokio::test]
async fn native_prepared_merge_ancestry_survives_batch_followers() -> TestResult {
    composed_merge(false, true).await
}

#[tokio::test]
async fn native_prepared_merge_ancestry_survives_stale_head_normalization() -> TestResult {
    composed_merge(true, true).await
}

async fn composed_merge(stale: bool, native: bool) -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    for name in ["a", "b"] {
        create(&mut seed, name, OwnedBytes::from_static(b"old"), &cancel).await?;
    }
    commit(&mut seed, &cancel).await?;
    let mut merge = open_checkout(&volume, writable_manual(), &cancel).await?;
    let mut other = open_checkout(&volume, writable_manual(), &cancel).await?;
    for (checkout, name) in [(&mut merge, "a"), (&mut other, "b")] {
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
    assert!(matches!(
        merge
            .prepare_merge(seed.generation_id(), 32, 8, B, &cancel)
            .await?
            .value,
        MergePreparation::Prepared { .. }
    ));
    let prepared = merge.generation_id();
    let parents = merge.root().parents.clone();
    assert_eq!(
        parents.len(),
        2,
        "fixture must contain a real prepared merge"
    );
    let original = crate::NativeMaterializationWork::new(B, &cancel).map_err(|(error, _)| error)?;
    let other_original =
        crate::NativeMaterializationWork::new(B, &cancel).map_err(|(error, _)| error)?;
    if native {
        // Copy through the production candidate owner; a standalone checkout
        // cannot borrow an unrelated native ledger for publication.
        merge =
            merge.private_candidate(&mut KernelAccounting::native(&original, &cancel), &cancel)?;
        merge.validate_native_work(&original)?;
        other = other.private_candidate(
            &mut KernelAccounting::native(&other_original, &cancel),
            &cancel,
        )?;
        other.validate_native_work(&other_original)?;
    }
    if stale {
        publish(&mut other, native.then_some(&other_original), &cancel).await?;
        publish(&mut merge, native.then_some(&original), &cancel).await?;
    } else {
        // Both members use the same publication mode. Poll the independent
        // edit first so the prepared merge must follow it inside this batch.
        let (first, second) = futures::future::join(
            publish(&mut other, native.then_some(&other_original), &cancel),
            publish(&mut merge, native.then_some(&original), &cancel),
        )
        .await;
        first?;
        second?;
        let records: Vec<_> = volume
            .fs
            .inner
            .authority
            .provider()
            .read(acyclic_stream::ReadRequest {
                path: acyclic_stream::StreamPath::new(format!(
                    "fs/authorities/{}/records",
                    hex::encode(volume_authority_id(volume.id).into_bytes())
                ))?,
                from: seed
                    .authority_head
                    .ok_or("missing seed head")?
                    .sequence
                    .get()
                    + 1,
                limit: 2,
            })
            .await?
            .try_collect()
            .await?;
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0].commit_id, records[1].commit_id,
            "prepared merge was not a batch follower"
        );
    }
    assert_ne!(
        merge.generation_id(),
        prepared,
        "fixture did not compose the merge"
    );
    assert_eq!(
        merge.root().parents.as_slice(),
        &[other.generation_id(), prepared],
        "composition discarded the original prepared merge ancestry"
    );
    let original_merge = volume
        .checkout(GenerationSelector::Exact(prepared), pinned(), B, &cancel)
        .await?
        .value;
    assert_eq!(original_merge.root().parents, parents);
    let mut final_tree = open_checkout(&volume, pinned(), &cancel).await?;
    assert_eq!(final_tree.generation_id(), merge.generation_id());
    for name in ["a", "b"] {
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

async fn publish(
    checkout: &mut TestCheckout,
    original: Option<&crate::NativeMaterializationWork>,
    cancel: &CancellationToken,
) -> TestResult {
    let outcome = if let Some(original) = original {
        let owner = original.clone();
        let mut guard = move |work: &crate::NativeMaterializationWork| {
            assert!(owner.shares_ledger(work));
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
            .await?
    } else {
        checkout
            .commit(
                OperationId::new(),
                None,
                None,
                None,
                PublicationPermit::Unrestricted,
                B,
                cancel,
                allow::<FsError>,
            )
            .await?
            .value
    };
    assert!(
        matches!(outcome, LivePublicationObservation::Committed { .. }),
        "{outcome:?}"
    );
    Ok(())
}

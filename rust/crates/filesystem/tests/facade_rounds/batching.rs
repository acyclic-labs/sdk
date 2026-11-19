use acyclic_fs::kernel::volume_authority_id;
use acyclic_fs::model::CheckoutMode;
use acyclic_fs::{
    ByteRange, CancellationToken, ContentChange, ContentTimes, FsError, LivePublicationObservation,
    PublicationPermit,
};
use acyclic_fs_format::identity::OperationId;
use acyclic_fs_test_support::{
    B, TestResult, allow, commit_head, create, fixture_with_providers, open_checkout, path,
    serialized_config, writable_manual,
};
use acyclic_stream::{OwnedBytes, StreamProvider};
use futures::TryStreamExt;

#[tokio::test]
async fn public_checkouts_batch_disjoint_edits_and_recover_each_original_operation() -> TestResult {
    let cancel = CancellationToken::new();
    let (providers, volume, mut seed) =
        fixture_with_providers(serialized_config(), &cancel).await?;
    for name in ["a", "b", "c", "d"] {
        create(&mut seed, name, OwnedBytes::from_static(b"old"), &cancel).await?;
    }
    let base = commit_head(&mut seed, &cancel).await?;
    let independent_fs = providers.filesystem();
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
    let records: Vec<_> = providers
        .stream
        .read(acyclic_stream::ReadRequest {
            key: None,
            path: acyclic_stream::StreamPath::new(format!(
                "fs/authorities/{}/records",
                hex::encode(volume_authority_id(volume.id()).into_bytes())
            ))?,
            from: base.sequence.get() + 1,
            limit: 4,
            at_least: Default::default(),
        })
        .await?
        .try_collect()
        .await?;
    assert_eq!(records.len(), 4);
    let first = records.first().ok_or("missing batch record")?;
    assert!(
        records
            .iter()
            .all(|record| record.commit_id == first.commit_id),
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
    let mut final_tree = open_checkout(&volume, CheckoutMode::read_only_pinned(), &cancel).await?;
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

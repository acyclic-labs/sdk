use acyclic_fs::async_storage::MAX_AUTHORITY_GROUP_MEMBERS;
use acyclic_fs::kernel::volume_authority_id;
use acyclic_fs::model::CheckoutMode;
use acyclic_fs::{
    ByteRange, CancellationToken, ContentChange, ContentTimes, FsError, LivePublicationObservation,
    PublicationPermit,
};
use acyclic_fs_format::identity::OperationId;
use acyclic_fs_test_support::{
    B, TestCheckout, TestResult, TestVolume, allow, commit_head, create, fixture,
    fixture_with_providers, open_checkout, path, serialized_config, writable_manual,
};
use acyclic_stream::{OwnedBytes, StreamProvider};
use futures::TryStreamExt;
use std::future::Future;

async fn proposal(
    volume: &TestVolume,
    name: &str,
    cancel: &CancellationToken,
) -> Result<TestCheckout, Box<dyn std::error::Error>> {
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

#[tokio::test]
async fn completed_round_receipt_custody_does_not_exclude_the_next_batch() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(serialized_config(), &cancel).await?;
    for name in ["a", "b", "c", "d"] {
        create(&mut seed, name, OwnedBytes::from_static(b"old"), &cancel).await?;
    }
    commit_head(&mut seed, &cancel).await?;
    let mut a = proposal(&volume, "a", &cancel).await?;
    let mut b = proposal(&volume, "b", &cancel).await?;
    let mut first = Box::pin(a.commit(
        OperationId::new(),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancel,
        allow::<FsError>,
    ));
    let mut second = Box::pin(b.commit(
        OperationId::new(),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancel,
        allow::<FsError>,
    ));
    let second = futures::future::poll_fn(|cx| {
        // Drive the scheduler while leaving the first completed round's receipt
        // unconsumed. Collection owns a funded, bounded scheduling window.
        assert!(first.as_mut().poll(cx).is_pending());
        second.as_mut().poll(cx)
    })
    .await?;
    assert!(matches!(
        second.value,
        LivePublicationObservation::Committed { .. }
    ));
    // The first future still owns its unconsumed receipt and round ticket.
    let mut c = proposal(&volume, "c", &cancel).await?;
    let mut d = proposal(&volume, "d", &cancel).await?;
    let outcomes = futures::future::join(
        c.commit(
            OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        ),
        d.commit(
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
        assert!(
            matches!(outcome?.value, LivePublicationObservation::Committed { .. }),
            "receipt ownership from a completed round disabled the next batch"
        );
    }
    assert!(matches!(
        first.await?.value,
        LivePublicationObservation::Committed { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn disjoint_proposals_with_different_bases_share_one_append_and_original_retries()
-> TestResult {
    let cancel = CancellationToken::new();
    let (providers, volume, mut seed) =
        fixture_with_providers(serialized_config(), &cancel).await?;
    for name in ["a", "b", "c"] {
        create(&mut seed, name, OwnedBytes::from_static(b"old"), &cancel).await?;
    }
    commit_head(&mut seed, &cancel).await?;
    let mut stale = proposal(&volume, "a", &cancel).await?;
    let mut retry = proposal(&volume, "a", &cancel).await?;
    let mut intervening = proposal(&volume, "b", &cancel).await?;
    let base = commit_head(&mut intervening, &cancel).await?;
    let mut fresh = proposal(&volume, "c", &cancel).await?;
    let operation = OperationId::new();
    let outcomes = futures::future::join(
        stale.commit(
            operation,
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        ),
        fresh.commit(
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
        assert!(
            matches!(outcome?.value, LivePublicationObservation::Committed { .. }),
            "disjoint stale proposal required a caller replay"
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
            limit: 2,
            at_least: Default::default(),
        })
        .await?
        .try_collect()
        .await?;
    assert_eq!(records.len(), 2);
    assert_eq!(
        records.first().map(|record| record.commit_id),
        records.last().map(|record| record.commit_id)
    );
    assert!(
        matches!(
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
        ),
        "normalization replaced the original operation identity"
    );
    let mut current = open_checkout(&volume, CheckoutMode::read_only_pinned(), &cancel).await?;
    for name in ["a", "b", "c"] {
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

#[tokio::test]
async fn proposals_beyond_one_provider_transaction_remain_in_batches_without_caller_rebase()
-> TestResult {
    let cancel = CancellationToken::new();
    let (providers, volume, mut seed) =
        fixture_with_providers(serialized_config(), &cancel).await?;
    let count = MAX_AUTHORITY_GROUP_MEMBERS + 1;
    for index in 0..count {
        create(
            &mut seed,
            &format!("file-{index}"),
            OwnedBytes::from_static(b"old"),
            &cancel,
        )
        .await?;
    }
    let base = commit_head(&mut seed, &cancel).await?;
    let mut proposals = Vec::new();
    for index in 0..count {
        proposals.push(proposal(&volume, &format!("file-{index}"), &cancel).await?);
    }
    let outcomes = futures::future::join_all(proposals.iter_mut().map(|checkout| {
        checkout.commit(
            OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        )
    }))
    .await;
    for outcome in outcomes {
        assert!(
            matches!(outcome?.value, LivePublicationObservation::Committed { .. }),
            "batch overflow escaped into caller rebase"
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
            limit: u32::try_from(count)?,
            at_least: Default::default(),
        })
        .await?
        .try_collect()
        .await?;
    assert_eq!(records.len(), count);
    assert_eq!(
        records
            .iter()
            .map(|record| record.commit_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn evidence_capacity_splits_disjoint_proposals_before_stream_admission() -> TestResult {
    let cancel = CancellationToken::new();
    let (providers, volume, mut seed) =
        fixture_with_providers(serialized_config(), &cancel).await?;
    let count = MAX_AUTHORITY_GROUP_MEMBERS + 1;
    for index in 0..count {
        create(
            &mut seed,
            &format!("evidence-{index}"),
            OwnedBytes::from_static(b"old"),
            &cancel,
        )
        .await?;
    }
    let base = commit_head(&mut seed, &cancel).await?;
    let mut proposals = Vec::new();
    for index in 0..count {
        proposals.push(proposal(&volume, &format!("evidence-{index}"), &cancel).await?);
    }
    let evidence = OwnedBytes::from(vec![42; 44 * 1024]);
    let outcomes = futures::future::join_all(proposals.iter_mut().map(|checkout| {
        checkout.commit(
            OperationId::new(),
            None,
            Some(evidence.clone()),
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        )
    }))
    .await;
    for outcome in outcomes {
        assert!(
            matches!(outcome?.value, LivePublicationObservation::Committed { .. }),
            "byte capacity escaped into a failed stream command or caller rebase"
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
            limit: u32::try_from(count)?,
            at_least: Default::default(),
        })
        .await?
        .try_collect()
        .await?;
    assert_eq!(records.len(), count);
    assert_eq!(
        records
            .iter()
            .map(|record| record.commit_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        2
    );
    Ok(())
}

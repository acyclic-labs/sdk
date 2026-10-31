use super::*;
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitMutation, CommitRequest, StreamPath,
    StreamProviderExt as _,
};

fn companion() -> TestResult<crate::FilesystemCommit> {
    let actor = StreamPath::new("agents/owner/state")?;
    Ok(crate::FilesystemCommit::new(CommitRequest {
        conditions: vec![CommitCondition::Absent {
            path: actor.clone(),
        }],
        mutations: vec![CommitMutation::Append {
            path: actor,
            records: vec![AppendRecord::from(OwnedBytes::from_static(b"alarm"))],
        }],
        idempotency_key: acyclic_stream::IdempotencyKey::new(OwnedBytes::from_static(
            b"authored-publication",
        ))?,
        deadline_unix_millis: None,
    }))
}

#[tokio::test]
async fn authored_generation_and_companion_share_both_conditional_fences() -> TestResult {
    for conflict in [None, Some("caller"), Some("generation")] {
        let cancel = CancellationToken::new();
        let (volume, mut authored) = fixture(serialized_config(), &cancel).await?;
        let fs = volume.fs.clone();
        let provider = fs.stream_provider();
        let actor = StreamPath::new("agents/owner/state")?;
        let before = authored.generation_id();
        create(
            &mut authored,
            "schedule",
            OwnedBytes::from_static(b"due"),
            &cancel,
        )
        .await?;
        let mut transaction = companion()?;
        let crate::PublicationContribution::Prepared(proposed) = authored
            .contribute_publication(
                OperationId::from_bytes([0x39; 16]),
                crate::kernel::PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
                &mut transaction,
                B,
                &cancel,
            )
            .await?
            .value
        else {
            return Err("fresh authored contribution was refused".into());
        };
        assert_ne!(proposed, before);
        assert_eq!(
            open_checkout(&volume, pinned(), &cancel)
                .await?
                .generation_id(),
            before,
            "preparation must not publish the generation"
        );
        assert!(matches!(
            provider.tail(actor.clone()).await,
            Err(acyclic_stream::StreamError::NotFound)
        ));
        if conflict == Some("caller") {
            let mut concurrent = companion()?.request().clone();
            concurrent.idempotency_key =
                acyclic_stream::IdempotencyKey::new(OwnedBytes::from_static(b"concurrent-caller"))?;
            provider.commit(concurrent).await?;
        } else if conflict == Some("generation") {
            let mut concurrent = open_checkout(&volume, writable_manual(), &cancel).await?;
            create(
                &mut concurrent,
                "other",
                OwnedBytes::from_static(b"concurrent"),
                &cancel,
            )
            .await?;
            commit(&mut concurrent, &cancel).await?;
        }
        let outcome = fs.submit_commit(transaction, B, &cancel).await?.value;
        assert_eq!(
            matches!(outcome, crate::FilesystemCommitOutcome::Committed(_)),
            conflict.is_none()
        );
        assert_eq!(
            matches!(outcome, crate::FilesystemCommitOutcome::Rejected(_)),
            conflict.is_some()
        );
        let mut visible = open_checkout(&volume, pinned(), &cancel).await?;
        assert_eq!(
            visible
                .lookup_no_follow(&path("schedule")?, B, &cancel)
                .await?
                .value
                .record
                .is_some(),
            conflict.is_none()
        );
        if conflict == Some("generation") {
            assert!(matches!(
                provider.tail(actor).await,
                Err(acyclic_stream::StreamError::NotFound)
            ));
        }
    }
    Ok(())
}

#[tokio::test]
async fn unresolved_authored_publication_keeps_the_exact_commit_for_resubmission() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut authored) = fixture(config(), &cancel).await?;
    let fs = volume.fs.clone();
    create(
        &mut authored,
        "monitor",
        OwnedBytes::from_static(b"cursor"),
        &cancel,
    )
    .await?;
    let mut transaction = companion()?;
    assert!(matches!(
        authored
            .contribute_publication(
                OperationId::from_bytes([0x40; 16]),
                crate::kernel::PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
                &mut transaction,
                B,
                &cancel,
            )
            .await?
            .value,
        crate::PublicationContribution::Prepared(_)
    ));
    fs.seal_commit(&mut transaction, B, &cancel)?;
    let request = transaction.request().clone();
    let stopped = CancellationToken::new();
    stopped.cancel();
    let crate::FilesystemCommitOutcome::Unknown { commit, .. } =
        fs.submit_commit(transaction, B, &stopped).await?.value
    else {
        return Err("cancelled submission lost its unresolved owner".into());
    };
    assert_eq!(commit.request(), &request);
    assert!(matches!(
        fs.submit_commit(commit, B, &cancel).await?.value,
        crate::FilesystemCommitOutcome::Committed(_)
    ));
    let mut visible = open_checkout(&volume, pinned(), &cancel).await?;
    assert!(
        visible
            .lookup_no_follow(&path("monitor")?, B, &cancel)
            .await?
            .value
            .record
            .is_some()
    );
    Ok(())
}

#[tokio::test]
async fn dropping_an_authored_contribution_releases_the_original_authority() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut authored) = fixture(serialized_config(), &cancel).await?;
    let fs = volume.fs.clone();
    create(
        &mut authored,
        "discarded",
        OwnedBytes::from_static(b"unpublished"),
        &cancel,
    )
    .await?;
    let mut transaction = companion()?;
    assert!(matches!(
        authored
            .contribute_publication(
                OperationId::from_bytes([0x41; 16]),
                crate::kernel::PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
                &mut transaction,
                B,
                &cancel,
            )
            .await?
            .value,
        crate::PublicationContribution::Prepared(_)
    ));
    drop(transaction);
    let mut next = open_checkout(&volume, writable_manual(), &cancel).await?;
    create(
        &mut next,
        "retained",
        OwnedBytes::from_static(b"published"),
        &cancel,
    )
    .await?;
    commit(&mut next, &cancel).await?;
    let mut visible = open_checkout(&volume, pinned(), &cancel).await?;
    assert!(
        visible
            .lookup_no_follow(&path("retained")?, B, &cancel)
            .await?
            .value
            .record
            .is_some()
    );
    assert!(
        visible
            .lookup_no_follow(&path("discarded")?, B, &cancel)
            .await?
            .value
            .record
            .is_none()
    );
    assert!(matches!(
        fs.stream_provider()
            .tail(StreamPath::new("agents/owner/state")?)
            .await,
        Err(acyclic_stream::StreamError::NotFound)
    ));
    Ok(())
}

use acyclic_fs::kernel::{
    FileKind, FilePayload, FileRecord, InlineFileData, Mutation, RebaseDecision,
    volume_authority_id,
};
use acyclic_fs::{
    AsyncAuthorityStore, AuthorityWork, CancellationToken, FenceOutcome, FsError,
    LiveMutationOutcome, StreamAuthorityStore,
};
use acyclic_fs_format::identity::{FileId, OperationId};
use acyclic_fs_test_support::{
    B, TestResult, create, fixture_with_providers, open_checkout, path, path_parts,
    serialized_config, writable_live, writable_manual,
};
use acyclic_fs_work::performance::OperationFailure;
use acyclic_stream::OwnedBytes;

#[tokio::test]
async fn direct_live_resume_preserves_original_identity_and_epoch_fence() -> TestResult {
    let cancel = CancellationToken::new();
    let (providers, volume, _) = fixture_with_providers(serialized_config(), &cancel).await?;
    let mut winner = open_checkout(&volume, writable_live(), &cancel).await?;
    let mut retrying = open_checkout(&volume, writable_live(), &cancel).await?;
    create(
        &mut winner,
        "winner",
        OwnedBytes::from_static(b"winner"),
        &cancel,
    )
    .await?;
    winner.resume_live(OperationId::new(), B, &cancel).await?;
    create(
        &mut retrying,
        "retrying",
        OwnedBytes::from_static(b"retrying"),
        &cancel,
    )
    .await?;
    let retry = OperationId::from_bytes([119; 16]);
    assert!(matches!(
        retrying.resume_live(retry, B, &cancel).await?.value,
        LiveMutationOutcome::Conflict { .. }
    ));
    assert!(retrying.has_pending_mutations());
    assert!(matches!(
        retrying.rebase_head(Some(8), B, &cancel).await?.value,
        RebaseDecision::Safe { .. }
    ));
    assert!(matches!(
        retrying.resume_live(retry, B, &cancel).await?.value,
        LiveMutationOutcome::Committed { .. }
    ));
    assert!(!retrying.has_pending_mutations());
    assert!(matches!(
        retrying.resume_live(retry, B, &cancel).await,
        Err(OperationFailure {
            error: FsError::NoPendingMutations,
            ..
        })
    ));
    let mut pending = open_checkout(&volume, writable_live(), &cancel).await?;
    create(
        &mut pending,
        "pending-guard",
        OwnedBytes::from_static(b"pending"),
        &cancel,
    )
    .await?;
    assert!(matches!(
        pending
            .mutate_live(Vec::new(), OperationId::new(), B, &cancel)
            .await,
        Err(OperationFailure {
            error: FsError::PendingLiveMutation,
            ..
        })
    ));
    assert!(matches!(
        pending
            .apply_authored_live(Vec::new(), OperationId::new(), B, &cancel)
            .await,
        Err(OperationFailure {
            error: FsError::PendingLiveMutation,
            ..
        })
    ));
    let mut first = open_checkout(&volume, writable_live(), &cancel).await?;
    let mut repeated = open_checkout(&volume, writable_live(), &cancel).await?;
    let metadata = first
        .lookup_no_follow(&path_parts(&[])?, B, &cancel)
        .await?
        .value
        .record
        .ok_or("root absent")?
        .metadata;
    let mutation = Mutation::Create {
        path: path("idempotent")?,
        record: FileRecord {
            bindings: None,
            file_id: FileId::from_bytes([120; 16]),
            kind: FileKind::Regular,
            link_count: 1,
            metadata,
            payload: FilePayload::InlineRegular(InlineFileData::new(b"same")?),
        },
    };
    first.mutate(vec![mutation.clone()], B, &cancel).await?;
    repeated.mutate(vec![mutation], B, &cancel).await?;
    let operation = OperationId::from_bytes([121; 16]);
    assert!(matches!(
        first.resume_live(operation, B, &cancel).await?.value,
        LiveMutationOutcome::Committed { .. }
    ));
    assert!(matches!(
        repeated.resume_live(operation, B, &cancel).await?.value,
        LiveMutationOutcome::AlreadyCommitted { .. }
    ));
    let mut different = open_checkout(&volume, writable_live(), &cancel).await?;
    create(
        &mut different,
        "different-operation",
        OwnedBytes::from_static(b"different"),
        &cancel,
    )
    .await?;
    assert!(matches!(
        different.resume_live(operation, B, &cancel).await?.value,
        LiveMutationOutcome::IdempotencyConflict { .. }
    ));
    assert!(different.has_pending_mutations());
    let mut fenced = open_checkout(&volume, writable_live(), &cancel).await?;
    create(
        &mut fenced,
        "fenced",
        OwnedBytes::from_static(b"fenced"),
        &cancel,
    )
    .await?;
    let authority = StreamAuthorityStore::new(providers.stream);
    let authority_id = volume_authority_id(volume.id());
    let mut work = AuthorityWork::new(B, &cancel);
    let expected = authority.head(authority_id, &mut work).await?;
    let FenceOutcome::Advanced(head) = authority.fence(authority_id, expected, &mut work).await?
    else {
        return Err("fresh fence conflicted".into());
    };
    assert!(
        matches!(fenced.resume_live(OperationId::new(), B, &cancel).await?.value,
        LiveMutationOutcome::Fenced { actual_epoch } if actual_epoch == head.epoch)
    );
    let mut manual = open_checkout(&volume, writable_manual(), &cancel).await?;
    assert!(manual.refresh_live(B, &cancel).await.is_err());
    assert!(
        manual
            .mutate_live(Vec::new(), OperationId::new(), B, &cancel)
            .await
            .is_err()
    );
    Ok(())
}

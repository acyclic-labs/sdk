use acyclic_fs::kernel::{DependencyRegion, Mutation, RebaseDecision};
use acyclic_fs::model::GenerationSelector;
use acyclic_fs::{
    AuthoredMutation, ByteRange, CancellationToken, ContentChange, ContentTimes, FsError,
    LiveMutationOutcome,
};
use acyclic_fs_format::identity::OperationId;
use acyclic_fs_format::metadata::FileMetadata;
use acyclic_stream::OwnedBytes;

use super::B;
use super::checkout_fixture::{config, path, writable_pinned};
use super::checkout_serialized::serialized_config;
use super::fixtures::{TestResult, create, fixture, open_checkout};
use super::live_fixture::writable_live;
use super::volume_commit::commit;
use super::volume_read::read;

#[tokio::test]
async fn direct_live_conflicts_return_before_explicit_rebase() -> TestResult {
    let cancel = CancellationToken::new();
    let (optimistic, _) = fixture(config(), &cancel).await?;
    let rejected = optimistic
        .checkout(GenerationSelector::Head, writable_live(), B, &cancel)
        .await
        .err()
        .ok_or("optimistic live admitted")?;
    assert!(matches!(
        rejected.error,
        FsError::LiveRequiresSerializedAuthority
    ));
    let (volume, mut seed) = fixture(serialized_config(), &cancel).await?;
    create(
        &mut seed,
        "file",
        OwnedBytes::from(vec![b'a'; 128]),
        &cancel,
    )
    .await?;
    commit(&mut seed, &cancel).await?;
    for overlap in [false, true] {
        let mut first = open_checkout(&volume, writable_live(), &cancel).await?;
        let mut second = open_checkout(&volume, writable_live(), &cancel).await?;
        let writes = if overlap {
            [(10, b"RACING".as_slice()), (12, b"OVERLAP".as_slice())]
        } else {
            [(0, b"FIRST".as_slice()), (64, b"SECOND".as_slice())]
        };
        let mut blobs = Vec::new();
        for (checkout, (_, bytes)) in [&mut first, &mut second].into_iter().zip(writes) {
            blobs.push(
                checkout
                    .stage_content(
                        &mut std::io::Cursor::new(bytes),
                        bytes.len() as u64,
                        B,
                        &cancel,
                    )
                    .await?
                    .value,
            );
        }
        for ((checkout, (offset, _)), blob) in [&mut first, &mut second]
            .into_iter()
            .zip(writes)
            .zip(&blobs)
        {
            checkout
                .mutate(
                    vec![Mutation::Write {
                        path: path("file")?,
                        offset,
                        length: blob.logical_bytes(),
                        content: blob.root(),
                        content_offset: 0,
                    }],
                    B,
                    &cancel,
                )
                .await?;
        }
        assert!(matches!(
            first
                .resume_live(OperationId::new(), B, &cancel)
                .await?
                .value,
            LiveMutationOutcome::Committed { .. }
        ));
        let operation = OperationId::new();
        let before = second.generation_id();
        let conflict = second.resume_live(operation, B, &cancel).await?;
        let LiveMutationOutcome::Conflict { actual } = conflict.value else {
            return Err("stale publication did not return its original conflict".into());
        };
        assert_eq!(second.generation_id(), before);
        assert!(second.has_pending_mutations());
        assert_eq!(
            second.resume_live(operation, B, &cancel).await?.value,
            LiveMutationOutcome::Conflict { actual }
        );
        let rebase = second.rebase_head(Some(8), B, &cancel).await?.value;
        if overlap {
            assert!(
                matches!(rebase, RebaseDecision::Conflicted { conflicts, .. }
                if conflicts.iter().any(|conflict| matches!(conflict.region, DependencyRegion::ContentRange { .. })))
            );
            assert_eq!(second.generation_id(), before);
        } else {
            assert!(matches!(rebase, RebaseDecision::Safe { .. }));
            assert!(matches!(
                second.resume_live(operation, B, &cancel).await?.value,
                LiveMutationOutcome::Committed { .. }
            ));
        }
    }
    Ok(())
}

#[tokio::test]
async fn explicit_live_refresh_advances_only_across_unobserved_regions() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(serialized_config(), &cancel).await?;
    let id = create(
        &mut seed,
        "refresh",
        OwnedBytes::from(vec![b'a'; 128]),
        &cancel,
    )
    .await?;
    commit(&mut seed, &cancel).await?;
    let mut live = open_checkout(&volume, writable_live(), &cancel).await?;
    for (offset, bytes) in [(64, b"SAFE"), (0, b"RACE")] {
        assert_eq!(read(&mut live, id, 0, 4, &cancel).await?.as_ref(), b"aaaa");
        let before = live.generation_id();
        let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
        remote
            .change_content(
                path("refresh")?,
                ContentChange::Write {
                    offset,
                    bytes: OwnedBytes::from_static(bytes),
                },
                ContentTimes::Preserve,
                B,
                &cancel,
            )
            .await?;
        commit(&mut remote, &cancel).await?;
        if offset == 0 {
            let conflict = live
                .refresh_live(B, &cancel)
                .await
                .err()
                .ok_or("overlapping refresh unexpectedly succeeded")?;
            assert!(
                matches!(conflict.error, FsError::LiveConflict { conflicts, .. }
                if conflicts.iter().any(|conflict| matches!(conflict.region, DependencyRegion::ContentRange { .. })))
            );
            assert_eq!(live.generation_id(), before);
        } else {
            assert_eq!(
                live.refresh_live(B, &cancel).await?.value,
                remote.generation_id()
            );
            assert_eq!(live.generation_id(), remote.generation_id());
        }
    }
    Ok(())
}

#[tokio::test]
async fn authored_live_transaction_preserves_create_ids_and_publishes_atomically() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, _) = fixture(serialized_config(), &cancel).await?;
    let mut checkout = open_checkout(&volume, writable_live(), &cancel).await?;
    let file = path("authored-live.txt")?;
    let applied = checkout
        .apply_authored_live(
            vec![
                AuthoredMutation::CreateFile {
                    file_id: None,
                    path: file.clone(),
                    bytes: OwnedBytes::from_static(b"initial"),
                    metadata: FileMetadata::default(),
                },
                AuthoredMutation::Write {
                    path: file.clone(),
                    offset: 7,
                    bytes: OwnedBytes::from_static(b"-published"),
                },
            ],
            OperationId::from_bytes([118; 16]),
            B,
            &cancel,
        )
        .await?
        .value;
    assert!(matches!(
        applied.outcome,
        LiveMutationOutcome::Committed { .. }
    ));
    assert!(matches!(
        applied.created_file_ids.as_slice(),
        [Some(_), None]
    ));
    assert!(!checkout.has_pending_mutations());
    assert_eq!(
        checkout
            .read_file_range(
                &file,
                ByteRange {
                    offset: 0,
                    length: 17
                },
                B,
                &cancel
            )
            .await?
            .value
            .bytes
            .as_ref(),
        b"initial-published"
    );
    Ok(())
}

use super::*;

#[tokio::test]
async fn checkout_generation_and_mode_guards_reject_without_candidate_damage()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let first =
        (fs.create_volume_with_id(VolumeId::from_bytes([119; 16]), config(), B, &cancellation))
            .await?
            .value;
    let second =
        (fs.create_volume_with_id(VolumeId::from_bytes([120; 16]), config(), B, &cancellation))
            .await?
            .value;
    let first_head = open_checkout(&first, pinned(), &cancellation)
        .await?
        .generation_id();
    let second_head = open_checkout(&second, pinned(), &cancellation)
        .await?
        .generation_id();
    assert!(
        (first.checkout(
            GenerationSelector::Exact(first_head),
            writable_pinned(),
            B,
            &cancellation,
        ))
        .await
        .is_err()
    );
    assert!(
        (first.checkout(
            GenerationSelector::Exact(second_head),
            pinned(),
            B,
            &cancellation,
        ))
        .await
        .is_err()
    );
    assert!(
        (first.diff_generations(first_head, second_head, 8, B, &cancellation,))
            .await
            .is_err()
    );

    let mut read_only = open_checkout(&first, pinned(), &cancellation).await?;
    assert!(
        (read_only.mutate(Vec::new(), B, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (read_only.commit(
            OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancellation,
            allow::<FsError>
        ))
        .await
        .is_err()
    );
    assert!((read_only.discard(B, &cancellation)).await.is_err());
    assert!((read_only.refresh_head(B, &cancellation)).await.is_err());
    assert!((read_only.refresh_live(B, &cancellation)).await.is_err());
    assert!(
        (read_only.mutate_live(Vec::new(), OperationId::new(), 1, 1, B, &cancellation,))
            .await
            .is_err()
    );
    assert!(
        (read_only.resume_live(OperationId::new(), 1, 1, B, &cancellation,))
            .await
            .is_err()
    );

    let mut clean = open_checkout(&first, writable_pinned(), &cancellation).await?;
    assert!(
        (clean.commit(
            OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancellation,
            allow::<FsError>
        ))
        .await
        .is_err()
    );
    assert!(
        (clean.resume_live(OperationId::new(), 1, 1, B, &cancellation,))
            .await
            .is_err()
    );

    let mut manual_checkout = open_checkout(&first, writable_manual(), &cancellation).await?;
    (manual_checkout.create(
        AuthoredMutation::CreateFile {
            path: path("dirty")?,
            bytes: OwnedBytes::from_static(b"dirty"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    assert!(
        (manual_checkout.refresh_head(B, &cancellation))
            .await
            .is_err()
    );
    assert!(manual_checkout.has_pending_mutations());
    (manual_checkout.discard(B, &cancellation)).await?;
    assert!(!manual_checkout.has_pending_mutations());
    assert_eq!(
        (manual_checkout.refresh_head(B, &cancellation))
            .await?
            .value,
        first_head
    );
    Ok(())
}

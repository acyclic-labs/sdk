use super::*;

#[tokio::test]
async fn every_object_cut_preserves_sparse_rebase_retry_and_candidate_state()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let mut writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut rebasing = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (rebasing.create(crate::AuthoredMutation::CreateFile { path: path("local")?, bytes: (bytes::Bytes::from_static(b"local sparse candidate")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (writer.create(crate::AuthoredMutation::CreateFile { path: path("remote")?, bytes: (bytes::Bytes::from_static(b"remote sparse head")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (writer.commit(OperationId::from_bytes([227; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

    let original = checkout_state(&rebasing);
    let mut completed = None;
    for cut in 1..=512 {
        control.arm(cut);
        match (rebasing.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await
        {
            Ok(receipt) => {
                completed = Some(receipt);
                break;
            }
            Err(failure) => {
                assert!(failure.work.backend_read_operations > 0);
                assert_eq!(checkout_state(&rebasing), original);
            }
        }
    }
    control.disable();
    let completed = completed.ok_or("rebase never passed the complete object cut sweep")?;
    assert!(matches!(completed.value, RebaseDecision::Safe { .. }));
    assert!(rebasing.has_pending_mutations());
    assert!(
        (rebasing.lookup_no_follow(
            &path("local")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .record
        .is_some()
    );
    assert!(
        (rebasing.lookup_no_follow(
            &path("remote")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .record
        .is_some()
    );
    Ok(())
}

#[tokio::test]
async fn every_object_cut_preserves_diff_export_and_merge_preparation()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let base = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value
    .generation_id();
    let mut ours = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut theirs = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (ours.create(crate::AuthoredMutation::CreateFile { path: path("ours")?, bytes: (bytes::Bytes::from(vec![b'o'; 4_096])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (theirs.create(crate::AuthoredMutation::CreateFile { path: path("theirs")?, bytes: (bytes::Bytes::from(vec![b't'; 4_096])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let ours_generation = (ours.checkpoint(WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let theirs_generation = (theirs.checkpoint(WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    (theirs.commit(OperationId::from_bytes([228; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

    macro_rules! sweep_read_only {
        ($operation:expr) => {{
            let original = checkout_state(&ours);
            let mut completed = None;
            for cut in 1..=4_096 {
                control.arm(cut);
                match ($operation).await {
                    Ok(receipt) => {
                        completed = Some(receipt);
                        break;
                    }
                    Err(failure) => {
                        assert!(failure.work.backend_read_operations > 0);
                        assert_eq!(checkout_state(&ours), original);
                    }
                }
            }
            control.disable();
            completed.ok_or_else(|| {
                format!(
                    "operation never passed object cut sweep: {}",
                    stringify!($operation)
                )
            })?
        }};
    }

    let diff = sweep_read_only!(volume.diff_generations(
        base,
        ours_generation,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(diff.value.files.len() + diff.value.bindings.len(), 3);
    let manifest = sweep_read_only!(ours.export_manifest(WorkBudget::UNBOUNDED, &cancellation));
    assert_eq!(manifest.value.volume_id, volume.id());
    assert_eq!(
        manifest.value.generation_root,
        ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: ours_generation.digest(),
        }
    );
    let prepared = sweep_read_only!(ours.prepare_merge(
        theirs_generation,
        8,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(prepared.value, MergePreparation::Prepared { .. }));
    assert!(ours.has_pending_mutations());
    for expected in ["ours", "theirs"] {
        assert!(
            (ours.lookup_no_follow(
                &path(expected)?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )).await?
            .value
            .record
            .is_some()
        );
    }
    Ok(())
}

#[tokio::test]
async fn every_object_cut_preserves_authenticated_publication_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (checkout.create(crate::AuthoredMutation::CreateFile { path: path("publish-large")?, bytes: (bytes::Bytes::from(vec![b'p'; 16_384])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.change_named_attribute(path("publish-large")?, &AttributeName::new(
            crate::kernel::AttributeClass::PosixXattr,
            b"user.publication".to_vec(),
            config().limits.maximum_component_bytes,
        )?, Some((&bytes::Bytes::from(vec![b'a'; 4_096]), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation)).await?;

    let original = checkout_state(&checkout);
    let operation_id = OperationId::from_bytes([229; 16]);
    let mut completed = None;
    for cut in 1..=4_096 {
        control.arm(cut);
        match (checkout.commit(operation_id, None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await
        {
            Ok(receipt) => {
                completed = Some(receipt);
                break;
            }
            Err(failure) => {
                assert!(
                    failure.work.backend_read_operations + failure.work.backend_write_operations
                        > 0
                );
                assert_eq!(checkout_state(&checkout), original);
            }
        }
    }
    control.disable();
    let completed = completed.ok_or("publication never passed the complete object cut sweep")?;
    assert!(matches!(
        completed.value,
        CheckoutCommitOutcome::Committed { .. }
    ));
    assert!(!checkout.has_pending_mutations());
    Ok(())
}

#[tokio::test]
async fn indeterminate_post_append_retry_resolves_one_durable_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let authority = Arc::new(crate::memory::MemoryAuthorityStore::default());
    let fs = Fs::new(
        PostAppendAuthorityStore {
            inner: Arc::clone(&authority),
            control: Arc::clone(&control),
        },
        crate::memory::MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (checkout.create(crate::AuthoredMutation::CreateFile { path: path("indeterminate-file")?, bytes: (bytes::Bytes::from_static(b"durable exactly once")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let before = checkout_state(&checkout);
    let operation_id = OperationId::from_bytes([49; 16]);
    control.arm(1);
    let failure = (checkout.commit(operation_id, None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await
        .err()
        .ok_or("post-append acknowledgement loss unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        FsError::Publication(PublicationError::Authority(
            AuthorityStoreError::Indeterminate { .. }
        ))
    ));
    assert_eq!(checkout_state(&checkout), before);
    assert!(({ let __authority_args = (volume_authority_id(volume.id()), operation_id, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result =
        AsyncAuthorityStore::find_operation(&*authority, __authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?
        .value
        .is_some()
    );

    control.disable();
    let resolved = (checkout.commit(operation_id, None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    assert!(matches!(
        resolved.value,
        CheckoutCommitOutcome::AlreadyCommitted { .. }
    ));
    assert!(!checkout.has_pending_mutations());
    let history =({ let __authority_args = (volume_authority_id(volume.id()), Sequence::GENESIS, ReplayLimit {
            records: 8,
            payload_bytes: 64 * 1024,
        }, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result =  AsyncAuthorityStore::replay(&*authority, __authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?
    .value;
    assert_eq!(
        history
            .iter()
            .filter(|commit| commit.operation_id == operation_id)
            .count(),
        1
    );
    let mut reopened = (volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    let bytes = (reopened.value.read_file_range(
        &path("indeterminate-file")?,
        ByteRange {
            offset: 0,
            length: 20,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(bytes.value.bytes.as_ref(), b"durable exactly once");
    Ok(())
}

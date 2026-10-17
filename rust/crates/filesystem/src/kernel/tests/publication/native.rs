use super::*;

#[derive(Debug, thiserror::Error)]
enum NativePublicationTestError {
    #[error(transparent)]
    Fs(#[from] crate::FsError),
    #[error("the exact publication owner became stale")]
    OwnerStale,
}

async fn native_fixture(objects: &MemoryObjectStore, volume_id: VolumeId) -> TestResult<ObjectId> {
    use crate::kernel::{BlobChunkRef, BlobNode, BlobPage, encode_blob_page};
    let metadata = metadata_object(objects).await?;
    let content = b"original closure content";
    let chunk = put(objects, ObjectKind::BlobChunk, content.to_vec()).await?;
    let logical_bytes = content.len() as u64;
    let blob = put(
        objects,
        ObjectKind::Blob,
        encode_blob_page(
            &BlobPage {
                first_offset: 0,
                end_offset: logical_bytes,
                node: BlobNode::Leaf(vec![BlobChunkRef {
                    first_offset: 0,
                    end_offset: logical_bytes,
                    chunk,
                }]),
            },
            8,
            accept,
            allocation_failed,
        )?,
    )
    .await?;
    let extents = put(
        objects,
        ObjectKind::ExtentPage,
        encode_extent_page(
            &ExtentPage::Leaf(vec![Extent {
                offset: 0,
                length: logical_bytes,
                kind: ExtentKind::Content {
                    object: blob,
                    object_offset: 0,
                },
            }]),
            8,
            accept,
            allocation_failed,
        )?,
    )
    .await?;
    let child = FileId::from_bytes([2; 16]);
    let [_, _, generation] = generation(
        objects,
        volume_id,
        FileId::from_bytes([1; 16]),
        metadata,
        vec![entry(b"content", child)?],
        vec![regular(child, metadata, logical_bytes, extents)],
        vec![crate::GenerationId::new(Digest::from_bytes([7; 32]))],
    )
    .await?;
    Ok(generation)
}

async fn native_request(
    authority: &MemoryAuthorityStore,
    volume_id: VolumeId,
    generation_root: ObjectId,
) -> TestResult<PublishGenerationRequest> {
    let authority_id = volume_authority_id(volume_id);
    let expected = created(authority, authority_id).await?;
    Ok(PublishGenerationRequest {
        authority_id,
        volume_id,
        epoch: Epoch::GENESIS,
        expected,
        operation_id: OperationId::from_bytes([8; 16]),
        generation_root,
    })
}

#[tokio::test]
async fn native_publication_owner_stale_after_await_fails_before_actual_cas() -> TestResult {
    use std::sync::atomic::{AtomicU64, Ordering};
    let objects = MemoryObjectStore::default();
    let authority = MemoryAuthorityStore::default();
    let volume_id = VolumeId::from_bytes([32; 16]);
    let root = native_fixture(&objects, volume_id).await?;
    let request = native_request(&authority, volume_id, root).await?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let owner_epoch = std::sync::Arc::new(AtomicU64::new(1));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
    let channels = std::sync::Arc::new(parking_lot::Mutex::new((
        Some(started_tx),
        Some(continue_rx),
    )));
    let guarded_epoch = owner_epoch.clone();
    let owner_original = original.clone();
    let mut guard = move |same: &crate::NativeMaterializationWork| {
        assert!(owner_original.shares_ledger(same));
        let observed = guarded_epoch.load(Ordering::SeqCst);
        let (sender, receiver) = {
            let mut pair = channels.lock();
            (
                pair.0.take().expect("one attempted CAS"),
                pair.1.take().expect("one attempted CAS"),
            )
        };
        let epoch = guarded_epoch.clone();
        async move {
            sender.send(()).expect("owner scheduling channel");
            receiver.await.expect("owner continuation");
            if epoch.load(Ordering::SeqCst) == observed {
                Ok(())
            } else {
                Err(NativePublicationTestError::OwnerStale)
            }
        }
    };
    let (result, ()) = tokio::join!(
        publish_generation_async_guarded_with_native_work(
            &objects,
            &authority,
            request,
            PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
            limits(),
            None,
            &original,
            &cancellation,
            &mut guard,
            None,
            None,
        ),
        async {
            started_rx
                .await
                .expect("actual postplanning owner callback");
            owner_epoch.store(2, Ordering::SeqCst);
            continue_tx.send(()).expect("owner resume");
        },
    );
    assert!(matches!(
        result,
        Err(NativePublicationTestError::OwnerStale)
    ));
    assert_eq!(
        read_head(&authority, request.authority_id).await?,
        request.expected
    );
    assert_eq!(original.receipt().authority_records_appended, 0);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn native_publication_partial_closure_and_allocation_denials_release_owned_evidence()
-> TestResult {
    let objects = MemoryObjectStore::default();
    let authority = MemoryAuthorityStore::default();
    let volume_id = VolumeId::from_bytes([33; 16]);
    let root = native_fixture(&objects, volume_id).await?;
    let request = native_request(&authority, volume_id, root).await?;
    let cancellation = CancellationToken::new();
    for early_allocation in [true, false] {
        let mut budget = WorkBudget::UNBOUNDED;
        if early_allocation {
            budget.peak_allocation_bytes =
                crate::NativeMaterializationWork::accounting_state_bytes();
        }
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let owner_calls = calls.clone();
        let mut guard = move |_: &crate::NativeMaterializationWork| {
            owner_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::future::ready(Ok::<(), NativePublicationTestError>(()))
        };
        let mut admitted = limits();
        if !early_allocation {
            admitted.maximum_objects = 3;
        }
        let failure = publish_generation_async_guarded_with_native_work(
            &objects,
            &authority,
            request,
            PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
            admitted,
            None,
            &original,
            &cancellation,
            &mut guard,
            None,
            None,
        )
        .await
        .err()
        .ok_or("unadmitted native publication succeeded")?;
        if early_allocation {
            assert!(matches!(
                failure,
                NativePublicationTestError::Fs(crate::FsError::Work(_))
                    | NativePublicationTestError::Fs(crate::FsError::NativeKernel(
                        crate::NativeTreeMaterializationError::Work(_)
                    ))
            ));
            assert_eq!(original.receipt().backend_read_operations, 0);
        } else {
            assert!(matches!(
                failure,
                NativePublicationTestError::Fs(crate::FsError::Closure(
                    ClosureError::TooManyObjects { .. }
                ))
            ));
            assert!(original.receipt().backend_read_operations > 0);
        }
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(original.receipt().authority_records_appended, 0);
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert_eq!(
            read_head(&authority, request.authority_id).await?,
            request.expected
        );
    }
    Ok(())
}

#[tokio::test]
async fn native_publication_lost_ack_reopens_original_without_resealing_or_second_append()
-> TestResult {
    use crate::simulation::{Simulation, SimulationFault, SimulationOperation, SimulationOptions};
    let objects = MemoryObjectStore::default();
    let authority = MemoryAuthorityStore::default();
    let volume_id = VolumeId::from_bytes([34; 16]);
    let root = native_fixture(&objects, volume_id).await?;
    let changed_root = fixture(&objects, volume_id).await?;
    let request = native_request(&authority, volume_id, root).await?;
    let simulation = Simulation::wrap(authority, objects, SimulationOptions::default())?;
    let (authority, objects) = simulation.stores();
    simulation.schedule_next(
        SimulationOperation::AuthorityAppend,
        SimulationFault::AmbiguousAuthorityAppend,
    )?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let (data, custody) = original.copy_bytes(b"immutable original operation data")?;
    let evidence =
        acyclic_stream::OwnedBytes::from_vec_with_guard(data, custody, original.runtime_meter())
            .expect("fresh evidence has fully initialized unique backing");
    let intent = PublicationIntent {
        operation_context: Some(Digest::from_bytes([59; 32])),
        original_evidence: Some(evidence),
        fingerprint_permit: None,
        permit: PublicationPermit::Unrestricted,
    };
    let mut guard = |_: &crate::NativeMaterializationWork| {
        std::future::ready(Ok::<(), NativePublicationTestError>(()))
    };
    let failure = publish_generation_async_guarded_with_native_work(
        &objects,
        &authority,
        request,
        intent.clone(),
        limits(),
        None,
        &original,
        &cancellation,
        &mut guard,
        None,
        None,
    )
    .await
    .err()
    .ok_or("acknowledgement loss unexpectedly returned success")?;
    assert!(matches!(
        failure,
        NativePublicationTestError::Fs(crate::FsError::Authority(
            AuthorityStoreError::Indeterminate { .. }
        ))
    ));
    assert_eq!(original.receipt().authority_records_appended, 1);
    let reopened = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let resolved = crate::AsyncAuthorityStore::find_operation(
        &authority,
        request.authority_id,
        request.operation_id,
        &mut crate::AuthorityWork::native(&reopened, &cancellation),
    )
    .await?;
    let commit = resolved.ok_or("lost acknowledgement operation was not durable")?;
    let predecessor = Head {
        epoch: commit.epoch,
        sequence: crate::Sequence::new(
            commit
                .sequence
                .get()
                .checked_sub(1)
                .ok_or("invalid committed sequence")?,
        ),
        digest: commit.previous_digest,
    };
    assert_eq!(predecessor, request.expected);
    {
        let mut accounting =
            crate::kernel::allocation::KernelAccounting::native(&reopened, &cancellation);
        let decoded =
            decode_publication_evidence_with_accounting(&commit.payload, 4_096, &mut accounting)?;
        assert_eq!(
            decoded.original_evidence,
            Some(b"immutable original operation data".as_slice())
        );
        let (payload, fingerprint) = construct_publication_commit(
            request,
            Some(intent.operation_context.ok_or("missing original context")?),
            decoded.original_evidence,
            &mut accounting,
        )?;
        payload.retire(&mut accounting)?;
        assert_eq!(fingerprint, commit.fingerprint);
    }
    let later = publish_generation_async_guarded_with_native_work(
        &objects,
        &authority,
        PublishGenerationRequest {
            expected: Head {
                epoch: commit.epoch,
                sequence: commit.sequence,
                digest: commit.digest,
            },
            operation_id: OperationId::from_bytes([60; 16]),
            generation_root: changed_root,
            ..request
        },
        PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
        limits(),
        None,
        &original,
        &cancellation,
        &mut guard,
        None,
        None,
    )
    .await?;
    assert!(matches!(later.0.outcome, AppendOutcome::Committed(_)));
    let seals = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let owner_seals = seals.clone();
    let mut stale_owner = move |_: &crate::NativeMaterializationWork| {
        owner_seals.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        std::future::ready(Err::<(), _>(NativePublicationTestError::OwnerStale))
    };
    let changed = publish_generation_async_guarded_with_native_work(
        &objects,
        &authority,
        PublishGenerationRequest {
            generation_root: changed_root,
            expected: predecessor,
            ..request
        },
        intent.clone(),
        limits(),
        None,
        &reopened,
        &cancellation,
        &mut stale_owner,
        Some(commit.clone()),
        None,
    )
    .await?;
    assert!(matches!(
        changed.0.outcome,
        AppendOutcome::IdempotencyConflict { .. }
    ));
    let publication = publish_generation_async_guarded_with_native_work(
        &objects,
        &authority,
        PublishGenerationRequest {
            expected: predecessor,
            ..request
        },
        intent,
        limits(),
        None,
        &reopened,
        &cancellation,
        &mut stale_owner,
        Some(commit),
        None,
    )
    .await?;
    assert!(matches!(
        publication.0.outcome,
        AppendOutcome::AlreadyCommitted(_)
    ));
    if let AppendOutcome::AlreadyCommitted(recovered) = &publication.0.outcome {
        assert_eq!(recovered.operation_id, request.operation_id);
        assert_eq!(recovered.previous_digest, request.expected.digest);
        assert_eq!(
            decode_published_generation(&recovered.payload, 4_096, accept)?.generation_root,
            root
        );
    }
    assert_eq!(seals.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(reopened.receipt().authority_records_appended, 0);
    assert_eq!(reopened.receipt().backend_write_operations, 0);
    Ok(())
}

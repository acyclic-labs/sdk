use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use crate::resource_preparation::{CreatedReferenceBinding, CreatedResourceAccess};
use crate::store::history::HistoryIo;

#[tokio::test]
async fn original_native_creator_scope_carries_custody_through_real_content_io() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let creator = fixture.host.resource_creation.iter()
        .find(|provider| provider.provider() == &created.request.provider)
        .expect("the original configured filesystem creator");
    let proof = &created.request.child.resource_proofs[0];
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(creation_control_budget(), &cancellation)
        .expect("finite original native invocation");
    let baseline = work.remaining().expect("remaining native quota").peak_allocation_bytes;
    let (receipt, custody) = creator.verify_with_native_work(&created.request, proof, &work)?.await?;
    assert!(custody.bytes() > 0);
    assert!(work.remaining().expect("remaining native quota").peak_allocation_bytes < baseline);
    assert!(work.receipt().backend_read_operations > 0);
    assert_eq!(receipt.resource, proof.resource);
    assert_eq!(receipt.origin.intended_child, created.request.child.operation_id);
    let binding = &receipt.content[0].binding;
    let writer = binding.writer.as_ref().expect("actual created writer");
    writer.supports_native_stage(&work)?;
    let (file, file_custody) = writer.stage_once_with_native_work(
        OperationId::from_bytes([177;16]), "native-creator/result.txt", b"actual native created bytes",
        "text/plain", "result.txt", &work,
    )?.await?;
    assert_eq!(file.volume(), &created.private);
    let (bytes, read_custody) = binding.reader.read_with_native_work(&file, 1024, &work)?.await?;
    assert_eq!(bytes, b"actual native created bytes");
    drop(bytes);
    drop(read_custody);
    drop(file);
    drop(file_custody);
    drop(receipt);
    drop(custody);
    assert_eq!(work.remaining().expect("released native quota").peak_allocation_bytes, baseline);
    Ok(())
}

#[tokio::test]
async fn original_native_creator_denies_quota_cancel_and_forged_evidence() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let creator = fixture.host.resource_creation.iter()
        .find(|provider| provider.provider() == &created.request.provider)
        .expect("the original configured filesystem creator");
    let proof = &created.request.child.resource_proofs[0];
    let cancellation = CancellationToken::new();
    let mut denied = creation_control_budget();
    denied.backend_read_operations = 0;
    let work = NativeMaterializationWork::new(denied, &cancellation).expect("finite denied invocation");
    assert!(matches!(creator.verify_with_native_work(&created.request, proof, &work)?.await,
        Err(Error::NativeWork { .. })));
    assert_eq!(work.receipt().backend_read_operations, 0);
    assert_eq!(work.receipt().backend_write_operations, 0);
    let mut allocator_denied = creation_control_budget();
    // Its sole allocation constructs the genuine original ledger, leaving no
    // attempt for the verifier's future or any IO body.
    allocator_denied.allocation_operations = 1;
    let exhausted = NativeMaterializationWork::new(allocator_denied, &cancellation)
        .expect("admitted original ledger with exhausted allocator allowance");
    assert!(matches!(creator.verify_with_native_work(&created.request, proof, &exhausted),
        Err(Error::NativeWork { .. })));
    assert_eq!(exhausted.receipt().allocation_operations, 1);
    assert_eq!(exhausted.receipt().backend_read_operations, 0);
    assert_eq!(exhausted.receipt().backend_write_operations, 0);
    let cancelled = NativeMaterializationWork::new(creation_control_budget(), &cancellation)
        .expect("finite original invocation");
    cancellation.cancel();
    assert!(matches!(creator.verify_with_native_work(&created.request, proof, &cancelled),
        Err(Error::NativeWork { .. })));
    assert_eq!(cancelled.receipt().backend_read_operations, 0);
    let live = CancellationToken::new();
    for change in 0..3 {
        let mut forged = proof.clone();
        match change {
            0 => forged.preparation = TaskId::from_bytes([178;16]),
            1 => forged.evidence = workspace_ref(fixture.filesystem_provider.clone(), "forged-native-evidence")?.as_resource().clone(),
            _ => forged.resource = workspace_ref(fixture.filesystem_provider.clone(), "forged-native-resource")?.as_resource().clone(),
        }
        let work = NativeMaterializationWork::new(creation_control_budget(), &live)
            .expect("finite adversarial native invocation");
        assert!(matches!(creator.verify_with_native_work(&created.request, &forged, &work)?.await,
            Err(Error::Unauthorized(_))));
        assert_eq!(work.receipt().backend_write_operations, 0);
    }
    Ok(())
}

async fn authenticated_native_receipts(
    creator: &dyn ResourceCreationProvider,
    request: &ResourcePreparationRequest,
    work: &NativeMaterializationWork,
    observer: &mut dyn HistoryIo,
) -> Result<Vec<CreatedResourceReceipt>> {
    let mut receipts = Vec::new();
    let slots = request.child.resource_proofs.len() * std::mem::size_of::<CreatedResourceReceipt>();
    observer.admit_allocation(slots as u64)?;
    receipts.try_reserve_exact(request.child.resource_proofs.len())
        .map_err(|cause| observer.allocation_failure(cause))?;
    observer.retain_owned((receipts.capacity() * std::mem::size_of::<CreatedResourceReceipt>()) as u64)?;
    for proof in &request.child.resource_proofs {
        let (receipt, custody) = creator.verify_with_native_work(request, proof, work)?.await?;
        observer.absorb_native_custody(custody)?;
        receipts.push(receipt);
    }
    Ok(receipts)
}

#[tokio::test]
async fn created_seal_keeps_exact_sdk_custody_after_original_observer_and_scope_escape() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let creator = fixture.host.resource_creation.iter()
        .find(|provider| provider.provider() == &created.request.provider)
        .expect("configured real filesystem allocation verifier");
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(creation_control_budget(), &cancellation)
        .expect("original finite native ledger");
    let baseline = work.remaining().expect("original remaining quota").peak_allocation_bytes;
    let mut observer = crate::store::history::native_work_io(&work)?;
    let mut receipts = authenticated_native_receipts(creator.as_ref(), &created.request, &work, &mut observer).await?;
    let (access, held) = CreatedResourceAccess::consume_with_work(
        &created.request.child, &mut receipts, Some(created.request.child.operation_id), &mut Some(&mut observer),
    )?;
    assert_eq!(held, 0, "native access carries its own final SDK payload guard");
    assert!(receipts.is_empty());
    assert_eq!(receipts.capacity(), 0, "the original receipt container is physically retired");
    let logical = access.logical_owned_bytes()?;
    let mut expected = std::mem::size_of::<CreatedResourceAccess>() as u64 + access.grants.logical_owned_bytes()?
        + (access.content.capacity() * std::mem::size_of::<crate::resource_preparation::CreatedContentBinding>()) as u64
        + (access.references.capacity() * std::mem::size_of::<CreatedReferenceBinding>()) as u64
        + (access.proofs.capacity() * std::mem::size_of::<ResourceCreationProofRef>()) as u64;
    for binding in &access.content {
        expected += binding.volume.logical_owned_bytes()?;
        expected += binding.authority.as_ref().map_or(0, |authority| authority.id.capacity() as u64);
    }
    for binding in &access.references { expected += binding.file.logical_owned_bytes()?; }
    for proof in &access.proofs {
        expected += proof.provider.logical_owned_bytes()? + proof.resource.logical_owned_bytes()?
            + proof.evidence.logical_owned_bytes()?;
    }
    assert_eq!(logical, expected, "only known SDK layout and actual capacities are included");
    assert_eq!(access.current_content()?.expect("original child's scratch role").volume, created.private);
    let (scope, scope_held) = fixture.root_scope.with_created_access_with_work(
        &created.request.child, Some(access), &mut Some(&mut observer),
    )?;
    let scope_custody = observer.take_native_custody(scope_held)?;
    // Any opaque provider backing remains under its genuine original custody;
    // it is not duplicated into the known SDK payload inventory.
    let provider_custody = observer.into_custody();
    let scope = Arc::new(scope);
    let escaped = Arc::clone(&scope);
    let retained = work.remaining().expect("retained original custody").peak_allocation_bytes;
    drop(scope);
    assert_eq!(work.remaining().expect("escaped scope retains its seal").peak_allocation_bytes, retained);
    let binding = escaped.created_content(Some(&created.private)).expect("real escaped content binding");
    let (file, file_custody) = binding.writer.as_ref().expect("original created writer").stage_once_with_native_work(
        OperationId::from_bytes([179;16]), "native-seal/escaped.txt", b"same original native seal",
        "text/plain", "escaped.txt", &work,
    )?.await?;
    let (bytes, read_custody) = binding.reader.read_with_native_work(&file, 1024, &work)?.await?;
    assert_eq!(bytes, b"same original native seal");
    drop(bytes);
    drop(read_custody);
    drop(file);
    drop(file_custody);
    drop(escaped);
    assert!(work.remaining().expect("last seal payload retired").peak_allocation_bytes >= retained + logical);
    drop(scope_custody);
    drop(provider_custody);
    assert_eq!(work.remaining().expect("all genuine custody retired").peak_allocation_bytes, baseline);
    Ok(())
}

#[tokio::test]
async fn native_created_seal_preserves_typed_identity_and_first_cancellation_failures() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let creator = fixture.host.resource_creation.iter()
        .find(|provider| provider.provider() == &created.request.provider)
        .expect("configured real filesystem allocation verifier");
    for cancel in [false, true] {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(creation_control_budget(), &cancellation)
            .expect("original adversarial finite ledger");
        let baseline = work.remaining().expect("initial quota").peak_allocation_bytes;
        let mut observer = crate::store::history::native_work_io(&work)?;
        let mut receipts = authenticated_native_receipts(creator.as_ref(), &created.request, &work, &mut observer).await?;
        if cancel {
            cancellation.cancel();
        } else {
            receipts[0].origin.preparation_task = TaskId::from_bytes([180;16]);
        }
        let prefix = work.receipt();
        let rejected = CreatedResourceAccess::consume_with_work(
            &created.request.child, &mut receipts, Some(created.request.child.operation_id), &mut Some(&mut observer),
        );
        if cancel {
            assert!(matches!(rejected, Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
            })));
            assert_eq!(work.receipt().allocation_operations, prefix.allocation_operations);
            assert_eq!(work.receipt().bytes_copied, prefix.bytes_copied);
            assert_eq!(work.receipt().bytes_encoded, prefix.bytes_encoded);
        } else {
            assert!(matches!(rejected, Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized, ..
            })));
        }
        drop(receipts);
        drop(observer);
        if !cancel {
            assert_eq!(work.remaining().expect("failed seal custody retired").peak_allocation_bytes, baseline);
        }
    }
    Ok(())
}

#[tokio::test]
async fn same_created_seal_engine_rejects_exact_file_ambiguity_and_inherited_default_roles() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let creator = fixture.host.resource_creation.iter()
        .find(|provider| provider.provider() == &created.request.provider)
        .expect("configured real filesystem allocation verifier");
    let mut receipts = Vec::new();
    for proof in &created.request.child.resource_proofs {
        receipts.push(creator.verify(&created.request, proof).await?);
    }
    let inherited = CreatedResourceAccess::consume(&created.request.child, receipts, None)?;
    assert!(inherited.content.iter().all(|binding| !binding.default_writer && !binding.artifact_writer));
    assert!(inherited.current_content()?.is_none(), "inherited allocation is not the current scratch origin");
    let private_resource = workspace_ref(
        fixture.filesystem_provider.clone(), &created.private.storage_name()?,
    )?;
    let private_proof = created.request.child.resource_proofs.iter()
        .find(|proof| &proof.resource == private_resource.as_resource())
        .expect("private allocation proof");
    let receipt = creator.verify(&created.request, private_proof).await?;
    let binding = &receipt.content[0].binding;
    let writer = binding.writer.as_ref().expect("genuine original allocation writer");
    let file = writer.stage_once(
        OperationId::from_bytes([181;16]), "exact-seal/one.txt", b"one", "text/plain", "one.txt",
    ).await?;
    let other = writer.stage_once(
        OperationId::from_bytes([182;16]), "exact-seal/two.txt", b"two", "text/plain", "two.txt",
    ).await?;
    drop(receipt);
    for change in 0..4 {
        let mut receipts = Vec::new();
        for proof in &created.request.child.resource_proofs {
            let mut receipt = creator.verify(&created.request, proof).await?;
            if proof == private_proof {
                match change {
                    0 => {
                        receipt.operations.push(CreatedResourceOperation::ReadFile(file.clone()));
                        receipt.operations.push(CreatedResourceOperation::ReadFile(file.clone()));
                    }
                    1 => {
                        receipt.operations.push(CreatedResourceOperation::ReadFile(file.clone()));
                        receipt.references.push(CreatedReferenceBinding {
                            file: other.clone(), reader: Arc::clone(&receipt.content[0].binding.reader),
                        });
                    }
                    2 => {
                        receipt.content[0].artifact_writer = true;
                    }
                    _ => {
                        receipt.origin.intended_child = OperationId::from_bytes([183;16]);
                    }
                }
            }
            receipts.push(receipt);
        }
        assert!(matches!(CreatedResourceAccess::consume(
            &created.request.child, receipts, Some(created.request.child.operation_id),
        ), Err(Error::Unauthorized(_))), "the same ordinary engine rejects altered creator seal {change}");
    }
    Ok(())
}

#[tokio::test]
async fn genuine_created_proof_quota_denial_precedes_any_deep_clone_allocation_or_copy() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let cancellation = CancellationToken::new();
    let mut budget = creation_control_budget();
    budget.peak_allocation_bytes = NativeMaterializationWork::accounting_state_bytes();
    let work = NativeMaterializationWork::new(budget, &cancellation)
        .expect("only the real original ledger's known accounting body is admitted");
    let mut observer = crate::store::history::native_work_io(&work)?;
    let prefix = work.receipt();
    assert!(matches!(crate::core::resource_creation_proof_clone_reservation(
        &created.request.child.resource_proofs[0], &mut Some(&mut observer),
    ), Err(Error::NativeWork { .. })));
    assert_eq!(work.receipt().allocation_operations, prefix.allocation_operations);
    assert_eq!(work.receipt().bytes_copied, prefix.bytes_copied);
    assert_eq!(work.receipt().backend_read_operations, 0);
    assert_eq!(work.receipt().backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn retained_preparation_native_decode_binds_every_original_field_and_releases_after_drop() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    // Fixture input is borrowed caller-owned authority, not new native request memory.
    let input = serde_json::to_value(&created.request)
        .expect("the actual retained filesystem preparation request");
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(creation_control_budget(), &cancellation)
        .expect("finite original native request invocation");
    let baseline = work.remaining().expect("original allowance").peak_allocation_bytes;
    let mut observer = crate::store::history::native_work_io(&work)?;
    let (decoded, held) = ResourcePreparationRequest::from_borrowed_value_with_work(
        &input, &mut Some(&mut observer),
    )?;
    assert_eq!(decoded, created.request);
    assert!(held > 0);
    let custody = observer.take_native_custody(held)?;
    assert!(work.remaining().expect("decoded payload retained").peak_allocation_bytes < baseline);
    drop(decoded);
    drop(custody);
    drop(observer);
    assert_eq!(work.remaining().expect("decoded payload retired").peak_allocation_bytes, baseline);
    for remove in ["command", "source_checkpoint"] {
        let mut changed = input.clone();
        let object = changed.as_object_mut().expect("original request object");
        if object.remove(remove).is_none() {
            object.insert(remove.into(), Value::Null);
        }
        let mut observer = crate::store::history::native_work_io(&work)?;
        let error = ResourcePreparationRequest::from_borrowed_value_with_work(
            &changed, &mut Some(&mut observer),
        ).expect_err("serde defaults cannot forgive changed retained field presence");
        assert!(matches!(error, Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. }));
        drop(error);
        drop(observer);
        assert_eq!(work.remaining().expect("rejected payload retired").peak_allocation_bytes, baseline);
    }
    assert_eq!(work.receipt().backend_read_operations, 0);
    assert_eq!(work.receipt().backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn retained_preparation_decode_cancel_and_allocator_denial_precede_owned_decode() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let input = serde_json::to_value(&created.request).expect("actual original retained request");
    let cancellation = CancellationToken::new();
    let mut budget = creation_control_budget();
    budget.allocation_operations = 1;
    let denied = NativeMaterializationWork::new(budget, &cancellation)
        .expect("original ledger consumes the sole allowed allocation");
    let mut observer = crate::store::history::native_work_io(&denied)?;
    assert!(matches!(ResourcePreparationRequest::from_borrowed_value_with_work(
        &input, &mut Some(&mut observer),
    ), Err(Error::NativeWork { .. })));
    assert_eq!(denied.receipt().allocation_operations, 1);
    assert_eq!(denied.receipt().backend_read_operations, 0);
    assert_eq!(denied.receipt().backend_write_operations, 0);
    drop(observer);
    let cancelled = NativeMaterializationWork::new(creation_control_budget(), &cancellation)
        .expect("original cancellable invocation");
    let mut observer = crate::store::history::native_work_io(&cancelled)?;
    let before = cancelled.receipt().allocation_operations;
    cancellation.cancel();
    assert!(matches!(ResourcePreparationRequest::from_borrowed_value_with_work(
        &input, &mut Some(&mut observer),
    ), Err(Error::NativeWork { .. })));
    assert_eq!(cancelled.receipt().allocation_operations, before);
    assert_eq!(cancelled.receipt().backend_read_operations, 0);
    assert_eq!(cancelled.receipt().backend_write_operations, 0);
    Ok(())
}

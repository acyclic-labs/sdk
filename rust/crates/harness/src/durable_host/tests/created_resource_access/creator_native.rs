use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};

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

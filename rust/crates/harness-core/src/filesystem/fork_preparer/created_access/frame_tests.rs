use super::*;
use crate::conversation::BoundedContentFuture;
use acyclic_fs::{Fs, NativeMaterializationWork};
use std::sync::atomic::{AtomicBool, Ordering};

#[test]
fn native_verification_has_no_unfunded_inner_frame() {
    for source in [
        include_str!("provider.rs"),
        include_str!("authentication.rs"),
        include_str!("../preparation.rs"),
        include_str!("../journal.rs"),
        include_str!("../attempt.rs"),
        include_str!("../../../durable_host/created.rs"),
        include_str!("../../../executor/dispatch.rs"),
        include_str!("../../../executor/turn.rs"),
        include_str!("../../task_runtime/run.rs"),
        include_str!("../../task_commands/model.rs"),
        include_str!("../../task_commands/dispatch.rs"),
        include_str!("../../../../../harness/src/local/children/lazy.rs"),
        include_str!("../../../../../harness/src/local/workspace.rs"),
        include_str!("../../../../../harness/src/local/checkout_tools/capture/mounted.rs"),
        include_str!("../../../tools/git/materialize.rs"),
    ] {
        assert!(
            !source.contains("acyclic_fs::in_heap("),
            "unfunded verification frame"
        );
    }
}

#[test]
fn receipt_and_authentication_frames_keep_original_custody_until_drop() -> Result<()> {
    let provider = ProviderRef::new(
        "frame-control".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = Arc::new(FilesystemHost::new(Fs::memory(), &provider, None)?);
    let issuer = AuthorityIssuer::new(
        "frame-control",
        [19; 32],
        Authority {
            kind: crate::core::AggregateKind::Task,
            id: "frame-control".into(),
        },
    );
    let creator = FilesystemResourceCreationProvider::new(Arc::clone(&host), &issuer, None)?;
    let child = crate::runtime::TaskAdmissionRecord::from_canonical_value(
        serde_json::from_str(include_str!(
            "../../../../../harness/fixtures/v2/task-admission.json"
        ))
        .map_err(|error| Error::Invalid(error.to_string()))?,
        &mut None,
    )?
    .0;
    let resource = crate::resources::ResourceRef::new(
        crate::resources::ResourceKind::Workspace,
        provider.clone(),
        vec![1],
        None,
        &mut None,
    )?;
    let proof = ResourceCreationProofRef {
        provider: provider.clone(),
        preparation: crate::TaskId::new(),
        resource: resource.clone(),
        evidence: resource,
    };
    let request = ResourcePreparationRequest {
        provider,
        child,
        creation_grant: "fork:publish".into(),
        maximum_resources: 1,
        maximum_work: 100,
        maximum_bytes: 4096,
        payload: serde_json::Value::Null,
        command: None,
        source_checkpoint: None,
        input_files: Vec::new(),
        read_authorization: None,
    };
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(100_000, 16_000_000),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let baseline = work.remaining()?.peak_allocation_bytes;
    let receipt = creator.receipt(&request, &proof, &io)?;
    assert!(work.remaining()?.peak_allocation_bytes < baseline);
    drop(receipt);
    assert_eq!(work.remaining()?.peak_allocation_bytes, baseline);
    let authentication = authenticate(&host, issuer.audience(), &request, &proof, Some(&io))?;
    assert!(work.remaining()?.peak_allocation_bytes < baseline);
    drop(authentication);
    assert_eq!(work.remaining()?.peak_allocation_bytes, baseline);

    // Leave no allocation attempt available for either real inner constructor.
    let remaining = work.remaining()?.allocation_operations;
    work.charge(WorkCounters {
        allocation_operations: remaining,
        ..Default::default()
    })?;
    let built = AtomicBool::new(false);
    assert!(
        BoundedContentFuture::new(Some(&io), || {
            built.store(true, Ordering::SeqCst);
            creator.receipt_body(&request, &proof, &io)
        })
        .is_err()
    );
    assert!(!built.load(Ordering::SeqCst));
    assert!(creator.receipt(&request, &proof, &io).is_err());
    assert!(authenticate(&host, issuer.audience(), &request, &proof, Some(&io)).is_err());
    cancellation.cancel();
    assert!(
        BoundedContentFuture::new(Some(&io), || {
            built.store(true, Ordering::SeqCst);
            creator.receipt_body(&request, &proof, &io)
        })
        .is_err()
    );
    assert!(!built.load(Ordering::SeqCst));
    assert!(creator.receipt(&request, &proof, &io).is_err());
    assert!(authenticate(&host, issuer.audience(), &request, &proof, Some(&io)).is_err());
    Ok(())
}

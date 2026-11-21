use acyclic_fs::kernel::{
    GenerationExportManifestError, GenerationTransferError, TransferCursor,
    encode_generation_export_manifest, export_generation_batch_async,
    validate_generation_export_manifest,
};
use acyclic_fs::{CancellationToken, MemoryObjectStore};
use acyclic_fs_format::{
    identity::Digest,
    object_reference::{GenerationId, ObjectKind},
};
use acyclic_fs_work::performance::{WorkBudget, WorkCounters};

use crate::export_manifest as fixture;
use fixture::manifest;

#[tokio::test]
async fn manifest_rejects_mismatch_and_missing_root() -> Result<(), Box<dyn std::error::Error>> {
    let mut value = manifest();
    value.generation_root.kind = ObjectKind::Blob;
    assert_eq!(
        validate_generation_export_manifest(&value, u64::MAX),
        Err(GenerationExportManifestError::WrongRootKind)
    );

    let value = manifest();
    assert_eq!(
        validate_generation_export_manifest(&value, 1),
        Err(GenerationExportManifestError::TooManyObjects)
    );

    let mut value = manifest();
    value.generation_id = GenerationId::new(Digest::ZERO);
    assert!(matches!(
        encode_generation_export_manifest(&value),
        Err(GenerationExportManifestError::GenerationMismatch)
    ));
    value = manifest();
    value.objects.pop().ok_or("missing fixture root")?;
    assert!(matches!(
        encode_generation_export_manifest(&value),
        Err(GenerationExportManifestError::MissingRoot)
    ));
    value = manifest();
    value.objects.reverse();
    let Err(failure) = export_generation_batch_async(
        &MemoryObjectStore::default(),
        &value,
        TransferCursor::START,
        1,
        u64::MAX,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    else {
        return Err("transfer accepted a nonterminal root".into());
    };
    assert!(matches!(
        failure.error,
        GenerationTransferError::Manifest(GenerationExportManifestError::MissingRoot)
    ));
    assert_eq!(*failure.work()?, WorkCounters::UNCHARGED);
    Ok(())
}

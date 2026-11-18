use super::*;
use crate::Digest;

#[path = "../../../tests/support/export_manifest.rs"]
mod fixture;
use fixture::manifest;

#[test]
fn manifest_rejects_mismatch_and_missing_root() {
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
    value.objects.pop();
    assert!(matches!(
        encode_generation_export_manifest(&value),
        Err(GenerationExportManifestError::MissingRoot)
    ));
    value = manifest();
    value.objects.reverse();
    assert!(matches!(
        super::validate_transfer_manifest(&value, u64::MAX),
        Err(GenerationExportManifestError::MissingRoot)
    ));
}

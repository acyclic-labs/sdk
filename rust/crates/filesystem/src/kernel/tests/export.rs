use super::*;
use crate::model::Lifecycle;

fn manifest() -> GenerationExportManifest {
    let root = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: Digest::from_bytes([9; 32]),
    };
    GenerationExportManifest {
        volume_id: VolumeId::from_bytes([3; 16]),
        config: VolumeConfig::portable(Lifecycle::Ephemeral),
        generation_root: root,
        generation_id: GenerationId::new(root.digest),
        objects: vec![
            ObjectId {
                kind: ObjectKind::BlobChunk,
                digest: Digest::from_bytes([1; 32]),
            },
            root,
        ],
        file_count: 1,
    }
}

#[test]
fn manifest_round_trip_is_canonical_and_bounded() -> Result<(), Box<dyn std::error::Error>> {
    let value = manifest();
    let encoded = encode_generation_export_manifest(&value)?;
    assert_eq!(
        decode_generation_export_manifest(&encoded, encoded.len() as u64, 2)?,
        value
    );
    assert!(matches!(
        decode_generation_export_manifest(&encoded, encoded.len() as u64, 1),
        Err(GenerationExportManifestError::TooManyObjects)
    ));
    Ok(())
}

#[test]
fn manifest_rejects_mismatch_missing_root_duplicates_and_disorder() {
    let mut value = manifest();
    value.generation_root.kind = ObjectKind::Blob;
    assert_eq!(
        validate_generation_export_manifest(&value, u64::MAX),
        Err(GenerationExportManifestError::WrongRootKind)
    );

    let mut value = manifest();
    value.file_count = value
        .config
        .limits
        .maximum_files_per_generation
        .saturating_add(1);
    assert_eq!(
        validate_generation_export_manifest(&value, u64::MAX),
        Err(GenerationExportManifestError::TooManyFiles)
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
    value.objects.push(value.generation_root);
    assert!(matches!(
        encode_generation_export_manifest(&value),
        Err(GenerationExportManifestError::NonCanonicalObjectOrder)
    ));
    value = manifest();
    value.objects.reverse();
    assert!(matches!(
        encode_generation_export_manifest(&value),
        Err(GenerationExportManifestError::NonCanonicalObjectOrder)
    ));
}

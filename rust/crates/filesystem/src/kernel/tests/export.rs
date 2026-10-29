use super::*;
use crate::Digest;
use crate::model::Lifecycle;

fn manifest() -> GenerationExportManifest {
    let root = ObjectId {
        storage: None,
        kind: ObjectKind::GenerationRoot,
        digest: Digest::from_bytes([9; 32]),
    };
    GenerationExportManifest {
        volume_id: VolumeId::from_bytes([3; 16]),
        config: VolumeConfig::portable(Lifecycle::Ephemeral),
        generation_root: root,
        generation_id: GenerationId::from_root(root),
        objects: vec![
            ObjectId {
                storage: None,
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
    let mut value = manifest();
    let plain = value.objects[0];
    for origin in [2, 3] {
        value.objects.push(ObjectId {
            storage: Some(crate::ObjectStorage {
                locator: Digest::from_bytes([origin; 32]),
                root_file_id: crate::FileId::from_bytes([4; 16]),
                origin_generation: Digest::from_bytes([origin; 32]),
            }),
            ..plain
        });
    }
    let plain_root = value.generation_root;
    value.generation_root.storage = value.objects.last().unwrap().storage;
    value.generation_id = GenerationId::from_root(value.generation_root);
    *value
        .objects
        .iter_mut()
        .find(|id| **id == plain_root)
        .unwrap() = value.generation_root;
    let root = value.objects.remove(1);
    value.objects.push(root);
    let encoded = encode_generation_export_manifest(&value)?;
    assert_eq!(
        decode_generation_export_manifest(
            &encoded,
            encoded.len() as u64,
            value.objects.len() as u64
        )?,
        value
    );
    assert!(matches!(
        decode_generation_export_manifest(&encoded, encoded.len() as u64, 1),
        Err(GenerationExportManifestError::TooManyObjects)
    ));
    // Equal plaintext with a different locator is a different root reference.
    value.generation_id = GenerationId::from_root(plain_root);
    assert!(matches!(
        encode_generation_export_manifest(&value),
        Err(GenerationExportManifestError::GenerationMismatch)
    ));
    Ok(())
}

#[test]
fn manifest_rejects_mismatch_and_missing_root() {
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
    value.objects.reverse();
    assert!(matches!(
        super::validate_transfer_manifest(&value, u64::MAX),
        Err(GenerationExportManifestError::MissingRoot)
    ));
}

use acyclic_fs::kernel::{
    CanonicalDecodeError, GenerationExportManifestError, decode_generation_export_manifest,
    encode_generation_export_manifest,
};
use acyclic_fs_format::{
    identity::{Digest, FileId},
    object_reference::{GenerationId, ObjectId, ObjectStorage},
};

#[path = "../../../support/export_manifest.rs"]
mod fixture;
use fixture::manifest;

const VERSION_OFFSET: usize = b"acyclic-fs-generation-export-v1\0".len();

#[test]
fn manifest_round_trip_is_canonical_and_bounded() -> Result<(), Box<dyn std::error::Error>> {
    let mut value = manifest();
    let plain = *value.objects.first().ok_or("missing fixture object")?;
    for origin in [2, 3] {
        value.objects.push(ObjectId {
            storage: Some(ObjectStorage {
                locator: Digest::from_bytes([origin; 32]),
                root_file_id: FileId::from_bytes([4; 16]),
                origin_generation: Digest::from_bytes([origin; 32]),
            }),
            ..plain
        });
    }
    let plain_root = value.generation_root;
    value.generation_root.storage = value.objects.last().ok_or("missing sealed object")?.storage;
    value.generation_id = GenerationId::from_root(value.generation_root);
    *value
        .objects
        .iter_mut()
        .find(|id| **id == plain_root)
        .ok_or("missing plain root")? = value.generation_root;
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
    assert_eq!(
        decode_generation_export_manifest(&encoded, None, None)?,
        value
    );
    assert!(matches!(
        decode_generation_export_manifest(&encoded, encoded.len() as u64, 1),
        Err(GenerationExportManifestError::TooManyObjects)
    ));
    // Old/unknown headers refuse before the removed configuration fields.
    for version in [0_u16, 1, u16::MAX] {
        let mut obsolete = encoded
            .get(..VERSION_OFFSET + 2)
            .ok_or("missing export version")?
            .to_vec();
        obsolete
            .get_mut(VERSION_OFFSET..)
            .ok_or("missing export version")?
            .copy_from_slice(&version.to_le_bytes());
        assert!(matches!(
            decode_generation_export_manifest(&obsolete, None, None),
            Err(GenerationExportManifestError::Decode(CanonicalDecodeError::UnsupportedVersion {
                found, expected: 2
            })) if found == version
        ));
    }
    // Equal plaintext with a different locator is a different root reference.
    value.generation_id = GenerationId::from_root(plain_root);
    assert!(matches!(
        encode_generation_export_manifest(&value),
        Err(GenerationExportManifestError::GenerationMismatch)
    ));
    Ok(())
}

#[test]
fn manifest_count_requires_present_identity_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let value = manifest();
    let mut encoded = encode_generation_export_manifest(&value)?;
    let count_offset = encoded.len() - value.objects.len() * 33 - 4;
    encoded
        .get_mut(count_offset..count_offset + 4)
        .ok_or("missing export object count")?
        .copy_from_slice(&1_000_000_u32.to_le_bytes());
    assert_eq!(
        decode_generation_export_manifest(&encoded, None, None),
        Err(GenerationExportManifestError::Decode(
            CanonicalDecodeError::Truncated
        )),
    );
    Ok(())
}

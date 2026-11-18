use acyclic_fs::CancellationToken;
use acyclic_fs::MemoryObjectStore;
use acyclic_fs::kernel::{
    CanonicalDecodeError, GenerationExportManifestError, GenerationTransferError, TransferCursor,
    decode_generation_export_manifest, encode_generation_export_manifest,
    export_generation_batch_async, validate_generation_export_manifest,
};
use acyclic_fs_format::{
    identity::{Digest, FileId},
    object_reference::{GenerationId, ObjectId, ObjectKind, ObjectStorage},
};

use acyclic_fs_work::performance::{WorkBudget, WorkCounters};

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

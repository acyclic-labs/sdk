use super::*;
use crate::access::POLICY_METADATA_KEY;
use crate::kernel::{MetadataField, OwnedSourceChunk};
use crate::{OperationFailure, OperationReceipt, PublicationPermit, WorkCounters};
use proptest::prelude::*;

fn policy_source(bytes: &[u8]) -> Result<MemorySource, Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Posix)?;
    source.runtime.block_on(
        || async {
            let mut checkout = source.checkout.lock().await;
            checkout
                .write_access_policy(
                    bytes,
                    NamedAttributeWriteMode::Create,
                    WorkBudget::UNBOUNDED,
                    source.cancellation(),
                )
                .await
                .map_err(|failure| failure.error)?;
            checkout
                .commit(
                    OperationId::new(),
                    None,
                    None,
                    None,
                    PublicationPermit::Unrestricted,
                    WorkBudget::UNBOUNDED,
                    source.cancellation(),
                    |_| async {
                        Ok::<_, OperationFailure<FsError>>(OperationReceipt {
                            value: (),
                            work: WorkCounters::UNCHARGED,
                        })
                    },
                )
                .await
                .map_err(|failure| failure.error)?;
            Ok::<_, FsError>(())
        },
        None,
    )??;
    access::bind_test_access(source, vec![], None)
}

fn native_policy_metadata(bytes: &[u8], timestamp: i64) -> Result<(), Box<dyn std::error::Error>> {
    let source = policy_source(bytes)?;
    let path = MountPath::root();
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let original = source.lookup(&path)?.ok_or("root is missing")?.metadata;
    assert!(matches!(original.named_attributes, MetadataField::Value(_)));
    let changed = FileMetadata {
        named_attributes: MetadataField::Unavailable,
        ..original
    };
    assert!(matches!(
        source.set_attributes(&path, changed, None, &work),
        Err(MountSourceError::AccessDenied)
    ));
    for mode in [
        MountAttributeWriteMode::Create,
        MountAttributeWriteMode::Replace,
        MountAttributeWriteMode::Upsert,
    ] {
        assert!(matches!(
            source.write_attribute(
                &path,
                POLICY_METADATA_KEY,
                OwnedSourceChunk::intrinsic(OwnedBytes::from_static(b"forged"), 0),
                mode,
                &work,
            ),
            Err(MountSourceError::AccessDenied)
        ));
    }
    assert!(matches!(
        source.remove_attribute(&path, POLICY_METADATA_KEY, &work),
        Err(MountSourceError::AccessDenied)
    ));
    assert_eq!(
        source.lookup(&path)?.ok_or("root is missing")?.metadata,
        original
    );
    // Ordinary scalar edits still work and preserve the exact policy payload.
    source.set_attributes(
        &path,
        FileMetadata {
            modified_ns: MetadataField::Value(timestamp),
            ..original
        },
        None,
        &work,
    )?;
    assert_eq!(
        source
            .read_attribute(&path, POLICY_METADATA_KEY, &work)?
            .ok_or("policy is missing")?
            .bytes
            .as_ref(),
        bytes
    );
    Ok(())
}

#[test]
fn native_access_policy_metadata_rejects_xattr_and_setattr_bypasses()
-> Result<(), Box<dyn std::error::Error>> {
    native_policy_metadata(b"authenticated policy", 42)
}

#[test]
fn native_access_handle_metadata_cannot_replace_attribute_roots()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Posix)?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let path = native_test_path("file");
    source.create_file(&path, metadata(), None, &work)?;
    source.write_attribute(
        &path,
        b"user.retained",
        OwnedSourceChunk::intrinsic(OwnedBytes::from_static(b"original"), 0),
        MountAttributeWriteMode::Create,
        &work,
    )?;
    for handle in [source.open_file(&path)?, source.detach_file(&path)?] {
        assert!(matches!(
            handle.set_attributes(metadata(), None, &work),
            Err(MountSourceError::AccessDenied)
        ));
        for result in [
            handle.write_attribute(
                POLICY_METADATA_KEY,
                OwnedSourceChunk::intrinsic(OwnedBytes::from_static(b"forged"), 0),
                MountAttributeWriteMode::Upsert,
                &work,
            ),
            handle.remove_attribute(POLICY_METADATA_KEY, &work),
        ] {
            assert!(matches!(result, Err(MountSourceError::AccessDenied)));
        }
        assert_eq!(
            handle
                .read_attribute(b"user.retained", &work)?
                .ok_or("attribute was erased")?
                .bytes
                .as_ref(),
            b"original"
        );
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn native_access_policy_metadata_is_preserved(
        bytes in prop::collection::vec(any::<u8>(), 0..96),
        timestamp in any::<i64>(),
    ) {
        native_policy_metadata(&bytes, timestamp)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

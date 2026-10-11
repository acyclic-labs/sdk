use crate::WorkError;
use super::*;
use crate::foundation::Digest;

fn metadata() -> FileMetadata {
    FileMetadata {
        posix_mode: MetadataField::Value(0o100_644),
        posix_uid: MetadataField::Value(1000),
        posix_gid: MetadataField::Value(1000),
        posix_flags: MetadataField::Unavailable,
        windows_attributes: MetadataField::Unavailable,
        created_ns: MetadataField::Unavailable,
        modified_ns: MetadataField::Value(-1),
        accessed_ns: MetadataField::Value(0),
        changed_ns: MetadataField::Unavailable,
        named_attributes: MetadataField::Unavailable,
        acl: MetadataField::Value(ObjectId {
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([3; 32]),
        }),
        security_descriptor: MetadataField::Unavailable,
    }
}

#[test]
fn unavailable_zero_and_negative_remain_distinct() -> Result<(), Box<dyn std::error::Error>> {
    let value = metadata();
    let encoded = encode_file_metadata(value)?;
    assert_eq!(
        decode_file_metadata(&encoded, DecodeLimits::default())?,
        value
    );
    assert_eq!(
        hex::encode(encoded),
        "41435946534d4554010001a481000001e803000001e803000000000001ffffffffffffffff010000000000000000000001030303030303030303030303030303030303030303030303030303030303030300"
    );
    assert_measured_metadata_encoder(value)?;
    Ok(())
}

#[test]
fn metadata_read_steps_record_only_actual_primitive_copies()
-> Result<(), Box<dyn std::error::Error>> {
    use super::super::codec::ReadAllocationStep;
    let value = metadata();
    let bytes = encode_file_metadata(value)?;
    let mut copied = 0;
    let mut items = 0;
    let decoded = decode_file_metadata_with_steps::<CanonicalDecodeError>(
        &bytes, DecodeLimits::default(), &mut |step| {
            match step {
                ReadAllocationStep::Copied(bytes) => copied += bytes,
                ReadAllocationStep::Item => items += 1,
                _ => {}
            }
            Ok(())
        },
    )?;
    assert_eq!(decoded, value);
    assert_eq!(copied, bytes.len() as u64 - DOMAIN.len() as u64);
    assert_eq!(items, 1);
    Ok(())
}

#[test]
fn malformed_metadata_presence_keeps_the_actual_copy_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    use super::super::codec::ReadAllocationStep;
    let mut bytes = encode_file_metadata(metadata())?;
    bytes[DOMAIN.len() + 2] = 2;
    let mut copied = 0;
    let mut items = 0;
    let decoded = decode_file_metadata_with_steps::<CanonicalDecodeError>(
        &bytes, DecodeLimits::default(), &mut |step| {
            match step {
                ReadAllocationStep::Copied(bytes) => copied += bytes,
                ReadAllocationStep::Item => items += 1,
                _ => {}
            }
            Ok(())
        },
    );
    assert!(matches!(decoded, Err(CanonicalDecodeError::UnknownTag {
        field: "posix_mode", tag: 2,
    })));
    assert_eq!(copied, 3);
    assert_eq!(items, 0);
    Ok(())
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn original_metadata_budget_keeps_only_completed_primitive_copies()
-> Result<(), Box<dyn std::error::Error>> {
    let bytes = encode_file_metadata(metadata())?;
    let cancellation = crate::CancellationToken::new();
    let budget = WorkBudget { bytes_copied: 3, ..WorkBudget::UNBOUNDED };
    let original = crate::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = super::super::allocation::KernelAccounting::native(&original, &cancellation);
    let error = decode_file_metadata_with_accounting(&bytes, DecodeLimits::default(), &mut accounting)
        .err().ok_or("unfunded metadata scalar copy was accepted")?;
    assert!(matches!(error, crate::FsError::NativeKernel(
        crate::NativeTreeMaterializationError::Work(WorkError::BudgetExceeded { counter: "bytes_copied", .. })
    )));
    assert_eq!(original.receipt().bytes_copied, 3);
    assert_eq!(original.receipt().items_examined, 0);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[test]
fn wrong_reference_kind_fails_before_identity() {
    let mut value = metadata();
    value.named_attributes = MetadataField::Value(ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::ZERO,
    });
    assert!(file_metadata_id(value).is_err());
}

#[test]
fn every_metadata_field_round_trips_and_identity_is_exact() -> Result<(), Box<dyn std::error::Error>>
{
    let blob = |byte| ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([byte; 32]),
    };
    let value = FileMetadata {
        posix_mode: MetadataField::Value(u32::MAX),
        posix_uid: MetadataField::Value(0),
        posix_gid: MetadataField::Value(1),
        posix_flags: MetadataField::Value(u64::MAX),
        windows_attributes: MetadataField::Value(2),
        created_ns: MetadataField::Value(i64::MIN),
        modified_ns: MetadataField::Value(-1),
        accessed_ns: MetadataField::Value(0),
        changed_ns: MetadataField::Value(i64::MAX),
        named_attributes: MetadataField::Value(ObjectId {
            kind: ObjectKind::AttributePage,
            digest: Digest::from_bytes([4; 32]),
        }),
        acl: MetadataField::Value(blob(5)),
        security_descriptor: MetadataField::Value(blob(6)),
    };
    let encoded = encode_file_metadata(value)?;
    assert_eq!(
        decode_file_metadata(&encoded, DecodeLimits::default())?,
        value
    );
    assert_eq!(
        file_metadata_id(value)?,
        ObjectId {
            kind: ObjectKind::Metadata,
            digest: object_digest(ObjectKind::Metadata, &encoded),
        }
    );
    assert_measured_metadata_encoder(value)?;
    Ok(())
}

#[test]
fn every_metadata_presence_tag_fails_closed_when_noncanonical()
-> Result<(), Box<dyn std::error::Error>> {
    let encoded = encode_file_metadata(FileMetadata {
        posix_mode: MetadataField::Value(1),
        posix_uid: MetadataField::Value(1),
        posix_gid: MetadataField::Value(1),
        posix_flags: MetadataField::Value(1),
        windows_attributes: MetadataField::Value(1),
        created_ns: MetadataField::Value(1),
        modified_ns: MetadataField::Value(1),
        accessed_ns: MetadataField::Value(1),
        changed_ns: MetadataField::Value(1),
        named_attributes: MetadataField::Value(ObjectId {
            kind: ObjectKind::AttributePage,
            digest: Digest::ZERO,
        }),
        acl: MetadataField::Value(ObjectId {
            kind: ObjectKind::Blob,
            digest: Digest::ZERO,
        }),
        security_descriptor: MetadataField::Value(ObjectId {
            kind: ObjectKind::Blob,
            digest: Digest::ZERO,
        }),
    })?;
    for (offset, field) in [
        (10, "posix_mode"),
        (15, "posix_uid"),
        (20, "posix_gid"),
        (25, "posix_flags"),
        (34, "windows_attributes"),
        (39, "created_ns"),
        (48, "modified_ns"),
        (57, "accessed_ns"),
        (66, "changed_ns"),
        (75, "named_attributes"),
        (108, "acl"),
        (141, "security_descriptor"),
    ] {
        let mut malformed = encoded.clone();
        malformed[offset] = 2;
        assert!(matches!(
            decode_file_metadata(&malformed, DecodeLimits::default()),
            Err(CanonicalDecodeError::UnknownTag { field: actual, tag: 2 }) if actual == field
        ));
    }
    Ok(())
}

fn assert_measured_metadata_encoder(value: FileMetadata) -> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let receipt = encode_file_metadata_measured(value, WorkBudget::UNBOUNDED, &cancellation)?;
    assert_eq!(receipt.value, encode_file_metadata(value)?);
    assert_eq!(receipt.work.bytes_encoded, receipt.value.len() as u64);
    assert_eq!(receipt.work.allocation_operations, 1);
    assert_eq!(receipt.work.peak_allocation_bytes, receipt.value.capacity() as u64);
    assert_eq!(receipt.work.items_examined, 13);
    let copied = [value.named_attributes, value.acl, value.security_descriptor]
        .into_iter().filter(|field| matches!(field, MetadataField::Value(_))).count() as u64 * 32;
    assert_eq!(receipt.work.bytes_copied, copied);
    assert_eq!(encode_file_metadata_measured(value, receipt.work, &cancellation)?.value, receipt.value);
    for budget in [
        WorkBudget { bytes_encoded: receipt.work.bytes_encoded - 1, ..WorkBudget::UNBOUNDED },
        WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { peak_allocation_bytes: receipt.value.len() as u64 - 1, ..WorkBudget::UNBOUNDED },
        WorkBudget { items_examined: 12, ..WorkBudget::UNBOUNDED },
    ] {
        let failure = encode_file_metadata_measured(value, budget, &cancellation)
            .expect_err("known metadata work must be denied before allocation");
        assert!(matches!(failure.error, FileMetadataEncodeError::Work(_)));
        assert_eq!(failure.work.items_examined, 1);
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.bytes_encoded, 0);
    }
    if copied != 0 {
        let failure = encode_file_metadata_measured(
            value, WorkBudget { bytes_copied: copied - 1, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).expect_err("digest copy bound must reject before allocation");
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.bytes_copied, 0);
    }
    cancellation.cancel();
    let failure = encode_file_metadata_measured(value, WorkBudget::UNBOUNDED, &cancellation)
        .expect_err("precancelled metadata must reject without work");
    assert!(matches!(failure.error, FileMetadataEncodeError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn metadata_encoder_borrows_the_existing_live_ledger() -> Result<(), Box<dyn std::error::Error>> {
    use super::super::allocation::{AllocationLedger, KernelAccounting, KernelOwnedAllocation};
    let cancellation = CancellationToken::new();
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let admitted = allocations.claim_elements::<u8>(17, &mut work, WorkBudget::UNBOUNDED)?;
    let mut caller = Vec::<u8>::new();
    caller.try_reserve_exact(17)?;
    let caller_bytes = caller.capacity() as u64;
    if caller_bytes > admitted {
        allocations.record_owned_bytes(caller_bytes - admitted, &mut work)?;
    }
    let value = metadata();
    let (bytes, custody) = {
        let mut accounting = KernelAccounting::borrowed_standalone(
            &mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation,
        );
        encode_file_metadata_with_accounting(&value, &mut accounting)?
    };
    let encoded_capacity = bytes.capacity() as u64;
    assert_eq!(bytes, encode_file_metadata(value)?);
    assert!(matches!(custody, KernelOwnedAllocation::Standalone));
    assert_eq!(allocations.live_bytes(), caller_bytes + encoded_capacity);
    assert_eq!(work.peak_allocation_bytes, caller_bytes + encoded_capacity);
    assert_eq!(work.allocation_operations, 2);
    drop(bytes);
    drop(custody);
    allocations.release(encoded_capacity)?;
    drop(caller);
    allocations.release(caller_bytes)?;
    assert_eq!(allocations.live_bytes(), 0);
    Ok(())
}

#[test]
fn cancelled_after_real_header_write_keeps_the_completed_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    use super::super::allocation::{AllocationLedger, KernelAccounting};
    use super::super::codec::{EncodeAccounting, EncodeAllocationStep, EncodeWorkStep};
    let cancellation = CancellationToken::new();
    let value = metadata();
    let budget = encode_file_metadata_measured(value, WorkBudget::UNBOUNDED, &cancellation)?.work;
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let result = {
        let mut accounting = KernelAccounting::borrowed_standalone(
            &mut allocations, &mut work, budget, &cancellation,
        );
        let mut funding = EncodeAccounting::new(&mut accounting);
        let result = encode_file_metadata_with_steps(
            value,
            |step| {
                if matches!(&step, EncodeWorkStep::Allocation(EncodeAllocationStep::HeaderWritten { .. })) {
                    cancellation.cancel();
                }
                funding.observe(step)
            },
            |_| crate::FsError::Decode(CanonicalDecodeError::AllocationFailed),
        );
        funding.finish(result)
    };
    assert!(matches!(result, Err(crate::FsError::Cancelled(_))));
    assert_eq!(work.items_examined, 1);
    assert_eq!(work.allocation_operations, 1);
    assert_eq!(work.bytes_encoded, (DOMAIN.len() + 2) as u64);
    assert_eq!(work.bytes_copied, 0);
    assert_eq!(work.peak_allocation_bytes, budget.peak_allocation_bytes);
    assert_eq!(allocations.live_bytes(), 0);
    Ok(())
}

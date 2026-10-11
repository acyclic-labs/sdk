use super::*;

fn root() -> GenerationRoot {
    GenerationRoot {
        volume_id: VolumeId::from_bytes([1; 16]),
        root_file_id: FileId::from_bytes([2; 16]),
        file_table: ObjectId {
            kind: ObjectKind::FileTablePage,
            digest: Digest::from_bytes([3; 32]),
        },
        parents: vec![GenerationId::new(Digest::from_bytes([4; 32]))],
        required_features: 0,
    }
}

#[test]
fn filtered_fork_feature_round_trips_and_requires_one_parent()
-> Result<(), Box<dyn std::error::Error>> {
    let mut value = root();
    value.required_features = GenerationRoot::FILTERED_FORK;
    let encoded = encode_generation_root(&value)?;
    assert_eq!(
        decode_generation_root(&encoded, DecodeLimits::default())?,
        value
    );
    assert_eq!(value.continuation_features(), 0);
    value.parents.clear();
    assert!(encode_generation_root(&value).is_err());
    value.parents = vec![
        GenerationId::new(Digest::from_bytes([4; 32])),
        GenerationId::new(Digest::from_bytes([5; 32])),
    ];
    assert!(encode_generation_root(&value).is_err());
    Ok(())
}

#[test]
fn generation_root_round_trip_and_identity_are_locked() -> Result<(), Box<dyn std::error::Error>> {
    let value = root();
    let encoded = encode_generation_root(&value)?;
    assert_eq!(
        generation_root_parent_count(&encoded, DecodeLimits::default())?,
        1
    );
    assert!(u64::try_from(encoded.len())? <= MAXIMUM_GENERATION_ROOT_BYTES);
    assert_eq!(
        decode_generation_root(&encoded, DecodeLimits::default())?,
        value
    );
    assert_eq!(
        hex::encode(encoded),
        "414359465347454e0100010101010101010101010101010101010202020202020202020202020202020203030303030303030303030303030303030303030303030303030303030303030100000004040404040404040404040404040404040404040404040404040404040404040000000000000000"
    );
    Ok(())
}

#[test]
fn duplicate_merge_parents_and_unknown_features_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let mut value = root();
    value
        .parents
        .push(GenerationId::new(Digest::from_bytes([5; 32])));
    assert_eq!(
        u64::try_from(encode_generation_root(&value)?.len())?,
        MAXIMUM_GENERATION_ROOT_BYTES
    );
    value.parents[1] = value.parents[0];
    assert!(encode_generation_root(&value).is_err());
    value.parents.pop();
    value.required_features = 2;
    assert!(encode_generation_root(&value).is_err());
    Ok(())
}

#[test]
fn every_generation_invariant_fails_closed_on_encode_and_decode()
-> Result<(), Box<dyn std::error::Error>> {
    let mut wrong_kind = root();
    wrong_kind.file_table.kind = ObjectKind::Blob;
    assert!(matches!(
        wrong_kind.validate(),
        Err(CanonicalDecodeError::Invariant(_))
    ));
    assert!(encode_generation_root(&wrong_kind).is_err());

    let mut too_many = root();
    too_many.parents = vec![
        GenerationId::new(Digest::from_bytes([4; 32])),
        GenerationId::new(Digest::from_bytes([5; 32])),
        GenerationId::new(Digest::from_bytes([6; 32])),
    ];
    assert!(matches!(
        too_many.validate(),
        Err(CanonicalDecodeError::Invariant(_))
    ));

    let mut encoded = encode_generation_root(&root())?;
    let parent_count_offset = 8 + 2 + 16 + 16 + 32;
    encoded[parent_count_offset..parent_count_offset + 4].copy_from_slice(&3_u32.to_le_bytes());
    assert!(matches!(
        decode_generation_root(&encoded, DecodeLimits::default()),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 3,
            maximum: 2
        })
    ));
    assert!(matches!(
        generation_root_parent_count(&encoded, DecodeLimits::default()),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 3,
            maximum: 2
        })
    ));
    Ok(())
}

#[test]
fn generation_decoder_revalidates_duplicate_parents_and_features()
-> Result<(), Box<dyn std::error::Error>> {
    let mut two_parents = root();
    two_parents
        .parents
        .push(GenerationId::new(Digest::from_bytes([5; 32])));
    let mut encoded = encode_generation_root(&two_parents)?;
    let parents_offset = 8 + 2 + 16 + 16 + 32 + 4;
    let first_parent = encoded[parents_offset..parents_offset + 32].to_vec();
    encoded[parents_offset + 32..parents_offset + 64].copy_from_slice(&first_parent);
    assert!(matches!(
        decode_generation_root(&encoded, DecodeLimits::default()),
        Err(CanonicalDecodeError::Invariant(_))
    ));

    let mut unsupported = encode_generation_root(&root())?;
    let features_offset = unsupported.len() - 8;
    unsupported[features_offset..].copy_from_slice(&2_u64.to_le_bytes());
    assert!(matches!(
        decode_generation_root(&unsupported, DecodeLimits::default()),
        Err(CanonicalDecodeError::Invariant(_))
    ));

    let value = root();
    let (object, generation) = generation_root_id(&value)?;
    assert_eq!(object.kind, ObjectKind::GenerationRoot);
    assert_eq!(generation.digest(), object.digest);
    assert_eq!(generation_root_id(&value)?, (object, generation));
    assert!(generation_root_id(&too_many_parent_root()).is_err());

    let mut trailing = encode_generation_root(&value)?;
    trailing.push(0);
    assert!(decode_generation_root(&trailing, DecodeLimits::default()).is_err());
    let mut wrong_domain = encode_generation_root(&value)?;
    wrong_domain[0] ^= 1;
    assert!(decode_generation_root(&wrong_domain, DecodeLimits::default()).is_err());
    Ok(())
}

fn too_many_parent_root() -> GenerationRoot {
    let mut value = root();
    value.parents.extend([
        GenerationId::new(Digest::from_bytes([5; 32])),
        GenerationId::new(Digest::from_bytes([6; 32])),
    ]);
    value
}

#[test]
fn measured_root_encoding_preserves_canonical_bytes_and_real_capacity()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut value = root();
    for parent_count in 0..=2 {
        value.parents = [4, 5].into_iter().take(parent_count)
            .map(|seed| GenerationId::new(Digest::from_bytes([seed; 32]))).collect();
        let baseline = encode_generation_root(&value)?;
        let receipt = encode_generation_root_measured(&value, WorkBudget::UNBOUNDED, &cancellation, 37)?;
        assert_eq!(receipt.value, baseline);
        assert_eq!(receipt.work.items_examined, 1);
        assert_eq!(receipt.work.bytes_encoded, receipt.value.len() as u64);
        assert_eq!(receipt.work.bytes_copied, 64 + parent_count as u64 * 32);
        assert_eq!(receipt.work.allocation_operations, 1);
        assert_eq!(receipt.work.peak_allocation_bytes, 37 + receipt.value.capacity() as u64);
        let permit = WorkBudget {
            items_examined: receipt.work.items_examined,
            bytes_encoded: receipt.work.bytes_encoded,
            bytes_copied: receipt.work.bytes_copied,
            allocation_operations: receipt.work.allocation_operations,
            peak_allocation_bytes: receipt.work.peak_allocation_bytes,
            ..WorkBudget::UNBOUNDED
        };
        let finite = encode_generation_root_measured(&value, permit, &cancellation, 37)?;
        assert_eq!(finite, receipt);
        assert_eq!(object_digest(ObjectKind::GenerationRoot, &finite.value), generation_root_id(&value)?.0.digest);
    }
    Ok(())
}

#[test]
fn measured_root_encoding_refuses_before_allocation_and_retains_invalid_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let value = root();
    let cancellation = CancellationToken::new();
    for permit in [
        WorkBudget { bytes_encoded: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED },
        WorkBudget { peak_allocation_bytes: 117, ..WorkBudget::UNBOUNDED },
    ] {
        let failure = encode_generation_root_measured(&value, permit, &cancellation, 0).expect_err("finite encoder must refuse");
        assert!(matches!(failure.error, GenerationRootEncodeError::Work(_)));
        assert_eq!(failure.work.items_examined, 1);
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.bytes_encoded, 0);
        assert_eq!(failure.work.bytes_copied, 0);
    }
    let mut invalid = value;
    invalid.parents.push(invalid.parents[0]);
    let failure = encode_generation_root_measured(&invalid, WorkBudget::UNBOUNDED, &cancellation, 41).expect_err("duplicate parents");
    assert!(matches!(failure.error, GenerationRootEncodeError::Canonical(CanonicalDecodeError::Invariant(_))));
    assert_eq!(failure.work.items_examined, 1);
    assert_eq!(failure.work.allocation_operations, 0);
    assert_eq!(failure.work.bytes_encoded, 0);
    assert_eq!(failure.work.peak_allocation_bytes, 41);
    let mut wrong_kind = root();
    wrong_kind.file_table.kind = ObjectKind::Blob;
    let mut unsupported = root();
    unsupported.required_features = 2;
    let mut filtered_without_parent = root();
    filtered_without_parent.required_features = GenerationRoot::FILTERED_FORK;
    filtered_without_parent.parents.clear();
    for invalid in [wrong_kind, unsupported, filtered_without_parent, too_many_parent_root()] {
        let failure = encode_generation_root_measured(&invalid, WorkBudget::UNBOUNDED, &cancellation, 0).expect_err("invalid canonical root");
        assert!(matches!(failure.error, GenerationRootEncodeError::Canonical(CanonicalDecodeError::Invariant(_))));
        assert_eq!(failure.work.items_examined, 1);
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.bytes_encoded, 0);
    }
    cancellation.cancel();
    let failure = encode_generation_root_measured(&invalid, WorkBudget::UNBOUNDED, &cancellation, 91).expect_err("cancelled before validation");
    assert!(matches!(failure.error, GenerationRootEncodeError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_root_encoder_retains_real_output_capacity_on_original_ledger()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let before = work.receipt();
    let live_before = work.live_allocation_bytes();
    let value = root();
    let mut accounting = KernelAccounting::native(&work, &cancellation);
    let (encoded, guard) = encode_generation_root_with_accounting(&value, &mut accounting, &cancellation)?;
    assert_eq!(encoded, encode_generation_root(&value)?);
    match &guard {
        KernelOwnedAllocation::NativeOriginal(allocation) => {
            assert!(allocation.work().shares_ledger(&work));
            assert_eq!(allocation.bytes(), encoded.capacity() as u64);
        }
        KernelOwnedAllocation::Standalone => panic!("original writer returned standalone custody"),
    }
    let after = work.receipt();
    assert_eq!(after.items_examined - before.items_examined, 1);
    assert_eq!(after.bytes_encoded - before.bytes_encoded, encoded.len() as u64);
    assert_eq!(after.bytes_copied - before.bytes_copied, 96);
    assert_eq!(after.allocation_operations - before.allocation_operations, 1);
    assert_eq!(after.peak_allocation_bytes, live_before + encoded.capacity() as u64);
    assert_eq!(work.live_allocation_bytes(), live_before + encoded.capacity() as u64);
    drop(encoded);
    drop(guard);
    assert_eq!(work.live_allocation_bytes(), live_before);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_root_encoder_refuses_without_allocating_and_preserves_original_cancel()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(
        WorkBudget { bytes_encoded: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).map_err(|(error, _)| error)?;
    let before = work.receipt();
    let live_before = work.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&work, &cancellation);
    let result = encode_generation_root_with_accounting(&root(), &mut accounting, &cancellation);
    assert!(matches!(result, Err(crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(_)))));
    assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
    assert_eq!(work.receipt().bytes_encoded, before.bytes_encoded);
    assert_eq!(work.receipt().items_examined, before.items_examined + 1);
    assert_eq!(work.live_allocation_bytes(), live_before);
    cancellation.cancel();
    let before_cancelled = work.receipt();
    let result = encode_generation_root_with_accounting(&root(), &mut accounting, &cancellation);
    assert!(matches!(result, Err(crate::FsError::Cancelled(_))));
    assert_eq!(work.receipt(), before_cancelled);
    Ok(())
}

#[test]
fn measured_root_allocation_error_retains_the_actual_reservation_cause() {
    let source = Vec::<u8>::new().try_reserve_exact(usize::MAX)
        .expect_err("byte capacity exceeds Vec's addressable allocation bound");
    let error = GenerationRootEncodeError::AllocationFailed(source);
    assert!(std::error::Error::source(&error)
        .is_some_and(|cause| cause.downcast_ref::<std::collections::TryReserveError>().is_some()));
}

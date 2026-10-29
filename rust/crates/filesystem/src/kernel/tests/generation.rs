use super::*;

fn root() -> GenerationRoot {
    GenerationRoot {
        volume_id: VolumeId::from_bytes([1; 16]),
        root_file_id: FileId::from_bytes([2; 16]),
        file_table: ObjectId {
            storage: Some(crate::ObjectStorage {
                locator: Digest::from_bytes([5; 32]),
                root_file_id: FileId::from_bytes([2; 16]),
                origin_generation: Digest::from_bytes([6; 32]),
            }),
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
    let parent = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: Digest::from_bytes([4; 32]),
        storage: value.file_table.storage,
    };
    value.parents[0] = GenerationId::from_root(parent);
    let encoded = encode_generation_root(
        &value,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    assert_eq!(
        decode_generation_root(&encoded, DecodeLimits::default(), |_| Ok::<
            _,
            crate::kernel::CanonicalDecodeError,
        >(()))?,
        value
    );
    assert_eq!(value.continuation_features(), 0);
    value.parents.clear();
    assert!(
        encode_generation_root(
            &value,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed
        )
        .is_err()
    );
    value.parents = vec![
        GenerationId::new(Digest::from_bytes([4; 32])),
        GenerationId::new(Digest::from_bytes([5; 32])),
    ];
    assert!(
        encode_generation_root(
            &value,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn generation_root_round_trip_and_identity_are_locked() -> Result<(), Box<dyn std::error::Error>> {
    let value = root();
    let encoded = encode_generation_root(
        &value,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    assert_eq!(
        decode_generation_root(&encoded, DecodeLimits::default(), |_| Ok::<
            _,
            CanonicalDecodeError,
        >(()))?
        .parents
        .len(),
        1
    );
    assert_eq!(
        decode_generation_root(&encoded, DecodeLimits::default(), |_| Ok::<
            _,
            crate::kernel::CanonicalDecodeError,
        >(()))?,
        value
    );
    assert_eq!(
        hex::encode(encoded),
        "414359465347454e010001010101010101010101010101010101020202020202020202020202020202028503030303030303030303030303030303030303030303030303030303030303030505050505050505050505050505050505050505050505050505050505050505020202020202020202020202020202020606060606060606060606060606060606060606060606060606060606060606010000000604040404040404040404040404040404040404040404040404040404040404040000000000000000"
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
        u64::try_from(
            encode_generation_root(
                &value,
                |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
                |_| crate::kernel::CanonicalDecodeError::AllocationFailed
            )?
            .len()
        )?,
        54 + root().file_table.encoded_len() as u64 + 66
    );
    value.parents[1] = value.parents[0];
    assert!(
        encode_generation_root(
            &value,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed
        )
        .is_err()
    );
    value.parents.pop();
    value.required_features = 2;
    assert!(
        encode_generation_root(
            &value,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed
        )
        .is_err()
    );
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
    assert!(
        encode_generation_root(
            &wrong_kind,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed
        )
        .is_err()
    );

    let mut encoded = encode_generation_root(
        &root(),
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    let parent_count_offset = 8 + 2 + 16 + 16 + root().file_table.encoded_len();
    encoded[parent_count_offset..parent_count_offset + 4].copy_from_slice(&3_u32.to_le_bytes());
    assert!(matches!(
        decode_generation_root(&encoded, DecodeLimits::default(), |_| Ok::<
            _,
            crate::kernel::CanonicalDecodeError,
        >(())),
        Err(CanonicalDecodeError::Truncated)
    ));
    assert!(matches!(
        decode_generation_root(&encoded, DecodeLimits::default(), |_| Ok::<
            _,
            CanonicalDecodeError,
        >(())),
        Err(CanonicalDecodeError::Truncated)
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
    let mut encoded = encode_generation_root(
        &two_parents,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    let parents_offset = 8 + 2 + 16 + 16 + two_parents.file_table.encoded_len() + 4;
    let first_parent = encoded[parents_offset..parents_offset + 33].to_vec();
    encoded[parents_offset + 33..parents_offset + 66].copy_from_slice(&first_parent);
    assert!(matches!(
        decode_generation_root(&encoded, DecodeLimits::default(), |_| Ok::<
            _,
            crate::kernel::CanonicalDecodeError,
        >(())),
        Err(CanonicalDecodeError::Invariant(_))
    ));

    let mut unsupported = encode_generation_root(
        &root(),
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    let features_offset = unsupported.len() - 8;
    unsupported[features_offset..].copy_from_slice(&2_u64.to_le_bytes());
    assert!(matches!(
        decode_generation_root(&unsupported, DecodeLimits::default(), |_| Ok::<
            _,
            crate::kernel::CanonicalDecodeError,
        >(())),
        Err(CanonicalDecodeError::Invariant(_))
    ));

    let value = root();
    let (object, generation) = generation_root_id(&value)?;
    assert_eq!(object.kind, ObjectKind::GenerationRoot);
    assert_eq!(generation.digest(), object.digest);
    assert_eq!(generation_root_id(&value)?, (object, generation));

    let mut trailing = encode_generation_root(
        &value,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    trailing.push(0);
    assert!(
        decode_generation_root(&trailing, DecodeLimits::default(), |_| Ok::<
            _,
            crate::kernel::CanonicalDecodeError,
        >(()))
        .is_err()
    );
    let mut wrong_domain = encode_generation_root(
        &value,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?;
    wrong_domain[0] ^= 1;
    assert!(
        decode_generation_root(&wrong_domain, DecodeLimits::default(), |_| Ok::<
            _,
            crate::kernel::CanonicalDecodeError,
        >(()))
        .is_err()
    );
    Ok(())
}

#[test]
fn large_parent_lineage_preserves_order_and_rejects_nonadjacent_duplicates()
-> Result<(), Box<dyn std::error::Error>> {
    let mut value = root();
    value.parents = (0_u64..8193)
        .rev()
        .map(|index| {
            let mut digest = [0; 32];
            digest[..8].copy_from_slice(&index.to_le_bytes());
            GenerationId::from_root(ObjectId {
                kind: ObjectKind::GenerationRoot,
                digest: Digest::from_bytes(digest),
                storage: (index % 2 == 0).then_some(crate::ObjectStorage {
                    locator: Digest::from_bytes(digest),
                    root_file_id: value.root_file_id,
                    origin_generation: Digest::from_bytes([91; 32]),
                }),
            })
        })
        .collect();
    let encoded = encode_generation_root(
        &value,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    assert_eq!(
        decode_generation_root(&encoded, DecodeLimits::default(), |_| Ok::<
            _,
            CanonicalDecodeError,
        >(()))?,
        value
    );
    let mut corrupted = encoded;
    let parents_offset = DOMAIN.len() + 2 + 16 + 16 + value.file_table.encoded_len() + 4;
    let last = parents_offset
        + value.parents[..value.parents.len() - 1]
            .iter()
            .map(|parent| parent.object_id().encoded_len())
            .sum::<usize>();
    let first = value.parents[0].object_id().to_bytes();
    assert_eq!(
        first.as_slice().len(),
        value
            .parents
            .last()
            .ok_or("missing last parent")?
            .object_id()
            .encoded_len()
    );
    corrupted[last..last + first.as_slice().len()].copy_from_slice(first.as_slice());
    assert!(matches!(
        decode_generation_root(&corrupted, DecodeLimits::default(), |_| Ok::<
            _,
            CanonicalDecodeError,
        >(())),
        Err(CanonicalDecodeError::Invariant(_))
    ));
    let last = value.parents.len() - 1;
    value.parents[last] = value.parents[0];
    assert!(matches!(
        encode_generation_root(
            &value,
            |_| Ok::<_, CanonicalDecodeError>(()),
            |_| CanonicalDecodeError::AllocationFailed
        ),
        Err(CanonicalDecodeError::Invariant(_))
    ));
    Ok(())
}

#[test]
fn forged_parent_count_is_rejected_before_allocating() -> Result<(), Box<dyn std::error::Error>> {
    let mut encoded = encode_generation_root(
        &root(),
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    let count_offset = DOMAIN.len() + 2 + 16 + 16 + root().file_table.encoded_len();
    encoded[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    let mut allocations = 0;
    let result = decode_generation_root(&encoded, DecodeLimits::default(), |step| {
        if matches!(step, ReadAllocationStep::Allocate { .. }) {
            allocations += 1;
        }
        Ok::<_, CanonicalDecodeError>(())
    });
    assert!(matches!(
        result,
        Err(CanonicalDecodeError::Truncated | CanonicalDecodeError::LengthOverflow)
    ));
    assert_eq!(allocations, 0);
    Ok(())
}

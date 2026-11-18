use acyclic_fs::GenerationId;
use acyclic_fs::kernel::{
    CanonicalDecodeError, DecodeLimits, ReadAllocationStep, decode_generation_root,
    encode_generation_root, generation_root_id,
};
use acyclic_fs_format::{
    identity::Digest,
    object_reference::{ObjectId, ObjectKind, ObjectStorage},
};

#[path = "../../../support/generation_root.rs"]
mod fixture;
use fixture::root;

#[test]
fn generation_root_round_trip_and_identity_are_locked() -> Result<(), Box<dyn std::error::Error>> {
    let value = root();
    let encoded = encode_generation_root(
        &value,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
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
            CanonicalDecodeError,
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
                |_| Ok::<_, CanonicalDecodeError>(()),
                |_| CanonicalDecodeError::AllocationFailed
            )?
            .len()
        )?,
        54 + root().file_table.encoded_len() as u64 + 66
    );
    let first = *value.parents.first().ok_or("missing first parent")?;
    *value.parents.get_mut(1).ok_or("missing second parent")? = first;
    assert!(
        encode_generation_root(
            &value,
            |_| Ok::<_, CanonicalDecodeError>(()),
            |_| CanonicalDecodeError::AllocationFailed
        )
        .is_err()
    );
    value.parents.pop();
    value.required_features = 2;
    assert!(
        encode_generation_root(
            &value,
            |_| Ok::<_, CanonicalDecodeError>(()),
            |_| CanonicalDecodeError::AllocationFailed
        )
        .is_err()
    );
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
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    let parents_offset = 8 + 2 + 16 + 16 + two_parents.file_table.encoded_len() + 4;
    let first_parent = encoded
        .get(parents_offset..parents_offset + 33)
        .ok_or("missing encoded first parent")?
        .to_vec();
    encoded
        .get_mut(parents_offset + 33..parents_offset + 66)
        .ok_or("missing encoded second parent")?
        .copy_from_slice(&first_parent);
    assert!(matches!(
        decode_generation_root(&encoded, DecodeLimits::default(), |_| Ok::<
            _,
            CanonicalDecodeError,
        >(())),
        Err(CanonicalDecodeError::Invariant(_))
    ));

    let mut unsupported = encode_generation_root(
        &root(),
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    let features_offset = unsupported.len() - 8;
    unsupported
        .get_mut(features_offset..)
        .ok_or("missing feature flags")?
        .copy_from_slice(&2_u64.to_le_bytes());
    assert!(matches!(
        decode_generation_root(&unsupported, DecodeLimits::default(), |_| Ok::<
            _,
            CanonicalDecodeError,
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
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    trailing.push(0);
    assert!(
        decode_generation_root(&trailing, DecodeLimits::default(), |_| Ok::<
            _,
            CanonicalDecodeError,
        >(()))
        .is_err()
    );
    let mut wrong_domain = encode_generation_root(
        &value,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    *wrong_domain.first_mut().ok_or("missing domain")? ^= 1;
    assert!(
        decode_generation_root(&wrong_domain, DecodeLimits::default(), |_| Ok::<
            _,
            CanonicalDecodeError,
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
            digest
                .split_at_mut(8)
                .0
                .copy_from_slice(&index.to_le_bytes());
            GenerationId::from_root(ObjectId {
                kind: ObjectKind::GenerationRoot,
                digest: Digest::from_bytes(digest),
                storage: (index % 2 == 0).then_some(ObjectStorage {
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
    let parents_offset = b"ACYFSGEN".len() + 2 + 16 + 16 + value.file_table.encoded_len() + 4;
    let (last_parent, preceding) = value.parents.split_last().ok_or("missing last parent")?;
    let last = parents_offset
        + preceding
            .iter()
            .map(|parent| parent.object_id().encoded_len())
            .sum::<usize>();
    let first = value
        .parents
        .first()
        .ok_or("missing first parent")?
        .object_id()
        .to_bytes();
    assert_eq!(
        first.as_slice().len(),
        last_parent.object_id().encoded_len()
    );
    corrupted
        .get_mut(last..last + first.as_slice().len())
        .ok_or("missing encoded last parent")?
        .copy_from_slice(first.as_slice());
    assert!(matches!(
        decode_generation_root(&corrupted, DecodeLimits::default(), |_| Ok::<
            _,
            CanonicalDecodeError,
        >(())),
        Err(CanonicalDecodeError::Invariant(_))
    ));

    let first = *value.parents.first().ok_or("missing first parent")?;
    *value.parents.last_mut().ok_or("missing last parent")? = first;
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
    let count_offset = b"ACYFSGEN".len() + 2 + 16 + 16 + root().file_table.encoded_len();
    encoded
        .get_mut(count_offset..count_offset + 4)
        .ok_or("missing encoded parent count")?
        .copy_from_slice(&u32::MAX.to_le_bytes());
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

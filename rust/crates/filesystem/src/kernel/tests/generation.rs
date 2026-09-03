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
    value.required_features = 1;
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
    unsupported[features_offset..].copy_from_slice(&1_u64.to_le_bytes());
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

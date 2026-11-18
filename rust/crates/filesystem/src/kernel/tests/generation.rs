use super::*;

#[path = "../../../tests/support/generation_root.rs"]
mod fixture;
use fixture::root;

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

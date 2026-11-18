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

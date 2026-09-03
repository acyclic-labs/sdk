use super::*;

#[test]
#[allow(clippy::too_many_lines)]
fn every_identity_round_trips_and_authority_hash_inputs_are_bound()
-> Result<(), Box<dyn std::error::Error>> {
    assert_ne!(AuthorityId::new().into_bytes(), [0; 16]);
    assert_ne!(AuthorityId::default().into_bytes(), [0; 16]);
    assert_eq!(AuthorityId::from_bytes([1; 16]).into_bytes(), [1; 16]);
    assert_ne!(OperationId::new().into_bytes(), [0; 16]);
    assert_ne!(OperationId::default().into_bytes(), [0; 16]);
    assert_eq!(OperationId::from_bytes([2; 16]).into_bytes(), [2; 16]);

    macro_rules! assert_uuid_identity {
        ($identity:ty, $byte:expr) => {{
            let fresh = <$identity>::new();
            let defaulted = <$identity>::default();
            assert_ne!(fresh.into_bytes(), [0; 16]);
            assert_ne!(defaulted.into_bytes(), [0; 16]);
            assert_eq!(
                <$identity>::from_bytes([$byte; 16]).into_bytes(),
                [$byte; 16]
            );
        }};
    }
    assert_uuid_identity!(VolumeId, 3);
    assert_uuid_identity!(CheckoutId, 4);
    assert_uuid_identity!(MountId, 5);
    assert_uuid_identity!(WatchId, 6);
    assert_uuid_identity!(FileId, 7);

    assert_eq!(Epoch::new(0), Err(IdentityError::ZeroEpoch));
    assert_eq!(Epoch::new(2)?.get(), 2);
    assert_eq!(Sequence::GENESIS.checked_next()?.get(), 1);
    assert_eq!(
        Sequence::new(u64::MAX).checked_next(),
        Err(IdentityError::SequenceExhausted)
    );
    let digest = Digest::from_bytes([8; 32]);
    assert_eq!(digest.as_bytes(), &[8; 32]);
    assert_eq!(digest.into_bytes(), [8; 32]);
    assert!(format!("{digest:?}").contains(&hex::encode([8; 32])));
    assert_eq!(GenerationId::new(digest).digest(), digest);
    assert_eq!(Head::genesis(Epoch::GENESIS).sequence, Sequence::GENESIS);
    assert_eq!(Head::genesis(Epoch::GENESIS).digest, Digest::ZERO);

    let authority = AuthorityId::from_bytes([9; 16]);
    let operation = OperationId::from_bytes([10; 16]);
    let base = authority_commit_digest(
        authority,
        Epoch::GENESIS,
        Sequence::new(1),
        operation,
        Digest::from_bytes([11; 32]),
        Digest::from_bytes([12; 32]),
        b"payload",
    );
    let changed = [
        authority_commit_digest(
            AuthorityId::from_bytes([13; 16]),
            Epoch::GENESIS,
            Sequence::new(1),
            operation,
            Digest::from_bytes([11; 32]),
            Digest::from_bytes([12; 32]),
            b"payload",
        ),
        authority_commit_digest(
            authority,
            Epoch::new(2)?,
            Sequence::new(1),
            operation,
            Digest::from_bytes([11; 32]),
            Digest::from_bytes([12; 32]),
            b"payload",
        ),
        authority_commit_digest(
            authority,
            Epoch::GENESIS,
            Sequence::new(2),
            operation,
            Digest::from_bytes([11; 32]),
            Digest::from_bytes([12; 32]),
            b"payload",
        ),
        authority_commit_digest(
            authority,
            Epoch::GENESIS,
            Sequence::new(1),
            OperationId::from_bytes([14; 16]),
            Digest::from_bytes([11; 32]),
            Digest::from_bytes([12; 32]),
            b"payload",
        ),
        authority_commit_digest(
            authority,
            Epoch::GENESIS,
            Sequence::new(1),
            operation,
            Digest::from_bytes([15; 32]),
            Digest::from_bytes([12; 32]),
            b"payload",
        ),
        authority_commit_digest(
            authority,
            Epoch::GENESIS,
            Sequence::new(1),
            operation,
            Digest::from_bytes([11; 32]),
            Digest::from_bytes([16; 32]),
            b"payload",
        ),
        authority_commit_digest(
            authority,
            Epoch::GENESIS,
            Sequence::new(1),
            operation,
            Digest::from_bytes([11; 32]),
            Digest::from_bytes([12; 32]),
            b"other",
        ),
    ];
    assert!(changed.into_iter().all(|candidate| candidate != base));
    Ok(())
}

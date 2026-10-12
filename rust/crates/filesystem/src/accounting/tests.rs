
use super::*;
use crate::foundation::Epoch;
use bytes::Bytes;

fn commit(
    authority: AuthorityId,
    sequence: u64,
    previous: Digest,
    stamp: Option<u64>,
) -> DurableCommit {
    let sequence = Sequence::new(sequence);
    let epoch = Epoch::GENESIS;
    let operation_id = OperationId::from_bytes([3; 16]);
    let fingerprint = Digest::from_bytes([4; 32]);
    let payload = Bytes::from_static(b"fact");
    DurableCommit {
        epoch,
        sequence,
        operation_id,
        fingerprint,
        previous_digest: previous,
        digest: authority_commit_digest(
            authority,
            epoch,
            sequence,
            operation_id,
            fingerprint,
            previous,
            &payload,
        ),
        payload,
        settled_at_micros: stamp,
    }
}

#[test]
fn page_cursor_rejects_gaps_tampering_and_time_regression() {
    let authority = AuthorityId::from_bytes([1; 16]);
    let first = commit(authority, 1, Digest::ZERO, Some(100));
    assert_eq!(
        verify_record(authority, AccountingCursor::genesis(), &first).ok(),
        Some(100)
    );
    let cursor = AccountingCursor {
        sequence: first.sequence,
        digest: first.digest,
        settled_at_micros: 100,
    };
    let second = commit(authority, 2, first.digest, Some(101));
    assert_eq!(verify_record(authority, cursor, &second).ok(), Some(101));
    assert!(matches!(
        verify_record(authority, cursor, &first),
        Err(AccountingError::InvalidHistory)
    ));
    assert!(matches!(
        verify_record(
            authority,
            cursor,
            &commit(authority, 3, first.digest, Some(101))
        ),
        Err(AccountingError::InvalidHistory)
    ));
    assert!(matches!(
        verify_record(
            authority,
            cursor,
            &commit(authority, 2, first.digest, Some(99))
        ),
        Err(AccountingError::InvalidHistory)
    ));
    assert!(matches!(
        verify_record(authority, cursor, &commit(authority, 2, first.digest, None)),
        Err(AccountingError::MissingTime)
    ));
    let mut tampered = second;
    tampered.payload = Bytes::from_static(b"other");
    assert!(matches!(
        verify_record(authority, cursor, &tampered),
        Err(AccountingError::InvalidHistory)
    ));
}

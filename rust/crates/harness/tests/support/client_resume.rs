//! Shared cursor oracle for native properties and arbitrary untrusted protobuf.
use acyclic_stream::{
    CommitId, wire,
    wire_codec::session::{record_anchor, record_cursor},
};
use prost::Message as _;

pub fn check(cursor: &wire::SessionCursor, next: u64, anchor: [u8; 32]) {
    assert_eq!(cursor.next, next);
    assert_eq!(
        record_anchor(cursor).expect("valid continuation must be admitted"),
        Some(CommitId::from_bytes(anchor))
    );
}

pub fn exercise(bytes: &[u8]) {
    if let Ok(cursor) = wire::SessionCursor::decode(bytes) {
        let valid = if cursor.next == 0 {
            cursor.opaque.is_empty()
        } else {
            cursor.opaque.len() == 32
        };
        let decoded = record_anchor(&cursor);
        assert_eq!(decoded.is_ok(), valid);
        if valid {
            assert_eq!(
                wire::SessionCursor::decode(cursor.encode_to_vec().as_slice()).unwrap(),
                cursor
            );
            if cursor.next == 0 {
                assert_eq!(decoded.unwrap(), None);
            } else {
                assert_eq!(
                    decoded.unwrap().unwrap().as_bytes().as_slice(),
                    cursor.opaque.as_ref()
                );
            }
        }
    }
    let mut anchor = [0; 32];
    for (out, byte) in anchor.iter_mut().zip(bytes) {
        *out = *byte;
    }
    let mut number = [0; 8];
    for (out, byte) in number.iter_mut().zip(bytes.iter().rev()) {
        *out = *byte;
    }
    let next = u64::from_le_bytes(number).max(1);
    let cursor = record_cursor(next, CommitId::from_bytes(anchor)).unwrap();
    check(&cursor, next, anchor);
    check(
        &wire::SessionCursor::decode(cursor.encode_to_vec().as_slice()).unwrap(),
        next,
        anchor,
    );
    assert!(record_cursor(0, CommitId::from_bytes(anchor)).is_err());
    assert!(
        record_anchor(&wire::SessionCursor {
            next: 0,
            opaque: anchor.to_vec().into()
        })
        .is_err()
    );
}

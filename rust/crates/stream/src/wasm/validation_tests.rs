use super::*;
use crate::{CommitCondition, CommitMutation, CommitRequest, IdempotencyKey, StreamPath};
use bytes::Bytes;

#[test]
fn append_validation_uses_domain_limits() {
    let request = wire::AppendRequest {
        path: "events".to_owned(),
        records: vec![Bytes::from_static(b"value")],
        if_tail: None,
        idempotency_key: None,
    };
    assert_eq!(validate_append_request(&request.encode_to_vec()), "");

    let invalid = wire::AppendRequest {
        path: "/events".to_owned(),
        ..request
    };
    assert_eq!(
        validate_append_request(&invalid.encode_to_vec()),
        "invalid_path"
    );
}

#[test]
fn path_validation_uses_the_canonical_stream_parser() {
    assert_eq!(validate_path("events/run_42"), "");
    assert_eq!(validate_path("/events"), "invalid_path");
    assert_eq!(validate_path("events//run_42"), "invalid_path");
    let mut too_many = "a".to_owned();
    for _ in 0..crate::MAX_ITEMS {
        too_many.push_str("/a");
    }
    assert_eq!(validate_path(&too_many), "limit_exceeded");
}

#[test]
fn sequence_validation_uses_the_canonical_unsigned_width() {
    assert_eq!(validate_sequence("0"), "");
    assert_eq!(validate_sequence("18446744073709551615"), "");
    assert_eq!(
        validate_sequence("18446744073709551616"),
        "invalid_argument"
    );
    assert_eq!(validate_sequence("-1"), "invalid_argument");
}

#[test]
fn commit_normalization_is_canonical_and_shape_checked() -> Result<(), StreamError> {
    let path = StreamPath::new("events")?;
    let key = IdempotencyKey::new(Bytes::from_static(b"request"))?;
    let request = CommitRequest {
        conditions: vec![CommitCondition::Absent { path: path.clone() }],
        mutations: vec![CommitMutation::Append {
            path,
            records: vec![crate::OwnedBytes::from_static(b"value")],
        }],
        idempotency_key: key,
    };
    let wire = wire_codec::commit_to_wire(&request);
    let normalized =
        normalize_commit_bytes(&wire.encode_to_vec()).map_err(|_| StreamError::InvalidArgument)?;
    assert_eq!(normalized, wire.encode_to_vec());

    let invalid = wire::CommitRequest {
        conditions: Vec::new(),
        ..wire
    };
    assert_eq!(
        normalize_commit_bytes(&invalid.encode_to_vec()),
        Err("limit_exceeded")
    );
    Ok(())
}

#[test]
fn request_validation_uses_wire_decoding_and_domain_limits() {
    let valid = wire::ReadRequest {
        path: "events".to_owned(),
        from: 0,
        limit: 10,
    };
    assert_eq!(validate_request("read", &valid.encode_to_vec()), "");

    let invalid_path = wire::TailRequest {
        path: "/events".to_owned(),
    };
    assert_eq!(
        validate_request("tail", &invalid_path.encode_to_vec()),
        "invalid_path"
    );

    let invalid_limit = wire::ChildrenRequest {
        parent: Some("events".to_owned()),
        limit: 0,
    };
    assert_eq!(
        validate_request("children", &invalid_limit.encode_to_vec()),
        "limit_exceeded"
    );
    assert_eq!(validate_request("unknown", &[]), "invalid_argument");
}

#[test]
fn fork_validation_rejects_same_paths_and_oversized_payloads() {
    let same = wire::ForkRequest {
        source: "events".to_owned(),
        destination: "events".to_owned(),
        at_tail: None,
        idempotency_key: None,
    };
    assert_eq!(
        validate_request("fork", &same.encode_to_vec()),
        "invalid_argument"
    );

    let oversized = wire::ForkRequest {
        source: "a".repeat(crate::MAX_PATH_BYTES + 1),
        destination: "b".to_owned(),
        at_tail: None,
        idempotency_key: None,
    };
    assert_eq!(
        validate_request("fork", &oversized.encode_to_vec()),
        "invalid_path"
    );
}

use crate::OwnedBytes as Bytes;

use super::*;
use crate::{CommitCondition, CommitMutation, CommitRequest, IdempotencyKey, StreamPath};

#[test]
fn stream_error_codes_are_closed_and_stable() {
    for error in StreamError::ALL {
        assert!(is_stream_error_code(error.code()));
    }
    assert!(!is_stream_error_code("stream_not_found"));
    assert!(!is_stream_error_code("unknown"));
}

#[test]
fn hosted_http_errors_use_the_public_route_aware_vocabulary() {
    assert_eq!(
        public_http_error_code("not_found", "tail").as_deref(),
        Some("stream_not_found")
    );
    assert_eq!(
        public_http_error_code("not_found", "commits/read").as_deref(),
        Some("commit_not_found")
    );
    assert_eq!(
        public_http_error_code("out_of_range", "read").as_deref(),
        Some("out_of_range")
    );
    assert_eq!(
        public_http_error_code("capacity", "append").as_deref(),
        Some("capacity_exhausted")
    );
    assert_eq!(
        public_http_error_code("stream_not_found", "read").as_deref(),
        Some("stream_not_found")
    );
    assert_eq!(public_http_error_code("commit_not_found", "read"), None);
    assert_eq!(public_http_error_code("unknown", "read"), None);
}

#[test]
fn append_validation_uses_domain_limits() {
    let request = wire::AppendRequest {
        path: "events".to_owned(),
        records: vec![wire::AppendRecord {
            value: bytes::Bytes::from_static(b"value"),
            ..Default::default()
        }],
        if_tail: None,
        idempotency_key: bytes::Bytes::from_static(b"key"),
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
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Absent { path: path.clone() }],
        mutations: vec![CommitMutation::Append {
            path,
            records: vec![Bytes::from_static(b"value").into()],
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
        key: None,
        path: "events".to_owned(),
        from: 0,
        limit: 10,
        at_least: Default::default(),
    };
    assert_eq!(validate_request("read", &valid.encode_to_vec()), "");

    let invalid_path = wire::TailRequest {
        path: "/events".to_owned(),
        ..Default::default()
    };
    assert_eq!(
        validate_request("tail", &invalid_path.encode_to_vec()),
        "invalid_path"
    );

    let invalid_limit = wire::ChildrenPageRequest {
        tail_stream: None,
        parent: Some("events".to_owned()),
        limit: 0,
        ..Default::default()
    };
    assert_eq!(
        validate_request("children_page", &invalid_limit.encode_to_vec()),
        "limit_exceeded"
    );
    assert_eq!(validate_request("unknown", &[]), "invalid_argument");
}

#[test]
fn fork_validation_rejects_same_paths_and_oversized_payloads() {
    let same = wire::ForkRequest {
        exclude: Vec::new(),
        source: "events".to_owned(),
        destination: "events".to_owned(),
        at_tail: None,
        idempotency_key: bytes::Bytes::new(),
    };
    assert_eq!(
        validate_request("fork", &same.encode_to_vec()),
        "invalid_argument"
    );

    let oversized = wire::ForkRequest {
        exclude: Vec::new(),
        source: "a".repeat(crate::MAX_PATH_BYTES + 1),
        destination: "b".to_owned(),
        at_tail: None,
        idempotency_key: bytes::Bytes::new(),
    };
    assert_eq!(
        validate_request("fork", &oversized.encode_to_vec()),
        "invalid_path"
    );
}

#[wasm_bindgen_test::wasm_bindgen_test]
fn page_tail_records_keep_keys_and_attributes() {
    let response = wire::ChildrenPageResponse {
        hierarchy_version: vec![7; 32].into(),
        children: vec![wire::Child {
            origin: None,
            path: "agents/a".to_owned(),
            tail_record: Some(wire::Record {
                sequence: 0,
                value: b"running".to_vec().into(),
                commit_id: vec![9; 32].into(),
                committed_at_micros: 1,
                origin: "agents/a".to_owned(),
                keys: vec![b"agent".to_vec().into()],
                attributes: vec![wire::Attribute {
                    name: "kind".to_owned(),
                    value: b"status".to_vec().into(),
                }],
            }),
        }],
        next_after: None,
    };
    let page =
        http::decode_wire_response("children_page", &response.encode_to_vec()).expect("valid page");
    let get = |value: &JsValue, name: &str| Reflect::get(value, &JsValue::from_str(name)).unwrap();
    let children = get(&page, "children");
    let record = get(&Array::from(&children).get(0), "tailRecord");
    let keys = Array::from(&get(&record, "keys"));
    assert_eq!(Uint8Array::from(keys.get(0)).to_vec(), b"agent");
    let attribute = Array::from(&get(&record, "attributes")).get(0);
    assert_eq!(get(&attribute, "name").as_string().as_deref(), Some("kind"));
    assert_eq!(
        Uint8Array::from(get(&attribute, "value")).to_vec(),
        b"status"
    );
}

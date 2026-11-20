use crate::{
    AppendRecord, AppendRequest, Attribute, CommitCondition, CommitEffect, CommitMutation,
    CommitRequest, IdempotencyKey, MAX_RECORD_BYTES, OwnedBytes, RecordHeaders, StreamCategory,
    StreamError, StreamPath,
};

#[cfg(not(target_arch = "wasm32"))]
#[path = "record_header_tests/native.rs"]
mod native;

fn body(key: &'static [u8], attribute: &'static [u8]) -> Result<AppendRecord, StreamError> {
    Ok(AppendRecord {
        value: OwnedBytes::from_static(b"same body"),
        headers: RecordHeaders::new(
            vec![key.into()],
            vec![Attribute {
                name: "topic".into(),
                value: attribute.into(),
            }],
        )?,
    })
}

#[test]
fn indexed_headers_share_the_value_payload_bound() -> Result<(), StreamError> {
    let mut request = AppendRequest {
        path: StreamPath::new("headers/bounds")?,
        records: vec![AppendRecord {
            value: vec![0; MAX_RECORD_BYTES - 1].into(),
            headers: RecordHeaders::new(vec![b"k".as_slice().into()], vec![])?,
        }],
        if_tail: None,
        idempotency_key: crate::IdempotencyKey::unique(),
    };
    crate::request::append_digest(&request)?;
    request
        .records
        .first_mut()
        .ok_or(StreamError::Unavailable)?
        .value = vec![0; MAX_RECORD_BYTES].into();
    assert_eq!(
        crate::request::append_digest(&request).map_err(|error| error.category()),
        Err(StreamCategory::LimitExceeded)
    );
    Ok(())
}

#[test]
fn indexed_header_order_is_canonical_and_unique() -> Result<(), StreamError> {
    for keys in [vec![b"b".as_slice(), b"a"], vec![b"a".as_slice(), b"a"]] {
        assert_eq!(
            RecordHeaders::new(keys.into_iter().map(Into::into).collect(), vec![])
                .map_err(|error| error.category()),
            Err(StreamCategory::InvalidArgument)
        );
    }
    assert_eq!(
        RecordHeaders::new(
            vec![vec![0; acyclic_stream_wire::headers::MAX_RECORD_KEY_BYTES + 1].into()],
            vec![]
        )
        .map_err(|error| error.category()),
        Err(StreamCategory::LimitExceeded)
    );
    let attribute = Attribute {
        name: "topic".into(),
        value: b"x".as_slice().into(),
    };
    assert_eq!(
        RecordHeaders::new(vec![], vec![attribute.clone(), attribute])
            .map_err(|error| error.category()),
        Err(StreamCategory::InvalidArgument)
    );
    Ok(())
}

fn append(record: AppendRecord) -> Result<AppendRequest, StreamError> {
    Ok(AppendRequest {
        path: StreamPath::new("headers/append")?,
        records: vec![record],
        if_tail: Some(0),
        idempotency_key: IdempotencyKey::new(b"same retry".as_slice())?,
    })
}

fn commit(record: AppendRecord, fork: bool) -> Result<CommitRequest, StreamError> {
    let destination = StreamPath::new("headers/commit")?;
    let mutation = if fork {
        CommitMutation::Fork {
            exclude: Vec::new(),
            source: StreamPath::new("headers/source")?,
            destination: destination.clone(),
            at_tail: 1,
            records: vec![record],
        }
    } else {
        CommitMutation::Append {
            path: destination.clone(),
            records: vec![record],
        }
    };
    Ok(CommitRequest {
        deadline_unix_millis: None,
        effects: vec![CommitEffect::hard(
            vec![CommitCondition::Absent { path: destination }],
            vec![mutation],
        )],
        idempotency_key: IdempotencyKey::new(b"same retry".as_slice())?,
    })
}

#[test]
fn every_write_family_authenticates_keys_and_attributes_in_retry_identity()
-> Result<(), StreamError> {
    let original = body(b"a", b"x")?;
    for changed in [body(b"b", b"x")?, body(b"a", b"y")?] {
        assert_ne!(
            crate::request::append_digest(&append(original.clone())?)?,
            crate::request::append_digest(&append(changed.clone())?)?
        );
        for fork in [false, true] {
            assert_ne!(
                crate::request::commit_digest(&mut commit(original.clone(), fork)?)?,
                crate::request::commit_digest(&mut commit(changed.clone(), fork)?)?
            );
        }
    }
    Ok(())
}

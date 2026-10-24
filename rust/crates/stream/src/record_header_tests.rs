use crate::{
    AppendRecord, AppendRequest, Attribute, CommitCondition, CommitMutation, CommitRequest,
    IdempotencyKey, MAX_RECORD_BYTES, OwnedBytes, RecordHeaders, StreamError, StreamPath,
};

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
        idempotency_key: None,
    };
    crate::request::append_digest(&request)?;
    request.records.first_mut().ok_or(StreamError::Unavailable)?.value = vec![0; MAX_RECORD_BYTES].into();
    assert_eq!(
        crate::request::append_digest(&request),
        Err(StreamError::LimitExceeded)
    );
    let wire = crate::wire_codec::append_record_wire(request.records.remove(0));
    assert_eq!(
        crate::wire_codec::append_record(wire),
        Err(StreamError::LimitExceeded)
    );
    Ok(())
}

#[test]
fn indexed_header_order_is_canonical_and_unique() -> Result<(), StreamError> {
    for keys in [vec![b"b".as_slice(), b"a"], vec![b"a".as_slice(), b"a"]] {
        assert_eq!(
            RecordHeaders::new(keys.into_iter().map(Into::into).collect(), vec![]),
            Err(StreamError::InvalidArgument)
        );
    }
    assert_eq!(
        RecordHeaders::new(
            vec![vec![0; crate::MAX_RECORD_KEY_BYTES + 1].into()],
            vec![]
        ),
        Err(StreamError::LimitExceeded)
    );
    let attribute = Attribute {
        name: "topic".into(),
        value: b"x".as_slice().into(),
    };
    assert_eq!(
        RecordHeaders::new(vec![], vec![attribute.clone(), attribute]),
        Err(StreamError::InvalidArgument)
    );
    assert_eq!(
        body(b"key", b"value")?.headers.keys(),
        &[OwnedBytes::from_static(b"key")]
    );
    Ok(())
}

fn append(record: AppendRecord) -> Result<AppendRequest, StreamError> {
    Ok(AppendRequest {
        path: StreamPath::new("headers/append")?,
        records: vec![record],
        if_tail: Some(0),
        idempotency_key: Some(IdempotencyKey::new(b"same retry".as_slice())?),
    })
}

fn commit(record: AppendRecord, fork: bool) -> Result<CommitRequest, StreamError> {
    let destination = StreamPath::new("headers/commit")?;
    let mutation = if fork {
        CommitMutation::Fork {
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
        conditions: vec![CommitCondition::Absent { path: destination }],
        mutations: vec![mutation],
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

#[test]
fn append_record_wire_roundtrip_preserves_both_indexes() -> Result<(), StreamError> {
    let original = body(b"key", b"attribute")?;
    let decoded =
        crate::wire_codec::append_record(crate::wire_codec::append_record_wire(original.clone()))?;
    assert_eq!(decoded, original);
    Ok(())
}

#[cfg(feature = "local")]
#[test]
fn manual_append_and_commit_codecs_match_the_generated_header_contract() -> Result<(), StreamError>
{
    use prost::Message;
    let request = append(body(b"key", b"attribute")?)?;
    let mut actual = Vec::new();
    crate::wire_codec::requests::append(&mut actual, &request);
    let expected = crate::wire::AppendRequest {
        path: request.path.to_string(),
        records: request
            .records
            .clone()
            .into_iter()
            .map(crate::wire_codec::append_record_wire)
            .collect(),
        if_tail: request.if_tail,
        idempotency_key: request
            .idempotency_key
            .map(|key| bytes::Bytes::copy_from_slice(key.as_bytes())),
    };
    assert_eq!(actual, expected.encode_to_vec());
    for fork in [false, true] {
        let request = commit(body(b"key", b"attribute")?, fork)?;
        let mut actual = Vec::new();
        crate::wire_codec::requests::commit(&mut actual, &request);
        let expected = crate::wire_codec::commit_to_wire(&request);
        assert_eq!(actual, expected.encode_to_vec());
    }
    Ok(())
}

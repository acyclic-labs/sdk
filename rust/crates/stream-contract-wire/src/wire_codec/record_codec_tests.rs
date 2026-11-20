//! The hand-written request encoders and the record codec against the
//! generated protobuf contract.
use acyclic_stream_contract_core::{
    AppendRecord, AppendRequest, Attribute, CommitCondition, CommitEffect, CommitMutation,
    CommitRequest, IdempotencyKey, MAX_RECORD_BYTES, OwnedBytes, RecordHeaders, StreamCategory,
    StreamError, StreamPath,
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
fn oversized_indexed_records_are_refused_on_decode() -> Result<(), StreamError> {
    let record = AppendRecord {
        value: vec![0; MAX_RECORD_BYTES].into(),
        headers: RecordHeaders::new(vec![b"k".as_slice().into()], vec![])?,
    };
    assert_eq!(
        super::append_record(super::append_record_wire(record)).map_err(|error| error.category()),
        Err(StreamCategory::LimitExceeded)
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn manual_append_and_commit_codecs_match_the_generated_header_contract() -> Result<(), StreamError>
{
    use prost::Message;
    let request = append(body(b"key", b"attribute")?)?;
    let mut actual = Vec::new();
    acyclic_stream_contract_core::request::encode::append(&mut actual, &request);
    let expected = acyclic_stream_wire::wire::AppendRequest {
        path: request.path.to_string(),
        records: request
            .records
            .clone()
            .into_iter()
            .map(super::append_record_wire)
            .collect(),
        if_tail: request.if_tail,
        idempotency_key: bytes::Bytes::copy_from_slice(request.idempotency_key.as_bytes()),
    };
    assert_eq!(actual, expected.encode_to_vec());
    for fork in [false, true] {
        let request = commit(body(b"key", b"attribute")?, fork)?;
        let mut actual = Vec::new();
        acyclic_stream_contract_core::request::encode::commit(&mut actual, &request);
        let expected = super::commit_to_wire(&request);
        assert_eq!(actual, expected.encode_to_vec());
    }
    Ok(())
}

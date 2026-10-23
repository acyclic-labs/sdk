use super::*;
use crate::{FollowFilter, MemoryStream, StreamPath, StreamProvider};
use futures::StreamExt;

fn filter() -> FollowFilter {
    use acyclic_auth::{contract::*, testing as signed};
    let token = signed::root(|block| {
        block.caveats.push(Caveat {
            kind: CaveatKind::Filter(Filter {
                keys: vec![KeyRange {
                    start: b"b".to_vec(),
                    end: b"c".to_vec(),
                }],
                attributes: vec![],
            }),
        });
    });
    let capability =
        acyclic_auth::verify::verify(&token, &signed::Snapshot::default(), signed::NOW).unwrap();
    FollowFilter::authorized(Default::default(), &capability).unwrap()
}

fn changes() -> AppendRecord {
    AppendRecord::changes(
        bytes::Bytes::from_static(b"publication-identity"),
        ["b", "z"]
            .into_iter()
            .map(|key| wire::ChangeEntry {
                key: key.into(),
                operation: "write".into(),
                metadata: key.into(),
            })
            .collect(),
        vec![],
    )
    .unwrap()
}

#[test]
fn typed_projection_removes_outside_entries_while_opaque_mixed_keys_are_denied() {
    let body = changes();
    let opaque = RecordHeaders::new(body.headers.keys().to_vec(), vec![]).unwrap();
    assert!(!filter().matches(&opaque));
    let record = crate::Record {
        sequence: 4,
        value: body.value,
        headers: body.headers,
        commit_id: crate::CommitId::from_bytes([1; 32]),
        committed_at_micros: 42,
    };
    assert!(filter().matches(&record.headers));
    let projected = filter().project(record.clone()).unwrap().unwrap();
    let value = decode(&projected.value, &projected.headers).unwrap();
    assert_eq!(value.entries.len(), 1);
    assert_eq!(value.entries.first().unwrap().key.as_ref(), b"b");
    assert_eq!(projected.sequence, record.sequence);
    assert_eq!(projected.commit_id, record.commit_id);
    assert_eq!(projected.committed_at_micros, record.committed_at_micros);
    let omitted = FollowFilter::default().project(record).unwrap().unwrap();
    assert_eq!(
        decode(&omitted.value, &omitted.headers)
            .unwrap()
            .entries
            .len(),
        2,
        "omitting authority projection discloses the outside path"
    );
}

#[test]
fn malformed_typed_lists_never_disagree_with_the_immutable_index() {
    let mut body = changes();
    let mut value = decode(&body.value, &body.headers).unwrap();
    value.entries.first_mut().unwrap().key = "a".into();
    body.value = value.encode_to_vec().into();
    assert_eq!(
        validate(&body.value, &body.headers),
        Err(StreamError::InvalidArgument)
    );
    assert!(crate::request::validate_records(&[body.clone()]).is_err());
    assert!(crate::wire_codec::append_record(crate::wire_codec::append_record_wire(body)).is_err());
    assert!(AppendRecord::changes(bytes::Bytes::new(), vec![], vec![]).is_err());
}

#[test]
fn canonical_change_validation_rejects_wire_malleability() {
    let headers = changes().headers;
    // Duplicate context, explicit empty context, unknown field, nonminimal
    // length, truncated entry, and a canonical list with the wrong key count.
    for bytes in [
        b"\x0a\x01a\x0a\x01a".as_slice(),
        b"\x0a\x00",
        b"\x1a\x01a",
        b"\x0a\x81\x00a",
        b"\x12\x02\x0a",
        b"\x12\x06\x0a\x01b\x12\x01w",
    ] {
        assert_eq!(validate(bytes, &headers), Err(StreamError::InvalidArgument));
    }
    let empty_key = AppendRecord::changes(
        bytes::Bytes::new(),
        vec![wire::ChangeEntry {
            key: bytes::Bytes::new(),
            operation: "write".into(),
            metadata: bytes::Bytes::new(),
        }],
        vec![],
    )
    .unwrap();
    assert_eq!(validate(&empty_key.value, &empty_key.headers), Ok(()));
}

#[cfg(not(target_arch = "wasm32"))]
async fn prepared_ingress(provider: &impl StreamProvider) {
    use crate::{CommitCondition, CommitMutation, CommitRequest, IdempotencyKey};
    let owner = StreamPath::new("changes-owner").unwrap();
    let target = StreamPath::new("changes-target").unwrap();
    for path in [&owner, &target] {
        provider
            .append(crate::AppendRequest {
                path: path.clone(),
                records: vec![b"seed".as_slice().into()],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await
            .unwrap();
    }
    let meter = std::sync::Arc::new(crate::test_meter::Meter::local());
    let mut body = changes();
    let mut value = decode(&body.value, &body.headers).unwrap();
    value.entries.first_mut().unwrap().key = "a".into();
    body.value = value.encode_to_vec().into();
    let request = CommitRequest {
        conditions: vec![
            CommitCondition::Tail {
                path: owner,
                expected: 1,
            },
            CommitCondition::Tail {
                path: target.clone(),
                expected: 1,
            },
        ],
        mutations: vec![CommitMutation::Append {
            path: target.clone(),
            records: vec![crate::OwnedBytes::default().into()],
        }],
        idempotency_key: IdempotencyKey::new("changes-invalid").unwrap(),
    };
    let (prepared, _guard) = provider
        .native_prepare_commit(request.clone(), crate::MAX_RECORD_BYTES, meter.original())
        .await
        .unwrap()
        .unwrap()
        .into_parts();
    assert!(matches!(
        prepared.finish(body).await.unwrap(),
        Err(StreamError::InvalidArgument)
    ));
    assert_eq!(provider.tail(target.clone()).await.unwrap(), 1);
    assert!(
        provider
            .inspect_idempotency(request.idempotency_key.clone())
            .await
            .unwrap()
            .is_none()
    );
    let (prepared, _guard) = provider
        .native_prepare_commit(request, crate::MAX_RECORD_BYTES, meter.original())
        .await
        .unwrap()
        .unwrap()
        .into_parts();
    assert!(matches!(
        prepared.finish(changes()).await.unwrap().unwrap().value(),
        crate::CommitOutcome::Committed(_)
    ));
    assert_eq!(provider.tail(target).await.unwrap(), 2);
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn memory_prepared_commit_rejects_a_forged_change_index_before_publication() {
    prepared_ingress(&MemoryStream::default()).await;
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn local_prepared_commit_rejects_a_forged_change_index_before_publication() {
    let directory = tempfile::tempdir().unwrap();
    let provider = crate::LocalStream::open(directory.path(), crate::LocalStreamLimits::default())
        .await
        .unwrap();
    prepared_ingress(&provider).await;
    drop(provider);
    let reopened = crate::LocalStream::open(directory.path(), crate::LocalStreamLimits::default())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .tail(StreamPath::new("changes-target").unwrap())
            .await
            .unwrap(),
        2
    );
}

async fn qualify(provider: &impl StreamProvider) {
    let path = StreamPath::new("changes").unwrap();
    let body = changes();
    provider
        .append(crate::AppendRequest {
            path: path.clone(),
            records: vec![body.clone()],
            if_tail: None,
            idempotency_key: None,
        })
        .await
        .unwrap();
    let mut history = provider.follow(path.clone(), 0, filter()).await.unwrap();
    let first = history.next().await.unwrap().unwrap();
    assert_eq!(
        decode(&first.value, &first.headers).unwrap().entries.len(),
        1
    );
    let mut live = provider
        .follow_checked(path.clone(), 1, first.commit_id, filter())
        .await
        .unwrap();
    assert!(futures::poll!(live.next()).is_pending());
    provider
        .append(crate::AppendRequest {
            path: path.clone(),
            records: vec![body],
            if_tail: None,
            idempotency_key: None,
        })
        .await
        .unwrap();
    let second = live.next().await.unwrap().unwrap();
    assert_eq!(
        decode(&second.value, &second.headers)
            .unwrap()
            .entries
            .len(),
        1
    );
    assert_eq!(second.sequence, 1);
    let mut resumed = provider
        .follow_checked(path, 1, first.commit_id, filter())
        .await
        .unwrap();
    assert_eq!(resumed.next().await.unwrap().unwrap(), second);
}

#[tokio::test]
async fn memory_projects_both_history_and_live_checked_follows() {
    qualify(&MemoryStream::default()).await;
}

#[cfg(feature = "local")]
#[tokio::test]
async fn local_projects_both_history_and_live_checked_follows() {
    let directory = tempfile::tempdir().unwrap();
    let provider = crate::LocalStream::open(directory.path(), crate::LocalStreamLimits::default())
        .await
        .unwrap();
    qualify(&provider).await;
}

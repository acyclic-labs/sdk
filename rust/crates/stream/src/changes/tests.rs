use futures::TryFutureExt as _;
use super::*;
use crate::{FollowFilter, MemoryStream, StreamPath, StreamProvider};
use futures::StreamExt;

pub(super) fn filter() -> FollowFilter {
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

pub(super) fn changes() -> AppendRecord {
    AppendRecord::changes(
        b"publication-identity",
        &["b", "z"]
            .into_iter()
            .map(|key| wire::ChangeEntry {
                key: key.into(),
                operation: "write".into(),
                metadata: key.into(),
            })
            .collect::<Vec<_>>(),
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

/// The value `changes()` would have with its first key replaced, under the
/// original headers: a forged index.
fn forged() -> AppendRecord {
    let mut body = changes();
    let mut value = decode(&body.value, &body.headers).unwrap();
    value.entries.first_mut().unwrap().key = "a".into();
    body.value = AppendRecord::changes(&value.context, &value.entries, vec![])
        .unwrap()
        .value;
    body
}

#[test]
fn malformed_typed_lists_never_disagree_with_the_immutable_index() {
    let body = forged();
    assert_eq!(
        validate(&body.value, &body.headers),
        Err(StreamError::InvalidArgument)
    );
    assert!(crate::request::validate_records(std::slice::from_ref(&body)).is_err());
    assert!(AppendRecord::changes(&[], &[], vec![]).is_err());
}

/// No wire or persisted form carries a change record's keys: every ingress
/// derives them from the value, so a disagreeing copy cannot cross one.
#[test]
fn every_wire_form_stores_each_key_once_and_derives_the_index() {
    let body = changes();
    let wire = crate::wire_codec::append_record_wire(body.clone());
    assert!(wire.keys.is_empty());
    assert_eq!(crate::wire_codec::append_record(wire).unwrap(), body);
    let forged =
        crate::wire_codec::append_record(crate::wire_codec::append_record_wire(forged())).unwrap();
    assert_eq!(
        forged
            .headers
            .keys()
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&[u8]>>(),
        [b"a".as_slice(), b"z"],
        "the index is the value's, never the forged headers'"
    );
    // A wire record that carries keys beside a change value is a second copy.
    let mut doubled = crate::wire_codec::append_record_wire(body);
    doubled.keys = vec![
        bytes::Bytes::from_static(b"b"),
        bytes::Bytes::from_static(b"z"),
    ];
    assert_eq!(
        crate::wire_codec::append_record(doubled),
        Err(StreamError::InvalidArgument)
    );
    // The negative control: an opaque record keeps its keys on the wire.
    let opaque = AppendRecord {
        value: b"opaque".as_slice().into(),
        headers: RecordHeaders::new(vec![b"k".as_slice().into()], vec![]).unwrap(),
    };
    assert_eq!(crate::wire_codec::append_record_wire(opaque).keys.len(), 1);
}

#[test]
fn keys_share_their_prefixes_and_metadata_is_stored_once() {
    let long = |suffix: &str| format!(".system/harness-uploads/{suffix}");
    let record = AppendRecord::changes(
        &[],
        &[long("a.json"), long("b.json")]
            .into_iter()
            .map(|key| wire::ChangeEntry {
                key: key.into(),
                operation: "create".into(),
                metadata: bytes::Bytes::new(),
            })
            .collect::<Vec<_>>(),
        vec![],
    )
    .unwrap();
    // context(1) count(1); then shared(1) suffix(1+30) op(1+6) meta(1), and
    // shared(1) suffix(1+6) op(1+6) meta(1): the second key stores 6 bytes.
    assert_eq!(record.value.len(), 2 + 1 + 31 + 7 + 1 + 1 + 7 + 7 + 1);
    let decoded = decode_changes(&record.value).unwrap();
    assert_eq!(
        decoded.entries.get(1).unwrap().key.as_ref(),
        long("b.json").as_bytes()
    );
}

#[test]
fn canonical_change_validation_rejects_wire_malleability() {
    let headers = changes().headers;
    let canonical = changes().value.to_vec();
    assert_eq!(validate(&canonical, &headers), Ok(()));
    let mut cases: Vec<Vec<u8>> = Vec::new();
    // A nonminimal context length.
    let (first, rest) = canonical.split_first().unwrap();
    let mut nonminimal = vec![0x80 | first, 0x00];
    nonminimal.extend_from_slice(rest);
    cases.push(nonminimal);
    // Trailing bytes, and a truncated list.
    cases.push([canonical.as_slice(), b"\0"].concat());
    cases.push(canonical.split_last().unwrap().1.to_vec());
    // No entries, and more entries than the value holds.
    cases.push(b"\x00\x00".to_vec());
    cases.push(b"\x00\x02\x00\x01b\x05write\x00".to_vec());
    // A first entry that claims a shared prefix, a shared length shorter
    // than the true common prefix, keys out of order, an equal key, and an
    // empty or control-character operation.
    for value in [
        b"\x00\x01\x01\x01b\x05write\x00".as_slice(),
        b"\x00\x02\x00\x02ab\x05write\x00\x00\x02ac\x05write\x00",
        b"\x00\x02\x00\x01b\x05write\x00\x00\x01a\x05write\x00",
        b"\x00\x02\x00\x01b\x05write\x00\x01\x00\x05write\x00",
        b"\x00\x01\x00\x01b\x00\x00",
        b"\x00\x01\x00\x01b\x02w\n\x00",
    ] {
        cases.push(value.to_vec());
    }
    for bytes in cases {
        assert!(decode_changes(&bytes).is_err(), "{bytes:?}");
        assert_eq!(
            validate(&bytes, &headers),
            Err(StreamError::InvalidArgument),
            "{bytes:?}"
        );
    }
    // The negative control: the same shapes written canonically decode.
    for value in [
        b"\x00\x01\x00\x01b\x05write\x00".as_slice(),
        b"\x00\x02\x00\x02ab\x05write\x00\x01\x01c\x05write\x00",
    ] {
        assert!(decode_changes(value).is_ok(), "{value:?}");
    }
    let empty_key = AppendRecord::changes(
        &[],
        &[wire::ChangeEntry {
            key: bytes::Bytes::new(),
            operation: "write".into(),
            metadata: bytes::Bytes::new(),
        }],
        vec![],
    )
    .unwrap();
    assert_eq!(validate(&empty_key.value, &empty_key.headers), Ok(()));
    // Any other claimed change format is refused, not read as opaque.
    let stale = RecordHeaders::new(
        vec![],
        vec![Attribute {
            name: ATTRIBUTE.into(),
            value: b"changes/v1".as_slice().into(),
        }],
    )
    .unwrap();
    assert_eq!(validate(b"", &stale), Err(StreamError::InvalidArgument));
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
    let body = forged();
    let request = CommitRequest {
        deadline_unix_millis: None,
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
    let mut history = provider.follow(crate::FollowRequest { path: path.clone(), from: 0, preceding: None, filter: filter(), progress: false }).map_ok(crate::records_only).await.unwrap();
    let first = history.next().await.unwrap().unwrap();
    assert_eq!(
        decode(&first.value, &first.headers).unwrap().entries.len(),
        1
    );
    let mut live = provider
        .follow(crate::FollowRequest { path: path.clone(), from: 1, preceding: Some(first.commit_id), filter: filter(), progress: false }).map_ok(crate::records_only)
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
        .follow(crate::FollowRequest { path: path, from: 1, preceding: Some(first.commit_id), filter: filter(), progress: false }).map_ok(crate::records_only)
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

//! Shared native storage preserves public path boundaries, namespace isolation
//! and opaque record bytes without relaxing canonical public transport decoding.
use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildrenPageRequest, CommitCondition, CommitId, CommitMutation,
    CommitOutcome, CommitRequest, CommittedAppend, CommittedEnvelope, CommittedMutation,
    ForkRequest, IdempotencyKey, IdempotencyObservation, IdempotencyOutcome, MAX_ITEMS,
    MAX_PATH_BYTES, MemoryStream, ReadRequest, Record, StreamError, StreamPath, StreamProvider,
    persistence, request,
};
use bytes::Bytes;
use futures::StreamExt;

#[test]
fn storage_namespace_preserves_every_public_path_boundary_without_becoming_public() {
    let bytes = StreamPath::new("x".repeat(MAX_PATH_BYTES)).expect("maximum public path");
    let segments =
        StreamPath::new(vec!["x"; MAX_ITEMS].join("/")).expect("maximum public segments");
    for path in [bytes, segments, StreamPath::new("parent/child").unwrap()] {
        let scoped = path.qualify_storage(&[17; 32]).unwrap();
        assert_eq!(scoped.unqualify_storage(&[17; 32]).unwrap(), path);
        assert_eq!(StreamPath::from_storage(scoped.as_str()).unwrap(), scoped);
        assert_eq!(
            StreamPath::new(scoped.as_str()),
            Err(StreamError::InvalidPath)
        );
        assert_eq!(
            scoped.unqualify_storage(&[18; 32]),
            Err(StreamError::InvalidPath)
        );
        assert_eq!(
            scoped.qualify_storage(&[17; 32]),
            Err(StreamError::InvalidPath)
        );
        assert_eq!(
            scoped
                .parent()
                .map(|parent| parent.unqualify_storage(&[17; 32]).unwrap()),
            path.parent()
        );
    }
    for invalid in [
        "\u{1f}",
        "\u{1f}garbage\u{1f}path",
        "\u{1f}000000000000000000000000000000000000000000000000000000000000000G\u{1f}path",
    ] {
        assert_eq!(
            StreamPath::from_storage(invalid),
            Err(StreamError::InvalidPath)
        );
    }
    let scoped = StreamPath::new("path")
        .unwrap()
        .qualify_storage(&[17; 32])
        .unwrap();
    let nested = format!("\u{1f}{}\u{1f}{}", "0".repeat(64), scoped.as_str());
    assert_eq!(
        StreamPath::from_storage(nested),
        Err(StreamError::InvalidPath)
    );
    assert_eq!(
        StreamPath::from_storage(format!("{}\u{1f}", scoped.as_str())),
        Err(StreamError::InvalidPath)
    );
}

#[tokio::test]
async fn accounts_with_identical_paths_keep_opaque_records_and_envelopes_isolated() {
    let provider = MemoryStream::default();
    let logical = StreamPath::new("same/events").unwrap();
    let paths = [
        logical.qualify_storage(&[17; 32]).unwrap(),
        logical.qualify_storage(&[18; 32]).unwrap(),
    ];
    let values = [
        Bytes::from_static(b"\0tenant-a\xff"),
        Bytes::from_static(b"\0tenant-b\xfe"),
    ];
    for index in 0..2 {
        let outcome = provider
            .append(AppendRequest {
                path: paths[index].clone(),
                records: vec![values[index].clone()],
                if_tail: Some(0),
                idempotency_key: Some(IdempotencyKey::new(vec![index as u8 + 1]).unwrap()),
            })
            .await
            .unwrap();
        assert!(matches!(outcome, AppendOutcome::Committed(_)));
    }
    for index in 0..2 {
        let records: Vec<_> = provider
            .read(ReadRequest {
                path: paths[index].clone(),
                from: 0,
                limit: 1,
            })
            .await
            .unwrap()
            .collect()
            .await;
        let record = records.into_iter().next().unwrap().unwrap();
        assert_eq!(record.value, values[index]);
        let envelope = CommittedEnvelope {
            commit_id: CommitId::from_bytes([index as u8 + 1; 32]),
            mutations: vec![CommittedMutation::Append(CommittedAppend {
                path: paths[index].clone(),
                start: 0,
                end: 1,
                tail: 1,
                records: vec![Record {
                    commit_id: CommitId::from_bytes([index as u8 + 1; 32]),
                    ..record
                }],
            })],
        };
        let encoded = persistence::encode_envelope(&envelope, 1 << 20).unwrap();
        assert_eq!(
            persistence::decode_storage_envelope(&encoded, 1 << 20).unwrap(),
            envelope
        );
        assert!(persistence::decode_envelope(&encoded, 1 << 20).is_err());
        let observation = IdempotencyObservation {
            idempotency_key: IdempotencyKey::new(vec![index as u8 + 1]).unwrap(),
            request_digest: [index as u8 + 1; 32],
            outcome: IdempotencyOutcome::Commit(CommitOutcome::Committed(envelope)),
        };
        let encoded = persistence::encode_observation(&observation, 1 << 20).unwrap();
        assert_eq!(
            persistence::decode_storage_observation(&encoded, 1 << 20).unwrap(),
            observation
        );
        assert!(persistence::decode_observation(&encoded, 1 << 20).is_err());
    }
}

#[tokio::test]
async fn qualified_paths_preserve_fork_atomic_commit_children_and_retained_retry_facts() {
    let provider = MemoryStream::default();
    let namespace = [23; 32];
    let parent = StreamPath::new("tree").unwrap();
    let maximum = StreamPath::new(format!("tree/{}", "x".repeat(MAX_PATH_BYTES - 5))).unwrap();
    let source = maximum.qualify_storage(&namespace).unwrap();
    let fork = StreamPath::new("tree/fork")
        .unwrap()
        .qualify_storage(&namespace)
        .unwrap();
    let first = Bytes::from_static(b"\0original\xff");
    provider
        .append(AppendRequest {
            path: source.clone(),
            records: vec![first.clone()],
            if_tail: Some(0),
            idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"first")).unwrap()),
        })
        .await
        .unwrap();
    let fork_request = ForkRequest {
        source: source.clone(),
        destination: fork.clone(),
        at_tail: Some(1),
        idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"fork")).unwrap()),
    };
    let original_fork = provider.fork(fork_request.clone()).await.unwrap();
    assert_eq!(provider.fork(fork_request).await.unwrap(), original_fork);
    let page = provider
        .children_page(ChildrenPageRequest {
            parent: Some(parent.qualify_storage(&namespace).unwrap()),
            after: None,
            hierarchy_version: None,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(page.children.len(), 1);
    let next = provider
        .children_page(ChildrenPageRequest {
            parent: Some(parent.qualify_storage(&namespace).unwrap()),
            after: page.next_after.clone(),
            hierarchy_version: Some(page.hierarchy_version),
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(next.children.len(), 1);
    let mut logical_children = vec![
        page.children[0].path.unqualify_storage(&namespace).unwrap(),
        next.children[0].path.unqualify_storage(&namespace).unwrap(),
    ];
    logical_children.sort();
    assert_eq!(
        logical_children,
        vec![StreamPath::new("tree/fork").unwrap(), maximum]
    );
    let key = IdempotencyKey::new(Bytes::from_static(b"atomic")).unwrap();
    let mut commit = CommitRequest {
        conditions: vec![
            CommitCondition::Tail {
                path: source.clone(),
                expected: 1,
            },
            CommitCondition::Tail {
                path: fork.clone(),
                expected: 1,
            },
        ],
        mutations: vec![
            CommitMutation::Append {
                path: source.clone(),
                records: vec![Bytes::from_static(b"source-next")],
            },
            CommitMutation::Append {
                path: fork.clone(),
                records: vec![Bytes::from_static(b"fork-next")],
            },
        ],
        idempotency_key: key.clone(),
    };
    let digest = request::commit_digest(&mut commit).unwrap();
    let outcome = provider.commit(commit.clone()).await.unwrap();
    let CommitOutcome::Committed(envelope) = &outcome else {
        panic!("atomic commit refused");
    };
    assert_eq!(provider.commit(commit).await.unwrap(), outcome);
    assert_eq!(
        provider.read_commit(envelope.commit_id).await.unwrap(),
        *envelope
    );
    let retained = provider
        .inspect_idempotency(key.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.idempotency_key, key);
    assert_eq!(retained.request_digest, digest);
    assert_eq!(retained.outcome, IdempotencyOutcome::Commit(outcome));
    for (path, next) in [
        (source.clone(), Bytes::from_static(b"source-next")),
        (fork.clone(), Bytes::from_static(b"fork-next")),
    ] {
        let records: Vec<_> = provider
            .read(ReadRequest {
                path,
                from: 0,
                limit: 2,
            })
            .await
            .unwrap()
            .collect()
            .await;
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].as_ref().unwrap().value, first);
        assert_eq!(records[1].as_ref().unwrap().value, next);
    }
    let refused = provider
        .commit(CommitRequest {
            conditions: vec![
                CommitCondition::Tail {
                    path: source.clone(),
                    expected: 0,
                },
                CommitCondition::Tail {
                    path: fork.clone(),
                    expected: 2,
                },
            ],
            mutations: vec![
                CommitMutation::Append {
                    path: source.clone(),
                    records: vec![Bytes::from_static(b"must-not-append")],
                },
                CommitMutation::Append {
                    path: fork.clone(),
                    records: vec![Bytes::from_static(b"must-not-append")],
                },
            ],
            idempotency_key: IdempotencyKey::new(Bytes::from_static(b"conflict")).unwrap(),
        })
        .await
        .unwrap();
    assert!(matches!(refused, CommitOutcome::Conflict(_)));
    assert_eq!(provider.tail(source).await.unwrap(), 2);
    assert_eq!(provider.tail(fork).await.unwrap(), 2);
}

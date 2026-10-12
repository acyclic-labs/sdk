
use super::*;
use crate::{AppendRequest, IdempotencyObservation, MemoryLimits, ReadRequest, StreamProvider};
use crate::wire_codec::observation_wire;
use futures::StreamExt as _;
use prost::Message as _;
use std::sync::atomic::{AtomicU64, Ordering};

#[tokio::test]
async fn snapshots_retain_retry_identity_at_capacity() -> Result<(), StreamError> {
    let limits = MemoryLimits {
        idempotency_results: 1,
        ..MemoryLimits::default()
    };
    let stream = MemoryStream::new(limits);
    let request = AppendRequest {
        path: StreamPath::new("snapshot/retained")?,
        records: vec![OwnedBytes::from_static(b"once")],
        if_tail: None,
        idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"snapshot-once"))?),
    };
    let first = stream.append(request.clone()).await?;
    let restored = MemoryStream::new(limits);
    restored.install_state(&stream.encode_state(None).await.expect("unmetered snapshot encoding").value).await?;
    assert_eq!(restored.append(request.clone()).await?, first);
    assert_eq!(restored.tail(request.path.clone()).await?, 1);
    let mut changed = request.clone();
    changed.records = vec![OwnedBytes::from_static(b"different")];
    assert_eq!(
        restored.append(changed).await,
        Err(StreamError::IdempotencyMismatch)
    );
    let mut fresh = request;
    fresh.idempotency_key = Some(IdempotencyKey::new(OwnedBytes::from_static(b"fresh"))?);
    assert_eq!(restored.append(fresh).await, Err(StreamError::Capacity));
    Ok(())
}

#[tokio::test]
async fn real_append_fork_and_commit_snapshots_preserve_exclusive_tails_and_retries()
-> Result<(), StreamError> {
    let stream = MemoryStream::default();
    let source = StreamPath::new("roundtrip/source")?;
    let destination = StreamPath::new("roundtrip/fork")?;
    let appended = AppendRequest {
        path: source.clone(),
        records: vec![OwnedBytes::from_static(b"first"), OwnedBytes::from_static(b"second")],
        if_tail: Some(0),
        idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"append"))?),
    };
    let append_result = stream.append(appended.clone()).await?;
    let forked = crate::ForkRequest {
        source: source.clone(),
        destination: destination.clone(),
        at_tail: Some(1),
        idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"fork"))?),
    };
    let fork_result = stream.fork(forked.clone()).await?;
    let committed = crate::CommitRequest {
        conditions: vec![crate::CommitCondition::Tail {
            path: destination.clone(),
            expected: 1,
        }],
        mutations: vec![crate::CommitMutation::Append {
            path: destination.clone(),
            records: vec![OwnedBytes::from_static(b"third")],
        }],
        idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"commit"))?,
    };
    let commit_result = stream.commit(committed.clone()).await?;
    let restored = MemoryStream::default();
    restored.install_state(&stream.encode_state(None).await.expect("unmetered snapshot encoding").value).await?;
    assert_eq!(restored.append(appended).await?, append_result);
    assert_eq!(restored.fork(forked).await?, fork_result);
    assert_eq!(restored.commit(committed).await?, commit_result);
    for (path, expected) in [
        (source, vec![b"first".as_slice(), b"second".as_slice()]),
        (destination, vec![b"first".as_slice(), b"third".as_slice()]),
    ] {
        assert_eq!(restored.tail(path.clone()).await?, 2);
        let records = restored
            .read(ReadRequest {
                path,
                from: 0,
                limit: 3,
            })
            .await?
            .collect::<Vec<_>>()
            .await;
        assert_eq!(records.len(), 2);
        for (index, (record, expected)) in records.into_iter().zip(expected).enumerate() {
            let record = record?;
            assert_eq!(
                record.sequence,
                u64::try_from(index).map_err(|_| StreamError::LimitExceeded)?
            );
            assert_eq!(record.value.as_ref(), expected);
        }
    }
    Ok(())
}

#[tokio::test]
async fn current_writer_empty_forks_and_atomic_source_changes_roundtrip()
-> Result<(), StreamError> {
    let stream = MemoryStream::default();
    let root = StreamPath::new("writer/root")?;
    let parent = StreamPath::new("writer")?;
    let no_history = StreamPath::new("writer/no-history")?;
    let source = StreamPath::new("writer/source")?;
    let empty = StreamPath::new("writer/empty")?;
    let destination = StreamPath::new("writer/destination")?;
    let append = |path: StreamPath, value: &'static [u8]| AppendRequest {
        path,
        records: vec![OwnedBytes::from_static(value)],
        if_tail: None,
        idempotency_key: None,
    };
    stream.append(append(root.clone(), b"root")).await?;
    stream
        .fork(crate::ForkRequest {
            source: parent.clone(),
            destination: no_history.clone(),
            at_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    stream
        .append(append(parent.clone(), b"parent-later"))
        .await?;
    stream
        .fork(crate::ForkRequest {
            source: root.clone(),
            destination: source.clone(),
            at_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    stream
        .fork(crate::ForkRequest {
            source: source.clone(),
            destination: empty.clone(),
            at_tail: None,
            idempotency_key: None,
        })
        .await?;
    stream.append(append(source.clone(), b"before")).await?;
    let request = crate::CommitRequest {
        conditions: vec![
            crate::CommitCondition::Tail {
                path: source.clone(),
                expected: 1,
            },
            crate::CommitCondition::Absent {
                path: destination.clone(),
            },
        ],
        mutations: vec![
            crate::CommitMutation::Append {
                path: source.clone(),
                records: vec![OwnedBytes::from_static(b"after")],
            },
            crate::CommitMutation::Fork {
                source: source.clone(),
                destination: destination.clone(),
                at_tail: 1,
                records: vec![OwnedBytes::from_static(b"forked")],
            },
        ],
        idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"atomic-fork"))?,
    };
    let outcome = stream.commit(request.clone()).await?;
    let restored = MemoryStream::default();
    restored.install_state(&stream.encode_state(None).await.expect("unmetered snapshot encoding").value).await?;
    assert_eq!(restored.commit(request).await?, outcome);
    assert_eq!(restored.tail(empty).await?, 0);
    assert_eq!(restored.tail(no_history).await?, 0);
    for (path, expected) in [
        (root, vec![b"root".as_slice()]),
        (parent, vec![b"parent-later".as_slice()]),
        (source, vec![b"before".as_slice(), b"after".as_slice()]),
        (
            destination,
            vec![b"before".as_slice(), b"forked".as_slice()],
        ),
    ] {
        assert_eq!(
            restored.tail(path.clone()).await?,
            u64::try_from(expected.len()).map_err(|_| StreamError::LimitExceeded)?
        );
        let records = restored
            .read(ReadRequest {
                path,
                from: 0,
                limit: 3,
            })
            .await?
            .collect::<Vec<_>>()
            .await;
        assert_eq!(records.len(), expected.len());
        for (record, expected) in records.into_iter().zip(expected) {
            assert_eq!(record?.value.as_ref(), expected);
        }
    }
    Ok(())
}

#[tokio::test]
async fn malformed_and_over_capacity_snapshots_leave_live_state_unchanged()
-> Result<(), StreamError> {
    let stream = MemoryStream::new(MemoryLimits::default());
    stream
        .append(AppendRequest {
            path: StreamPath::new("snapshot/atomic")?,
            records: vec![OwnedBytes::from_static(b"kept")],
            if_tail: None,
            idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"kept"))?),
        })
        .await?;
    let valid = stream.encode_state(None).await.expect("unmetered snapshot encoding").value;
    for length in 0..valid.len() {
        assert_eq!(
            stream.install_state(&valid[..length]).await,
            Err(StreamError::InvalidArgument)
        );
        assert_eq!(stream.encode_state(None).await.expect("unmetered snapshot encoding").value, valid);
    }
    let mut impossible_decision = valid.clone();
    impossible_decision[MAGIC.len()..MAGIC.len() + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    assert_eq!(
        stream.install_state(&impossible_decision).await,
        Err(StreamError::InvalidArgument)
    );
    let mut trailing = valid.clone();
    trailing.push(0);
    assert_eq!(
        stream.install_state(&trailing).await,
        Err(StreamError::InvalidArgument)
    );
    let mut wrong_magic = valid.clone();
    if let Some(first) = wrong_magic.first_mut() {
        *first ^= 1;
    }
    assert_eq!(
        stream.install_state(&wrong_magic).await,
        Err(StreamError::InvalidArgument)
    );
    // Reject obsolete version markers explicitly, without changing live state.
    for version in *b"024" {
        let mut obsolete = valid.clone();
        obsolete[MAGIC.len() - 2] = version;
        assert_eq!(
            stream.install_state(&obsolete).await,
            Err(StreamError::InvalidArgument)
        );
        assert_eq!(stream.encode_state(None).await.expect("unmetered snapshot encoding").value, valid);
    }
    for limits in [
        MemoryLimits {
            paths: 0,
            ..MemoryLimits::default()
        },
        MemoryLimits {
            path_bytes: 0,
            ..MemoryLimits::default()
        },
        MemoryLimits {
            records: 0,
            ..MemoryLimits::default()
        },
        MemoryLimits {
            payload_bytes: 0,
            ..MemoryLimits::default()
        },
        MemoryLimits {
            commits: 0,
            ..MemoryLimits::default()
        },
        MemoryLimits {
            idempotency_results: 0,
            ..MemoryLimits::default()
        },
    ] {
        let constrained = MemoryStream::new(limits);
        let empty = constrained.encode_state(None).await.expect("unmetered snapshot encoding").value;
        assert_eq!(
            constrained.install_state(&valid).await,
            Err(StreamError::Capacity)
        );
        assert_eq!(constrained.encode_state(None).await.expect("unmetered snapshot encoding").value, empty);
    }
    assert_eq!(stream.encode_state(None).await.expect("unmetered snapshot encoding").value, valid);
    Ok(())
}

#[test]
fn current_codec_rejects_duplicate_entries_and_impossible_history() {
    let header = || {
        let mut bytes = MAGIC.to_vec();
        put_u64(&mut bytes, 0);
        put_u64(&mut bytes, 0);
        bytes.extend_from_slice(CommitId::from_bytes([0; 32]).as_bytes());
        bytes
    };
    let mut duplicate = header();
    put_len(&mut duplicate, 0);
    put_len(&mut duplicate, 2);
    for _ in 0..2 {
        put_bytes(&mut duplicate, b"duplicate");
        put_len(&mut duplicate, 0);
        put_u64(&mut duplicate, 0);
    }
    put_len(&mut duplicate, 0);
    assert!(decode(&duplicate).is_none());
    let mut impossible = header();
    put_len(&mut impossible, 1);
    impossible.push(NODE_PREFIX);
    put_len(&mut impossible, 0);
    put_u64(&mut impossible, 1);
    assert!(decode(&impossible).is_none());
    let mut unknown_flags = header();
    put_len(&mut unknown_flags, 0);
    put_len(&mut unknown_flags, 0);
    put_len(&mut unknown_flags, 1);
    unknown_flags.push(4);
    assert!(decode(&unknown_flags).is_none());
}

#[test]
fn current_codec_rejects_duplicate_retry_identity_and_unknown_wire_fields()
-> Result<(), StreamError> {
    let observation = observation_wire(IdempotencyObservation {
        idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"conflict"))?,
        request_digest: [0; 32],
        outcome: crate::IdempotencyOutcome::Append(crate::AppendOutcome::TailConflict {
            actual_tail: 0,
        }),
    })
    .encode_to_vec();
    let fixture = |count: usize, observation: &[u8]| {
        let mut encoded = MAGIC.to_vec();
        put_u64(&mut encoded, 0);
        put_u64(&mut encoded, 0);
        encoded.extend_from_slice(CommitId::from_bytes([0; 32]).as_bytes());
        put_len(&mut encoded, 0);
        put_len(&mut encoded, 0);
        put_len(&mut encoded, count);
        for _ in 0..count {
            encoded.push(RETAINS_REPLAY);
            put_bytes(&mut encoded, observation);
        }
        encoded
    };
    assert!(decode(&fixture(1, &observation)).is_some());
    assert!(decode(&fixture(2, &observation)).is_none());
    let mut unknown = observation;
    unknown.extend_from_slice(&[0xa0, 0x06, 0x01]);
    assert!(decode(&fixture(1, &unknown)).is_none());
    Ok(())
}

#[tokio::test]
async fn retained_facts_reject_impossible_receipts_records_and_phantom_payloads()
-> Result<(), StreamError> {
    let stream = MemoryStream::default();
    let key = IdempotencyKey::new(OwnedBytes::from_static(b"inventory"))?;
    stream
        .append(AppendRequest {
            path: StreamPath::new("inventory/path")?,
            records: vec![OwnedBytes::from_static(b"real")],
            if_tail: Some(0),
            idempotency_key: Some(key.clone()),
        })
        .await?;
    let mut state = stream.state.write().await;
    let cached = state.replays.get_mut(key.as_bytes()).ok_or(StreamError::Unavailable)?;
    *cached = Arc::new(Replay {
        digest: cached.digest, result: cached.result.clone(), envelope: cached.envelope.clone(),
        _allocation: crate::memory::Allocation::default(),
    });
    let replay = state
        .replays
        .get_mut(key.as_bytes())
        .and_then(Arc::get_mut)
        .ok_or(StreamError::Unavailable)?;
    let original_result = replay.result.clone();
    let crate::IdempotencyOutcome::Append(crate::AppendOutcome::Committed(receipt)) =
        &mut replay.result
    else {
        return Err(StreamError::Unavailable);
    };
    receipt.start = receipt.end;
    assert!(decode(&encode(&state, None).expect("unmetered encoding cannot fail").value).is_none());
    state
        .replays
        .get_mut(key.as_bytes())
        .and_then(Arc::get_mut)
        .ok_or(StreamError::Unavailable)?
        .result = original_result.clone();
    let replay = state
        .replays
        .get_mut(key.as_bytes())
        .and_then(Arc::get_mut)
        .ok_or(StreamError::Unavailable)?;
    let crate::IdempotencyOutcome::Append(crate::AppendOutcome::Committed(receipt)) =
        &mut replay.result
    else {
        return Err(StreamError::Unavailable);
    };
    receipt.start = 1;
    receipt.end = 2;
    receipt.tail = 2;
    assert!(decode(&encode(&state, None).expect("unmetered encoding cannot fail").value).is_none());
    state
        .replays
        .get_mut(key.as_bytes())
        .and_then(Arc::get_mut)
        .ok_or(StreamError::Unavailable)?
        .result = original_result;
    let commit_id = *state
        .commits
        .keys()
        .next()
        .ok_or(StreamError::Unavailable)?;
    let envelope = state
        .commits
        .get_mut(&commit_id)
        .and_then(Arc::get_mut).and_then(Replay::envelope_mut)
        .ok_or(StreamError::Unavailable)?;
    let Some(crate::CommittedMutation::Append(append)) = envelope.mutations.first_mut() else {
        return Err(StreamError::Unavailable);
    };
    let record = append.records.first_mut().ok_or(StreamError::Unavailable)?;
    record.sequence = 9;
    assert!(decode(&encode(&state, None).expect("unmetered encoding cannot fail").value).is_none());
    let envelope = state
        .commits
        .get_mut(&commit_id)
        .and_then(Arc::get_mut).and_then(Replay::envelope_mut)
        .ok_or(StreamError::Unavailable)?;
    let Some(crate::CommittedMutation::Append(append)) = envelope.mutations.first_mut() else {
        return Err(StreamError::Unavailable);
    };
    let record = append.records.first_mut().ok_or(StreamError::Unavailable)?;
    record.sequence = 0;
    record.value = OwnedBytes::from_static(b"phantom payload absent from immutable history");
    assert!(decode(&encode(&state, None).expect("unmetered encoding cannot fail").value).is_none());
    Ok(())
}

#[tokio::test]
async fn retained_empty_forks_cannot_reference_future_destinations() -> Result<(), StreamError>
{
    let stream = MemoryStream::default();
    let root = StreamPath::new("chronology")?;
    let first = StreamPath::new("chronology/first")?;
    let later = StreamPath::new("chronology/later")?;
    stream
        .append(AppendRequest {
            path: StreamPath::new("chronology/seed")?,
            records: vec![OwnedBytes::from_static(b"seed")],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    let receipt = stream
        .fork(crate::ForkRequest {
            source: root,
            destination: first.clone(),
            at_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    stream
        .fork(crate::ForkRequest {
            source: first,
            destination: later.clone(),
            at_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    assert!(decode(&stream.encode_state(None).await.expect("unmetered snapshot encoding").value).is_some());
    let mut state = stream.state.write().await;
    let envelope = state
        .commits
        .get_mut(&receipt.commit_id)
        .and_then(Arc::get_mut).and_then(Replay::envelope_mut)
        .ok_or(StreamError::Unavailable)?;
    let Some(crate::CommittedMutation::Fork(fork)) = envelope.mutations.first_mut() else {
        return Err(StreamError::Unavailable);
    };
    fork.source = later;
    assert!(decode(&encode(&state, None).expect("unmetered encoding cannot fail").value).is_none());
    Ok(())
}

#[tokio::test]
async fn retained_fork_cannot_capture_a_future_source_head() -> Result<(), StreamError> {
    let stream = MemoryStream::default();
    let source = StreamPath::new("chronology/source")?;
    let destination = StreamPath::new("chronology/destination")?;
    let append = |value: &'static [u8]| AppendRequest {
        path: source.clone(),
        records: vec![OwnedBytes::from_static(value)],
        if_tail: None,
        idempotency_key: None,
    };
    stream.append(append(b"before")).await?;
    stream
        .fork(crate::ForkRequest {
            source: source.clone(),
            destination: destination.clone(),
            at_tail: Some(1),
            idempotency_key: None,
        })
        .await?;
    stream.append(append(b"future")).await?;
    assert!(decode(&stream.encode_state(None).await.expect("unmetered snapshot encoding").value).is_some());
    let mut state = stream.state.write().await;
    let future = state
        .paths
        .get(&source)
        .ok_or(StreamError::Unavailable)?
        .history
        .clone();
    state
        .paths
        .get_mut(&destination)
        .ok_or(StreamError::Unavailable)?
        .history = Some(Arc::new(History::Prefix {
        source: future,
        tail: 1,
        _allocation: super::super::Allocation::default(),
    }));
    assert!(decode(&encode(&state, None).expect("unmetered encoding cannot fail").value).is_none());
    Ok(())
}

#[test]
fn malformed_deep_history_releases_without_recursive_drop() {
    let mut encoded = MAGIC.to_vec();
    put_u64(&mut encoded, 0);
    put_u64(&mut encoded, 0);
    encoded.extend_from_slice(CommitId::from_bytes([0; 32]).as_bytes());
    put_len(&mut encoded, 100_001);
    for index in 0..100_000 {
        encoded.push(NODE_PREFIX);
        put_len(&mut encoded, index);
        put_u64(&mut encoded, 0);
    }
    encoded.push(255);
    put_len(&mut encoded, 100_000);
    assert!(decode(&encoded).is_none());
}

#[tokio::test]
async fn snapshots_preserve_committed_timestamps() -> Result<(), StreamError> {
    let clock = Arc::new(AtomicU64::default());
    clock.store(100, Ordering::SeqCst);
    let stream = MemoryStream::new_with_clock(MemoryLimits::default(), clock.clone());
    let path = StreamPath::new("snapshot/timed")?;
    stream
        .append(AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"one")],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    let timed = stream.encode_state(None).await.expect("unmetered snapshot encoding").value;
    clock.store(90, Ordering::SeqCst);
    let restored = MemoryStream::new_with_clock(MemoryLimits::default(), clock);
    restored.install_state(&timed).await?;
    let timed_record = restored
        .read(ReadRequest {
            path: path.clone(),
            from: 0,
            limit: 1,
        })
        .await?
        .next()
        .await
        .ok_or(StreamError::InvalidArgument)??;
    assert_eq!(timed_record.committed_at_micros, 100_000);
    let path = StreamPath::new("snapshot/after-rollback")?;
    restored
        .append(AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"two")],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    let next = restored
        .read(ReadRequest {
            path,
            from: 0,
            limit: 1,
        })
        .await?
        .next()
        .await
        .ok_or(StreamError::InvalidArgument)??;
    assert_eq!(next.committed_at_micros, 100_000);
    Ok(())
}

use super::*;
use crate::{
    AppendRequest, CommitCondition, CommitConflict, CommitMutation, CommitRequest, ForkRequest,
    IdempotencyKey, MemoryStream, StreamProvider,
};
use bytes::Bytes;

const BOUND: usize = 1024 * 1024;

fn key(value: &'static [u8]) -> Result<IdempotencyKey, StreamError> {
    IdempotencyKey::new(Bytes::from_static(value))
}

async fn round_trip(
    provider: &MemoryStream,
    key: IdempotencyKey,
) -> Result<(), Box<dyn std::error::Error>> {
    let value = provider
        .inspect_idempotency(key)
        .await?
        .ok_or("missing outcome")?;
    let bytes = encode_observation(&value, BOUND)?;
    assert_eq!(decode_observation(&bytes, bytes.len())?, value);
    assert_eq!(
        bytes,
        wire_codec::observation_wire(value.clone()).encode_to_vec()
    );
    if let IdempotencyOutcome::Commit(CommitOutcome::Committed(envelope)) = &value.outcome {
        let bytes = encode_envelope(envelope, BOUND)?;
        assert_eq!(decode_envelope(&bytes, bytes.len())?, *envelope);
    }
    Ok(())
}

#[tokio::test]
async fn real_provider_receipts_preserve_append_fork_atomic_commit_and_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = MemoryStream::default();
    let source = StreamPath::new("source")?;
    let append = AppendRequest {
        path: source.clone(),
        records: vec![Bytes::from_static(b"one")],
        if_tail: Some(0),
        idempotency_key: Some(key(b"append")?),
    };
    provider.append(append.clone()).await?;
    round_trip(&provider, key(b"append")?).await?;
    let mut conflict = append.clone();
    conflict.idempotency_key = Some(key(b"append-conflict")?);
    provider.append(conflict).await?;
    round_trip(&provider, key(b"append-conflict")?).await?;
    provider
        .fork(ForkRequest {
            source: source.clone(),
            destination: StreamPath::new("copy")?,
            at_tail: Some(1),
            idempotency_key: Some(key(b"fork")?),
        })
        .await?;
    round_trip(&provider, key(b"fork")?).await?;
    let request = CommitRequest {
        conditions: vec![
            CommitCondition::Absent {
                path: StreamPath::new("forked")?,
            },
            CommitCondition::Tail {
                path: source.clone(),
                expected: 1,
            },
        ],
        mutations: vec![
            CommitMutation::Append {
                path: source.clone(),
                records: vec![Bytes::from_static(b"two")],
            },
            CommitMutation::Fork {
                source: source.clone(),
                destination: StreamPath::new("forked")?,
                at_tail: 1,
                records: vec![Bytes::from_static(b"fork-two")],
            },
        ],
        idempotency_key: key(b"commit")?,
    };
    let outcome = provider.commit(request.clone()).await?;
    round_trip(&provider, key(b"commit")?).await?;
    let mut conflict = request.clone();
    conflict.idempotency_key = key(b"commit-conflict")?;
    provider.commit(conflict).await?;
    round_trip(&provider, key(b"commit-conflict")?).await?;
    assert_eq!(provider.commit(request).await?, outcome);
    assert_eq!(
        provider.append(append).await?.clone(),
        match provider
            .inspect_idempotency(key(b"append")?)
            .await?
            .ok_or("missing")?
            .outcome
        {
            IdempotencyOutcome::Append(value) => value,
            _ => return Err("wrong outcome".into()),
        }
    );
    Ok(())
}

fn observation() -> Result<IdempotencyObservation, StreamError> {
    Ok(IdempotencyObservation {
        idempotency_key: key(b"original")?,
        request_digest: [7; 32],
        outcome: IdempotencyOutcome::Commit(CommitOutcome::Conflict(vec![
            CommitConflict::Tail {
                path: StreamPath::new("a")?,
                expected: 2,
                actual: None,
            },
            CommitConflict::Exists {
                path: StreamPath::new("b")?,
            },
        ])),
    })
}

#[test]
fn bounds_and_malformed_persistence_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let value = observation()?;
    let bytes = encode_observation(&value, BOUND)?;
    assert_eq!(decode_observation(&bytes, BOUND)?, value);
    for maximum in [0, bytes.len() - 1] {
        assert_eq!(
            encode_observation(&value, maximum),
            Err(StreamError::LimitExceeded)
        );
        assert_eq!(
            decode_observation(&bytes, maximum),
            Err(StreamError::LimitExceeded)
        );
    }
    for end in 0..bytes.len() {
        assert!(decode_observation(bytes.get(..end).ok_or("invalid cut")?, BOUND).is_err());
    }
    let mut unknown = bytes;
    unknown.extend_from_slice(&[0xa0, 0x06, 1]);
    assert_eq!(
        decode_observation(&unknown, BOUND),
        Err(StreamError::InvalidArgument)
    );
    let mut wire = wire_codec::observation_wire(value);
    wire.request_digest = Bytes::from_static(b"short");
    assert_eq!(
        decode_observation(&wire.encode_to_vec(), BOUND),
        Err(StreamError::InvalidArgument)
    );
    assert_eq!(
        decode_envelope(&[], BOUND),
        Err(StreamError::InvalidArgument)
    );
    Ok(())
}

#[tokio::test]
async fn invalid_record_identity_sequence_time_and_positions_are_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = MemoryStream::default();
    let AppendOutcome::Committed(receipt) = provider
        .append(AppendRequest {
            path: StreamPath::new("source")?,
            records: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            if_tail: None,
            idempotency_key: None,
        })
        .await?
    else {
        return Err("unexpected conflict".into());
    };
    let envelope = provider.read_commit(receipt.commit_id).await?;
    assert_eq!(
        decode_envelope(&encode_envelope(&envelope, BOUND)?, BOUND)?,
        envelope
    );
    for fault in 0..6 {
        let mut broken = envelope.clone();
        let CommittedMutation::Append(append) =
            broken.mutations.first_mut().ok_or("missing mutation")?
        else {
            return Err("not append".into());
        };
        let first = append.records.first_mut().ok_or("missing record")?;
        match fault {
            0 => first.sequence += 1,
            1 => first.commit_id = CommitId::from_bytes([99; 32]),
            2 => first.committed_at_micros += 1,
            3 => append.end = u64::MAX,
            4 => {
                append.records.pop();
            }
            _ => first.value = Bytes::from(vec![0; MAX_RECORD_BYTES + 1]),
        }
        assert_eq!(
            encode_envelope(&broken, BOUND),
            Err(StreamError::InvalidArgument)
        );
        let bytes = wire_codec::envelope_wire(broken).encode_to_vec();
        assert_eq!(
            decode_envelope(&bytes, BOUND),
            Err(StreamError::InvalidArgument)
        );
    }
    Ok(())
}

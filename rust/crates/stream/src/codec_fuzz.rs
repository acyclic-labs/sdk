//! On-demand checks call the same codecs as transport and recovery.
use crate::{StreamError, persistence};

/// Exercise canonical journal commands and immutable retained facts.
pub fn check(bytes: &[u8]) -> bool {
    #[cfg(feature = "local")]
    if !crate::memory::check_snapshot_codec(bytes) {
        return false;
    }
    if let Ok(command) = crate::journal::decode_command(bytes) {
        if command.encode_to_vec() != bytes {
            return false;
        }
    }
    if let Ok(value) = persistence::decode_observation(bytes, usize::MAX) {
        if persistence::encode_observation(&value, usize::MAX).as_deref() != Ok(bytes) {
            return false;
        }
    }
    if let Ok(value) = persistence::decode_envelope(bytes, usize::MAX) {
        if persistence::encode_envelope(&value, usize::MAX).as_deref() != Ok(bytes) {
            return false;
        }
    }
    let _ = crate::changes::decode_changes(bytes);
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    if !crate::local::codec_fuzz::check(bytes) {
        return false;
    }
    true
}

fn commands() -> Result<[(&'static str, crate::WriteRequest); 3], StreamError> {
    use crate::{
        AppendRequest, CommitMutation, CommitRequest, ForkRequest, IdempotencyKey, OwnedBytes,
        StreamPath, WriteRequest,
    };
    Ok([
        (
            "append",
            WriteRequest::Append(AppendRequest {
                path: StreamPath::new("codec/events")?,
                records: vec![OwnedBytes::from_static(b"persisted codec corpus").into()],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"append"))?,
            }),
        ),
        (
            "fork",
            WriteRequest::Fork(ForkRequest {
                source: StreamPath::new("codec/events")?,
                destination: StreamPath::new("codec/fork")?,
                at_tail: Some(1),
                exclude: Vec::new(),
                idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"fork"))?,
            }),
        ),
        (
            "commit",
            WriteRequest::Commit(CommitRequest {
                deadline_unix_millis: None,
                conditions: Vec::new(),
                mutations: vec![CommitMutation::Append {
                    path: StreamPath::new("codec/events")?,
                    records: vec![OwnedBytes::from_static(b"committed codec corpus").into()],
                }],
                idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"commit"))?,
            }),
        ),
    ])
}

/// Capture actual journal and framing encoder output without filesystem IO.
pub async fn seeds() -> Result<Vec<(String, Vec<u8>)>, StreamError> {
    use crate::{StreamProvider as _, WriteRequest};
    let mut seeds = Vec::new();
    let memory = crate::MemoryStream::default();
    for (name, command) in commands()? {
        let key = match &command {
            WriteRequest::Append(request) => request.idempotency_key.clone(),
            WriteRequest::Fork(request) => request.idempotency_key.clone(),
            WriteRequest::Commit(request) => request.idempotency_key.clone(),
        };
        seeds.push((name.to_owned(), command.encode_to_vec()));
        #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
        seeds.push((
            format!("frame-{name}"),
            crate::local::codec_fuzz::frame(&command)?,
        ));
        memory.write(command).await?;
        let observation = memory
            .inspect_idempotency(key)
            .await?
            .ok_or(StreamError::InvalidResponse)?;
        seeds.push((
            format!("observation-{name}"),
            persistence::encode_observation(&observation, usize::MAX)?,
        ));
        let id = match &observation.outcome {
            crate::IdempotencyOutcome::Append(crate::AppendOutcome::Committed(receipt)) => {
                receipt.commit_id
            }
            crate::IdempotencyOutcome::Fork(receipt) => receipt.commit_id,
            crate::IdempotencyOutcome::Commit(envelope) => envelope.commit_id,
            crate::IdempotencyOutcome::Append(crate::AppendOutcome::TailConflict { .. }) => {
                return Err(StreamError::InvalidResponse);
            }
        };
        let envelope = memory.read_commit(id).await?;
        seeds.push((
            format!("envelope-{name}"),
            persistence::encode_envelope(&envelope, usize::MAX)?,
        ));
    }
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    seeds.push(("journal-header".into(), crate::local::codec_fuzz::header()?));
    #[cfg(feature = "local")]
    seeds.push(("snapshot".into(), memory.capture_state().await.encode()));
    let changes = crate::AppendRecord::changes(
        b"codec",
        &[crate::wire::ChangeEntry {
            key: bytes::Bytes::from_static(b"file"),
            operation: "write".into(),
            metadata: bytes::Bytes::from_static(b"metadata"),
        }],
        Vec::new(),
    )?;
    seeds.push(("changes".into(), changes.value.to_vec()));
    Ok(seeds)
}

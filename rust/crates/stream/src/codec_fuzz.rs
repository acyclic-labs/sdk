//! On-demand checks call the same codecs as transport and recovery.
use crate::{StreamError, persistence};
use prost::Message as _;

#[cfg(all(feature = "session", not(target_arch = "wasm32")))]
fn session_admission() -> crate::wire::SessionFrame {
    static CREDENTIAL: std::sync::OnceLock<bytes::Bytes> = std::sync::OnceLock::new();
    crate::wire::SessionFrame {
        body: Some(crate::wire::session_frame::Body::Hello(
            crate::wire::SessionHello {
                version: crate::session::VERSION,
                client_id: bytes::Bytes::from_static(b"codec"),
                destination_node: "node".into(),
                receive_bytes: 1024,
                receive_frames: 2,
                credentials: vec![
                    CREDENTIAL
                        .get_or_init(crate::session::testing::credential)
                        .clone(),
                ],
                interest: Some(crate::wire::SessionInterest {
                    revision: 1,
                    ..Default::default()
                }),
            },
        )),
        ..Default::default()
    }
}

/// Actual SessionHello output, using deterministic public test credentials.
#[cfg(all(feature = "session", not(target_arch = "wasm32")))]
pub fn session_seed() -> Vec<u8> {
    session_admission().encode_to_vec()
}

/// Exercise the real session admission and dispatcher with finite input.
///
/// An arbitrary Hello is the first request, so version and credential refusals
/// cannot be hidden behind successful admission. Other frames follow a real
/// Hello. Closing the request producer must end every source without a timer.
#[cfg(all(feature = "session", not(target_arch = "wasm32")))]
pub fn session(bytes: &[u8]) -> bool {
    use crate::{MemoryStream, session, wire::session_frame::Body};
    use futures::{StreamExt as _, stream};
    let Ok(frame) = crate::wire::SessionFrame::decode(bytes) else {
        return true;
    };
    let version = match &frame.body {
        Some(Body::Hello(hello)) => Some(hello.version),
        _ => None,
    };
    let input = if version.is_some() {
        vec![Ok(frame)]
    } else {
        vec![Ok(session_admission()), Ok(frame)]
    };
    let Ok(output) = session::serve(
        std::sync::Arc::new(MemoryStream::default()),
        session::testing::authority(),
        "node".into(),
        session::Limits::default(),
        stream::iter(input).boxed(),
    ) else {
        return false;
    };
    let output = futures::executor::block_on(output.collect::<Vec<_>>());
    if version.is_some_and(|version| version != session::VERSION) {
        return matches!(output.as_slice(), [Err(StreamError::Unsupported)]);
    }
    true
}

/// Exercise canonical journal commands and immutable retained facts.
pub fn check(bytes: &[u8]) -> bool {
    #[cfg(feature = "local")]
    if !crate::memory::check_snapshot_codec(bytes) {
        return false;
    }
    #[cfg(any(
        feature = "local",
        feature = "postgres",
        all(
            any(feature = "wasm", feature = "wasm-journal"),
            target_arch = "wasm32"
        )
    ))]
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
    if let Ok(changes) = crate::changes::decode_changes(bytes) {
        if !crate::AppendRecord::changes(&changes.context, &changes.entries, Vec::new())
            .is_ok_and(|record| record.value.as_ref() == bytes)
        {
            return false;
        }
    }
    if let Ok(value) = crate::wire::AppendRecord::decode(bytes) {
        if let Ok(record) = crate::wire_codec::append_record(value) {
            if crate::wire_codec::append_record(crate::wire_codec::append_record_wire(
                record.clone(),
            )) != Ok(record)
            {
                return false;
            }
        }
    }
    if let Ok(value) = crate::wire::Record::decode(bytes) {
        let followed = crate::StreamPath::new("codec/events").expect("canonical fixture path");
        for context in [None, Some(&followed)] {
            if let Ok(record) = crate::wire_codec::record(value.clone(), context) {
                if crate::wire_codec::record(
                    crate::wire_codec::record_wire(record.clone(), context),
                    context,
                ) != Ok(record)
                {
                    return false;
                }
            }
        }
    }
    if let Ok(value) = crate::wire::ReadResponse::decode(bytes) {
        if let Ok(records) = crate::wire_codec::read_response_records(value) {
            if crate::wire_codec::read_response_records(crate::wire_codec::read_response_wire(
                records.clone(),
            )) != Ok(records)
            {
                return false;
            }
        }
    }
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
    struct Clock;
    impl crate::UnixMillisClock for Clock {
        fn now_unix_millis(&self) -> u64 {
            42
        }
    }
    let mut seeds = Vec::new();
    let memory = crate::MemoryStream::new_with_clock(
        crate::MemoryLimits::default(),
        std::sync::Arc::new(Clock),
    );
    #[cfg(feature = "local")]
    let memory = memory.with_authority(crate::RevisionScope::new(b"codec-corpus".to_vec()), 7);
    for (name, command) in commands()? {
        let key = match &command {
            WriteRequest::Append(request) => request.idempotency_key.clone(),
            WriteRequest::Fork(request) => request.idempotency_key.clone(),
            WriteRequest::Commit(request) => request.idempotency_key.clone(),
        };
        #[cfg(any(
            feature = "local",
            feature = "postgres",
            all(
                any(feature = "wasm", feature = "wasm-journal"),
                target_arch = "wasm32"
            )
        ))]
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
    {
        let state = memory.capture_state().await.encode();
        #[cfg(not(target_arch = "wasm32"))]
        {
            seeds.push((
                "snapshot-container".into(),
                crate::local::codec_fuzz::snapshot(&state)?,
            ));
            seeds.push((
                "incarnation".into(),
                crate::local::codec_fuzz::incarnation(),
            ));
        }
        seeds.push(("snapshot".into(), state));
    }
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
    seeds.push((
        "changes-append-record".into(),
        crate::wire_codec::append_record_wire(changes.clone()).encode_to_vec(),
    ));
    let record = crate::wire_codec::record_wire(
        crate::Record {
            sequence: 0,
            value: changes.value,
            headers: changes.headers,
            commit_id: crate::CommitId::from_bytes([1; 32]),
            committed_at_micros: 42,
            origin: crate::StreamPath::new("codec/events")?,
        },
        None,
    );
    seeds.push(("changes-record".into(), record.encode_to_vec()));
    seeds.push((
        "record-batch".into(),
        crate::wire_codec::read_response_wire(vec![record]).encode_to_vec(),
    ));
    Ok(seeds)
}

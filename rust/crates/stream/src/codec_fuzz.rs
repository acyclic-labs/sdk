//! On-demand checks call the same codecs as transport and recovery.
use crate::{StreamError, persistence};
use futures::FutureExt as _;
use prost::Message as _;

/// Indexed filesystem selection must retain every path admitted by the original
/// signed credential. Lossless byte inputs use the owning resource encoder.
#[cfg(all(feature = "session", not(target_arch = "wasm32")))]
pub fn filesystem_paths(path: &[u8]) -> Result<(), StreamError> {
    use acyclic_auth::{authority::PolicyDecision, contract::*};
    if path.contains(&0)
        || path.split(|byte| *byte == b'/').any(|part| part.is_empty()) && !path.is_empty()
    {
        return Ok(());
    }
    static CREDENTIAL: std::sync::OnceLock<bytes::Bytes> = std::sync::OnceLock::new();
    let credential = CREDENTIAL.get_or_init(|| {
        crate::session::testing::credential_with_grants(vec![ResourceGrant {
            resource: ResourcePattern {
                name: "fs:/volume".into(),
                prefix: true,
            },
            actions: vec!["fs/list".into()],
        }])
    });
    let mut resource = "fs:/volume".to_owned();
    if !path.is_empty() {
        for component in path.split(|byte| *byte == b'/') {
            resource.push('/');
            acyclic_auth::append_component(component, &mut resource);
        }
    }
    let attributes = RecordAttributes::try_new(vec![]).map_err(|_| StreamError::InvalidArgument)?;
    for suffix in ["", "/"] {
        let authorization = crate::session::testing::resource_authorization(
            credential,
            &format!("{resource}{suffix}"),
        )?;
        let filter = authorization.filter_paths("fs:/volume", &crate::FollowFilter::default())?;
        let mut child = path.to_vec();
        if !child.is_empty() {
            child.push(b'/');
        }
        child.extend_from_slice(b"child");
        let child_resource = format!("{resource}/child");
        for (key, resource) in [
            (path, resource.as_str()),
            (child.as_slice(), &child_resource),
        ] {
            assert!(
                authorization
                    .authorize(resource, &[], &attributes, PolicyDecision::Allow)
                    .is_ok()
            );
            assert!(
                filter.matches(&crate::RecordHeaders::new(
                    vec![
                        key.get(..crate::MAX_RECORD_KEY_BYTES)
                            .unwrap_or(key)
                            .to_vec()
                            .into()
                    ],
                    vec![]
                )?),
                "resource index differs from its original authorized scope"
            );
        }
    }
    Ok(())
}

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

fn truncated_header_is_invalid<T>(
    bytes: &[u8],
    domain: &[u8],
    decoded: &Result<T, StreamError>,
) -> bool {
    !bytes.starts_with(domain)
        || bytes.len() >= domain.len() + persistence::FACT_VERSION.len()
        || matches!(decoded, Err(StreamError::InvalidArgument))
}

#[cfg(any(
    feature = "local",
    feature = "postgres",
    all(
        any(feature = "wasm", feature = "wasm-journal"),
        target_arch = "wasm32"
    )
))]
pub(crate) fn command(bytes: &[u8]) -> bool {
    match crate::journal::decode_command(bytes) {
        Ok(command) => command.encode_to_vec() == bytes,
        Err(_) => true,
    }
}

fn record(value: &crate::wire::Record) -> bool {
    let Ok(followed) = crate::StreamPath::new("codec/events") else {
        return false;
    };
    [None, Some(&followed)].into_iter().all(|context| {
        match crate::wire_codec::record(value.clone(), context) {
            Err(_) => true,
            Ok(record) => {
                crate::wire_codec::record(
                    crate::wire_codec::record_wire(record.clone(), context),
                    context,
                ) == Ok(record.clone())
                    && crate::follow_filter::projection_contract(&record).is_ok()
            }
        }
    })
}

/// Exercise canonical journal commands and immutable retained facts.
pub fn check(bytes: &[u8]) -> bool {
    let value = crate::OwnedBytes::copy_from_slice(bytes);
    if let Ok(text) = core::str::from_utf8(bytes) {
        let expected = crate::TokenOperation::ALL
            .iter()
            .copied()
            .find(|operation| operation.as_str() == text);
        match text.parse::<crate::TokenOperation>() {
            Ok(operation) if Some(operation) == expected => {}
            Err(StreamError::InvalidArgument) if expected.is_none() => {}
            _ => return false,
        }
    }
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
    {
        if !command(bytes) {
            return false;
        }
        // Canonicality after successful decoding cannot detect rejection of
        // valid writer output. Use the same request factory as the corpus,
        // varying its record values and fork position with this input.
        let commands = match commands(Some(value.clone())) {
            Ok(commands) => Some(commands),
            Err(StreamError::LimitExceeded) => None,
            Err(_) => return false,
        };
        for (_, command) in commands.into_iter().flatten() {
            let command = match command.validated() {
                Ok(command) => command,
                Err(StreamError::LimitExceeded) => continue,
                Err(_) => return false,
            };
            if !crate::journal::decode_command(&command.encode_to_vec())
                .is_ok_and(|decoded| decoded == command)
            {
                return false;
            }
        }
    }
    // Obtain the original facts from the public provider, independently of the
    // decoders under test. Accepted-only canonicality misses valid refusals.
    // The provider is private and has no competing lock holders or external
    // producers. A pending operation violates this synchronous oracle's progress
    // contract; reject it immediately rather than waiting on another task.
    let version_change = bytes.first().copied().unwrap_or(0).max(1);
    match publications(Some(value)).now_or_never() {
        Some(Ok((_, publications))) => {
            for publication in publications {
                if !wire_records_admitted(&publication.envelope)
                    || !persisted_fact_admitted(
                        persistence::OBSERVATION_MAGIC,
                        version_change,
                        &publication.observation,
                        persistence::encode_observation,
                        persistence::decode_observation,
                    )
                    || !persisted_fact_admitted(
                        persistence::ENVELOPE_MAGIC,
                        version_change,
                        &publication.envelope,
                        persistence::encode_envelope,
                        persistence::decode_envelope,
                    )
                {
                    return false;
                }
            }
        }
        Some(Err(StreamError::LimitExceeded)) => {}
        Some(Err(_)) | None => return false,
    }
    let observation = persistence::decode_observation(bytes, usize::MAX);
    if !truncated_header_is_invalid(bytes, persistence::OBSERVATION_DOMAIN, &observation) {
        return false;
    }
    if let Ok(value) = observation {
        if persistence::encode_observation(&value, usize::MAX).as_deref() != Ok(bytes) {
            return false;
        }
    }
    let envelope = persistence::decode_envelope(bytes, usize::MAX);
    if !truncated_header_is_invalid(bytes, persistence::ENVELOPE_DOMAIN, &envelope) {
        return false;
    }
    if let Ok(value) = envelope {
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
            )) != Ok(record.clone())
                || crate::follow_filter::projection_contract(&crate::Record {
                    sequence: 0,
                    value: record.value,
                    headers: record.headers,
                    commit_id: crate::CommitId::from_bytes([1; 32]),
                    committed_at_micros: 0,
                    origin: crate::StreamPath::new("codec/events").expect("canonical fixture path"),
                })
                .is_err()
            {
                return false;
            }
        }
    }
    if let Ok(value) = crate::wire::Record::decode(bytes) {
        if !record(&value) {
            return false;
        }
    }
    if let Ok(value) = crate::wire::ReadResponse::decode(bytes) {
        if let Ok(records) = crate::wire_codec::read_response_records(value) {
            if records.iter().any(|value| !record(value))
                || crate::wire_codec::read_response_records(crate::wire_codec::read_response_wire(
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

fn persisted_fact_admitted<T: PartialEq>(
    magic: &[u8],
    version_change: u8,
    value: &T,
    encode: fn(&T, usize) -> Result<Vec<u8>, StreamError>,
    decode: fn(&[u8], usize) -> Result<T, StreamError>,
) -> bool {
    let Ok(mut bytes) = encode(value, usize::MAX) else {
        return false;
    };
    if !bytes.starts_with(magic) {
        return false;
    }
    // Include the format header in the real byte cap. Exact-size admission
    // distinguishes an inclusive limit from an unlimited-only roundtrip.
    if decode(&bytes, bytes.len()).as_ref() != Ok(value)
        || encode(value, bytes.len()).as_deref() != Ok(bytes.as_slice())
        || [0, bytes.len() - 1].into_iter().any(|maximum| {
            !matches!(encode(value, maximum), Err(StreamError::LimitExceeded))
                || !matches!(decode(&bytes, maximum), Err(StreamError::LimitExceeded))
        })
        || !matches!(
            decode(&bytes[magic.len()..], usize::MAX),
            Err(StreamError::InvalidArgument)
        )
        // A valid domain with no version or only its first byte must refuse
        // as malformed before payload interpretation or capacity accounting.
        || [magic.len() - 2, magic.len() - 1].into_iter().any(|end| {
            !matches!(decode(&bytes[..end], 0), Err(StreamError::InvalidArgument))
        })
    {
        return false;
    }
    // Change only the actual v1 writer's version, then omit its entire payload
    // and grant no byte capacity. Version refusal must precede both checks.
    bytes[magic.len() - 2] ^= version_change;
    matches!(
        decode(&bytes[..magic.len()], 0),
        Err(StreamError::Unsupported)
    )
}

fn wire_records_admitted(envelope: &crate::CommittedEnvelope) -> bool {
    let Ok(foreign) = crate::StreamPath::new("codec/other") else {
        return false;
    };
    for mutation in &envelope.mutations {
        let records = match mutation {
            crate::CommittedMutation::Append(append) => &append.records,
            crate::CommittedMutation::Fork(fork) => &fork.records,
        };
        let mut encoded = Vec::with_capacity(records.len());
        for record in records {
            // Persisted envelopes already exercise the elided-origin path.
            // The same spelled origin must work without a follow context and
            // when following another path, preserving the original record.
            let value = crate::wire_codec::record_wire(record.clone(), None);
            for followed in [None, Some(&foreign)] {
                if crate::wire_codec::record(value.clone(), followed) != Ok(record.clone()) {
                    return false;
                }
            }
            encoded.push(value);
        }
        if crate::wire_codec::read_response_records(crate::wire_codec::read_response_wire(
            encoded.clone(),
        )) != Ok(encoded)
        {
            return false;
        }
    }
    true
}

fn commands(
    value: Option<crate::OwnedBytes>,
) -> Result<[(&'static str, crate::WriteRequest); 3], StreamError> {
    use crate::{
        AppendRequest, CommitMutation, CommitRequest, ForkRequest, IdempotencyKey, OwnedBytes,
        StreamPath, WriteRequest,
    };
    let headers = match &value {
        None => crate::RecordHeaders::default(),
        Some(value) => crate::RecordHeaders::new(
            vec![value.slice(..value.len().min(crate::MAX_RECORD_KEY_BYTES))],
            vec![crate::Attribute {
                name: "topic".into(),
                value: value.clone(),
            }],
        )?,
    };
    Ok([
        (
            "append",
            WriteRequest::Append(AppendRequest {
                path: StreamPath::new("codec/events")?,
                records: vec![crate::AppendRecord {
                    value: value
                        .clone()
                        .unwrap_or_else(|| OwnedBytes::from_static(b"persisted codec corpus")),
                    headers: headers.clone(),
                }],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"append"))?,
            }),
        ),
        (
            "fork",
            WriteRequest::Fork(ForkRequest {
                source: StreamPath::new("codec/events")?,
                destination: StreamPath::new("codec/fork")?,
                at_tail: Some(value.as_ref().map_or(1, |value| {
                    value
                        .as_slice()
                        .iter()
                        .take(8)
                        .fold(0u64, |position, byte| (position << 8) | u64::from(*byte))
                })),
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
                    records: vec![crate::AppendRecord {
                        value: value
                            .unwrap_or_else(|| OwnedBytes::from_static(b"committed codec corpus")),
                        headers,
                    }],
                }],
                idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"commit"))?,
            }),
        ),
    ])
}

struct Publication {
    name: &'static str,
    command: crate::WriteRequest,
    observation: crate::IdempotencyObservation,
    envelope: crate::CommittedEnvelope,
}

async fn publications(
    value: Option<crate::OwnedBytes>,
) -> Result<(crate::MemoryStream, Vec<Publication>), StreamError> {
    use crate::{StreamProvider as _, WriteRequest};
    struct Clock;
    impl crate::UnixMillisClock for Clock {
        fn now_unix_millis(&self) -> u64 {
            42
        }
    }
    let memory = crate::MemoryStream::new_with_clock(
        crate::MemoryLimits::default(),
        std::sync::Arc::new(Clock),
    );
    #[cfg(feature = "local")]
    let memory = memory.with_authority(crate::RevisionScope::new(b"codec-corpus".to_vec()), 7);
    let mut publications = Vec::with_capacity(3);
    for (name, mut command) in commands(value)? {
        // This history has one source record. Both zero and one are real fork
        // positions; journal codec checks still exercise the full u64.
        if let WriteRequest::Fork(request) = &mut command {
            request.at_tail = request.at_tail.map(|position| position % 2);
        }
        let key = match &command {
            WriteRequest::Append(request) => request.idempotency_key.clone(),
            WriteRequest::Fork(request) => request.idempotency_key.clone(),
            WriteRequest::Commit(request) | WriteRequest::CommitAccepted(request) => {
                request.idempotency_key.clone()
            }
        };
        memory.write(command.clone()).await?;
        let observation = memory
            .inspect_idempotency(key)
            .await?
            .ok_or(StreamError::InvalidResponse)?;
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
        publications.push(Publication {
            name,
            command,
            observation,
            envelope,
        });
    }
    Ok((memory, publications))
}

/// Capture actual journal and framing encoder output without filesystem IO.
pub async fn seeds() -> Result<Vec<(String, Vec<u8>)>, StreamError> {
    let mut seeds = Vec::new();
    for operation in crate::TokenOperation::ALL {
        seeds.push((
            format!("token-{}", operation.as_str()),
            operation.as_str().as_bytes().to_vec(),
        ));
    }
    let (_memory, publications) = publications(None).await?;
    for Publication {
        name,
        command: _command,
        observation,
        envelope,
    } in publications
    {
        #[cfg(any(
            feature = "local",
            feature = "postgres",
            all(
                any(feature = "wasm", feature = "wasm-journal"),
                target_arch = "wasm32"
            )
        ))]
        seeds.push((name.to_owned(), _command.encode_to_vec()));
        #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
        seeds.push((
            format!("frame-{name}"),
            crate::local::codec_fuzz::frame(&_command)?,
        ));
        seeds.push((
            format!("observation-{name}"),
            persistence::encode_observation(&observation, usize::MAX)?,
        ));
        seeds.push((
            format!("envelope-{name}"),
            persistence::encode_envelope(&envelope, usize::MAX)?,
        ));
    }
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    seeds.push(("journal-header".into(), crate::local::codec_fuzz::header()?));
    #[cfg(feature = "local")]
    {
        let state = _memory.capture_state().await.encode();
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
    // Existing names retain their original writer inputs and captured bytes.
    for (name, keys, batch) in [
        ("changes", &[b"file".as_slice()][..], "record-batch"),
        (
            "changes-projection",
            &[b"file".as_slice(), b"file/child".as_slice()][..],
            "changes-projection-record-batch",
        ),
    ] {
        let entries = keys
            .iter()
            .map(|key| crate::wire::ChangeEntry {
                key: bytes::Bytes::copy_from_slice(key),
                operation: "write".into(),
                metadata: bytes::Bytes::from_static(b"metadata"),
            })
            .collect::<Vec<_>>();
        let changes = crate::AppendRecord::changes(b"codec", &entries, Vec::new())?;
        seeds.push((name.into(), changes.value.to_vec()));
        seeds.push((
            format!("{name}-append-record"),
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
        seeds.push((format!("{name}-record"), record.encode_to_vec()));
        seeds.push((
            batch.into(),
            crate::wire_codec::read_response_wire(vec![record]).encode_to_vec(),
        ));
    }
    seeds.extend(projection_seeds()?);
    Ok(seeds)
}

fn projection_seeds() -> Result<Vec<(String, Vec<u8>)>, StreamError> {
    use crate::{AppendRecord, Attribute, RecordHeaders, wire, wire_codec};
    let changes = wire::ChangeRecord {
        context: bytes::Bytes::from_static(b"publication"),
        entries: [b"file/a", b"file/b", b"file/c"]
            .into_iter()
            .map(|key| wire::ChangeEntry {
                key: bytes::Bytes::from_static(key),
                operation: "write".into(),
                metadata: bytes::Bytes::from_static(key),
            })
            .collect(),
    };
    let attributes = vec![Attribute {
        name: "owner".into(),
        value: b"author".as_slice().into(),
    }];
    let current = AppendRecord::changes(&changes.context, &changes.entries, attributes.clone())?;
    let mut retained_attributes = vec![Attribute {
        name: "acyclic.record".into(),
        value: b"changes/v1".as_slice().into(),
    }];
    retained_attributes.extend(attributes.clone());
    let retained = AppendRecord {
        value: changes.encode_to_vec().into(),
        headers: RecordHeaders::new(
            changes
                .entries
                .iter()
                .map(|entry| entry.key.clone().into())
                .collect(),
            retained_attributes,
        )?,
    };
    let opaque = AppendRecord {
        value: current.value.clone(),
        headers: RecordHeaders::new(current.headers.keys().to_vec(), attributes)?,
    };
    let mut seeds = Vec::new();
    for (format, body) in [("v1", retained), ("v2", current), ("opaque", opaque)] {
        seeds.push((
            format!("projection-{format}-append"),
            wire_codec::append_record_wire(body.clone()).encode_to_vec(),
        ));
        let record = wire_codec::record_wire(
            crate::Record {
                sequence: 7,
                value: body.value,
                headers: body.headers,
                commit_id: crate::CommitId::from_bytes([1; 32]),
                committed_at_micros: 42,
                origin: crate::StreamPath::new("codec/events")?,
            },
            None,
        );
        seeds.push((
            format!("projection-{format}-record"),
            record.encode_to_vec(),
        ));
        seeds.push((
            format!("projection-{format}-batch"),
            wire_codec::read_response_wire(vec![record]).encode_to_vec(),
        ));
    }
    Ok(seeds)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[test]
fn emitted_persistence_seeds_satisfy_shared_oracle() -> Result<(), StreamError> {
    let seeds = futures::executor::block_on(seeds())?;
    for operation in crate::TokenOperation::ALL {
        assert!(
            seeds.iter().any(|(name, bytes)| {
                name == &format!("token-{}", operation.as_str())
                    && bytes == operation.as_str().as_bytes()
            }),
            "token corpus omitted declared operation {}",
            operation.as_str()
        );
    }
    for (name, bytes) in seeds {
        for length in [bytes.len(), bytes.len().saturating_sub(1)] {
            assert!(
                check(&bytes[..length]),
                "shared persistence oracle rejected {name} at {length} bytes"
            );
        }
    }
    #[cfg(feature = "local")]
    for (name, command) in commands(None)? {
        let frame = crate::local::codec_fuzz::repeated_command_frame(&command)?;
        assert!(
            check(&frame),
            "shared persistence oracle rejected repeated {name} frame"
        );
    }
    Ok(())
}

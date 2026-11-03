//! On-demand oracles call the production decoders; no alternate wire parser.
use prost::Message;
pub mod agent_controls;
pub mod auth_addressing;
pub mod auth_capability;
pub mod auth_credential;
mod auth_resource;
pub mod auth_socket;
pub mod filesystem_pages;
mod harness_manifest;
pub mod inference_http;
pub mod log_frame;
mod mcp;
#[cfg(feature = "storage-schedules")]
pub mod object_schedule;
mod objects_json;
mod semantic;

/// Original Objects service writer outputs, without invoking other producers.
pub use semantic::object_seeds as object_semantic_seeds;

/// Real service-writer outputs to capture under both protobuf syntax targets.
pub fn semantic_seeds(
    streams: &[(String, Vec<u8>)],
) -> Result<Vec<(&'static str, &'static str, Vec<u8>)>, Box<dyn std::error::Error>> {
    semantic::seeds(streams)
}

/// External captures must reach their recorded successful service or callback
/// admission; chunk-equivalent refusal alone cannot qualify a positive seed.
pub fn admit_external(codec: &str, method: &str, bytes: &[u8]) -> Result<(), String> {
    use acyclic_harness_core::{
        OperationId, contract,
        mcp::{
            callbacks::{McpCallbackPolicy, McpCallbackRequest},
            services,
        },
    };
    let result = match codec {
        "stdio" => {
            if method != "tools/list" {
                return Err("stdio capture is not discovery".into());
            }
            return acyclic_harness_core::mcp::stdio::codec_fuzz::admit(bytes)
                .map(|_| ())
                .map_err(|error| format!("captured stdio discovery refused: {error:?}"));
        }
        "json" | "sse" => acyclic_harness_core::mcp::http::codec_fuzz::admit(
            bytes,
            if codec == "json" {
                "application/json"
            } else {
                "text/event-stream"
            },
            OperationId::from_bytes([if method == "initialize" { 6 } else { 7 }; 16]),
            method == "initialize",
        ),
        "callback" => {
            let message = contract::json_from_slice(bytes).map_err(|error| error.to_string())?;
            let request = McpCallbackRequest {
                parent: OperationId::from_bytes([7; 16]),
                policy: McpCallbackPolicy {
                    server: "capture".into(),
                    revision: "2.1.0".into(),
                    sampling: true,
                    maximum_requests: 1,
                },
                message,
            };
            if request
                .message
                .get("method")
                .and_then(serde_json::Value::as_str)
                != Some(method)
            {
                return Err("captured callback method differs from provenance".into());
            }
            request.validate().map_err(|error| error.to_string())?;
            return Ok(());
        }
        _ => return Err(format!("unknown external capture codec {codec}")),
    }
    .map_err(|error| format!("captured {codec} response refused: {error:?}"))?;
    if method != "initialize" {
        services::validate_result(method, result, None).map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// Replay one named target through the same entry point used by libFuzzer.
pub fn exercise(target: &str, bytes: &[u8]) -> Result<(), String> {
    match target {
        "protobuf" => protobuf(bytes),
        "protobuf_json" => protobuf_json(bytes),
        "session_frames" => session_frames(bytes),
        "session_protocol" => assert!(acyclic_stream::codec_fuzz::session(bytes)),
        "stream_persistence" => stream_persistence(bytes),
        "object_persistence" => assert!(acyclic_objects::codec_fuzz::check(bytes)),
        #[cfg(feature = "object-revisions")]
        "object_revisions" => assert!(acyclic_objects::grpc::check_revision_metadata(bytes)),
        #[cfg(not(feature = "object-revisions"))]
        "object_revisions" => return Err("revision transport replay requires object-revisions; recovery: cargo run --features object-revisions --example replay".into()),
        #[cfg(feature = "storage-schedules")]
        "object_schedule" => {
            object_schedule::check(bytes).map_err(|error| error.to_string())?;
        }
        #[cfg(not(feature = "storage-schedules"))]
        "object_schedule" => return Err("physical schedule replay requires storage-schedules; recovery: cargo run --features storage-schedules --example replay".into()),
        "ids" => ids(bytes),
        "auth" => auth(bytes),
        "identities" => identities(bytes),
        "actor_codecs" => actor_codecs(bytes),
        "call_record" => {
            futures::executor::block_on(acyclic_actors::call::model::check(bytes))?;
        }
        "filesystem_codecs" => filesystem_codecs(bytes),
        "filesystem_pages" => filesystem_pages::check(bytes),
        "machine_journal" => {
            assert!(acyclic_machines::codec_fuzz::check(bytes));
            assert!(acyclic_machines::results::codec_fuzz::check(bytes));
        }
        "harness_codecs" => harness_codecs(bytes),
        "agent_controls" => agent_controls::check(bytes),
        "inference_http" => inference_http::check(bytes),
        _ => return Err(format!("unknown corpus target {target}")),
    }
    Ok(())
}

pub fn harness_codecs(bytes: &[u8]) {
    acyclic_harness_core::agent::codec_fuzz::check(bytes);
    acyclic_harness_core::filesystem::execution_codec_fuzz::check(bytes);
    harness_manifest::check(bytes);
    acyclic_harness_core::mcp::http::codec_fuzz::check(bytes);
    acyclic_harness_core::mcp::stdio::codec_fuzz::check(bytes);
    mcp::check(bytes);
}

pub struct Decoder {
    pub name: &'static str,
    family: usize,
    pub check: fn(&[u8], prost_reflect::MessageDescriptor),
    pub seed: fn() -> Vec<u8>,
}

fn check<T: Message + Default>(bytes: &[u8], descriptor: prost_reflect::MessageDescriptor) {
    if let Ok(value) = T::decode(bytes) {
        objects_json::typed(&descriptor, &value);
        let encoded = value.encode_to_vec();
        // Unknown fields may be discarded at a wire boundary; the projection
        // must preserve its value after that first decode. Map iteration may
        // change wire order, and valid NaNs cannot use Rust float equality.
        let repeated = T::decode(encoded.as_slice()).unwrap().encode_to_vec();
        if repeated != encoded {
            let json = |bytes: &[u8]| {
                let encoded = acyclic_protobuf_json::encode(descriptor.clone(), bytes)
                    .expect("typed wire writer agrees with its descriptor");
                serde_json::from_slice::<serde_json::Value>(&encoded)
                    .expect("production protobuf JSON writer output")
            };
            assert_eq!(json(&repeated), json(&encoded));
        }
    }
}

fn check_semantic<W, S>(bytes: &[u8], descriptor: prost_reflect::MessageDescriptor)
where
    W: Message + Default + From<S>,
    S: TryFrom<W> + Clone + std::fmt::Debug + PartialEq,
    S::Error: std::fmt::Debug,
{
    check::<W>(bytes, descriptor);
    semantic::project(bytes, S::try_from, W::from);
}

fn seed<T: Message + Default>() -> Vec<u8> {
    T::default().encode_to_vec()
}

include!(concat!(env!("OUT_DIR"), "/decoders.rs"));

/// Resolve a message against the exact descriptor used to generate its type.
pub fn descriptor(decoder: &Decoder) -> prost_reflect::MessageDescriptor {
    static POOLS: std::sync::OnceLock<Vec<prost_reflect::DescriptorPool>> =
        std::sync::OnceLock::new();
    POOLS.get_or_init(|| {
        DESCRIPTORS
            .iter()
            .map(|bytes| {
                prost_reflect::DescriptorPool::decode(*bytes).expect("generated descriptor")
            })
            .collect()
    })[decoder.family]
        .get_message_by_name(decoder.name)
        .expect("generated message belongs to its descriptor")
}

pub fn protobuf_json(bytes: &[u8]) {
    let Some((selector, payload)) = bytes.split_first_chunk::<8>() else {
        return;
    };
    let index = (u64::from_le_bytes(*selector) % DECODERS.len() as u64) as usize;
    let descriptor = descriptor(&DECODERS[index]);
    objects_json::json(&descriptor, payload);
    if let Ok(value) = acyclic_protobuf_json::decode(descriptor.clone(), payload, usize::MAX) {
        let binary = value.encode_to_vec();
        (DECODERS[index].check)(&binary, descriptor.clone());
        semantic::check(DECODERS[index].name, &binary);
        let canonical = acyclic_protobuf_json::encode(descriptor.clone(), &binary).unwrap();
        let mut written = Vec::new();
        acyclic_protobuf_json::encode_to_writer(descriptor.clone(), &binary, &mut written).unwrap();
        let reparsed =
            acyclic_protobuf_json::decode(descriptor.clone(), &canonical, usize::MAX).unwrap();
        let repeated =
            acyclic_protobuf_json::encode(descriptor.clone(), &reparsed.encode_to_vec()).unwrap();
        // Object order is not part of JSON identity; NaNs use protobuf's string
        // spelling. Comparing values avoids both map order and NaN false alarms.
        let json = |bytes: &[u8]| serde_json::from_slice::<serde_json::Value>(bytes).unwrap();
        assert_eq!(json(&canonical), json(&written));
        assert_eq!(json(&canonical), json(&repeated));
        if !payload.is_empty() {
            assert!(matches!(
                acyclic_protobuf_json::decode(descriptor.clone(), payload, payload.len() - 1),
                Err(acyclic_protobuf_json::Error::Limit)
            ));
        }
        let mut trailing = payload.to_vec();
        trailing.extend_from_slice(b" false");
        let (stage, cause) =
            acyclic_protobuf_json::decode_admitted(descriptor, &trailing, |stage, cause| {
                (stage, cause)
            })
            .expect_err("derived trailing input refuses");
        assert_eq!(stage, acyclic_protobuf_json::Error::Trailing);
        assert!(
            cause.is_syntax(),
            "original trailing refusal retains its syntax cause"
        );
    }
}

pub fn protobuf(bytes: &[u8]) {
    let Some((selector, payload)) = bytes.split_first_chunk::<8>() else {
        return;
    };
    let index = (u64::from_le_bytes(*selector) % DECODERS.len() as u64) as usize;
    let descriptor = descriptor(&DECODERS[index]);
    objects_json::binary(&descriptor, payload);
    (DECODERS[index].check)(payload, descriptor);
    semantic::check(DECODERS[index].name, payload);
}

fn framed(
    bytes: &[u8],
    grpc: bool,
    chunk: usize,
) -> Result<Vec<acyclic_stream::wire::SessionFrame>, acyclic_stream::StreamError> {
    use acyclic_stream::wire_codec::session::FrameDecoder;
    let mut decoder = if grpc {
        FrameDecoder::grpc()
    } else {
        FrameDecoder::default()
    };
    let mut frames = Vec::new();
    for mut input in bytes.chunks(chunk) {
        while !input.is_empty() {
            let before = input.len();
            if let Some(frame) = decoder.read(&mut input)? {
                frames.push(frame);
            }
            assert!(input.len() < before, "a decoder step must consume input");
        }
    }
    decoder.finish()?;
    Ok(frames)
}

pub fn session_frames(bytes: &[u8]) {
    for grpc in [false, true] {
        let whole = framed(bytes, grpc, bytes.len().max(1));
        for chunk in [1, 2, 5, 10, 127] {
            assert_eq!(framed(bytes, grpc, chunk), whole);
        }
    }
}

pub fn stream_persistence(bytes: &[u8]) {
    log_frame::check(bytes);
    assert!(acyclic_stream::codec_fuzz::check(bytes));
}

pub fn ids(bytes: &[u8]) {
    match acyclic_fs::ObjectId::from_prefix(bytes) {
        Ok((id, tail)) => {
            let encoded = id.to_bytes();
            assert_eq!(&bytes[..encoded.len()], encoded.as_slice());
            assert_eq!(tail, &bytes[encoded.len()..]);
            assert_eq!(tail.as_ptr(), bytes[encoded.len()..].as_ptr());
            assert_eq!(
                acyclic_fs::ObjectId::try_from(bytes).is_ok(),
                tail.is_empty()
            );
            assert_eq!(
                acyclic_fs::GenerationId::from_bytes(bytes).is_some(),
                tail.is_empty() && id.kind == acyclic_fs::ObjectKind::GenerationRoot
            );
            assert_eq!(acyclic_fs::ObjectId::try_from(encoded.as_slice()), Ok(id));
            for cut in 0..encoded.len() {
                assert!(acyclic_fs::ObjectId::from_prefix(&bytes[..cut]).is_err());
            }
            // Concatenate actual writer outputs: the second identity must remain
            // borrowed in full, regardless of the first identity's seal.
            let mut pair = [0; 2 * acyclic_fs::ObjectId::ENCODED_LEN];
            let width = encoded.len();
            pair[..width].copy_from_slice(encoded.as_slice());
            pair[width..2 * width].copy_from_slice(encoded.as_slice());
            let pair = &pair[..2 * width];
            let (first, second) = acyclic_fs::ObjectId::from_prefix(pair).unwrap();
            assert_eq!(first, id);
            assert_eq!(second, encoded.as_slice());
            assert_eq!(second.as_ptr(), pair[width..].as_ptr());
            assert!(acyclic_fs::ObjectId::try_from(pair).is_err());
            assert!(acyclic_fs::GenerationId::from_bytes(pair).is_none());
            assert!(acyclic_fs::ObjectId::try_from(id.digest.as_bytes().as_slice()).is_err());
        }
        Err(_) => {
            assert!(acyclic_fs::ObjectId::try_from(bytes).is_err());
            assert!(acyclic_fs::GenerationId::from_bytes(bytes).is_none());
        }
    }
    if let Some(id) = acyclic_fs::GenerationId::from_bytes(bytes) {
        assert_eq!(id.to_bytes().as_slice(), bytes);
        assert_eq!(
            acyclic_fs::GenerationId::from_bytes(id.to_bytes().as_slice()),
            Some(id)
        );
    }
    if let Ok(id) = serde_json::from_slice::<acyclic_fs::GenerationId>(bytes) {
        let canonical = id.to_bytes();
        let encoded = serde_json::to_vec(&id).unwrap();
        assert_eq!(encoded, serde_json::to_vec(canonical.as_slice()).unwrap());
        assert_eq!(
            serde_json::from_slice::<acyclic_fs::GenerationId>(&encoded).unwrap(),
            id
        );
        let digest_only = serde_json::to_vec(id.digest().as_bytes()).unwrap();
        assert!(serde_json::from_slice::<acyclic_fs::GenerationId>(&digest_only).is_err());
        let mut trailing = canonical.as_slice().to_vec();
        trailing.push(0);
        assert!(
            serde_json::from_slice::<acyclic_fs::GenerationId>(
                &serde_json::to_vec(&trailing).unwrap()
            )
            .is_err()
        );
    }
}

pub fn auth(bytes: &[u8]) {
    auth_credential::check(bytes);
    auth_resource::check(bytes);
    auth_socket::check(bytes);
    use acyclic_auth::{authority, contract::*};
    if let Ok(value) = authority::decode_block(bytes) {
        assert_eq!(BlockProto::from(value).encode_to_vec(), bytes);
    }
    if let Ok(value) = authority::decode_capability(bytes) {
        for version in [0, 1, 3, u32::MAX] {
            let mut refused = CapabilityProto::from(value.clone());
            refused.version = version;
            // Removing every block also makes the body semantically invalid:
            // the version must decide admission before block or signature work.
            for without_blocks in [false, true] {
                if without_blocks {
                    refused.blocks.clear();
                }
                let encoded = refused.encode_canonical();
                let header_len = 1 + prost::encoding::encoded_len_varint(u64::from(version));
                for input in [encoded.as_slice(), &encoded[..header_len]] {
                    assert_eq!(
                        authority::decode_capability(input),
                        Err(acyclic_auth::Error::UnsupportedVersion(version))
                    );
                    assert!(matches!(
                        acyclic_auth::verify::verify(
                            input,
                            &acyclic_auth::testing::Snapshot::default(),
                            acyclic_auth::testing::NOW,
                        ),
                        Err(acyclic_auth::verify::VerifyError::Version(found)) if found == version
                    ));
                }
            }
        }
        assert_eq!(
            authority::decode_capability(&CapabilityProto::from(value.clone()).encode_canonical()),
            Ok(value)
        );
    }
    if let Ok(value) = authority::decode_caveat(bytes) {
        assert_eq!(CaveatProto::from(value).encode_to_vec(), bytes);
    }
    if let Ok(value) = RecordAttributes::decode(bytes) {
        assert_eq!(
            RecordAttributesProto::from(value.clone()).encode_to_vec(),
            bytes
        );
        if let Some(first) = value.as_slice().first() {
            let mut duplicate = value.as_slice().to_vec();
            duplicate.push(first.clone());
            assert_eq!(
                RecordAttributes::try_new(duplicate),
                Err(acyclic_auth::Error::Invalid)
            );
        }
    }
    if let Ok(value) = GrantSetProto::decode(bytes) {
        if let Ok(grants) = GrantSet::try_from(value) {
            let encoded = GrantSetProto::from(grants.clone()).encode_to_vec();
            assert_eq!(
                GrantSet::try_from(GrantSetProto::decode(encoded.as_slice()).unwrap()),
                Ok(grants)
            );
        }
    }
    auth_capability::check(bytes);
}

pub fn identities(bytes: &[u8]) {
    auth_addressing::check(bytes);
    use acyclic_auth::addressing::{ActorReference, ResourceIdentity};
    if let Ok(value) = ResourceIdentity::decode(bytes) {
        auth_addressing::identity(&value);
        assert_eq!(value.encode(), bytes);
        assert_eq!(ResourceIdentity::decode(&value.encode()), Ok(value));
    }
    if let Ok(value) = ActorReference::decode(bytes) {
        assert_eq!(value.encode(), bytes);
        assert_eq!(ActorReference::decode(&value.encode()), Ok(value));
    }
}

/// Capture the deployed Actors writers and require their original values to
/// survive admission. The fuzz oracle shares these producers with capture so
/// a decoder which rejects everything cannot vacuously satisfy round trips.
pub fn actor_seeds()
-> Result<Vec<(&'static str, Vec<u8>, &'static str)>, Box<dyn std::error::Error>> {
    use acyclic_actors::{
        domain,
        guest::{self, Handler as _, wire},
        invocation::Invocation,
        local::{State, Store},
    };
    use acyclic_auth::addressing::ActorReference;
    use acyclic_stream::{CommitId, CommitMutation, MemoryStream, StreamPath};
    use bytes::Bytes;
    use std::{collections::BTreeMap, sync::Arc};

    let actor = ActorReference::new(b"account", b"namespace", b"codec")?;
    let invocation = Invocation::new(
        StreamPath::new("invoke-control/caller")?,
        actor.clone(),
        "inbox".into(),
        Bytes::from_static(b"operation"),
        Bytes::from_static(b"request"),
    )?;
    // Fix time at the provider boundary; retain all other metadata from the
    // original Store acceptance instead of constructing a wire envelope.
    struct Clock;
    impl acyclic_stream::UnixMillisClock for Clock {
        fn now_unix_millis(&self) -> u64 {
            42
        }
    }
    let clock: Arc<dyn acyclic_stream::UnixMillisClock> = Arc::new(Clock);
    let input = StreamPath::new("codec/actors/inbox")?;
    let ticket = futures::executor::block_on(async {
        use acyclic_actors::local::{PreparedPublish, Subscription};
        use acyclic_stream::{IdempotencyKey, MemoryLimits, StreamProviderExt as _};

        let provider = Arc::new(MemoryStream::new_with_clock(
            MemoryLimits::default(),
            Arc::clone(&clock),
        ));
        let store = Store::new(Arc::clone(&provider), StreamPath::new("codec/actors")?);
        let mut create =
            PreparedPublish::foreign(IdempotencyKey::new(b"codec/invocation/create".to_vec())?);
        for effect in store.create(
            &actor,
            BTreeMap::from([(
                "inbox".into(),
                Subscription {
                    path: input.clone(),
                    cursor: 0,
                },
            )]),
            None,
        )? {
            create.join(effect)?;
        }
        PreparedPublish::settle(&provider.commit(create.into_request()?).await?)?;
        Ok::<_, Box<dyn std::error::Error>>(store.enqueue_invoke(&invocation).await?)
    })?;
    assert_eq!(ticket.invocation(), &invocation);
    assert_eq!(ticket.record().origin, input);
    assert_eq!(ticket.record().sequence, 0);
    assert_eq!(ticket.record().committed_at_micros, 42_000);
    let invocation_bytes =
        acyclic_stream::wire_codec::record_wire(ticket.record().clone(), None).encode_to_vec();
    let record = acyclic_stream::wire_codec::record(
        acyclic_stream::wire::Record::decode(invocation_bytes.as_slice())?,
        None,
    )?;
    assert_eq!(
        record,
        *ticket.record(),
        "wire must retain original acceptance"
    );
    assert_eq!(
        Invocation::decode(&record)?,
        Some(invocation),
        "invocation writer must be admitted"
    );
    let mut seeds = vec![(
        "invocation-committed-v2",
        invocation_bytes,
        "Store::enqueue_invoke original InvocationTicket::record and wire_codec::record_wire",
    )];
    for (name, bytes) in
        futures::executor::block_on(acyclic_actors::call::model::codec_seeds(clock))?
    {
        seeds.push((
            name,
            bytes,
            "CallPath builders, MemoryStream commit/follow and wire_codec::record_wire",
        ));
    }
    for (name, bytes) in acyclic_actors::reservation::codec_fuzz::seeds()? {
        seeds.push((
            name,
            bytes,
            "actors::reservation canonical request/decision/checkpoint encoder",
        ));
    }
    let artifact = guest::Checkpoint {
        codec: guest::CodecIdentity {
            name: "codec".into(),
            version: "1".into(),
        },
        bytes: Bytes::from_static(b"checkpoint"),
    };
    let bytes = serde_json::to_vec(&artifact)?;
    assert_eq!(
        serde_json::from_slice::<guest::Checkpoint>(&bytes)?,
        artifact
    );
    seeds.push((
        "artifact-checkpoint",
        bytes,
        "serde_json::to_vec(guest::Checkpoint) with current artifact codec identity",
    ));
    let package = acyclic_actors::reservation::Package::new(
        domain::CodeSha256::new(vec![1; 32])?,
        domain::ActorLimits::new(1, 1024, 4096)?,
    );
    let manifest = package.manifest();
    let bytes = wire::encode_manifest(manifest);
    let admitted = wire::decode_manifest(&bytes)?;
    assert_eq!(admitted.code, manifest.code);
    assert_eq!(admitted.codec, manifest.codec);
    assert_eq!(admitted.subscriptions, manifest.subscriptions);
    assert_eq!(admitted.interrupts, manifest.interrupts);
    assert_eq!(admitted.bindings, manifest.bindings);
    assert_eq!(admitted.limits, manifest.limits);
    seeds.push((
        "manifest",
        bytes,
        "guest::wire::encode_manifest(reservation::Package::manifest)",
    ));
    let store = Store::new(
        Arc::new(MemoryStream::default()),
        StreamPath::new("codec/actors")?,
    );
    let checkpoint = acyclic_actors::reservation::initial_checkpoint(7)?;
    let effects = store.create(&actor, BTreeMap::new(), Some(checkpoint.clone()))?;
    let CommitMutation::Append { records, .. } = &effects[0].mutation else {
        return Err("actor creation did not append state".into());
    };
    let bytes = records
        .first()
        .ok_or("actor creation emitted no state")?
        .value
        .to_vec();
    let state = State::decode(&bytes)?;
    assert_eq!(state.checkpoint(), Some(&checkpoint));
    assert_eq!(state.epoch(), 0);
    assert!(state.subscriptions().is_empty());
    assert_eq!(state.encode(), bytes);
    seeds.push(("state-v2", bytes, "local::Store::create state encoder"));
    let delivery = guest::Delivery {
        actor,
        epoch: 1,
        trigger: guest::Trigger::Alarm {
            generation: 1,
            now_unix_ms: 42,
        },
        now_unix_ms: 42,
        record: None,
        subscriptions: guest::SubscriptionCuts::new(
            StreamPath::new("codec/actors/state")?,
            CommitId::from_bytes([1; 32]),
            BTreeMap::new(),
        ),
    };
    let bytes = wire::encode_delivery(&delivery, Some(&checkpoint));
    assert_eq!(
        wire::decode_delivery(&bytes)?,
        (delivery, Some(checkpoint.clone()))
    );
    seeds.push(("delivery", bytes, "guest::wire::encode_delivery"));
    let mut transition = guest::Transition {
        response: None,
        checkpoint,
        alarm: guest::AlarmChange::Keep,
        effects: effects.into(),
        participant: None,
    };
    for (name, alarm) in [
        ("transition-state-v2", guest::AlarmChange::Keep),
        (
            "transition-set-zero",
            guest::AlarmChange::Set {
                deadline_unix_ms: 0,
            },
        ),
        (
            "transition-set",
            guest::AlarmChange::Set {
                deadline_unix_ms: 42,
            },
        ),
        (
            "transition-set-max",
            guest::AlarmChange::Set {
                deadline_unix_ms: u64::MAX,
            },
        ),
        ("transition-clear", guest::AlarmChange::Clear),
    ] {
        transition.alarm = alarm;
        let bytes = wire::encode_transition(&transition)?;
        assert_eq!(
            wire::decode_transition(&bytes)?,
            transition,
            "transition writer must retain its alarm"
        );
        seeds.push((name, bytes, "guest::wire::encode_transition"));
    }
    seeds.extend(futures::executor::block_on(actor_alarm_seeds())?);
    seeds.extend(actor_socket_seeds()?);
    seeds.extend(actor_socket_input_seeds()?);
    seeds.extend(actor_socket_ledger_seeds()?);
    Ok(seeds)
}

fn actor_socket_ledger_seeds()
-> Result<Vec<(&'static str, Vec<u8>, &'static str)>, Box<dyn std::error::Error>> {
    use acyclic_actors::local::socket_ledger_codec as codec;
    let captures: serde_json::Value =
        serde_json::from_str(include_str!("../captures/socket-ledger.capture"))?;
    let mut seeds = Vec::new();
    for name in [
        "socket-ledger-epoch-initial",
        "socket-ledger-epoch-fenced",
        "socket-ledger-epoch-recovered",
        "socket-ledger-opened",
        "socket-ledger-closed",
    ] {
        let capture = &captures[name];
        let bytes: Vec<u8> = serde_json::from_value(capture["bytes"].clone())?;
        let mut noncanonical = bytes.clone();
        noncanonical.extend_from_slice(&[0x78, 1]);
        if capture["kind"] == "epoch" {
            let segment = capture["segment"].as_u64().ok_or("original segment")?;
            let from = capture["from"].as_u64().ok_or("original frontier")?;
            assert_eq!(
                codec::epoch(&bytes)?,
                (segment, from, segment + 1),
                "socket epoch retains the committed writer frontier"
            );
            assert!(
                codec::epoch(&noncanonical).is_err(),
                "socket epoch rejects unknown fields"
            );
            let unversioned = bytes
                .strip_prefix(&[8, 1])
                .ok_or("original epoch version")?;
            assert!(
                codec::epoch(unversioned).is_err(),
                "socket epoch rejects unknown version"
            );
        } else {
            let connection = capture["connection"]
                .as_str()
                .ok_or("original connection")?
                .to_owned();
            let target = if capture["kind"] == "opened" {
                Some((
                    serde_json::from_value(capture["actor"].clone())?,
                    capture["inbox"]
                        .as_str()
                        .ok_or("original inbox")?
                        .to_owned(),
                ))
            } else {
                None
            };
            assert_eq!(
                codec::entry(&bytes)?,
                (connection, target),
                "socket ledger retains the committed writer target"
            );
            assert!(
                codec::entry(&noncanonical).is_err(),
                "socket ledger rejects unknown fields"
            );
        }
        seeds.push((
            name,
            bytes,
            "local::Sockets::open/commit_open/commit_close -> LocalStream",
        ));
    }
    for (name, segment, from) in [
        ("socket-ledger-exhausted-zero-frontier", u64::MAX, 0),
        ("socket-ledger-exhausted", u64::MAX, u64::MAX),
        ("socket-ledger-last-incarnation", u64::MAX - 1, u64::MAX),
        ("socket-ledger-invalid-frontier", 1, u64::MAX),
    ] {
        let bytes = codec::write_epoch(segment, from);
        let next = u128::from(segment) + 1;
        let expected = next <= u128::from(u64::MAX) && u128::from(from) <= next;
        let actual = codec::epoch(&bytes);
        assert_eq!(
            actual.is_ok(),
            expected,
            "socket epoch checks format capacity and frontier"
        );
        if let Ok(actual) = actual {
            assert_eq!(actual, (segment, from, next as u64));
        }
        seeds.push((name, bytes.to_vec(), "local::sockets::epoch_record"));
    }
    for tail in [0, 1, u64::MAX - 2, u64::MAX - 1, u64::MAX] {
        let expected = u128::from(tail) + 2;
        let actual = codec::reserve_epochs(tail);
        assert_eq!(
            actual.is_ok(),
            expected <= u128::from(u64::MAX),
            "socket ownership reserves both epoch positions"
        );
        if let Ok(actual) = actual {
            assert_eq!(u128::from(actual), expected);
        }
    }
    Ok(seeds)
}

fn actor_socket_input_seeds()
-> Result<Vec<(&'static str, Vec<u8>, &'static str)>, Box<dyn std::error::Error>> {
    use acyclic_actors::guest::wire;
    // Original Runtime deliveries, captured at SocketPackage::on_record while
    // the shared physical conformance suite checks its independently sent events.
    let captures: serde_json::Value =
        serde_json::from_str(include_str!("../captures/socket-deliveries.capture"))?;
    let mut seeds = Vec::new();
    for name in [
        "socket-input-open",
        "socket-input-text",
        "socket-input-binary",
        "socket-input-close-peer",
        "socket-input-close-actor",
        "socket-input-close-runtime",
    ] {
        let capture = &captures[name];
        let bytes: Vec<u8> = serde_json::from_value(capture["bytes"].clone())?;
        let (delivery, checkpoint) = wire::decode_delivery(&bytes)?;
        assert_eq!(wire::encode_delivery(&delivery, checkpoint.as_ref()), bytes);
        let socket = delivery
            .socket()?
            .ok_or("socket input writer must be admitted")?;
        assert_eq!(
            socket.connection_id,
            capture["connection"].as_str().unwrap()
        );
        assert_eq!(
            serde_json::to_value(socket.event)?,
            capture["event"],
            "socket input must retain the original runtime event"
        );
        actor_socket_input_refusals(&delivery)?;
        seeds.push((
            name,
            bytes,
            "Runtime::run -> SocketPackage::on_record -> guest::wire::encode_delivery",
        ));
    }
    Ok(seeds)
}

fn actor_socket_input_refusals(
    delivery: &acyclic_actors::guest::Delivery,
) -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_actors::{guest::Trigger, sockets::SOCKET_RECORD};
    let mut invalid = delivery.clone();
    let Trigger::Record { sequence, .. } = &mut invalid.trigger else {
        return Err("original socket capture is not a record delivery".into());
    };
    *sequence ^= 1;
    assert!(
        invalid.socket().is_err(),
        "socket input rejects wrong sequence"
    );
    invalid.trigger = Trigger::Alarm {
        generation: 1,
        now_unix_ms: delivery.now_unix_ms,
    };
    assert!(
        invalid.socket().is_err(),
        "socket input rejects alarm trigger"
    );

    let mut invalid = delivery.clone();
    let record = invalid
        .record
        .as_mut()
        .ok_or("socket capture has no record")?;
    let mut attributes = record
        .headers
        .attributes()
        .iter()
        .map(|a| a.to_owned())
        .collect::<Vec<_>>();
    if let Some(marker) = attributes.iter_mut().find(|a| a.name == SOCKET_RECORD) {
        marker.value = bytes::Bytes::from_static(b"0").into();
        record.headers =
            acyclic_stream::RecordHeaders::new(record.headers.keys().to_vec(), attributes)?;
        assert!(
            invalid.socket().is_err(),
            "socket input rejects unknown version"
        );

        let mut invalid = delivery.clone();
        invalid
            .record
            .as_mut()
            .ok_or("socket capture has no record")?
            .sequence = u64::MAX;
        if let Trigger::Record { sequence, .. } = &mut invalid.trigger {
            *sequence = u64::MAX;
        }
        assert!(
            invalid.socket().is_err(),
            "socket input rejects offset overflow"
        );
    }
    Ok(())
}

fn actor_socket_open() -> &'static acyclic_actors::sockets::SocketOpen {
    static OPEN: std::sync::LazyLock<acyclic_actors::sockets::SocketOpen> =
        std::sync::LazyLock::new(|| acyclic_actors::sockets::SocketOpen {
            actor: acyclic_auth::addressing::ActorReference::new(
                b"account",
                b"namespace",
                b"socket",
            )
            .unwrap(),
            subscription: "sockets".into(),
            app_channel: Some("presence".into()),
            subprotocols: vec!["chat".into()],
            headers: vec![("x-device".into(), "desktop".into())],
            resume_after: u64::MAX,
            reconnected: true,
        });
    &OPEN
}

fn actor_socket_verdict(bytes: &[u8]) {
    use acyclic_actors::{guest::GuestEventResult, sockets::socket_verdict};
    let actual = socket_verdict(bytes, actor_socket_open());
    match serde_json::from_slice::<GuestEventResult>(bytes) {
        Ok(GuestEventResult::SocketAccept {
            subprotocol: Some(selected),
            ..
        }) if selected != "chat" => {
            assert!(
                actual.is_err(),
                "socket verdict must refuse unoffered subprotocols"
            );
        }
        Ok(expected) => assert_eq!(
            actual.unwrap(),
            expected,
            "socket verdict must retain its reply"
        ),
        Err(_) => assert!(actual.is_err(), "socket verdict must refuse invalid JSON"),
    }
}

fn actor_socket_seeds()
-> Result<Vec<(&'static str, Vec<u8>, &'static str)>, Box<dyn std::error::Error>> {
    use acyclic_actors::{
        guest::{GuestEventResult, SocketCloseInitiator, SocketPayload},
        sockets::{SocketFrame, SocketOpen, socket_reply},
    };
    use bytes::Bytes;
    let open = actor_socket_open();
    let bytes = open.encode();
    assert_eq!(
        SocketOpen::decode(&bytes)?,
        *open,
        "socket open writer must be admitted"
    );
    let mut seeds = vec![(
        "socket-open-v1",
        bytes.to_vec(),
        "sockets::SocketOpen::encode",
    )];
    for (name, frame) in [
        (
            "socket-text-v1",
            SocketFrame::Message(SocketPayload::Text {
                text: "ping".into(),
            }),
        ),
        (
            "socket-binary-v1",
            SocketFrame::Message(SocketPayload::Binary {
                binary: Bytes::from_static(b"\0\xff"),
            }),
        ),
        (
            "socket-close-peer-v1",
            SocketFrame::Close {
                code: 1000,
                reason: "bye".into(),
                initiator: SocketCloseInitiator::Peer,
            },
        ),
        (
            "socket-close-actor-v1",
            SocketFrame::Close {
                code: 1001,
                reason: "actor".into(),
                initiator: SocketCloseInitiator::Actor,
            },
        ),
        (
            "socket-close-runtime-v1",
            SocketFrame::Close {
                code: u16::MAX,
                reason: "runtime".into(),
                initiator: SocketCloseInitiator::Runtime,
            },
        ),
        (
            "socket-accepted-v1",
            SocketFrame::Accepted {
                subprotocol: Some("chat".into()),
            },
        ),
        (
            "socket-accepted-none-v1",
            SocketFrame::Accepted { subprotocol: None },
        ),
        (
            "socket-rejected-v1",
            SocketFrame::Rejected {
                status: 403,
                reason: "denied".into(),
            },
        ),
    ] {
        let bytes = frame.clone().encode();
        assert_eq!(
            SocketFrame::decode(&bytes)?,
            frame,
            "socket frame writer must retain its fields"
        );
        seeds.push((name, bytes.to_vec(), "sockets::SocketFrame::encode"));
    }
    for (name, reply) in [
        (
            "socket-verdict-accept-v1",
            GuestEventResult::SocketAccept {
                subprotocol: Some("chat".into()),
                attachment: Bytes::from_static(b"attachment"),
                tags: vec!["room".into()],
            },
        ),
        (
            "socket-verdict-none-v1",
            GuestEventResult::SocketAccept {
                subprotocol: None,
                attachment: Bytes::new(),
                tags: Vec::new(),
            },
        ),
        (
            "socket-verdict-unoffered-v1",
            GuestEventResult::SocketAccept {
                subprotocol: Some("unoffered".into()),
                attachment: Bytes::new(),
                tags: Vec::new(),
            },
        ),
        (
            "socket-verdict-reject-v1",
            GuestEventResult::SocketReject {
                status: 403,
                reason: "denied".into(),
            },
        ),
        (
            "socket-verdict-reply-v1",
            GuestEventResult::Reply {
                payload: Bytes::from_static(b"reply"),
            },
        ),
    ] {
        let bytes = socket_reply(&reply)?;
        assert_eq!(
            serde_json::from_slice::<GuestEventResult>(&bytes)?,
            reply,
            "socket reply writer must be admitted"
        );
        actor_socket_verdict(&bytes);
        seeds.push((name, bytes.to_vec(), "sockets::socket_reply"));
    }
    Ok(seeds)
}

// Obtain persisted tuples from the original Store publication path, rather
// than manufacturing State fields or a parallel alarm record format.
async fn actor_alarm_seeds()
-> Result<Vec<(&'static str, Vec<u8>, &'static str)>, Box<dyn std::error::Error>> {
    use acyclic_actors::{
        guest::{AlarmChange, Effect, Transition, Trigger},
        local::{PreparedPublish, State, Store, Subscription},
    };
    use acyclic_auth::addressing::ActorReference;
    use acyclic_stream::{
        CommitCondition, CommitMutation, IdempotencyKey, MemoryStream, ReadRequest, StreamPath,
        StreamProvider as _, StreamProviderExt as _,
    };
    use bytes::Bytes;
    use futures::TryStreamExt as _;
    use std::{collections::BTreeMap, sync::Arc};

    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), StreamPath::new("codec/alarms")?);
    let actor = ActorReference::new(b"account", b"namespace", b"alarm-codec")?;
    let input = StreamPath::new("codec/alarm-input")?;
    let checkpoint = acyclic_actors::reservation::initial_checkpoint(7)?;
    let mut create = PreparedPublish::foreign(IdempotencyKey::unique());
    for effect in store.create(
        &actor,
        BTreeMap::from([(
            "inbox".into(),
            Subscription {
                path: input.clone(),
                cursor: 0,
            },
        )]),
        Some(checkpoint.clone()),
    )? {
        create.join(effect)?;
    }
    create.join(Effect {
        conditions: vec![CommitCondition::Absent {
            path: input.clone(),
        }],
        mutation: CommitMutation::Append {
            path: input.clone(),
            records: vec![
                Bytes::from_static(b"set").into(),
                Bytes::from_static(b"clear").into(),
            ],
        },
    })?;
    PreparedPublish::settle(&provider.commit(create.into_request()?).await?)?;
    let inputs: Vec<_> = provider
        .read(ReadRequest {
            key: None,
            path: input,
            from: 0,
            limit: 2,
            at_least: Default::default(),
        })
        .await?
        .try_collect()
        .await?;
    assert_eq!(inputs.len(), 2, "alarm seed inputs must be committed");
    let mut snapshot = store
        .claim(&store.load(&actor).await?, 1, IdempotencyKey::unique())
        .await?;
    let mut seeds = Vec::new();
    for (name, trigger, alarm, cursor, generation, deadline, pending) in [
        (
            "state-alarm-set-v2",
            Trigger::Record {
                subscription: "inbox".into(),
                sequence: 0,
            },
            AlarmChange::Set {
                deadline_unix_ms: 42,
            },
            1,
            1,
            Some(42),
            Some(42),
        ),
        (
            "state-alarm-delivered-v2",
            Trigger::Alarm {
                generation: 1,
                now_unix_ms: 42,
            },
            AlarmChange::Keep,
            1,
            1,
            Some(42),
            None,
        ),
        (
            "state-alarm-clear-v2",
            Trigger::Record {
                subscription: "inbox".into(),
                sequence: 1,
            },
            AlarmChange::Clear,
            2,
            2,
            None,
            None,
        ),
    ] {
        let delivered = match &trigger {
            Trigger::Record { sequence, .. } => Some(&inputs[usize::try_from(*sequence)?]),
            Trigger::Alarm { .. } => None,
        };
        store
            .publish(
                &snapshot,
                1,
                &trigger,
                delivered,
                Transition {
                    response: None,
                    checkpoint: checkpoint.clone(),
                    alarm,
                    effects: Vec::new(),
                    participant: None,
                },
                IdempotencyKey::unique(),
            )
            .await?;
        snapshot = store.load(&actor).await?;
        let state = snapshot.state().ok_or("published alarm state is absent")?;
        assert_eq!(
            (
                state.cursor("inbox"),
                state.alarm().generation(),
                state.alarm().deadline_unix_ms(),
                state.alarm().pending()
            ),
            (Some(cursor), generation, deadline, pending),
            "persisted alarm writer must retain its committed tuple"
        );
        assert_eq!(state.checkpoint(), Some(&checkpoint));
        assert_eq!(state.epoch(), 1);
        let bytes = state.encode();
        assert_eq!(State::decode(&bytes)?, *state);
        seeds.push((
            name,
            bytes,
            "local::Store::publish and local::State::encode",
        ));
    }
    Ok(seeds)
}

pub fn actor_codecs(bytes: &[u8]) {
    static WRITERS: std::sync::LazyLock<()> = std::sync::LazyLock::new(|| {
        actor_seeds().expect("Actors decoders must admit their production writers");
    });
    std::sync::LazyLock::force(&WRITERS);
    use acyclic_actors::sockets::{SocketFrame, SocketOpen};
    if let Ok(open) = SocketOpen::decode(bytes) {
        assert_eq!(
            open.encode().as_ref(),
            bytes,
            "socket open retains every field"
        );
    }
    if let Ok(frame) = SocketFrame::decode(bytes) {
        assert_eq!(SocketFrame::decode(&frame.clone().encode()).unwrap(), frame);
    }
    actor_socket_verdict(bytes);
    acyclic_actors::local::socket_ledger_codec::check(bytes);
    use acyclic_actors::{guest::wire, local::State};
    assert!(acyclic_actors::reservation::codec_fuzz::check(bytes));
    if let Ok(checkpoint) = serde_json::from_slice::<acyclic_actors::guest::Checkpoint>(bytes) {
        use acyclic_actors::guest::{CheckpointCodec, CodecIdentity, GuestError, prepare_update};
        let codec = CheckpointCodec {
            identity: CodecIdentity {
                name: "codec".into(),
                version: "1".into(),
            },
            encode: |bytes: &Vec<u8>| Ok(bytes.clone().into()),
            decode: |checkpoint| Ok(checkpoint.bytes.to_vec()),
        };
        let expected = if checkpoint.codec == codec.identity {
            Ok(Some(checkpoint.bytes.to_vec()))
        } else {
            Err(GuestError::IncompatibleCodec)
        };
        assert_eq!(prepare_update(Some(&checkpoint), &codec), expected);
    }
    if let Ok(value) = State::decode(bytes) {
        let canonical = value.encode();
        assert_eq!(State::decode(&canonical).unwrap(), value);
    }
    if let Ok(value) = wire::decode_manifest(bytes) {
        let canonical = wire::encode_manifest(&value);
        assert_eq!(
            canonical.as_slice(),
            bytes,
            "admitted manifest retains every field"
        );
        assert_eq!(
            wire::encode_manifest(&wire::decode_manifest(&canonical).unwrap()),
            canonical
        );
    }
    if let Ok((value, checkpoint)) = wire::decode_delivery(bytes) {
        let _ = value.socket();
        let canonical = wire::encode_delivery(&value, checkpoint.as_ref());
        assert_eq!(
            canonical.as_slice(),
            bytes,
            "admitted delivery retains every field"
        );
        let (value, checkpoint) = wire::decode_delivery(&canonical).unwrap();
        assert_eq!(
            wire::encode_delivery(&value, checkpoint.as_ref()),
            canonical
        );
    }
    if let Ok(value) = wire::decode_transition(bytes) {
        let canonical = wire::encode_transition(&value).unwrap();
        assert_eq!(
            canonical.as_slice(),
            bytes,
            "admitted transition retains every effect guard"
        );
        assert_eq!(
            wire::encode_transition(&wire::decode_transition(&canonical).unwrap()).unwrap(),
            canonical
        );
    }
    if let Ok(value) = acyclic_stream::wire::Record::decode(bytes)
        && let Ok(record) = acyclic_stream::wire_codec::record(value, None)
    {
        // Both readers consume this same committed Stream wire envelope.
        let _ = acyclic_actors::call::Entry::decode(&record);
        let _ = acyclic_actors::call::fence(&record);
        let mut journal = acyclic_actors::call::Journal::default();
        let before = journal.clone();
        if journal.apply(&record).is_err() {
            assert_eq!(journal, before, "refusal must preserve the call reader");
        }
        let Ok(Some(invocation)) = acyclic_actors::invocation::Invocation::decode(&record) else {
            return;
        };
        let encoded = invocation
            .record(&acyclic_stream::StreamPath::new("codec/state").unwrap())
            .unwrap();
        let round_trip = acyclic_stream::Record {
            value: encoded.value,
            headers: encoded.headers,
            ..record
        };
        assert_eq!(
            acyclic_actors::invocation::Invocation::decode(&round_trip).unwrap(),
            Some(invocation)
        );
    }
}

pub fn filesystem_codecs(bytes: &[u8]) {
    acyclic_fs::s3_http::codec_fuzz::check(bytes);
    assert!(acyclic_fs::core_state::codec_fuzz::check(bytes));
    assert!(acyclic_fs::native_exchange::codec_fuzz::check(bytes));
    assert!(acyclic_fs::lazy_workspace::codec_fuzz::check(bytes));
    assert!(acyclic_fs::materializer::codec_fuzz::check(bytes));
    assert!(acyclic_fs::native_ownership_codec::check(bytes));
    use acyclic_fs::{
        streams_record::{StreamsAuthorityRecord, StreamsDurableRecord},
        workspace_context_wire as context,
    };
    if let Ok(value) = StreamsAuthorityRecord::decode(bytes, u64::MAX) {
        assert_eq!(
            StreamsAuthorityRecord::encode(&value.proposed_commit(), value.epoch, u64::MAX)
                .unwrap()
                .as_ref(),
            bytes
        );
    }
    if let Ok(value) = StreamsDurableRecord::decode(bytes, u64::MAX) {
        assert_eq!(
            StreamsDurableRecord::encode(&value.0, u64::MAX)
                .unwrap()
                .as_ref(),
            bytes
        );
    }
    assert!(acyclic_fs::distributed::state_record::codec_fuzz::check(
        bytes
    ));
    if let Ok(value) = acyclic_fs::windows_usn_codec::WindowsUsnCheckpoint::from_bytes(bytes) {
        assert_eq!(value.to_bytes().as_slice(), bytes);
    }
    macro_rules! context {
        ($decode:ident, $encode:ident) => {
            if let Ok(value) = context::$decode(bytes) {
                let canonical = context::$encode(&value).unwrap();
                assert_eq!(
                    context::$encode(&context::$decode(&canonical).unwrap()).unwrap(),
                    canonical
                );
            }
        };
    }
    context!(decode_roots, encode_roots);
    context!(decode_root, encode_root);
    context!(decode_context, encode_context);
    context!(decode_discard, encode_discard);
    if let Ok(text) = std::str::from_utf8(bytes) {
        use acyclic_fs::compat_wire;
        macro_rules! control {
            ($decode:ident, $encode:ident) => {
                if let Ok(value) = compat_wire::$decode(text) {
                    let canonical = compat_wire::$encode(&value).unwrap();
                    assert_eq!(
                        compat_wire::$encode(&compat_wire::$decode(&canonical).unwrap()).unwrap(),
                        canonical
                    );
                }
            };
        }
        control!(decode_merge_plan, encode_merge_plan);
        control!(decode_merge_candidate, encode_merge_candidate);
        control!(decode_multi_root_plan, encode_multi_root_plan);
        control!(decode_multi_root_candidate, encode_multi_root_candidate);
        control!(decode_publication, encode_publication);
    }
}

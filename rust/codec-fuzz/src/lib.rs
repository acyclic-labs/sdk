//! On-demand oracles call the production decoders; no alternate wire parser.
use prost::Message;
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
        "filesystem_codecs" => filesystem_codecs(bytes),
        "filesystem_pages" => filesystem_pages::check(bytes),
        "machine_journal" => {
            assert!(acyclic_machines::codec_fuzz::check(bytes));
            assert!(acyclic_machines::results::codec_fuzz::check(bytes));
        }
        "harness_codecs" => harness_codecs(bytes),
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
        assert!(matches!(
            acyclic_protobuf_json::decode(descriptor, &trailing, usize::MAX),
            Err(acyclic_protobuf_json::Error::Trailing)
        ));
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
    if let Ok(id) = acyclic_fs::ObjectId::try_from(bytes) {
        assert_eq!(id.to_bytes().as_slice(), bytes);
        assert_eq!(
            acyclic_fs::ObjectId::try_from(id.to_bytes().as_slice()),
            Ok(id)
        );
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

pub fn actor_codecs(bytes: &[u8]) {
    use acyclic_actors::{guest::wire, local::State, timers::TimerRecord};
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
    if let Ok(value) = TimerRecord::decode(bytes) {
        assert_eq!(
            TimerRecord::decode(&value.encode().unwrap()).unwrap(),
            value
        );
    }
    if let Ok(value) = acyclic_stream::wire::Record::decode(bytes)
        && let Ok(record) = acyclic_stream::wire_codec::record(value, None)
        && let Ok(Some(invocation)) = acyclic_actors::invocation::Invocation::decode(&record)
    {
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

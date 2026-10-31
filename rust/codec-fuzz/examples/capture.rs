//! Capture corpus bytes from SDK encoders. Run from this package's directory.
use acyclic_codec_fuzz::DECODERS;
use prost::Message;
use sha2::{Digest as _, Sha256};
use std::{error::Error, path::Path};

fn json_seed(
    decoder: &acyclic_codec_fuzz::Decoder,
    payload: &[u8],
) -> Result<Vec<u8>, Box<dyn Error>> {
    if let Some(name) = decoder.name.strip_prefix("acyclic.objects.v1.") {
        return Ok(acyclic_objects::json::encode_binary(
            name,
            payload,
            usize::MAX,
        )?);
    }
    acyclic_protobuf_json::encode(acyclic_codec_fuzz::descriptor(decoder), payload)
        .map_err(|error| format!("protobuf JSON seed {}: {error:?}", decoder.name).into())
}

// Protobuf maps and JSON object members have no wire-order contract. Keep the
// original captured packet, while comparing fresh writers through the original
// descriptor/JSON codec. Canonical persisted formats still compare exact bytes.
fn equivalent(target: &str, fresh: &[u8], saved: &[u8]) -> Result<bool, Box<dyn Error>> {
    if fresh == saved {
        return Ok(true);
    }
    if !matches!(target, "protobuf" | "protobuf_json") {
        return Ok(false);
    }
    let (Some((selector, fresh)), Some((original, saved))) = (
        fresh.split_first_chunk::<8>(),
        saved.split_first_chunk::<8>(),
    ) else {
        return Ok(false);
    };
    if selector != original {
        return Ok(false);
    }
    let index = usize::try_from(u64::from_le_bytes(*selector))?;
    let decoder = DECODERS
        .get(index)
        .ok_or("golden selector has no decoder")?;
    let value = |bytes: &[u8]| -> Result<serde_json::Value, Box<dyn Error>> {
        if target == "protobuf" {
            let json =
                acyclic_protobuf_json::encode(acyclic_codec_fuzz::descriptor(decoder), bytes)
                    .map_err(|error| format!("golden protobuf projection: {error:?}"))?;
            Ok(serde_json::from_slice(&json)?)
        } else {
            Ok(serde_json::from_slice(bytes)?)
        }
    };
    Ok(value(fresh)? == value(saved)?)
}

fn golden_controls() -> Result<(), Box<dyn Error>> {
    let name = "acyclic.harness.v2.ConversationMessage-user";
    let saved = acyclic_proto_build::golden::frame(&format!("corpus/protobuf/{name}"))?;
    let (selector, payload) = saved
        .split_first_chunk::<8>()
        .ok_or("short golden selector")?;
    let mut message = acyclic_harness_core::wire::ConversationMessage::decode(payload)?;
    let mut fresh = selector.to_vec();
    message.encode(&mut fresh)?;
    assert!(equivalent("protobuf", &fresh, &saved)?);
    // A real writer change must still fail even if extension-map order differs.
    message.sequence = message
        .sequence
        .checked_add(1)
        .ok_or("golden sequence overflow")?;
    let mut changed = selector.to_vec();
    message.encode(&mut changed)?;
    assert!(!equivalent("protobuf", &changed, &saved)?);
    fresh[0] ^= 1;
    assert!(!equivalent("protobuf", &fresh, &saved)?);

    let saved = acyclic_proto_build::golden::frame(&format!("corpus/protobuf_json/{name}"))?;
    let (selector, payload) = saved
        .split_first_chunk::<8>()
        .ok_or("short JSON selector")?;
    let mut fresh = selector.to_vec();
    fresh.push(b' ');
    fresh.extend_from_slice(payload);
    fresh.push(b'\n');
    assert!(equivalent("protobuf_json", &fresh, &saved)?);
    assert!(!equivalent("filesystem_codecs", &fresh, &saved)?);
    Ok(())
}

fn save(
    target: &str,
    name: &str,
    bytes: &[u8],
    producer: &str,
    manifest: &mut String,
) -> Result<(), Box<dyn Error>> {
    let accept = std::env::args().any(|arg| arg == "--accept-goldens");
    let extend = std::env::args().any(|arg| arg == "--extend-goldens");
    let frame = format!("corpus/{target}/{name}");
    let golden = match acyclic_proto_build::golden::frame(&frame) {
        Ok(golden) => Some(golden),
        Err("golden frame is missing") if accept || extend => None,
        Err(error) => return Err(format!("golden {frame}: {error}").into()),
    };
    let golden = if let Some(golden) = golden {
        if !equivalent(target, bytes, &golden)? {
            if !accept {
                return Err(format!("encoder changed golden {frame}; recovery after reviewing the format change: cargo run --example capture -- --accept-goldens").into());
            }
            None
        } else {
            if bytes != golden {
                println!("fresh {frame}: equal protobuf value with different wire order/spelling");
            }
            Some(golden)
        }
    } else {
        None
    };
    match std::panic::catch_unwind(|| acyclic_codec_fuzz::exercise(target, bytes)) {
        Ok(result) => result?,
        Err(_) => return Err(format!(
            "decoder oracle {target}/{name} panicked; recovery: cargo +nightly fuzz run --fuzz-dir . {target} corpus/{target}/{name}"
        ).into()),
    }
    let captured = golden.as_deref().unwrap_or(bytes);
    let directory = Path::new("corpus").join(target);
    std::fs::create_dir_all(&directory)?;
    std::fs::write(directory.join(name), captured)?;
    manifest.push_str(&format!(
        "{target}/{name}\t{}\t{}\t{producer}\n",
        captured.len(),
        hex::encode(Sha256::digest(captured))
    ));
    Ok(())
}

fn capture() -> Result<(), Box<dyn Error>> {
    std::env::set_current_dir(env!("CARGO_MANIFEST_DIR"))?;
    if std::env::args().any(|arg| arg == "--only-storage") {
        #[cfg(feature = "storage-schedules")]
        {
            let mut manifest = std::fs::read_to_string("corpus/SOURCES.tsv")?
                .lines()
                .filter(|line| !line.starts_with("object_schedule/"))
                .map(|line| format!("{line}\n"))
                .collect::<String>();
            capture_storage(&mut manifest)?;
            return finish(&manifest, "object_schedule/");
        }
        #[cfg(not(feature = "storage-schedules"))]
        return Err("storage capture requires storage-schedules; recovery: cargo run --features storage-schedules --example capture -- --only-storage --extend-goldens".into());
    }
    golden_controls()?;
    capture_identity_vectors()?;
    let mut manifest =
        String::from("# Captured SDK encoder output\n# path\tbytes\tsha256\tproducer\n");
    save(
        "session_protocol",
        "admission",
        &acyclic_stream::codec_fuzz::session_seed(),
        "codec_fuzz::session_seed from SessionHello encoder and test credential issuer",
        &mut manifest,
    )?;
    for (index, decoder) in DECODERS.iter().enumerate() {
        let mut bytes = u64::try_from(index)?.to_le_bytes().to_vec();
        bytes.extend_from_slice(&(decoder.seed)());
        save(
            "protobuf",
            decoder.name,
            &bytes,
            "prost::Message::default().encode_to_vec (first 8 bytes select type)",
            &mut manifest,
        )?;
        let mut json = u64::try_from(index)?.to_le_bytes().to_vec();
        json.extend_from_slice(&json_seed(decoder, &(decoder.seed)())?);
        save(
            "protobuf_json",
            decoder.name,
            &json,
            "Objects json::encode_binary or shared acyclic_protobuf_json::encode from typed protobuf default output (first 8 bytes select type)",
            &mut manifest,
        )?;
    }
    let streams = futures::executor::block_on(acyclic_stream::codec_fuzz::seeds())?;
    for (name, bytes) in acyclic_codec_fuzz::inference_http::seeds()? {
        save(
            "inference_http",
            &name,
            &bytes,
            "SDK inference HTTP route descriptors, protobuf JSON request writer and actual HTTP response writer (first 8 bytes select route/direction)",
            &mut manifest,
        )?;
    }
    for (message, variant, payload) in acyclic_codec_fuzz::semantic_seeds(&streams)? {
        if message == "acyclic.harness.v2.ReferencedAttachments" && variant == "inline" {
            use acyclic_harness_core::{conversation as c, wire};
            let c::ReferencedAttachments::Inline { items } = c::ReferencedAttachments::try_from(
                wire::ReferencedAttachments::decode(payload.as_slice())?,
            )?
            else {
                return Err("inline attachment producer changed its representation".into());
            };
            save(
                "harness_codecs",
                "attachment-manifest",
                &c::encode_attachment_manifest(&items)?,
                "conversation::encode_attachment_manifest from positively admitted semantic_seeds inline attachments",
                &mut manifest,
            )?;
        }
        for (name, bytes) in
            acyclic_codec_fuzz::inference_http::semantic_seeds(message, variant, &payload)?
        {
            save(
                "inference_http",
                &name,
                &bytes,
                "SDK semantic_seeds encoder output through its actual Inference HTTP route writer and decoder (first 8 bytes select route/direction)",
                &mut manifest,
            )?;
        }
        let (index, decoder) = DECODERS
            .iter()
            .enumerate()
            .find(|(_, decoder)| decoder.name == message)
            .ok_or("semantic seed has no generated message decoder")?;
        let name = format!("{message}-{variant}");
        let selector = u64::try_from(index)?.to_le_bytes();
        let mut binary = selector.to_vec();
        binary.extend_from_slice(&payload);
        save(
            "protobuf",
            &name,
            &binary,
            "SDK semantic_seeds production wire writers (first 8 bytes select type)",
            &mut manifest,
        )?;
        let mut json = selector.to_vec();
        json.extend_from_slice(&json_seed(decoder, &payload)?);
        save(
            "protobuf_json",
            &name,
            &json,
            "Objects json::encode_binary or shared acyclic_protobuf_json::encode from SDK semantic_seeds writer output (first 8 bytes select type)",
            &mut manifest,
        )?;
    }
    for (name, bytes) in streams {
        save(
            "stream_persistence",
            &name,
            &bytes,
            "acyclic_stream::codec_fuzz::seeds",
            &mut manifest,
        )?;
    }
    for (name, bytes) in acyclic_codec_fuzz::log_frame::seeds()? {
        save(
            "stream_persistence",
            &name,
            &bytes,
            "acyclic_native_runtime::LogFrame::encode with narrow/wide test domains and empty/split bodies; first 16 bytes retain caller minimum/maximum bounds",
            &mut manifest,
        )?;
    }
    for (name, bytes) in acyclic_objects::codec_fuzz::seeds() {
        save(
            "object_persistence",
            name,
            &bytes,
            "acyclic_objects::codec_fuzz::seeds",
            &mut manifest,
        )?;
    }
    #[cfg(feature = "storage-schedules")]
    capture_storage(&mut manifest)?;
    #[cfg(not(feature = "storage-schedules"))]
    {
        let previous = match std::fs::read_to_string("corpus/SOURCES.tsv") {
            Ok(previous) => previous,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(format!("load retained physical corpus provenance failed: {error}; recovery: cargo run --example capture").into()),
        };
        // A syntax-only capture cannot replace physical schedule evidence.
        for line in previous
            .lines()
            .filter(|line| line.starts_with("object_schedule/"))
        {
            manifest.push_str(line);
            manifest.push('\n');
        }
    }
    capture_semantic(&mut manifest)?;
    save(
        "harness_codecs",
        "attachment-manifest-empty",
        &acyclic_harness_core::conversation::encode_attachment_manifest(&[])?,
        "conversation::encode_attachment_manifest from an empty admitted attachment list",
        &mut manifest,
    )?;
    for (name, bytes) in acyclic_harness_core::agent::codec_fuzz::seeds()? {
        save(
            "harness_codecs",
            name,
            &bytes,
            "harness-core canonical agent record, effect and checkpoint encoders",
            &mut manifest,
        )?;
    }
    for (name, bytes) in acyclic_harness_core::filesystem::execution_codec_fuzz::seeds()? {
        save(
            "harness_codecs",
            name,
            &bytes,
            "Filesystem execution observation, checkpoint, index node, model/summary cell and step-link writers; metadata parsing does not authenticate proofs, node digests or selected-step authority",
            &mut manifest,
        )?;
    }
    for (name, bytes) in acyclic_harness_core::mcp::http::codec_fuzz::seeds()? {
        save(
            "harness_codecs",
            name,
            &bytes,
            "mcp::rpc::request production client writer",
            &mut manifest,
        )?;
    }
    save(
        "harness_codecs",
        "mcp-stdio-initialize-request",
        &acyclic_harness_core::mcp::stdio::codec_fuzz::seed()?,
        "McpStdioRequest::exchange production initialization write",
        &mut manifest,
    )?;
    // Preserve actual prior writer output as refusal input; never relabel it v2.
    for name in [
        "state-root-false",
        "state-root-true",
        "state-context",
        "state-sparse",
        "state-refs",
        "state-outcome",
    ] {
        let bytes =
            acyclic_proto_build::golden::frame(&format!("corpus/filesystem_codecs/{name}"))?;
        if !matches!(
            acyclic_fs::distributed::state_record::StateRecord::decode(
                bytes::Bytes::copy_from_slice(&bytes).into()
            ),
            Err(acyclic_fs::AuthorityStoreError::NativeStream(
                acyclic_stream::StreamError::Unsupported
            ))
        ) {
            return Err(format!("old state record {name} was not refused before fields; recovery: cargo run --example capture").into());
        }
        save(
            "filesystem_codecs",
            name,
            &bytes,
            "AuthorityWork::encode_state",
            &mut manifest,
        )?;
    }
    for (name, bytes) in acyclic_fs::distributed::state_record::codec_fuzz::seeds()? {
        save(
            "filesystem_codecs",
            &name,
            &bytes,
            "AuthorityWork::encode_state",
            &mut manifest,
        )?;
    }
    for (name, bytes) in acyclic_fs::s3_http::codec_fuzz::seeds()? {
        save(
            "filesystem_codecs",
            name,
            &bytes,
            "S3 multipart production created/part/completing/completed/aborted writers with explicit upload context",
            &mut manifest,
        )?;
    }
    for (name, bytes) in acyclic_fs::core_state::codec_fuzz::seeds()? {
        save(
            "filesystem_codecs",
            &name,
            &bytes,
            "core_state production checkpoint and checksum frame writers",
            &mut manifest,
        )?;
    }
    for (name, bytes) in acyclic_fs::native_exchange::codec_fuzz::seeds()? {
        save(
            "filesystem_codecs",
            &name,
            &bytes,
            "native_exchange production durable journal encoder",
            &mut manifest,
        )?;
    }
    for (name, bytes) in acyclic_fs::lazy_workspace::codec_fuzz::seeds()? {
        save(
            "filesystem_codecs",
            &name,
            &bytes,
            "LazyDirectoryCursor::encode production funded continuation writer",
            &mut manifest,
        )?;
    }
    for (name, bytes) in acyclic_fs::materializer::codec_fuzz::seeds()? {
        save(
            "filesystem_codecs",
            &name,
            &bytes,
            "materializer production witness and metadata image writers",
            &mut manifest,
        )?;
    }
    for (name, bytes) in acyclic_fs::native_ownership_codec::seeds() {
        save(
            "filesystem_codecs",
            name,
            &bytes,
            "native ownership receipt production encoder with explicit test identities",
            &mut manifest,
        )?;
    }
    save(
        "filesystem_codecs",
        "usn-checkpoint",
        &acyclic_fs::windows_usn_codec::seed(),
        "WindowsUsnCheckpoint::to_bytes",
        &mut manifest,
    )?;
    for (name, bytes) in acyclic_machines::codec_fuzz::seeds()? {
        save(
            "machine_journal",
            name,
            &bytes,
            "machines::provider::journal or results::record production envelope encoder",
            &mut manifest,
        )?;
    }
    for (name, bytes) in acyclic_codec_fuzz::filesystem_pages::seeds()? {
        save(
            "filesystem_pages",
            name,
            &bytes,
            if name.starts_with("historical-") {
                "kernel::encode_original_generation_evidence after LogicalObjectStore<MemoryObjects> writes and prove_generation_closure; original creation/retention writers and exact original caller-context controls (preparation only)"
            } else {
                "filesystem::kernel canonical encoders"
            },
            &mut manifest,
        )?;
    }
    use acyclic_fs::{Digest, ObjectId, ObjectKind, ObjectStorage};
    for tag in 1..=8 {
        for sealed in [false, true] {
            let value = ObjectId {
                kind: ObjectKind::from_canonical_tag(tag)?,
                digest: Digest::from_bytes([1; 32]),
                storage: sealed.then_some(ObjectStorage {
                    locator: Digest::from_bytes([2; 32]),
                    root_file_id: acyclic_fs::FileId::from_bytes([3; 16]),
                    origin_generation: Digest::from_bytes([4; 32]),
                }),
            };
            save(
                "ids",
                &format!("kind-{tag}-sealed-{sealed}"),
                value.to_bytes().as_slice(),
                "ObjectId::to_bytes",
                &mut manifest,
            )?;
            if value.kind == ObjectKind::GenerationRoot {
                save(
                    "ids",
                    &format!("generation-json-sealed-{sealed}"),
                    &serde_json::to_vec(&acyclic_fs::GenerationId::from_root(value))?,
                    "GenerationId::Serialize delegates to the complete canonical to_bytes output",
                    &mut manifest,
                )?;
            }
        }
    }
    use acyclic_stream::{wire, wire_codec::session::encode_grpc_frame};
    let frame = wire::SessionFrame {
        body: Some(wire::session_frame::Body::Window(wire::SessionWindow {
            bytes: 1024,
            frames: 2,
        })),
        ..Default::default()
    };
    save(
        "session_frames",
        "window-delimited",
        &frame.encode_length_delimited_to_vec(),
        "SessionFrame::encode_length_delimited_to_vec",
        &mut manifest,
    )?;
    save(
        "session_frames",
        "window-grpc",
        &encode_grpc_frame(&frame)?,
        "encode_grpc_frame",
        &mut manifest,
    )?;
    for (name, bytes) in acyclic_codec_fuzz::auth_socket::seeds() {
        save(
            "auth",
            &name,
            &bytes,
            "WebsocketConnectGrantProto::from through admitted DNS, suffix, literal IP and CIDR grants with default or explicit ports",
            &mut manifest,
        )?;
    }
    let opaque = b"\0./%\xff\xc3\xa9";
    save(
        "auth",
        "resource-component",
        acyclic_auth::encode_component(opaque).as_bytes(),
        "acyclic_auth::encode_component from opaque control, separator and non-UTF-8 bytes",
        &mut manifest,
    )?;
    save(
        "auth",
        "objects-resource",
        acyclic_auth::objects_resource(b"bucket", opaque).as_bytes(),
        "acyclic_auth::objects_resource from an opaque key containing separators",
        &mut manifest,
    )?;
    let token = acyclic_auth::testing::root(|block| {
        block.caveats.push(acyclic_auth::contract::Caveat {
            kind: acyclic_auth::contract::CaveatKind::ExpiresAtUnixSeconds(500),
        });
    });
    // Preserve the actual former writer capture as a refusal seed, at its
    // original path. Successor producers receive distinct corpus names.
    let previous = include_bytes!("../corpus/auth/signed-capability");
    assert!(matches!(
        acyclic_auth::authority::decode_capability(previous),
        Err(acyclic_auth::Error::UnsupportedVersion(0))
    ));
    save(
        "auth",
        "signed-capability",
        previous,
        "captured v1 acyclic_auth::testing::root (test-only deterministic signing keys)",
        &mut manifest,
    )?;
    save(
        "auth",
        "v2-opaque-bearer",
        &acyclic_codec_fuzz::auth_credential::seed(&token),
        "credential::bearer_bytes from hex-encoded testing::root output using deterministic SDK test keys; opaque transport admission only",
        &mut manifest,
    )?;
    save(
        "auth",
        "v2-signed-capability",
        &token,
        "acyclic_auth::testing::root (test-only deterministic signing keys)",
        &mut manifest,
    )?;
    for (name, bytes) in acyclic_codec_fuzz::auth_capability::seeds(&token)? {
        save(
            "auth",
            name,
            &bytes,
            "VerifiedCapability::attenuate/encode/encode_sealed from the actual signed SDK root (test-only deterministic signing keys)",
            &mut manifest,
        )?;
    }
    let capability = acyclic_auth::authority::decode_capability(&token)?;
    let attributes = acyclic_auth::contract::RecordAttributes::try_new(vec![
        acyclic_auth::contract::Attribute {
            name: "content-type".into(),
            value: b"application/octet-stream".to_vec(),
        },
    ])?;
    save(
        "auth",
        "record-attributes",
        &acyclic_auth::contract::RecordAttributesProto::from(attributes).encode_to_vec(),
        "RecordAttributes::try_new and RecordAttributesProto::encode_to_vec",
        &mut manifest,
    )?;
    for (index, signed) in capability.blocks.into_iter().enumerate() {
        save(
            "auth",
            &format!("block-{index}"),
            &signed.payload,
            "signed capability block payload",
            &mut manifest,
        )?;
        let block = acyclic_auth::authority::decode_block(&signed.payload)?;
        for (ordinal, caveat) in block.caveats.into_iter().enumerate() {
            save(
                "auth",
                &format!("caveat-{index}-{ordinal}"),
                &acyclic_auth::contract::CaveatProto::from(caveat).encode_to_vec(),
                "CaveatProto::encode_to_vec from signed capability",
                &mut manifest,
            )?;
        }
        if let Some(certificates) = block.certificates {
            for (ordinal, certificate) in certificates.keys.into_iter().enumerate() {
                save(
                    "auth",
                    &format!("grants-{index}-{ordinal}"),
                    &acyclic_auth::contract::GrantSetProto::from(certificate.key.grants)
                        .encode_to_vec(),
                    "GrantSetProto::encode_to_vec from signed issuer certificate",
                    &mut manifest,
                )?;
            }
        }
    }
    finish(&manifest, "")
}

#[cfg(feature = "storage-schedules")]
fn capture_storage(manifest: &mut String) -> Result<(), Box<dyn Error>> {
    for (name, bytes) in acyclic_codec_fuzz::object_schedule::seeds() {
        save(
            "object_schedule",
            &name,
            &bytes,
            "Manifest::encode and complete GenerationId::to_bytes with explicit mutation/flush/compaction, fault, original-site selector and torn-prefix configuration; original LocalMedium operation and Schedule oracle",
            manifest,
        )?;
    }
    Ok(())
}

fn finish(manifest: &str, replaced_prefix: &str) -> Result<(), Box<dyn Error>> {
    if std::env::args().any(|arg| matches!(arg.as_str(), "--accept-goldens" | "--extend-goldens")) {
        let path = Path::new("../../compatibility/frames/golden.hex");
        let previous = std::fs::read_to_string(path)?;
        let mut frames = previous
            .lines()
            .filter(|line| {
                !line
                    .strip_prefix("corpus/")
                    .is_some_and(|name| name.starts_with(replaced_prefix))
            })
            .map(|line| format!("{line}\n"))
            .collect::<String>();
        for line in manifest.lines().filter(|line| !line.starts_with('#')) {
            let name = line.split('\t').next().ok_or("missing capture name")?;
            if !name.starts_with(replaced_prefix) {
                continue;
            }
            let bytes = std::fs::read(Path::new("corpus").join(name))?;
            frames.push_str(&format!("corpus/{name} {}\n", hex::encode(bytes)));
        }
        std::fs::write(path, frames)?;
    }
    std::fs::write("corpus/SOURCES.tsv", manifest)?;
    Ok(())
}

fn capture_identity_vectors() -> Result<(), Box<dyn Error>> {
    fn bytes(row: &serde_json::Value, key: &str) -> Result<Vec<u8>, Box<dyn Error>> {
        let value = row
            .get(key)
            .and_then(serde_json::Value::as_str)
            .ok_or("missing identity field")?;
        Ok(hex::decode(value)?)
    }
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../crates/auth/tests/fixtures/resource-identity-v1.json");
    let mut vectors: serde_json::Value = serde_json::from_slice(&std::fs::read(source)?)?;
    let rows = vectors
        .get_mut("pairs")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or("missing identity vectors")?;
    for row in rows {
        let value = acyclic_auth::addressing::ResourceIdentity::new(
            &bytes(row, "account_hex")?,
            &bytes(row, "id_hex")?,
        )?;
        row["encoded_hex"] = hex::encode(value.encode()).into();
    }
    vectors["encoding"] = "acyclic.resource-v1-u64be-length-account-u64be-length-id".into();
    std::fs::create_dir_all("corpus")?;
    let mut output = serde_json::to_vec_pretty(&vectors)?;
    output.push(b'\n');
    std::fs::write("corpus/resource-identity-v1.json", output)?;
    std::fs::write("corpus/actor-guest.ts", acyclic_actors::guest::typescript())?;
    Ok(())
}

fn capture_semantic(manifest: &mut String) -> Result<(), Box<dyn Error>> {
    use acyclic_actors::{
        domain,
        guest::{self, Handler as _},
    };
    use acyclic_auth::addressing::{ActorReference, ResourceIdentity};
    use acyclic_stream::{CommitId, CommitMutation, MemoryStream, StreamPath};
    use bytes::Bytes;
    use std::{collections::BTreeMap, sync::Arc};
    for (name, bytes) in acyclic_codec_fuzz::auth_addressing::seeds()? {
        save(
            "identities",
            &name,
            &bytes,
            "ResourceIdentity::hostname or RoutePattern::as_str with caller URL context derived from the SDK hostname (NUL-separated fuzz tuple)",
            manifest,
        )?;
    }
    let actor = ActorReference::new(b"account", b"namespace", b"codec")?;
    save(
        "identities",
        "actor",
        &actor.encode(),
        "ActorReference::encode",
        manifest,
    )?;
    save(
        "identities",
        "resource",
        &ResourceIdentity::new(b"account", b"resource")?.encode(),
        "ResourceIdentity::encode",
        manifest,
    )?;
    let invocation = acyclic_actors::invocation::Invocation::new(
        actor.clone(),
        "inbox".into(),
        Bytes::from_static(b"operation"),
        Bytes::from_static(b"request"),
    )?;
    let encoded = invocation.record(&StreamPath::new("codec/actors/state")?)?;
    let record = acyclic_stream::Record {
        sequence: 0,
        value: encoded.value,
        headers: encoded.headers,
        commit_id: CommitId::from_bytes([1; 32]),
        committed_at_micros: 42,
        origin: StreamPath::new("codec/actors/inbox")?,
    };
    save(
        "actor_codecs",
        "invocation",
        &acyclic_stream::wire_codec::record_wire(record, None).encode_to_vec(),
        "Invocation::record and wire_codec::record_wire",
        manifest,
    )?;
    for (name, bytes) in acyclic_actors::reservation::codec_fuzz::seeds()? {
        save(
            "actor_codecs",
            name,
            &bytes,
            "actors::reservation canonical request/decision/checkpoint encoder",
            manifest,
        )?;
    }
    save(
        "actor_codecs",
        "artifact-checkpoint",
        &serde_json::to_vec(&guest::Checkpoint {
            codec: guest::CodecIdentity {
                name: "codec".into(),
                version: "1".into(),
            },
            bytes: Bytes::from_static(b"checkpoint"),
        })?,
        "serde_json::to_vec(guest::Checkpoint) with current artifact codec identity",
        manifest,
    )?;
    let package = acyclic_actors::reservation::Package::new(
        domain::CodeSha256::new(vec![1; 32])?,
        domain::ActorLimits::new(1, 1024, 4096)?,
    );
    save(
        "actor_codecs",
        "manifest",
        &guest::wire::encode_manifest(package.manifest()),
        "guest::wire::encode_manifest(reservation::Package::manifest)",
        manifest,
    )?;
    let store = acyclic_actors::local::Store::new(
        Arc::new(MemoryStream::default()),
        StreamPath::new("codec/actors")?,
    );
    let checkpoint = acyclic_actors::reservation::initial_checkpoint(7)?;
    let effects = store.create(&actor, BTreeMap::new(), Some(checkpoint.clone()))?;
    if let CommitMutation::Append { records, .. } = &effects[0].mutation {
        let record = records.first().ok_or("actor creation emitted no state")?;
        save(
            "actor_codecs",
            "state",
            &record.value,
            "local::Store::create state encoder",
            manifest,
        )?;
    } else {
        return Err("actor creation did not append state".into());
    }
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
    save(
        "actor_codecs",
        "delivery",
        &guest::wire::encode_delivery(&delivery, Some(&checkpoint)),
        "guest::wire::encode_delivery",
        manifest,
    )?;
    let transition = guest::Transition {
        response: None,
        checkpoint,
        alarm: guest::AlarmChange::Keep,
        effects: effects.into(),
        participant: None,
    };
    save(
        "actor_codecs",
        "transition",
        &guest::wire::encode_transition(&transition)?,
        "guest::wire::encode_transition",
        manifest,
    )?;
    let timer = acyclic_actors::timers::TimerRecord::new(
        "11111111-1111-4111-8111-111111111111".into(),
        "22222222-2222-4222-8222-222222222222".into(),
        42,
    )?;
    save(
        "actor_codecs",
        "timer",
        &timer.encode()?,
        "TimerRecord::encode",
        manifest,
    )?;
    use acyclic_fs::{
        Digest, DurableCommit, Epoch, OperationId, ProposedCommit, Sequence,
        streams_record::{StreamsAuthorityRecord, StreamsDurableRecord},
    };
    let proposed = ProposedCommit {
        publication: None,
        operation_id: OperationId::from_bytes([1; 16]),
        fingerprint: Digest::from_bytes([2; 32]),
        payload: Bytes::from_static(b"codec payload"),
    };
    let epoch = Epoch::new(1)?;
    save(
        "filesystem_codecs",
        "authority",
        &StreamsAuthorityRecord::encode(&proposed, epoch, u64::MAX)?,
        "StreamsAuthorityRecord::encode",
        manifest,
    )?;
    let durable = DurableCommit {
        epoch,
        sequence: Sequence::new(1),
        operation_id: proposed.operation_id,
        fingerprint: proposed.fingerprint,
        previous_digest: Digest::from_bytes([3; 32]),
        digest: Digest::from_bytes([4; 32]),
        payload: proposed.payload,
        settled_at_micros: None,
    };
    save(
        "filesystem_codecs",
        "durable",
        &StreamsDurableRecord::encode(&durable, u64::MAX)?,
        "StreamsDurableRecord::encode",
        manifest,
    )?;
    let ids = [acyclic_fs::WorkspaceContextId::from_bytes([1; 16])];
    save(
        "filesystem_codecs",
        "discard",
        &acyclic_fs::workspace_context_wire::encode_discard(&ids)?,
        "workspace_context_wire::encode_discard",
        manifest,
    )?;
    Ok(())
}

fn main() {
    match std::panic::catch_unwind(capture) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            eprintln!("corpus capture failed: {error}; recovery: cargo run --example capture");
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("corpus encoder validation panicked; recovery: cargo run --example capture");
            std::process::exit(1);
        }
    }
}

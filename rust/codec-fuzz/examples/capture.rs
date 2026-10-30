//! Capture corpus bytes from SDK encoders. Run from this package's directory.
use acyclic_codec_fuzz::DECODERS;
use prost::Message;
use sha2::{Digest as _, Sha256};
use std::{error::Error, path::Path};

fn save(
    target: &str,
    name: &str,
    bytes: &[u8],
    producer: &str,
    manifest: &mut String,
) -> Result<(), Box<dyn Error>> {
    acyclic_codec_fuzz::exercise(target, bytes)?;
    let directory = Path::new("corpus").join(target);
    std::fs::create_dir_all(&directory)?;
    std::fs::write(directory.join(name), bytes)?;
    manifest.push_str(&format!(
        "{target}/{name}\t{}\t{:x}\t{producer}\n",
        bytes.len(),
        Sha256::digest(bytes)
    ));
    Ok(())
}

async fn capture() -> Result<(), Box<dyn Error>> {
    let mut manifest =
        String::from("# Captured SDK encoder output\n# path\tbytes\tsha256\tproducer\n");
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
        json.extend_from_slice(
            &acyclic_protobuf_json::encode(
                acyclic_codec_fuzz::descriptor(decoder),
                &(decoder.seed)(),
            )
            .map_err(|error| format!("protobuf JSON seed {}: {error:?}", decoder.name))?,
        );
        save(
            "protobuf_json",
            decoder.name,
            &json,
            "acyclic_protobuf_json::encode from typed protobuf default output (first 8 bytes select type)",
            &mut manifest,
        )?;
    }
    for (name, bytes) in acyclic_stream::codec_fuzz::seeds().await? {
        save(
            "stream_persistence",
            &name,
            &bytes,
            "acyclic_stream::codec_fuzz::seeds",
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
    capture_semantic(&mut manifest)?;
    for (name, bytes) in acyclic_codec_fuzz::filesystem_pages::seeds()? {
        save(
            "filesystem_pages",
            name,
            &bytes,
            "filesystem::kernel canonical encoders",
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
    let token = acyclic_auth::testing::root(|block| {
        block.caveats.push(acyclic_auth::contract::Caveat {
            kind: acyclic_auth::contract::CaveatKind::ExpiresAtUnixSeconds(500),
        });
    });
    save(
        "auth",
        "signed-capability",
        &token,
        "acyclic_auth::testing::root (test-only deterministic signing keys)",
        &mut manifest,
    )?;
    let capability = acyclic_auth::authority::decode_capability(&token)?;
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
    std::fs::write("corpus/SOURCES.tsv", manifest)?;
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
    match std::panic::catch_unwind(|| futures::executor::block_on(capture())) {
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

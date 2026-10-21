use super::*;
use crate::core::tests::{command, issuer, operation, reducer_fixture, request_file};
use serde_json::json;

fn restore(
    snapshot: Snapshot,
    owner: &AuthorityIssuer,
    registry: SchemaRegistry,
) -> Result<Reducer> {
    Reducer::restore(snapshot, owner.verifier(&mut None)?, registry, &mut None)
}

#[test]
fn checkpoint_proof_binds_issuer_key_identity_audience_and_format() -> Result<()> {
    let mut reducer = reducer_fixture();
    let content = request_file(br#"{"text":"hello"}"#)?;
    let action = Action::AppendCustom {
        schema: "example.message".into(),
        version: 1,
        content,
    };
    let ApplyResult::Applied { .. } = reducer.apply(&command(operation(70), 0, action))? else {
        panic!("fresh append replayed")
    };
    let snapshot = reducer.snapshot(usize::MAX, &mut None)?.0;
    for other in [
        AuthorityIssuer::new("test", [8; 32], reducer.authority.clone()),
        AuthorityIssuer::new("other", [7; 32], reducer.authority.clone()),
        AuthorityIssuer::new(
            "test",
            [7; 32],
            Authority {
                kind: AggregateKind::Agent,
                id: "other-agent".into(),
            },
        ),
    ] {
        assert!(restore(snapshot.copy(&mut None)?, &other, reducer.schemas.clone()).is_err());
    }
    let mut old = snapshot.copy(&mut None)?;
    old.format_version = 2;
    assert!(restore(old, &issuer(), reducer.schemas.clone()).is_err());
    assert!(restore(snapshot.copy(&mut None)?, &issuer(), SchemaRegistry::new()).is_err());
    let mut changed = SchemaRegistry::new();
    changed.register(
        "example.message",
        1,
        json!({"type":"string"}),
        [8; 32],
        ExtensionForkPolicy::Reset,
        [],
        None,
    )?;
    assert!(restore(snapshot.copy(&mut None)?, &issuer(), changed).is_err());
    assert!(
        reducer
            .schemas
            .register(
                "example.message",
                1,
                json!({"type":"object"}),
                [8; 32],
                ExtensionForkPolicy::Inherit,
                [],
                None
            )
            .is_err()
    );
    let mut expanded = reducer.schemas.clone();
    expanded.register(
        "example.additional",
        1,
        json!({"type":"string"}),
        [9; 32],
        ExtensionForkPolicy::Reset,
        [],
        None,
    )?;
    let restored = restore(snapshot, &issuer(), expanded.clone())?;
    assert_eq!(restored.schemas, expanded);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn canonical_component_peak(value: &impl Serialize) -> Result<u64> {
    let mut converted = None;
    let allocation = acyclic_test_support::measure(|| {
        converted = Some(serde_json::to_value(value).map(|mut value| {
            value.sort_all_objects();
            value
        }));
    });
    std::hint::black_box(converted.expect("conversion ran"))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(allocation.bytes_max)
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn conversation_checkpoint_digest_streams_messages_without_changing_canonical_bytes() -> Result<()>
{
    let mut snapshot = reducer_fixture().snapshot(1, &mut None)?.0;
    for count in [1, 128, 1024] {
        snapshot.projection.conversation.agent = Some(AgentId::from_bytes([3; 16]));
        snapshot.projection.conversation.messages = (1..=count)
            .map(|sequence| {
                Ok(ConversationMessage {
                    id: uuid::Uuid::from_u128(u128::from(sequence)),
                    sequence,
                    kind: crate::conversation::MessageKind::User,
                    content: request_file(b"checkpoint content")?,
                    attachments: crate::conversation::ReferencedAttachments::Inline {
                        items: Vec::new(),
                    },
                    reply_to: None,
                    tool_call_id: None,
                    extensions: BTreeMap::new(),
                })
            })
            .collect::<Result<_>>()?;
        let tuple = (
            snapshot.format_version,
            &snapshot.authority,
            snapshot.revision,
            &snapshot.events,
            &snapshot.projection,
        );
        let bytes = crate::contract::canonical_json_bytes(&tuple)?;
        let mut digest = None;
        let allocation = acyclic_test_support::measure(|| {
            digest = Some(snapshot.digest_with_work(&mut None));
        });
        assert_eq!(
            digest.expect("digest ran")?,
            *blake3::hash(&bytes).as_bytes()
        );
        assert_eq!(allocation.bytes_current, 0);
        if count > 1 {
            let legacy_peak = canonical_component_peak(&snapshot.projection)?;
            assert!(
                allocation.bytes_max < legacy_peak / 2,
                "digest retained the conversation: {} versus legacy {legacy_peak}",
                allocation.bytes_max
            );
        }
        check_streamed_checkpoint_encoding(&snapshot)?;
        if count > 1 {
            // Exercise the optional archive metadata, skipped fields, and
            // nonempty map/set components of the canonical wire projection.
            let conversation = &snapshot.projection.conversation;
            snapshot.projection.conversation = serde_json::from_value(json!({
                "agent": conversation.agent,
                "messages": &conversation.messages()[1..],
                "archived_prefix": {
                    "through_sequence": 1,
                    "history_digest": ([5_u8; 32].to_vec()),
                    "checkpoint": request_file(b"archived checkpoint")?,
                },
            }))
            .map_err(|error| Error::Invalid(error.to_string()))?;
            snapshot.projection.effects_archived = true;
            snapshot.projection.latest_context_checkpoint = Some(request_file(b"checkpoint")?);
            snapshot.projection.active_extension_revision = Some(7);
            snapshot
                .projection
                .published_merges
                .insert(("merge".into(), vec![1, 2, 3]));
            let tuple = (
                snapshot.format_version,
                &snapshot.authority,
                snapshot.revision,
                &snapshot.events,
                &snapshot.projection,
            );
            assert_eq!(
                snapshot.digest_with_work(&mut None)?,
                *blake3::hash(&crate::contract::canonical_json_bytes(&tuple)?).as_bytes()
            );
            check_streamed_checkpoint_encoding(&snapshot)?;
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn check_streamed_checkpoint_encoding(snapshot: &Snapshot) -> Result<()> {
    let expected = crate::contract::canonical_json_bytes(snapshot)?;
    let legacy_peak = canonical_component_peak(snapshot)?;
    let mut encoded = None;
    let allocation = acyclic_test_support::measure(|| {
        encoded = Some(snapshot.encode_canonical_with_work(u64::MAX, &mut None));
    });
    let encoded = encoded.expect("encoder ran")?;
    assert_eq!(encoded, expected);
    assert!(
        allocation.bytes_max < legacy_peak / 2 + encoded.capacity() as u64,
        "checkpoint encoder retained its JSON tree: peak {} versus legacy {legacy_peak}",
        allocation.bytes_max
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn snapshot_digest_peak_is_bounded_by_one_canonical_component() -> Result<()> {
    for (events, reason_bytes) in [(1_u64, 262_144), (128, 256), (1000, 0)] {
        let mut reducer = reducer_fixture();
        for revision in 0..events {
            reducer.apply(&command(
                OperationId::from_bytes(u128::from(revision + 1).to_be_bytes()),
                revision,
                Action::TransitionLifecycle {
                    to: if revision % 2 == 0 {
                        LifecycleState::Active
                    } else {
                        LifecycleState::Waiting
                    },
                    reason: Some("\n\0\"Ã©".repeat(reason_bytes / 5)),
                },
            ))?;
        }
        let snapshot = reducer.snapshot(usize::MAX, &mut None)?.0;
        let tuple = (
            snapshot.format_version,
            &snapshot.authority,
            snapshot.revision,
            &snapshot.events,
            &snapshot.projection,
        );
        let bytes = crate::contract::canonical_json_bytes(&tuple)?;
        let expected = *blake3::hash(&bytes).as_bytes();
        let mut peak = canonical_component_peak(&snapshot.format_version)?
            .max(canonical_component_peak(&snapshot.authority)?)
            .max(canonical_component_peak(&snapshot.revision)?)
            .max(canonical_component_peak(&snapshot.projection)?);
        for event in &snapshot.events {
            peak = peak.max(canonical_component_peak(event)?);
        }
        let mut digest = None;
        let allocation = acyclic_test_support::measure(|| {
            digest = Some(snapshot.digest_with_work(&mut None));
        });
        assert_eq!(digest.expect("digest ran")?, expected);
        assert_eq!(snapshot.state_digest, expected);
        assert_eq!(allocation.bytes_current, 0);
        assert_eq!(allocation.count_current, 0);
        assert!(
            allocation.bytes_max <= peak,
            "{events} events: digest peak {} exceeds component peak {peak}",
            allocation.bytes_max
        );
    }
    Ok(())
}

fn lifecycle_snapshot(events: u64) -> Result<Snapshot> {
    let mut reducer = reducer_fixture();
    for revision in 0..events {
        reducer.apply(&command(
            OperationId::from_bytes(u128::from(revision + 1).to_be_bytes()),
            revision,
            Action::TransitionLifecycle {
                to: if revision % 2 == 0 {
                    LifecycleState::Active
                } else {
                    LifecycleState::Waiting
                },
                reason: None,
            },
        ))?;
    }
    Ok(reducer.snapshot(usize::MAX, &mut None)?.0)
}

fn resign_snapshot(snapshot: &mut Snapshot, verifier: &AuthorityVerifier) -> Result<()> {
    snapshot.state_digest = snapshot.digest_with_work(&mut None)?;
    snapshot.attestation = verifier.canonical_proof(
        b"harness/v6/reducer-checkpoint\0",
        &(&verifier.id, &verifier.audience, snapshot.state_digest),
        &mut None,
    )?;
    Ok(())
}

#[test]
fn snapshot_unique_operations_restore_across_history_sizes() -> Result<()> {
    for events in [0, 1, 4, 16, 64, 255, 256, 257, 1024] {
        let snapshot = lifecycle_snapshot(events)?;
        let restored = restore(snapshot, &issuer(), crate::core::tests::schemas())?;
        assert_eq!(restored.revision(), events);
        assert_eq!(restored.events.len() as u64, events);
    }
    Ok(())
}

#[test]
fn signed_snapshot_duplicate_operations_and_invalid_proofs_are_rejected() -> Result<()> {
    let verifier = issuer().verifier(&mut None)?;
    for events in [256, 257] {
        let original = lifecycle_snapshot(events)?;
        for duplicate_index in [1, 7, events as usize - 1] {
            let mut snapshot = original.copy(&mut None)?;
            snapshot.events[duplicate_index].operation_id = snapshot.events[0].operation_id;
            snapshot.events[duplicate_index].attestation =
                verifier.attest_event(&snapshot.events[duplicate_index], &mut None)?;
            resign_snapshot(&mut snapshot, &verifier)?;
            assert!(matches!(
                restore(snapshot.copy(&mut None)?, &issuer(), crate::core::tests::schemas()),
                Err(Error::Invalid(message)) if message == "snapshot event suffix is invalid"
            ));
            snapshot.events[duplicate_index].attestation[0] ^= 1;
            resign_snapshot(&mut snapshot, &verifier)?;
            assert!(matches!(
                restore(snapshot, &issuer(), crate::core::tests::schemas()),
                Err(Error::Unauthorized(message)) if message == "event admission attestation is invalid"
            ));
        }
    }
    Ok(())
}

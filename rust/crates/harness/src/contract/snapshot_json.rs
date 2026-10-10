//! Closed Snapshot-schema quota for the original authenticated checkpoint path.
//! This is logical payload/collection storage, not allocator nodes or process RSS.

use super::{JsonBounds, JsonCodecCharge};
use crate::{Error, Result, core::Snapshot};
use serde_json::Value;
use std::mem::size_of;

/// Audited upper bounds, never actual allocation operations or retained receipts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotJsonAdmission {
    pub input_bytes: u64,
    pub lexical_items: u64,
    pub maximum_depth: u32,
    pub maximum_owned_bytes: u64,
    pub serde_content_bytes: u64,
    pub typed_collection_bytes: u64,
    pub value_overlap_bytes: u64,
    pub boxed_payload_bytes: u64,
    pub string_payload_bytes: u64,
    pub scratch_bytes: u64,
    pub diagnostic_bytes: u64,
    pub conversation_cache_bytes: u64,
    pub conversation_canonical_scratch_bytes: u64,
}

// Snapshot/Projection's additional fields and the ConversationStateWire moving
// converter. EventPayload's nested labels and diagnostics are audited there.
const SNAPSHOT_DIAGNOSTICS: &str = concat!(
    "Snapshot Projection ConversationStateWire ArchivedConversationPrefix ",
    "format_version authority revision events projection state_digest attestation ",
    "lifecycle active_extensions active_extension_revision extension_states ",
    "configured_extensions active_configurations effects indexed_terminal_effects ",
    "effects_archived forks published_merges conversation latest_context_checkpoint ",
    "interactions bindings capabilities issuer intent_digest scope causal_parent ",
    "messages archived_prefix through_sequence history_digest ",
    "archived conversation prefix is unbound conversation order or identity is invalid ",
    "message identity is nil message sequence must start at one ",
    "extension key must be namespaced tool linkage does not match message kind ",
    "file or volume label is invalid checkpoint snapshot is invalid ",
);

fn multiply(value: u64, factor: u64) -> Result<u64> {
    value.checked_mul(factor).ok_or_else(|| Error::Invalid("snapshot JSON admission overflow".into()))
}
fn sum(values: &[u64]) -> Result<u64> {
    values.iter().try_fold(0u64, |total, value| total.checked_add(*value)
        .ok_or_else(|| Error::Invalid("snapshot JSON admission overflow".into())))
}

/// Allocation-free lexical admission, followed by ONE concrete ownership quota.
/// The callback must reserve `Admission` with a real RAII quota/highwater before
/// the original Snapshot Deserialize runs, release it on failure/cancellation,
/// and adopt actual retained capacities without double counting on success.
/// Keep custody through the last Snapshot use or move into restore's custody.
/// This does not validate the checkpoint MAC, replace serde, claim exact opaque
/// serde CPU/allocation counters, or interrupt arbitrary custom Deserialize.
/// The pinned EventPayload audit and Snapshot's original closed slot inventory
/// must be updated if a reachable type/custom converter changes. Caller-owned
/// input and verifier credentials are not newly owned request memory.
pub(crate) fn snapshot_json_admission<F: FnMut(JsonCodecCharge) -> Result<()> + ?Sized>(
    bytes: &[u8], bounds: JsonBounds, charge: &mut F,
) -> Result<SnapshotJsonAdmission> {
    // A checkpoint spans bounded chunks and is not subject to ONE event's byte
    // ceiling. Preserve the caller's finite total checkpoint allowance instead.
    let bounds = JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds };
    let mut input_bytes = 0u64;
    let mut lexical_items = 0u64;
    super::json::admit_json(bytes, bounds, &mut |event| {
        match event {
            JsonCodecCharge::Read { bytes } => input_bytes = sum(&[input_bytes, bytes])?,
            JsonCodecCharge::Work { items, .. } => lexical_items = sum(&[lexical_items, items])?,
            _ => {}
        }
        charge(event)
    })?;
    let event = super::event_json::event_payload_ownership(input_bytes, lexical_items, bounds.maximum_depth)?;
    let slots = multiply(lexical_items, 2 + 8)?;
    // Reuse core/snapshot.rs's actual reachable ABI inventory. Projection's
    // largest map entry has three components: e.g. String/(EventReference,
    // ExtensionRecord) and Uuid/(InteractionTicket, Option<Resolution>). Each
    // component fits the same slot. Opaque BTreeMap/BTreeSet nodes are excluded.
    let typed_collection_bytes = multiply(slots, multiply(Snapshot::LOGICAL_SCHEMA_SLOT_BYTES, 3)?)?;
    // Unlike one EventPayload, a Snapshot can retain many root payload boxes.
    // Each box has a distinct container/scalar token in the encoded input.
    let boxed_payload_bytes = multiply(lexical_items, event.boxed_payload_bytes)?;
    let diagnostic_bytes = sum(&[event.diagnostic_bytes,
        multiply(SNAPSHOT_DIAGNOSTICS.len() as u64 + 4, 2)?])?;

    // ConversationState::try_from moves its wire messages, but also builds the
    // omitted caches. Messages <= lexical items. Prefix digests use exact length;
    // model_positions uses geometric/minimum Vec growth; each message can own
    // one by_id and one outcomes logical map entry. No map-node size is invented.
    let conversation_cache_bytes = sum(&[
        multiply(lexical_items, size_of::<[u8; 32]>() as u64)?,
        multiply(slots, size_of::<usize>() as u64)?,
        multiply(lexical_items, 2 * size_of::<(uuid::Uuid, usize)>() as u64)?,
    ])?;

    // The SAME original moving converter calls advance_history_digest for each
    // message, which uses the shared canonical engine. Only one such tree/output
    // is live at a time, but it overlaps the wire/typed Snapshot and serde scratch.
    // ConversationMessage has eight fields, the maximum of its reachable fixed
    // structs. Missing optional fields may serialize as null: per source object
    // allow all eight field-key/value pairs in addition to its lexical tokens.
    const MESSAGE_FIELDS: u64 = 8;
    let canonical_items = multiply(lexical_items, 1 + 2 * MESSAGE_FIELDS)?;
    let canonical_slots = multiply(canonical_items, 2 + 8)?;
    let value_slot = size_of::<Value>().max(size_of::<(String, Value)>()) as u64;
    // Fixed scalar strings are UUIDs (32 hex bytes + four hyphens); field names,
    // MessageKind/VolumeOwner/attachment labels are shorter. Numeric scalars are
    // at most full-width u64 (20 bytes), booleans/null are shorter. Two extra
    // bytes cover quotes; another two cover per-token delimiters/punctuation.
    const UUID_TEXT_BYTES: u64 = 2 * size_of::<uuid::Uuid>() as u64 + 4;
    let canonical_tree_strings = sum(&[input_bytes, multiply(canonical_items, UUID_TEXT_BYTES)?])?;
    let canonical_output = sum(&[multiply(input_bytes, 6)?,
        multiply(canonical_items, UUID_TEXT_BYTES + 2 + 2)?])?;
    // collect_str's one live UUID formatting buffer overlaps its copied Value
    // string. Include its minimum/geometric capacity independently of the tree.
    let canonical_format_scratch = multiply(UUID_TEXT_BYTES.max(8), 2)?;
    let conversation_canonical_scratch_bytes = sum(&[
        multiply(canonical_slots, value_slot)?, canonical_tree_strings, canonical_output, canonical_format_scratch,
    ])?;
    let maximum_owned_bytes = sum(&[
        event.serde_content_bytes, typed_collection_bytes, event.value_overlap_bytes,
        boxed_payload_bytes, event.string_payload_bytes, event.scratch_bytes,
        diagnostic_bytes, conversation_cache_bytes, conversation_canonical_scratch_bytes,
    ])?;
    let admission = SnapshotJsonAdmission {
        input_bytes, lexical_items, maximum_depth: bounds.maximum_depth, maximum_owned_bytes,
        serde_content_bytes: event.serde_content_bytes, typed_collection_bytes,
        value_overlap_bytes: event.value_overlap_bytes, boxed_payload_bytes,
        string_payload_bytes: event.string_payload_bytes, scratch_bytes: event.scratch_bytes,
        diagnostic_bytes, conversation_cache_bytes, conversation_canonical_scratch_bytes,
    };
    charge(JsonCodecCharge::Admission { maximum_owned_bytes })?;
    Ok(admission)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Capabilities, IdempotencyKey, OperationId, core::{Action, AggregateKind,
        Authority, AuthorityIssuer, Command, LifecycleState, Reducer, SchemaRegistry}};
    use crate::{AgentId, ProviderRef, conversation::{ConversationMessage, FileDescriptor,
        FileRef, MessageKind, ReferencedAttachments, VolumeClass, VolumeOwner, VolumeRef}};
    use std::collections::BTreeMap;
    use std::cell::Cell;

    fn fixture(reason: String) -> Result<(Snapshot, AuthorityIssuer)> {
        let authority = Authority { kind: AggregateKind::Agent, id: "snapshot-codec-agent".into() };
        let issuer = AuthorityIssuer::new("snapshot-codec-owner", [23; 32], authority.clone());
        let mut reducer = Reducer::new(authority, issuer.verifier(), SchemaRegistry::new());
        reducer.apply(Command {
            operation_id: OperationId::from_bytes([7; 16]),
            idempotency_key: IdempotencyKey::new("snapshot-codec-transition")?,
            expected_revision: 0,
            scope: issuer.root("snapshot-codec-scope", Capabilities::new(["event:append", "lifecycle:manage"])),
            causal_parent: None,
            action: Action::TransitionLifecycle { to: LifecycleState::Active, reason: Some(reason) },
        })?;
        Ok((reducer.snapshot()?, issuer))
    }
    fn bounds(bytes: &[u8]) -> JsonBounds {
        JsonBounds { maximum_bytes: bytes.len() as u64, maximum_items: bytes.len() as u64, maximum_depth: 256 }
    }
    // This is a real logical reservation, not a count of hypothetical mallocs.
    struct Quota<'a> { bytes: u64, live: &'a Cell<u64> }
    impl Drop for Quota<'_> {
        fn drop(&mut self) { self.live.set(self.live.get().checked_sub(self.bytes).unwrap()); }
    }

    #[test]
    fn original_producer_decode_and_authenticated_restore_hold_quota() -> Result<()> {
        let (snapshot, issuer) = fixture("escaped\n\"\\\u{1f600}".into())?;
        let bytes = super::super::canonical_json_bytes(&snapshot)?;
        let live = Cell::new(0);
        let highwater = Cell::new(0);
        let mut quota = None;
        let admission = snapshot_json_admission(&bytes, bounds(&bytes), &mut |event| {
            if let JsonCodecCharge::Admission { maximum_owned_bytes } = event {
                assert!(quota.is_none());
                live.set(maximum_owned_bytes);
                highwater.set(maximum_owned_bytes);
                quota = Some(Quota { bytes: maximum_owned_bytes, live: &live });
            }
            Ok(())
        })?;
        let decoded: Snapshot = super::super::json_from_slice(&bytes)
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(snapshot, decoded);
        assert_eq!(super::super::canonical_json_bytes(&decoded)?, bytes);
        assert_eq!(live.get(), admission.maximum_owned_bytes);
        let restored = Reducer::restore(decoded, issuer.verifier(), SchemaRegistry::new())?;
        assert_eq!(live.get(), highwater.get());
        drop(restored);
        drop(quota);
        assert_eq!(live.get(), 0);
        assert!(highwater.get() > bytes.len() as u64);
        Ok(())
    }

    #[test]
    fn original_conversation_cache_hash_converter_is_in_the_admitted_graph() -> Result<()> {
        let authority = Authority { kind: AggregateKind::Conversation, id: "snapshot-codec-conversation".into() };
        let issuer = AuthorityIssuer::new("snapshot-codec-owner", [23; 32], authority.clone());
        let mut reducer = Reducer::new(authority, issuer.verifier(), SchemaRegistry::new());
        let scope = issuer.root("snapshot-codec-conversation-scope",
            Capabilities::new(["event:append", "conversation:bind", "conversation:append"]));
        let agent = AgentId::from_bytes([9; 16]);
        reducer.apply(Command {
            operation_id: OperationId::from_bytes([10; 16]),
            idempotency_key: IdempotencyKey::new("snapshot-codec-bind")?,
            expected_revision: 0, scope: scope.clone(), causal_parent: None,
            action: Action::BindConversation { agent },
        })?;
        let content = FileRef::new(VolumeRef::new(
            ProviderRef::new("acyclic", "filesystem", "2")?, "scratch",
            VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?,
            "snapshot-history.txt", "version-1",
            FileDescriptor::from_bytes(b"real descriptor bytes", "text/plain")?, "history.txt")?;
        for sequence in 1..=16u64 {
            reducer.apply(Command {
                operation_id: OperationId::from_bytes([sequence as u8 + 10; 16]),
                idempotency_key: IdempotencyKey::new(format!("snapshot-codec-message-{sequence}"))?,
                expected_revision: sequence, scope: scope.clone(), causal_parent: None,
                action: Action::AppendConversationMessage { message: Box::new(ConversationMessage {
                    id: uuid::Uuid::from_u128(sequence as u128), sequence,
                    kind: if sequence % 2 == 1 { MessageKind::User } else { MessageKind::Assistant },
                    content: content.clone(),
                    attachments: ReferencedAttachments::Inline { items: Vec::new() },
                    reply_to: (sequence % 2 == 0).then(|| uuid::Uuid::from_u128((sequence - 1) as u128)),
                    tool_call_id: None, extensions: BTreeMap::new(),
                }) },
            })?;
        }
        let snapshot = reducer.snapshot()?;
        let bytes = super::super::canonical_json_bytes(&snapshot)?;
        let live = Cell::new(0);
        let mut quota = None;
        let admission = snapshot_json_admission(&bytes, bounds(&bytes), &mut |event| {
            if let JsonCodecCharge::Admission { maximum_owned_bytes } = event {
                live.set(maximum_owned_bytes);
                quota = Some(Quota { bytes: maximum_owned_bytes, live: &live });
            }
            Ok(())
        })?;
        let decoded: Snapshot = super::super::json_from_slice(&bytes)
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(decoded, snapshot);
        let restored = Reducer::restore(decoded, issuer.verifier(), SchemaRegistry::new())?;
        let conversation = restored.conversation().unwrap();
        assert_eq!(conversation.history_prefix_digest(16)?,
            reducer.conversation().unwrap().history_prefix_digest(16)?);
        // This method measures cloned cache payload, not allocator/node bytes;
        // the schema quota additionally covers Vec spare capacity and scratch.
        assert!(conversation.clone_cache_logical_bytes()? <= admission.conversation_cache_bytes);
        assert!(admission.conversation_canonical_scratch_bytes > 0);
        assert_eq!(live.get(), admission.maximum_owned_bytes);
        drop(restored);
        drop(quota);
        assert_eq!(live.get(), 0);
        Ok(())
    }

    #[test]
    fn every_lexical_and_reservation_cancellation_precedes_decode() -> Result<()> {
        let (snapshot, _) = fixture("cancel schedule".into())?;
        let bytes = super::super::canonical_json_bytes(&snapshot)?;
        let mut callbacks = 0;
        snapshot_json_admission(&bytes, bounds(&bytes), &mut |_| { callbacks += 1; Ok(()) })?;
        for cancel_at in 0..callbacks {
            let mut index = 0;
            let mut reservations = 0;
            let error = snapshot_json_admission(&bytes, bounds(&bytes), &mut |event| {
                if index == cancel_at { return Err(Error::Invalid("scheduled snapshot cancellation".into())); }
                index += 1;
                if matches!(event, JsonCodecCharge::Admission { .. }) { reservations += 1; }
                Ok(())
            }).unwrap_err();
            assert!(error.to_string().contains("scheduled snapshot cancellation"));
            assert_eq!(reservations, 0);
        }
        Ok(())
    }

    #[test]
    fn byte_item_stack_limits_reject_before_schema_reservation() -> Result<()> {
        let (snapshot, _) = fixture("bounds".into())?;
        let bytes = super::super::canonical_json_bytes(&snapshot)?;
        for bounded in [JsonBounds { maximum_bytes: bytes.len() as u64 - 1, ..bounds(&bytes) },
            JsonBounds { maximum_items: 1, ..bounds(&bytes) },
            JsonBounds { maximum_depth: 1, ..bounds(&bytes) }] {
            let mut reservations = 0;
            assert!(snapshot_json_admission(&bytes, bounded, &mut |event| {
                if matches!(event, JsonCodecCharge::Admission { .. }) { reservations += 1; }
                Ok(())
            }).is_err());
            assert_eq!(reservations, 0);
        }
        Ok(())
    }

    #[test]
    fn one_below_ownership_quota_rejects_and_multichunk_allowance_is_not_an_event_cap() -> Result<()> {
        let (snapshot, _) = fixture("quota boundary".into())?;
        let mut bytes = super::super::canonical_json_bytes(&snapshot)?;
        let target = bytes.len().max(acyclic_stream::MAX_RECORD_BYTES + 1);
        bytes.resize(target, b' ');
        let admission = snapshot_json_admission(&bytes, bounds(&bytes), &mut |_| Ok(()))?;
        assert_eq!(admission.input_bytes, bytes.len() as u64);
        let limit = admission.maximum_owned_bytes - 1;
        let mut live = 0;
        let rejected = snapshot_json_admission(&bytes, bounds(&bytes), &mut |event| {
            if let JsonCodecCharge::Admission { maximum_owned_bytes } = event {
                if maximum_owned_bytes > limit { return Err(Error::Invalid("snapshot logical quota exhausted".into())); }
                live = maximum_owned_bytes;
            }
            Ok(())
        });
        assert!(rejected.is_err());
        assert_eq!(live, 0);
        let retained = Cell::new(0);
        let mut quota = None;
        snapshot_json_admission(&bytes, bounds(&bytes), &mut |event| {
            if let JsonCodecCharge::Admission { maximum_owned_bytes } = event {
                retained.set(maximum_owned_bytes);
                quota = Some(Quota { bytes: maximum_owned_bytes, live: &retained });
            }
            Ok(())
        })?;
        let decoded: Snapshot = super::super::json_from_slice(&bytes)
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(decoded, snapshot);
        drop(decoded);
        drop(quota);
        assert_eq!(retained.get(), 0);
        Ok(())
    }

    #[test]
    fn bounded_original_malformed_duplicate_errors_release_quota() -> Result<()> {
        let (snapshot, _) = fixture("diagnostics".into())?;
        let canonical = super::super::canonical_json_bytes(&snapshot)?;
        for prefix in ["\"format_version\":6,", "\"unknown\\u0000field\":\"value\","] {
            let mut bytes = Vec::from(b"{".as_slice());
            bytes.extend_from_slice(prefix.as_bytes());
            bytes.extend_from_slice(&canonical[1..]);
            let live = Cell::new(0);
            let mut quota = None;
            snapshot_json_admission(&bytes, bounds(&bytes), &mut |event| {
                if let JsonCodecCharge::Admission { maximum_owned_bytes } = event {
                    live.set(maximum_owned_bytes);
                    quota = Some(Quota { bytes: maximum_owned_bytes, live: &live });
                }
                Ok(())
            })?;
            assert!(super::super::json_from_slice::<Snapshot>(&bytes).is_err());
            drop(quota);
            assert_eq!(live.get(), 0);
        }
        Ok(())
    }

    proptest::proptest! {
        #[test]
        fn producer_escape_and_retained_event_slots_are_admitted(reason in ".{0,128}", copies in 1usize..64) {
            let (mut snapshot, _) = fixture(reason).unwrap();
            let event = snapshot.events[0].clone();
            snapshot.events = vec![event; copies];
            // This adversarial cache is intentionally not MAC-authenticated;
            // admission bounds storage, never promotes it to authority.
            let bytes = super::super::canonical_json_bytes(&snapshot).unwrap();
            let live = Cell::new(0);
            let mut quota = None;
            let admission = snapshot_json_admission(&bytes, bounds(&bytes), &mut |event| {
                if let JsonCodecCharge::Admission { maximum_owned_bytes } = event {
                    live.set(maximum_owned_bytes);
                    quota = Some(Quota { bytes: maximum_owned_bytes, live: &live });
                }
                Ok(())
            }).unwrap();
            let decoded: Snapshot = super::super::json_from_slice(&bytes).unwrap();
            let visible_slots = decoded.events.capacity() * size_of::<crate::core::Event>();
            proptest::prop_assert!(visible_slots as u64 <= admission.typed_collection_bytes);
            proptest::prop_assert_eq!(&decoded, &snapshot);
            proptest::prop_assert!(admission.boxed_payload_bytes >= admission.lexical_items);
            drop(decoded);
            drop(quota);
            proptest::prop_assert_eq!(live.get(), 0);
        }
    }
}

//! Closed Snapshot-schema quota for the original authenticated checkpoint path.

use super::{JsonBounds, JsonCodecCharge};
use crate::{Error, Result, core::Snapshot};

pub(super) const SNAPSHOT_DIAGNOSTICS: &str = concat!(
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

/// Same-pass lexical and selected Raw-Value admission, then ONE ownership quota.
pub(crate) fn snapshot_json_admission<F: FnMut(JsonCodecCharge) -> Result<()> + ?Sized>(
    bytes: &[u8], bounds: JsonBounds, charge: &mut F,
) -> Result<u64> {
    let bounds = JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds };
    let value_shape = super::json::admit_json_value_shape(
        bytes, bounds, super::json::JsonValueSchema::Snapshot, &mut |event| charge(event),
    )?;
    let admission = super::event_json::json_decode_owned_bytes(value_shape, true, None)?.0;
    charge(JsonCodecCharge::Admission { maximum_owned_bytes: admission, value_shape })?;
    Ok(admission)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Capabilities, IdempotencyKey, OperationId, core::{Action, AggregateKind,
        Authority, AuthorityIssuer, Command, LifecycleState, Reducer, SchemaRegistry}};
    use crate::{AgentId, resources::ProviderRef, conversation::{ConversationMessage, FileDescriptor,
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
    fn interaction_fixture() -> Result<Snapshot> {
        use crate::interaction::{InteractionKind, InteractionOutcome, InteractionResolution, InteractionTicket};
        let (snapshot, issuer) = fixture("interaction outcomes".into())?;
        let mut reducer = Reducer::restore(snapshot, issuer.verifier(), SchemaRegistry::new())?;
        let agent = AgentId::from_bytes([9; 16]);
        let ticket = InteractionTicket {
            id: uuid::Uuid::from_bytes([10; 16]),
            kind: InteractionKind::Question,
            request: FileRef::new(VolumeRef::new(
                ProviderRef::new("acyclic", "filesystem", "2")?, "scratch",
                VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?,
                "request.json", "version-1",
                FileDescriptor::from_bytes(b"{}", "application/json")?, "request.json")?,
            deadline_unix_ms: None, approval: None,
        };
        let scope = issuer.root("snapshot-codec-interactions", Capabilities::new([
            "event:append", "interaction:open", "interaction:resolve",
            ticket.responder_grant().as_str(),
        ]));
        reducer.apply(Command {
            operation_id: OperationId::from_bytes([8; 16]),
            idempotency_key: IdempotencyKey::new("snapshot-codec-open")?,
            expected_revision: 1, scope: scope.clone(), causal_parent: None,
            action: Action::OpenInteraction { ticket: ticket.clone() },
        })?;
        reducer.apply(Command {
            operation_id: OperationId::from_bytes([9; 16]),
            idempotency_key: IdempotencyKey::new("snapshot-codec-resolve")?,
            expected_revision: 2, scope, causal_parent: None,
            action: Action::ResolveInteraction { resolution: InteractionResolution {
                id: ticket.id, expected_version: 1,
                outcome: InteractionOutcome::Declined, detail: None,
            } },
        })?;
        reducer.snapshot()
    }
    fn raw_outcomes(snapshot: &Snapshot, raw: &str, event: bool, projection: bool) -> Vec<u8> {
        let mut wire = serde_json::to_value(snapshot).unwrap();
        let raw = serde_json::json!({ "$serde_json::private::RawValue": raw });
        if event {
            let payload = wire["events"].as_array_mut().unwrap().iter_mut()
                .map(|event| &mut event["payload"])
                .find(|payload| payload["kind"] == "interaction_resolved").unwrap();
            payload["resolution"]["outcome"] = raw.clone();
        }
        if projection {
            let interaction = wire["projection"]["interactions"].as_object_mut().unwrap()
                .values_mut().next().unwrap();
            interaction[1]["outcome"] = raw;
        }
        serde_json::to_vec(&wire).unwrap()
    }

    #[test]
    fn selected_raw_hidden_items_and_stack_reject_before_original_serde() -> Result<()> {
        let snapshot = interaction_fixture()?;
        let wide = format!(r#"{{"kind":"declined","hidden":[{}]}}"#, vec!["0"; 128].join(","));
        let deep = format!(r#"{{"kind":"declined","hidden":{}0{}}}"#, "[".repeat(300), "]".repeat(300));
        for (event, projection) in [(true, false), (false, true)] {
            let bytes = raw_outcomes(&snapshot, &wide, event, projection);
            assert!(super::super::json_from_slice::<Snapshot>(&bytes).is_err());
            let item_bounds = JsonBounds { maximum_items: super::super::json::admit_json_value_shape(&bytes, bounds(&bytes), super::super::JsonValueSchema::Snapshot, &mut |_| Ok(()))?.lexical_items(), ..bounds(&bytes) };
            let deep_bytes = raw_outcomes(&snapshot, &deep, event, projection);
            for (bytes, limits) in [(&bytes, item_bounds), (&deep_bytes, bounds(&deep_bytes))] {
                let mut reservations = 0;
                let mut decoded = false;
                let result = snapshot_json_admission(bytes, limits, &mut |charge| {
                    if matches!(charge, JsonCodecCharge::Admission { .. }) { reservations += 1; }
                    Ok(())
                }).and_then(|_| {
                    decoded = true;
                    super::super::json_from_slice::<Snapshot>(bytes)
                        .map_err(|error| Error::Storage(error.to_string()))
                });
                assert!(result.is_err());
                assert_eq!(reservations, 0);
                assert!(!decoded);
            }
        }
        Ok(())
    }

    fn bounds(bytes: &[u8]) -> JsonBounds {
        JsonBounds { maximum_bytes: bytes.len() as u64, maximum_items: bytes.len() as u64, maximum_depth: 256 }
    }
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
            if let JsonCodecCharge::Admission { maximum_owned_bytes, .. } = event {
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
        assert_eq!(live.get(), admission);
        let restored = Reducer::restore(decoded, issuer.verifier(), SchemaRegistry::new())?;
        assert_eq!(live.get(), highwater.get());
        drop(restored);
        drop(quota);
        assert_eq!(live.get(), 0);
        assert!(highwater.get() > bytes.len() as u64);
        Ok(())
    }

}

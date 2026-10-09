//! Authenticated restoration of the reducer projection, without transition replay.

use super::{
    Authority, AuthorityVerifier, EffectState, Event, EventReference, ExtensionConfiguration,
    ExtensionDependency, ExtensionRecord, LifecycleState, Reducer, SchemaRegistry,
};
use crate::{
    EffectId, Error, Result,
    conversation::{ConversationState, FileRef},
    fork::ForkSeed,
    interaction::{InteractionResolution, InteractionTicket},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Versioned, issuer-authenticated accelerator; Stream events remain authoritative.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    /// Snapshot format version. Earlier replay-based formats are unsupported.
    pub format_version: u32,
    /// Aggregate represented by the snapshot.
    pub authority: Authority,
    /// Last included event revision.
    pub revision: u64,
    /// Retained canonical event cache, also covered by the attestation.
    pub events: Vec<Event>,
    projection: Projection,
    /// Canonical digest of the complete state and retained cache.
    pub state_digest: [u8; 32],
    /// Domain-separated issuer proof over the digest and aggregate identity.
    pub attestation: [u8; 32],
}

// Keep configuration and fork maps as ordered entry arrays: their structured
// keys cannot be represented by JSON object keys. UUID-keyed maps remain maps.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Projection {
    lifecycle: LifecycleState,
    active_extensions: Vec<ExtensionDependency>,
    active_extension_revision: Option<u64>,
    extension_states: BTreeMap<String, (EventReference, ExtensionRecord)>,
    configured_extensions: Vec<(ExtensionDependency, ExtensionConfiguration)>,
    active_configurations: Vec<ExtensionConfiguration>,
    effects: BTreeMap<EffectId, EffectState>,
    forks: Vec<(Authority, ForkSeed)>,
    published_merges: BTreeSet<(String, Vec<u8>)>,
    conversation: ConversationState,
    latest_context_checkpoint: Option<FileRef>,
    interactions: BTreeMap<uuid::Uuid, (InteractionTicket, Option<InteractionResolution>)>,
    // Rebuild the retry map from the authenticated cache, without applying
    // payloads. This avoids storing every retained Event a second time.
    bindings: Vec<(ExtensionDependency, [u8; 32])>,
}

impl Reducer {
    /// Captures issuer-authenticated state for direct projection restoration.
    /// Closed checkpoint-covered conversation prefixes are represented by their
    /// authenticated logical cut and digest. Terminal projections still need cold retention.
    pub fn snapshot(&self) -> Result<Snapshot> {
        let conversation = self.checkpointed_conversation()?;
        let projection = Projection {
            lifecycle: self.lifecycle,
            active_extensions: self.active_extensions.clone(),
            active_extension_revision: self.active_extension_revision,
            extension_states: self.extension_states.clone(),
            configured_extensions: self.configured_extensions.clone().into_iter().collect(),
            active_configurations: self.active_configurations.clone(),
            effects: self.effects.clone(),
            forks: self.forks.clone().into_iter().collect(),
            published_merges: self.published_merges.clone(),
            conversation,
            latest_context_checkpoint: self.latest_context_checkpoint.clone(),
            interactions: self.interactions.clone(),
            bindings: registry_bindings(&self.schemas)?,
        };
        let mut snapshot = Snapshot {
            format_version: 6,
            authority: self.authority.clone(),
            revision: self.revision,
            events: self.events.iter().cloned().collect(),
            projection,
            state_digest: [0; 32],
            attestation: [0; 32],
        };
        snapshot.state_digest = snapshot.digest()?;
        snapshot.attestation = self.authority_verifier.attest_snapshot(&snapshot)?;
        Ok(snapshot)
    }

    fn resident_checkpoint_selection(&self) -> Option<&crate::conversation::ModelContextSelection> {
        self.events
            .iter()
            .rev()
            .find_map(|event| match &event.payload {
                super::EventPayload::ModelContextSelected { selection }
                    if selection.checkpoint.as_ref() == self.latest_context_checkpoint.as_ref()
                        && selection.checkpoint.is_some() =>
                {
                    Some(selection)
                }
                _ => None,
            })
    }

    fn checkpointed_conversation(&self) -> Result<ConversationState> {
        let checkpoint_selection = self.resident_checkpoint_selection();
        match checkpoint_selection {
            Some(selection) => self.conversation.checkpointed_suffix(selection),
            None => Ok(self.conversation.clone()),
        }
    }

    pub(crate) fn compact_conversation_projection(&mut self) -> Result<()> {
        let selection = self.resident_checkpoint_selection();
        let Some(selection) = selection else {
            return Ok(());
        };
        if selection.conversation_revision <= self.conversation.resident_after_sequence()
            || self.conversation.unresolved_user().is_some()
        {
            return Ok(());
        }
        self.conversation = self.conversation.checkpointed_suffix(selection)?;
        Ok(())
    }

    /// Restores authenticated state without invoking any historical transition.
    /// Original registry bindings must still be installed exactly; additional
    /// bindings may be installed without invalidating the checkpoint.
    pub fn restore(
        snapshot: Snapshot,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
    ) -> Result<Self> {
        snapshot.verify(&authority_verifier, &schemas)?;
        let operation_positions = snapshot
            .events
            .iter()
            .map(|event| (event.operation_id, event.revision))
            .collect();
        let projection = snapshot.projection;
        Ok(Self {
            authority: snapshot.authority,
            authority_verifier,
            schemas,
            revision: snapshot.revision,
            lifecycle: projection.lifecycle,
            active_extensions: projection.active_extensions,
            active_extension_revision: projection.active_extension_revision,
            extension_states: projection.extension_states,
            configured_extensions: projection.configured_extensions.into_iter().collect(),
            active_configurations: projection.active_configurations,
            events: snapshot.events.into(),
            resident_event_limit: usize::MAX,
            operation_positions,
            effects: projection.effects,
            forks: projection.forks.into_iter().collect(),
            published_merges: projection.published_merges,
            conversation: projection.conversation,
            latest_context_checkpoint: projection.latest_context_checkpoint,
            interactions: projection.interactions,
        })
    }
}

impl Snapshot {
    fn digest(&self) -> Result<[u8; 32]> {
        crate::contract::canonical_json_digest(&(
            self.format_version,
            &self.authority,
            self.revision,
            &self.events,
            &self.projection,
        ))
    }

    fn verify(&self, verifier: &AuthorityVerifier, schemas: &SchemaRegistry) -> Result<()> {
        if self.format_version != 6 {
            return Err(Error::Unsupported(format!(
                "snapshot format {}",
                self.format_version
            )));
        }
        verifier.verify_audience(&self.authority)?;
        if self.digest()? != self.state_digest {
            return Err(Error::Invalid("snapshot digest mismatch".into()));
        }
        if verifier.attest_snapshot(self)? != self.attestation {
            return Err(Error::Unauthorized(
                "snapshot admission attestation is invalid".into(),
            ));
        }
        let mut identities = BTreeSet::new();
        let mut previous = None;
        for event in &self.events {
            verifier.verify_event(event)?;
            if event.revision == 0
                || event.revision > self.revision
                || previous
                    .is_some_and(|revision: u64| revision.checked_add(1) != Some(event.revision))
                || !identities.insert(event.operation_id)
            {
                return Err(Error::Invalid("snapshot event suffix is invalid".into()));
            }
            previous = Some(event.revision);
        }
        if previous.unwrap_or(0) != self.revision {
            return Err(Error::Invalid(
                "snapshot event suffix does not reach its head".into(),
            ));
        }
        if self
            .projection
            .active_extension_revision
            .is_some_and(|revision| revision == 0 || revision > self.revision)
        {
            return Err(Error::Invalid(
                "snapshot extension activation revision is invalid".into(),
            ));
        }
        for (extension, expected) in &self.projection.bindings {
            let binding = schemas.pinned_binding(&extension.name, extension.version, None)?;
            if crate::contract::canonical_json_digest(binding)? != *expected {
                return Err(Error::Conflict(
                    "snapshot extension binding mismatch".into(),
                ));
            }
        }
        Ok(())
    }
}

impl AuthorityVerifier {
    fn attest_snapshot(&self, snapshot: &Snapshot) -> Result<[u8; 32]> {
        let canonical = crate::contract::canonical_json_bytes(&(
            &self.id,
            &self.audience,
            snapshot.state_digest,
        ))?;
        let mut hasher = blake3::Hasher::new_keyed(&self.key);
        hasher.update(b"harness/v6/reducer-checkpoint\0");
        hasher.update(&(canonical.len() as u64).to_le_bytes());
        hasher.update(&canonical);
        Ok(*hasher.finalize().as_bytes())
    }
}

fn registry_bindings(schemas: &SchemaRegistry) -> Result<Vec<(ExtensionDependency, [u8; 32])>> {
    schemas
        .schemas
        .iter()
        .map(|((name, version), binding)| {
            Ok((
                ExtensionDependency {
                    name: name.clone(),
                    version: *version,
                },
                crate::contract::canonical_json_digest(binding)?,
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{AggregateKind, AuthorityIssuer, ExtensionForkPolicy};
    use serde_json::json;

    fn fixture() -> (Reducer, AuthorityIssuer) {
        let authority = Authority {
            kind: AggregateKind::Agent,
            id: "checkpoint-agent".into(),
        };
        let issuer = AuthorityIssuer::new("checkpoint-owner", [23; 32], authority.clone());
        (
            Reducer::new(authority, issuer.verifier(), SchemaRegistry::new()),
            issuer,
        )
    }

    #[test]
    fn recomputing_public_digest_cannot_authenticate_forged_projection() -> Result<()> {
        let (reducer, issuer) = fixture();
        let original = reducer.snapshot()?;
        let mut forged = original.clone();
        forged.projection.lifecycle = LifecycleState::Completed;
        forged.state_digest = forged.digest()?;
        assert!(matches!(
            Reducer::restore(forged, issuer.verifier(), SchemaRegistry::new()),
            Err(Error::Unauthorized(_))
        ));
        let mut forged_cache = original;
        forged_cache.revision = 50;
        forged_cache.state_digest = forged_cache.digest()?;
        assert!(matches!(
            Reducer::restore(forged_cache, issuer.verifier(), SchemaRegistry::new()),
            Err(Error::Unauthorized(_))
        ));
        Ok(())
    }

    #[test]
    fn checkpoint_proof_binds_issuer_key_identity_audience_and_format() -> Result<()> {
        let (reducer, issuer) = fixture();
        let snapshot = reducer.snapshot()?;
        for other in [
            AuthorityIssuer::new("checkpoint-owner", [24; 32], reducer.authority.clone()),
            AuthorityIssuer::new("other-owner", [23; 32], reducer.authority),
            AuthorityIssuer::new(
                "checkpoint-owner",
                [23; 32],
                Authority {
                    kind: AggregateKind::Agent,
                    id: "other-agent".into(),
                },
            ),
        ] {
            assert!(matches!(
                Reducer::restore(snapshot.clone(), other.verifier(), SchemaRegistry::new()),
                Err(Error::Unauthorized(_))
            ));
        }
        let mut old = snapshot;
        old.format_version = 2;
        assert!(matches!(
            Reducer::restore(old, issuer.verifier(), SchemaRegistry::new()),
            Err(Error::Unsupported(_))
        ));
        Ok(())
    }

    #[test]
    fn restore_pins_complete_installed_bindings_and_allows_new_namespaces() -> Result<()> {
        let (mut reducer, issuer) = fixture();
        reducer.schemas.register_configured(
            "example.pinned",
            1,
            json!({"type":"object"}),
            [8; 32],
            ExtensionForkPolicy::Inherit,
            [],
            Some(json!({"type":"object"})),
        )?;
        let snapshot = reducer.snapshot()?;
        assert!(matches!(
            Reducer::restore(snapshot.clone(), issuer.verifier(), SchemaRegistry::new()),
            Err(Error::Unsupported(_))
        ));
        for edit in 0..5 {
            let mut changed = reducer.schemas.clone();
            let binding = changed
                .schemas
                .get_mut(&("example.pinned".into(), 1))
                .ok_or_else(|| Error::Storage("fixture binding missing".into()))?;
            match edit {
                0 => binding.schema = json!({"type":"string"}),
                1 => binding.implementation_digest = [9; 32],
                2 => binding.fork_policy = ExtensionForkPolicy::Reset,
                3 => {
                    binding.dependencies.insert(ExtensionDependency {
                        name: "example.dependency".into(),
                        version: 1,
                    });
                }
                _ => binding.configuration_schema = Some(json!({"type":"string"})),
            }
            assert!(matches!(
                Reducer::restore(snapshot.clone(), issuer.verifier(), changed),
                Err(Error::Conflict(_))
            ));
        }
        let mut expanded = reducer.schemas.clone();
        expanded.register(
            "example.additional",
            1,
            json!({"type":"string"}),
            [9; 32],
            ExtensionForkPolicy::Reset,
        )?;
        let restored = Reducer::restore(snapshot, issuer.verifier(), expanded.clone())?;
        reducer.schemas = expanded;
        assert_eq!(restored, reducer);
        Ok(())
    }
}

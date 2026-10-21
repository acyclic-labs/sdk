use super::*;
use serde::ser::{SerializeMap, SerializeSeq};
#[cfg(all(test, not(target_arch = "wasm32")))]
mod message_tests;

struct Canonical<T>(T);

impl<T: Serialize> Serialize for Canonical<T> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut value = serde_json::to_value(&self.0).map_err(serde::ser::Error::custom)?;
        value.sort_all_objects();
        value.serialize(serializer)
    }
}

struct Events<'a>(&'a [Event]);

impl Serialize for Events<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for event in self.0 {
            sequence.serialize_element(&Canonical(event))?;
        }
        sequence.end()
    }
}

pub(super) fn without_work(snapshot: &Snapshot) -> Result<[u8; 32]> {
    crate::executor::json::serialized_json_digest(&(
        snapshot.format_version,
        Canonical(&snapshot.authority),
        snapshot.revision,
        Events(&snapshot.events),
        CanonicalProjection(&snapshot.projection),
    ))
}

pub(super) fn encode_without_work(snapshot: &Snapshot) -> Result<Vec<u8>> {
    serde_json::to_vec(&CanonicalSnapshot(snapshot))
        .map_err(|error| Error::Invalid(error.to_string()))
}

struct CanonicalSnapshot<'a>(&'a Snapshot);

impl Serialize for CanonicalSnapshot<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let Snapshot {
            format_version,
            authority,
            revision,
            events,
            projection,
            state_digest,
            attestation,
        } = self.0;
        let mut map = serializer.serialize_map(Some(7))?;
        map.serialize_entry("attestation", &Canonical(attestation))?;
        map.serialize_entry("authority", &Canonical(authority))?;
        map.serialize_entry("events", &Events(events))?;
        map.serialize_entry("format_version", format_version)?;
        map.serialize_entry("projection", &CanonicalProjection(projection))?;
        map.serialize_entry("revision", revision)?;
        map.serialize_entry("state_digest", &Canonical(state_digest))?;
        map.end()
    }
}

// Stream the unbounded conversation one message at a time. Converting the
// entire projection into a JSON value duplicates every historical message at
// each checkpoint. Keys here retain the canonical byte order of that value.
struct CanonicalProjection<'a>(&'a Projection);

impl Serialize for CanonicalProjection<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let Projection {
            lifecycle,
            active_extensions,
            active_extension_revision,
            extension_states,
            configured_extensions,
            active_configurations,
            effects,
            indexed_terminal_effects,
            effects_archived,
            forks,
            published_merges,
            conversation,
            latest_context_checkpoint,
            interactions,
            bindings,
        } = self.0;
        let mut map = serializer.serialize_map(None)?;
        macro_rules! field {
            ($name:ident) => {
                map.serialize_entry(stringify!($name), &Canonical($name))?;
            };
        }
        field!(active_configurations);
        field!(active_extension_revision);
        field!(active_extensions);
        field!(bindings);
        map.serialize_entry(
            "configured_extensions",
            &CanonicalEntries(configured_extensions),
        )?;
        map.serialize_entry("conversation", &CanonicalConversation(conversation))?;
        field!(effects);
        if *effects_archived {
            field!(effects_archived);
        }
        field!(extension_states);
        map.serialize_entry("forks", &CanonicalEntries(forks))?;
        if !indexed_terminal_effects.is_empty() {
            field!(indexed_terminal_effects);
        }
        field!(interactions);
        field!(latest_context_checkpoint);
        field!(lifecycle);
        field!(published_merges);
        map.end()
    }
}

struct CanonicalEntries<'a, K, V>(&'a BTreeMap<K, V>);

impl<K: Serialize, V: Serialize> Serialize for CanonicalEntries<'_, K, V> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for entry in self.0 {
            sequence.serialize_element(&Canonical(entry))?;
        }
        sequence.end()
    }
}

struct CanonicalConversation<'a>(&'a ConversationState);

impl Serialize for CanonicalConversation<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("agent", &Canonical(&self.0.agent))?;
        if let Some(prefix) = self.0.archived_prefix() {
            map.serialize_entry("archived_prefix", &Canonical(prefix))?;
        }
        map.serialize_entry("messages", &CanonicalMessages(self.0.messages()))?;
        map.end()
    }
}

struct CanonicalMessages<'a>(&'a [ConversationMessage]);

impl Serialize for CanonicalMessages<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        let mut previous = None;
        for message in self.0 {
            // Repeated file references are immutable complete descriptors. Keep
            // only the previous reference's canonical component, not a JSON
            // tree for every message or an unbounded cache of distinct files.
            if !previous
                .as_ref()
                .is_some_and(|(reference, _)| *reference == &message.content)
            {
                let mut content =
                    serde_json::to_value(&message.content).map_err(serde::ser::Error::custom)?;
                content.sort_all_objects();
                previous = Some((&message.content, content));
            }
            let (_, content) = previous
                .as_ref()
                .ok_or_else(|| serde::ser::Error::custom("canonical content is absent"))?;
            sequence.serialize_element(&CanonicalMessage { message, content })?;
        }
        sequence.end()
    }
}

struct CanonicalMessage<'a> {
    message: &'a ConversationMessage,
    content: &'a serde_json::Value,
}

impl Serialize for CanonicalMessage<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let ConversationMessage {
            attachments,
            content: _,
            extensions,
            id,
            kind,
            reply_to,
            sequence,
            tool_call_id,
        } = self.message;
        let mut map = serializer.serialize_map(Some(8))?;
        map.serialize_entry("attachments", &Canonical(attachments))?;
        map.serialize_entry("content", self.content)?;
        map.serialize_entry("extensions", &Canonical(extensions))?;
        map.serialize_entry("id", id)?;
        map.serialize_entry("kind", kind)?;
        map.serialize_entry("reply_to", reply_to)?;
        map.serialize_entry("sequence", sequence)?;
        map.serialize_entry("tool_call_id", tool_call_id)?;
        map.end()
    }
}

//! Logical ownership of the main reducer's records.
use super::*;
use std::io::Write;

pub(crate) fn owned_sum(parts: impl IntoIterator<Item = u64>) -> Result<u64> {
    parts.into_iter().try_fold(0_u64, |sum, bytes| {
        sum.checked_add(bytes)
            .ok_or_else(|| Error::Invalid("component ownership overflows".into()))
    })
}
pub(crate) fn owned_buffer<T>(capacity: usize) -> Result<u64> {
    capacity
        .checked_mul(std::mem::size_of::<T>())
        .map(|bytes| bytes as u64)
        .ok_or_else(|| Error::Invalid("component buffer ownership overflows".into()))
}
pub(crate) fn owned_vector<T>(
    values: &Vec<T>,
    mut item: impl FnMut(&T) -> Result<u64>,
) -> Result<u64> {
    values
        .iter()
        .try_fold(owned_buffer::<T>(values.capacity())?, |sum, value| {
            owned_sum([sum, item(value)?])
        })
}

#[derive(Default)]
struct LogicalSize {
    bytes: u64,
    nodes: u64,
    depth: u32,
    quoted: bool,
    escaped: bool,
}
impl Write for LogicalSize {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let invalid = || std::io::Error::from(std::io::ErrorKind::InvalidData);
        self.bytes = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(invalid)?;
        for byte in bytes {
            if self.quoted {
                if self.escaped {
                    self.escaped = false;
                } else if *byte == b'\\' {
                    self.escaped = true;
                } else if *byte == b'"' {
                    self.quoted = false;
                }
                continue;
            }
            match byte {
                b'"' => self.quoted = true,
                b'{' | b'[' => {
                    self.depth += 1;
                    if self.depth > 256 {
                        return Err(invalid());
                    }
                    self.nodes = self.nodes.checked_add(1).ok_or_else(invalid)?;
                }
                b'}' | b']' => self.depth -= 1,
                b':' | b',' => self.nodes = self.nodes.checked_add(1).ok_or_else(invalid)?,
                _ => {}
            }
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
/// Conservative closed-DTO admission; strings are counted without building a second DTO.
pub(crate) fn owned_bytes<T: Serialize + ?Sized>(value: &T) -> Result<u64> {
    let mut size = LogicalSize::default();
    serde_json::to_writer(&mut size, value)
        .map_err(|_| Error::Invalid("record ownership cannot be bounded".into()))?;
    size.bytes
        .checked_mul(2)
        .and_then(|bytes| {
            size.nodes
                .checked_mul(Snapshot::LOGICAL_SCHEMA_SLOT_BYTES)
                .and_then(|slots| bytes.checked_add(slots))
        })
        .and_then(|bytes| bytes.checked_add(std::mem::size_of_val(value) as u64))
        .ok_or_else(|| Error::Invalid("record ownership overflows".into()))
}

pub(in crate::core) fn owned_effect(effect: &EffectState) -> Result<u64> {
    owned_sum([
        effect.provider.capacity() as u64,
        effect.effect_kind.capacity() as u64,
        effect.request.logical_owned_bytes()?,
        effect.result_schema.logical_owned_bytes()?,
        owned_bytes(&effect.status)?,
        owned_buffer::<EffectAttemptId>(effect.attempts.capacity())?,
    ])
}
// Match ConversationState::copy's existing message claims.
pub(in crate::core) fn owned_message(message: &ConversationMessage) -> Result<u64> {
    let attachments = match &message.attachments {
        crate::conversation::ReferencedAttachments::Inline { items } => {
            owned_vector(items, |item| {
                owned_sum([
                    item.file.logical_owned_bytes()?,
                    item.label.as_ref().map_or(0, |text| text.capacity() as u64),
                ])
            })?
        }
        crate::conversation::ReferencedAttachments::Manifest { manifest, .. } => {
            manifest.logical_owned_bytes()?
        }
    };
    let mut bytes = owned_sum([
        message.content.logical_owned_bytes()?,
        attachments,
        message
            .tool_call_id
            .as_ref()
            .map_or(0, |text| text.capacity() as u64),
    ])?;
    for (key, file) in &message.extensions {
        bytes = owned_sum([
            bytes,
            std::mem::size_of::<(String, FileRef)>() as u64,
            key.capacity() as u64,
            file.logical_owned_bytes()?,
        ])?;
    }
    Ok(bytes)
}
/// Retained scope, causal parent and payload storage.
pub(crate) fn event_owned_bytes(event: &Event) -> Result<u64> {
    owned_sum([
        event.scope.logical_owned_bytes()?,
        event
            .causal_parent
            .as_ref()
            .map_or(0, |reference| reference.authority.id.capacity() as u64),
        owned_bytes(&event.payload)?,
    ])
}

impl<B> snapshot::Projection<ConversationState, B> {
    /// Logical bytes of the projection and event cache, excluding conversation messages.
    pub(in crate::core) fn owned_bytes<'a>(
        &self,
        authority: &Authority,
        events: impl Iterator<Item = &'a Event>,
        event_capacity: usize,
    ) -> Result<u64> {
        let mut bytes = owned_sum([
            authority.id.capacity() as u64,
            owned_buffer::<Event>(event_capacity)?,
            owned_buffer::<EffectId>(self.indexed_terminal_effects.capacity())?,
            owned_vector(&self.active_extensions, |extension| {
                Ok(extension.name.capacity() as u64)
            })?,
            owned_vector(&self.active_configurations, owned_bytes)?,
        ])?;
        for event in events {
            bytes = owned_sum([bytes, event_owned_bytes(event)?])?;
        }
        for effect in self.effects.values() {
            bytes = owned_sum([
                bytes,
                std::mem::size_of::<(EffectId, EffectState)>() as u64,
                owned_effect(effect)?,
            ])?;
        }
        for (key, (reference, record)) in &self.extension_states {
            bytes = owned_sum([
                bytes,
                std::mem::size_of::<(String, (EventReference, ExtensionRecord))>() as u64,
                key.capacity() as u64,
                reference.authority.id.capacity() as u64,
                owned_bytes(record)?,
            ])?;
        }
        for (key, record) in &self.configured_extensions {
            bytes = owned_sum([
                bytes,
                std::mem::size_of::<(ExtensionDependency, ExtensionConfiguration)>() as u64,
                key.name.capacity() as u64,
                owned_bytes(record)?,
            ])?;
        }
        for (key, seed) in &self.forks {
            bytes = owned_sum([
                bytes,
                std::mem::size_of::<(Authority, ForkSeed)>() as u64,
                key.id.capacity() as u64,
                owned_bytes(seed)?,
            ])?;
        }
        for (name, operation) in &self.published_merges {
            bytes = owned_sum([
                bytes,
                std::mem::size_of::<(String, Vec<u8>)>() as u64,
                name.capacity() as u64,
                operation.capacity() as u64,
            ])?;
        }
        if let Some(checkpoint) = &self.latest_context_checkpoint {
            bytes = owned_sum([bytes, checkpoint.logical_owned_bytes()?])?;
        }
        for (ticket, resolution) in self.interactions.values() {
            bytes = owned_sum([
                bytes,
                std::mem::size_of::<(
                    uuid::Uuid,
                    (InteractionTicket, Option<InteractionResolution>),
                )>() as u64,
                owned_bytes(&ticket.request)?,
                resolution.as_ref().map_or(Ok(0), owned_bytes)?,
            ])?;
        }
        Ok(bytes)
    }
}

pub(crate) fn inspect_metadata<T: Serialize + ?Sized>(
    source: &T,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    if work.is_none() {
        return Ok(());
    }
    crate::contract::json_byte_length(source, u64::MAX, work).map(|_| ())
}

#[cfg(feature = "filesystem")]
pub(crate) fn resource_creation_clone_reservation(
    request: &crate::resource_preparation::ResourcePreparationRequest,
    proof: &crate::resource_preparation::ResourceCreationProofRef,
    origin: &crate::resource_preparation::ResourceCreationOrigin,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    inspect_metadata(&(request, proof, origin), work)?;
    let bytes = owned_sum([
        request.logical_owned_bytes()?,
        proof.provider.logical_owned_bytes()?,
        proof.resource.logical_owned_bytes()?,
        proof.evidence.logical_owned_bytes()?,
        origin.owner.id.capacity() as u64,
        origin.source_grants.logical_owned_bytes()?,
    ])?;
    if let Some(io) = work.as_deref_mut() {
        io.retain_owned(bytes)?;
    }
    Ok(bytes)
}

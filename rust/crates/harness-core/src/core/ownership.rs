//! Logical ownership of the main reducer's records.
use super::*;

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

/// Conservative closed-DTO admission without building a second DTO.
pub(crate) fn owned_bytes<T: Serialize + ?Sized>(value: &T) -> Result<u64> {
    use crate::executor::json::{JsonBounds, JsonCodecCharge, count_json_bytes_trusted};
    let mut nodes = 0_u64;
    let bytes = count_json_bytes_trusted(
        value,
        JsonBounds {
            maximum_bytes: u64::MAX,
            maximum_items: u64::MAX,
            maximum_depth: 256,
        },
        None,
        &mut |charge| {
            if let JsonCodecCharge::Work { items, .. } = charge {
                nodes = owned_sum([nodes, items])?;
            }
            Ok(())
        },
    )?;
    bytes
        .checked_mul(2)
        .and_then(|bytes| {
            nodes
                .checked_mul(Snapshot::LOGICAL_SCHEMA_SLOT_BYTES)
                .and_then(|slots| bytes.checked_add(slots))
        })
        .and_then(|bytes| bytes.checked_add(std::mem::size_of_val(value) as u64))
        .ok_or_else(|| Error::Invalid("record ownership overflows".into()))
}

pub(crate) fn owned_effect(effect: &EffectState) -> Result<u64> {
    owned_sum([
        effect.provider.capacity() as u64,
        effect.effect_kind.capacity() as u64,
        std::mem::size_of::<FileRef>() as u64,
        effect.request.logical_owned_bytes()?,
        effect.planning.as_ref().map_or(Ok(0), |event| {
            owned_sum([
                std::mem::size_of::<Event>() as u64,
                event_owned_bytes(event)?,
            ])
        })?,
        effect.result_schema.logical_owned_bytes()?,
        owned_bytes(&effect.status)?,
        owned_buffer::<EffectAttemptId>(effect.attempts.capacity())?,
    ])
}
/// Retained scope, causal parent and payload storage.
pub(crate) fn event_owned_bytes(event: &Event) -> Result<u64> {
    owned_sum([
        event.scope.logical_owned_bytes()?,
        event.attestation.capacity() as u64,
        event
            .causal_parent
            .as_ref()
            .map_or(0, |reference| reference.authority.id.capacity() as u64),
        owned_bytes(&event.payload)?,
    ])
}

fn owned_records<T: Serialize>(values: impl IntoIterator<Item = T>) -> Result<u64> {
    values
        .into_iter()
        .try_fold(0, |sum, value| owned_sum([sum, owned_bytes(&value)?]))
}

impl<B> snapshot::Projection<ConversationState, B> {
    /// Admission for cloned projection records; conversation owns its separate quota.
    pub(in crate::core) fn owned_bytes<'a>(
        &self,
        authority: &Authority,
        events: impl Iterator<Item = &'a Event>,
    ) -> Result<u64> {
        [
            owned_bytes(authority),
            owned_bytes(&self.active_extensions),
            owned_bytes(&self.active_configurations),
            owned_bytes(&self.indexed_terminal_effects),
            owned_records(self.effects.iter()),
            owned_records(self.extension_states.iter()),
            owned_records(self.configured_extensions.iter()),
            owned_records(self.forks.iter()),
            owned_records(self.published_merges.iter()),
            owned_bytes(&self.latest_context_checkpoint),
            owned_records(self.interactions.iter()),
            owned_records(events),
        ]
        .into_iter()
        .try_fold(0, |sum, part| owned_sum([sum, part?]))
    }
}

pub(crate) fn inspect_metadata<T: Serialize + ?Sized>(
    source: &T,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
        let io = crate::executor::PreparationIo::from_native_work(&native.recheck_admitted())?;
        return crate::executor::with_history_io(Some(&io), |work| {
            crate::executor::json::json_byte_length(source, u64::MAX, work).map(|_| ())
        });
    }
    if work.is_none() {
        return Ok(());
    }
    crate::executor::json::json_byte_length(source, u64::MAX, work).map(|_| ())
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

//! Logical ownership of the main reducer's records.
use super::*;
mod walk;
pub(crate) use walk::{OwnedBytes, owned_enum, owned_struct, owned_tuple};
#[cfg(test)]
mod tests;

pub(in crate::core) fn admit_tree_insertion<T>(
    len: usize,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    if let Some(io) = work.as_deref_mut().filter(|io| !copying::is_native(&**io)) {
        let bytes = walk::tree_insertion_bytes::<T>(len)?;
        io.admit_owned(bytes)?;
        io.retain_owned(bytes)?;
    }
    Ok(())
}

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

pub(crate) fn owned_effect(effect: &EffectState) -> Result<u64> {
    effect.owned_heap_bytes()
}
/// Retained scope, causal parent and payload storage.
pub(crate) fn event_owned_bytes(event: &Event) -> Result<u64> {
    event.owned_heap_bytes()
}

impl<B> snapshot::Projection<ConversationState, B> {
    /// Admission for cloned projection records; conversation owns its separate quota.
    pub(in crate::core) fn owned_bytes<'a>(
        &self,
        authority: &Authority,
        mut events: impl ExactSizeIterator<Item = &'a Event>,
    ) -> Result<u64> {
        let Self {
            lifecycle: _,
            active_extensions,
            active_extension_revision: _,
            extension_states,
            configured_extensions,
            active_configurations,
            effects,
            indexed_terminal_effects,
            effects_archived: _,
            forks,
            published_merges,
            conversation: _,
            latest_context_checkpoint,
            interactions,
            bindings: _,
        } = self;
        let event_slots = owned_buffer::<Event>(events.len())?;
        let event_bytes = events.try_fold(event_slots, |sum, event| {
            owned_sum([sum, event.owned_heap_bytes()?])
        })?;
        [
            authority.owned_heap_bytes(),
            active_extensions.owned_heap_bytes(),
            active_configurations.owned_heap_bytes(),
            indexed_terminal_effects.owned_heap_bytes(),
            effects.owned_heap_bytes(),
            extension_states.owned_heap_bytes(),
            configured_extensions.owned_heap_bytes(),
            forks.owned_heap_bytes(),
            published_merges.owned_heap_bytes(),
            latest_context_checkpoint.owned_heap_bytes(),
            interactions.owned_heap_bytes(),
            Ok(event_bytes),
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

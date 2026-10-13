use super::*;
/// Immutable provider selection and ref-only task request.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskEffectPlan {
    /// Registered provider identity.
    pub provider: String,
    /// Pinned delivery guarantee.
    pub guarantee: crate::core::EffectGuarantee,
    /// Provider operation kind.
    pub effect_kind: String,
    /// Immutable request.
    pub request: crate::conversation::FileRef,
    /// Immutable result schema.
    pub result_schema: crate::conversation::FileRef,
}
#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for TaskEffectPlan {}
/// Returns the canonical identity of one task command's effect.
#[cfg(feature = "filesystem")]
pub fn task_effect_id(
    task: crate::TaskId,
    command: OperationId,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<EffectId> {
    identity(&("harness/v2/task-effect", task, command), work).map(EffectId::from_bytes)
}
pub(super) fn identity<T: serde::Serialize>(
    value: &T,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<[u8; 16]> {
    let digest = crate::executor::json::canonical_json_digest(
        value,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        work,
    )?;
    if let Some(io) = work.as_deref_mut() {
        io.admit_copy(16)?;
    }
    let mut id = [0; 16];
    id.copy_from_slice(&digest[..16]);
    if let Some(io) = work.as_deref_mut() {
        io.copy_bytes(16)?;
    }
    Ok(id)
}

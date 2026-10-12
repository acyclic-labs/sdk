use super::*;

/// Immutable provider selection and ref-only request for one task command.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskEffectPlan {
    /// Exact registered provider identity.
    pub provider: String,
    /// Pinned delivery strength supported by that provider.
    pub guarantee: crate::core::EffectGuarantee,
    /// Versioned provider operation kind.
    pub effect_kind: String,
    /// Immutable request, readable under the retained task grants.
    pub request: crate::conversation::FileRef,
    /// Immutable result schema, readable under the same grants.
    pub result_schema: crate::conversation::FileRef,
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for TaskEffectPlan {
    fn maximum_slot_bytes() -> u64 {
        crate::filesystem::preparation_slot_bytes::<Self>()
    }
}

/// Resolves the canonical effect identity for one original task command.
#[cfg(feature = "filesystem")]
pub fn task_effect_id(
    task: crate::TaskId,
    command: OperationId,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<EffectId> {
    let digest = crate::executor::json::canonical_json_digest(
        &("harness/v2/task-effect", task, command),
        acyclic_stream::MAX_RECORD_BYTES as u64,
        work,
    )?;
    if let Some(io) = work.as_deref_mut() {
        io.admit_copy(16)?;
    }
    let mut identity = [0; 16];
    identity.copy_from_slice(&digest[..16]);
    if let Some(io) = work.as_deref_mut() {
        io.copy_bytes(16)?;
    }
    Ok(EffectId::from_bytes(identity))
}

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
impl TaskEffectPlan {
    fn retained_bytes(&self) -> Result<u64> {
        crate::core::owned_sum([
            std::mem::size_of::<Self>() as u64,
            self.provider.capacity() as u64,
            self.effect_kind.capacity() as u64,
            self.request.logical_owned_bytes()?,
            self.result_schema.logical_owned_bytes()?,
        ])
    }
}
#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for TaskEffectPlan {
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        self.retained_bytes().map(Some)
    }
}
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

#[cfg(all(test, feature = "filesystem"))]
mod tests {
    use super::*;
    use crate::conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef};
    use crate::filesystem::PreparationRecord;
    use crate::resources::ProviderRef;

    #[test]
    fn decode_hook_retains_header_and_owned_fields() -> Result<()> {
        let file = FileRef::new(
            VolumeRef::new(
                ProviderRef::new("test".into(), "filesystem".into(), "1".into(), &mut None)?,
                "volume".into(),
                VolumeClass::Project,
                VolumeOwner::Project("project".into()),
                &mut None,
            )?,
            "request.json".into(),
            "revision".into(),
            FileDescriptor::from_bytes(b"{}", "application/json", &mut None)?,
            "request.json".into(),
            &mut None,
        )?;
        let mut plan = TaskEffectPlan {
            provider: "provider".into(),
            guarantee: crate::core::EffectGuarantee::AtMostOnce,
            effect_kind: "operation".into(),
            request: file.clone(),
            result_schema: file,
        };
        plan.provider.reserve(64);
        plan.effect_kind.reserve(96);
        let heap_bytes = plan.provider.capacity() as u64
            + plan.effect_kind.capacity() as u64
            + plan.request.logical_owned_bytes()?
            + plan.result_schema.logical_owned_bytes()?;
        assert_eq!(
            plan.decoded_owned_bytes()?,
            Some(std::mem::size_of::<TaskEffectPlan>() as u64 + heap_bytes)
        );
        Ok(())
    }
}

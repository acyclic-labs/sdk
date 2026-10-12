use super::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LaunchReceipt<'a> {
    pub(super) dispatch: Cow<'a, EffectDispatch>,
    pub(super) plan: Cow<'a, TaskEffectPlan>,
    pub(super) conversation: Cow<'a, crate::core::Authority>,
    pub(super) task: crate::TaskId,
    pub(super) command: OperationId,
    pub(super) fence: Cow<'a, crate::scheduler::LeaseFence>,
    pub(super) approval: InteractionId,
    pub(super) approval_digest: [u8; 32],
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Receipt<L = LaunchReceipt<'static>, O = NativeObservation> { Launch(L), Observed(O) }
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeObservation {
    pub(super) effect: EffectObservation,
    pub(super) diagnostic: Option<crate::conversation::FileRef>,
    pub(super) publication_failure: Option<NativePublicationFailure>,
    pub(super) invocation_failure: Option<String>,
    #[serde(default)]
    pub(super) published: Vec<NativeProcessPublishedVolume>,
    pub(super) work: WorkCounters,
}

/// Final SDK snapshot for one approved mapping.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeProcessPublishedVolume {
    /// Index in the immutable view manifest.
    pub index: usize,
    /// Qualified snapshot and retained data operation.
    pub snapshot: acyclic_fs::NativePublicationSnapshot,
}

/// Authenticated original process observation.
#[derive(Debug, Serialize)]
pub struct NativeProcessReceipt {
    /// Original task.
    pub task: crate::TaskId,
    /// Admitted command.
    pub command: OperationId,
    /// Original dispatch fence.
    pub original_fence: crate::scheduler::LeaseFence,
    /// Retained effect status.
    pub effect: EffectObservation,
    /// Whether the observation committed.
    pub observed: bool,
    /// Retained bounded process output.
    pub diagnostic: Option<crate::conversation::FileRef>,
    /// Destination failure and completed publications.
    pub publication_failure: Option<NativePublicationFailure>,
    /// Retained invocation failure.
    pub invocation_failure: Option<String>,
    /// Final SDK snapshots.
    pub published: Vec<NativeProcessPublishedVolume>,
    /// Original capture and publication work.
    pub work: WorkCounters,
    #[serde(skip)]
    pub(super) _custody: NativeAllocation,
}
impl NativeProcessReceipt {
    /// Returns the effect with its retained allocation.
    pub fn into_effect(self) -> Result<(EffectObservation, NativeAllocation)> {
        let Self { original_fence, effect, diagnostic, publication_failure,
            invocation_failure, published, _custody: mut custody, .. } = self;
        drop((original_fence, diagnostic, publication_failure, invocation_failure, published));
        custody.release(custody.bytes().checked_sub(receipt_observation::effect_bytes(&effect)?).ok_or(acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow))?)?;
        Ok((effect, custody))
    }
}

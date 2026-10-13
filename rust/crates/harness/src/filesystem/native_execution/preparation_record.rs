use super::super::PreparationRecord;
use super::*;
use acyclic_fs::{MaterializationJournal, NativeAllocation};

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    pub(super) task: crate::TaskId,
    pub(super) original_fence: crate::scheduler::LeaseFence,
    pub(super) operation_directory: PathBuf,
    pub(super) manifest: NativeViewManifest,
}
impl PreparationRecord for Intent {}
impl PreparationRecord for MaterializationJournal {}

/// Original protected preparation, without opening its host paths.
pub struct NativeRecordedPreparation {
    /// Original task.
    pub task: crate::TaskId,
    /// Creation fence.
    pub original_fence: crate::scheduler::LeaseFence,
    /// Original selection.
    pub manifest: NativeViewManifest,
    /// Original staging directory.
    pub operation_directory: PathBuf,
    /// SDK journal.
    pub journal: MaterializationJournal,
    pub(super) custody: NativeAllocation,
}

pub(super) fn invalid() -> Error {
    Error::NativeBoundary {
        code: crate::wire::ErrorCode::Storage,
        reason: "invalid protected native preparation",
    }
}

pub(super) fn intent(journal: &MaterializationJournal, io: &PreparationIo) -> Result<Intent> {
    let acyclic_fs::MaterializationIntent::NativePreparation { original, .. } =
        &journal.plan.intent
    else {
        return Err(invalid());
    };
    let decoded = io.decode(
        std::marker::PhantomData::<Intent>,
        original,
        crate::wire::ErrorCode::Storage,
    )?;
    let intent = super::super::retain_decoded(decoded);
    intent.manifest.validate(Some(io))?;
    Ok(intent)
}

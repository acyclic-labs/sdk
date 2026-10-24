//! Selected control records and bounded response shapes.
use super::*;

// Audited serde labels for selected commands, cursors and imports.
pub(crate) const SELECTED_DIAGNOSTIC_LABEL_BYTES: u64 =
    crate::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES + 855;
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    bound(deserialize = "S: Deserialize<'de> + Default, P: Deserialize<'de> + Default")
)]
pub(crate) struct PiSelectedCommand<S = String, P = Vec<String>> {
    pub(crate) caller: TaskId,
    pub(crate) sdk_operation: S,
    pub(crate) source: TaskId,
    pub(crate) root: Option<S>,
    #[serde(default)]
    pub(crate) directory: S,
    pub(crate) cursor: Option<S>,
    pub(crate) maximum_entries: Option<u32>,
    pub(crate) destination: Option<TaskId>,
    #[serde(default)]
    pub(crate) paths: P,
    #[serde(default)]
    pub(crate) confirm: bool,
    pub(crate) approval_digest: Option<S>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SelectedImportIntent<S = String, V = VolumeRef, P = Vec<String>> {
    pub(super) caller: TaskId,
    pub(super) source: TaskId,
    pub(super) root: S,
    pub(super) destination: V,
    pub(super) source_snapshot: LazySnapshotRef,
    pub(super) destination_snapshot: LazySnapshotRef,
    pub(super) paths: P,
    pub(super) operation: OperationId,
}

impl<'a> From<&'a SelectedImportIntent>
    for SelectedImportIntent<&'a str, &'a VolumeRef, &'a [String]>
{
    fn from(value: &'a SelectedImportIntent) -> Self {
        Self {
            caller: value.caller,
            source: value.source,
            root: &value.root,
            destination: &value.destination,
            source_snapshot: value.source_snapshot,
            destination_snapshot: value.destination_snapshot,
            paths: &value.paths,
            operation: value.operation,
        }
    }
}

#[derive(Serialize)]
pub(super) struct PiPageResponse<'a, E: Serialize> {
    pub(super) source: TaskId,
    pub(super) root: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) volume: Option<&'a VolumeRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) before_root: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) before_snapshot: Option<LazySnapshotRef>,
    pub(super) snapshot: LazySnapshotRef,
    pub(super) directory: &'a str,
    pub(super) entries: &'a [E],
    pub(super) cursor: Option<&'a str>,
}
#[derive(Serialize)]
pub(super) struct PiImportResponse<'a> {
    pub(super) status: &'static str,
    pub(super) source: TaskId,
    pub(super) destination: TaskId,
    pub(super) root: &'a str,
    pub(super) operation: &'a str,
    pub(super) original_operation: OperationId,
    pub(super) snapshot: LazySnapshotRef,
}
#[derive(Serialize)]
pub(super) struct PiWritebackResponse<'a> {
    pub(super) status: &'static str,
    pub(super) source: TaskId,
    pub(super) root: &'a str,
    pub(super) paths: &'a [String],
    pub(super) approval_digest: &'a str,
    pub(super) operation: &'a str,
    pub(super) original_operation: OperationId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) source_snapshot: Option<LazySnapshotRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) selected_snapshot: Option<LazySnapshotRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) imported: Option<acyclic_fs::GenerationId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) receipt: Option<&'a acyclic_fs::MaterializationJournal>,
}

use super::*;

/// Exact bounded native directory source configuration.
#[napi(object)]
pub struct NativeSourceOptions {
    /// `pinned` or `tracking`.
    pub mode: String,
    /// Maximum paths admitted by one baseline or reconciliation.
    pub maximum_paths: NapiU32,
    /// Maximum sparse spans admitted per regular file.
    pub maximum_extent_spans: NapiU32,
    /// Maximum pending native changes before fail-closed rescan.
    pub maximum_queued_changes: NapiU32,
    /// Canonical portable prefixes omitted from capture and deletion inference.
    pub excluded_paths: Option<Vec<String>>,
}

/// Current or terminal source reconciliation state.
#[napi(object)]
pub struct NativeSourceResult {
    /// `none`, `clean`, `pending-capture`, `needs-rescan`, `conflict`, or `sealed`.
    pub status: String,
    /// Exact invalidation reason for `needs-rescan`.
    pub reason: Option<String>,
    /// Exact immutable generation selected by a clean terminal operation.
    pub generation_id: Option<Buffer>,
}

pub(in crate::bindings) fn native_source_state(state: SourceState) -> NativeSourceResult {
    let (status, reason) = match state {
        SourceState::Clean => ("clean", None),
        SourceState::PendingCapture => ("pending-capture", None),
        SourceState::NeedsRescan(reason) => ("needs-rescan", Some(watch_reason(reason).to_owned())),
        SourceState::Conflict => ("conflict", None),
        SourceState::Sealed => ("sealed", None),
    };
    NativeSourceResult {
        status: status.to_owned(),
        reason,
        generation_id: None,
    }
}

use super::*;

/// Exact bounded native directory source configuration.
#[napi(object)]
pub struct NativeSourceOptions {
    pub mode: String,
    pub maximum_paths: NapiU32,
    pub maximum_extent_spans: NapiU32,
    pub maximum_queued_changes: NapiU32,
    pub excluded_paths: Option<Vec<String>>,
}

/// Current or terminal source reconciliation state.
#[napi(object)]
pub struct NativeSourceResult {
    pub status: String,
    pub reason: Option<String>,
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

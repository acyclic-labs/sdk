//! Model identity is a disposable fold over each immutable turn request file.
use super::{ClientViews, FileQueryView};
use crate::task_volume::{Component, Cursor, FileKey, FileValue, TaskFile};
use serde::Serialize;

/// Selected identity from a request; model options and prompt bytes stay in the file.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ModelFileView {
    /// The file has no valid model identity; never retain a prior selection.
    Invalid,
    /// Identity declared in the canonical provider-neutral request.
    Selected {
        /// Provider catalog entry.
        provider: String,
        /// Provider-owned model name.
        name: String,
        /// Immutable model revision.
        revision: String,
    },
}

impl ModelFileView {
    pub(in crate::client) fn from_file(file: &FileValue) -> Self {
        #[cfg(feature = "test-support")]
        crate::test_support::model_view_work::parsed(file.bytes.len());
        match super::model_identity::read(&file.bytes) {
            Some(model) => Self::Selected {
                provider: model.provider,
                name: model.name,
                revision: model.revision,
            },
            None => Self::Invalid,
        }
    }
}

pub(in crate::client) fn turn_request(key: &FileKey) -> Option<(&Component, u64)> {
    match key {
        FileKey::Task {
            task,
            file: TaskFile::TurnRequest(turn),
        } => Some((task, *turn)),
        _ => None,
    }
}

/// Model identity at one accepted query cut, scoped to its task and turn.
#[derive(Debug, Serialize)]
pub struct ModelQueryView<'a> {
    /// Exact volume, query and history position supplying this identity.
    pub cursor: &'a Cursor,
    /// Task owning the request.
    pub task: &'a Component,
    /// Immutable turn/attempt number.
    pub turn: u64,
    /// Derived identity; the authoritative request bytes remain queryable.
    pub model: &'a ModelFileView,
}

impl ClientViews {
    /// Borrows model choices from already observed turn-request files.
    pub fn models(&self) -> impl Iterator<Item = ModelQueryView<'_>> {
        self.queries.values().flat_map(FileQueryView::models)
    }
}

pub(in crate::client) struct Models<'a>(pub &'a ClientViews);
impl Serialize for Models<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.models())
    }
}

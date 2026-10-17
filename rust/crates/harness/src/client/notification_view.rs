//! Notification rows are derived once from the sole followed file index.
use super::{AttentionNotification, ClientViews, FileQueryView};
use crate::task_volume::{Component, Cursor, FileKey, FileValue, TaskFile};
use serde::Serialize;

/// Interpretation of an authoritative notification file at a query cut.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub enum NotificationFileView {
    /// Malformed or cross-task content remains readable as its original file.
    Invalid,
    /// The canonical attention record; no lifecycle or diagnostic-string inference.
    Present {
        /// Immutable notification identity and event supplied by the writer.
        notification: AttentionNotification,
    },
}

pub(super) fn identity(key: &FileKey) -> Option<(&Component, &Component)> {
    match key {
        FileKey::Task {
            task,
            file: TaskFile::Notification(id),
        } => Some((task, id)),
        _ => None,
    }
}
impl NotificationFileView {
    /// Parses only the existing attention schema and binds it to its task folder.
    pub fn from_file(file: &FileValue) -> Self {
        let Some((task, _)) = identity(&file.key) else {
            return Self::Invalid;
        };
        match serde_json::from_slice::<AttentionNotification>(&file.bytes) {
            Ok(notification)
                if notification.task == task.as_str() && notification.sequence != 0 =>
            {
                Self::Present { notification }
            }
            _ => Self::Invalid,
        }
    }
}

/// One notification from an already followed task folder.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub struct NotificationQueryView<'a> {
    /// Exact source volume and snapshot/follow cut.
    pub cursor: &'a Cursor,
    /// Owner from the canonical file key.
    pub task: &'a Component,
    /// Immutable file identity; reconnect never creates another queue entry.
    pub id: &'a Component,
    /// Parsed row, updated only when its authoritative file changes.
    pub notification: &'a NotificationFileView,
}
impl FileQueryView {
    /// Iterates cached rows without reading, parsing or joining file payloads.
    pub fn notifications(&self) -> impl Iterator<Item = NotificationQueryView<'_>> {
        self.notifications.iter().filter_map(|(key, notification)| {
            let (task, id) = identity(key)?;
            Some(NotificationQueryView {
                cursor: self.cursor(),
                task,
                id,
                notification,
            })
        })
    }
}
impl ClientViews {
    /// All notification files in the current followed selections, including invalid rows.
    pub fn notifications(&self) -> impl Iterator<Item = NotificationQueryView<'_>> {
        self.selections()
            .flat_map(|(_, files)| files.notifications())
    }
}

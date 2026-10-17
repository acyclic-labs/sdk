//! Borrowed task projections over the one selected file index.
use super::{ClientViews, FileQueryView};
use crate::task_volume::{
    Channel, Component, Cursor, FileKey, HeadFile, Query, TaskFile, ToolFile,
};
use std::collections::BTreeMap;
#[cfg(feature = "tui")]
use std::sync::Arc;

/// One task at an accepted query cut; payloads remain shared file segments.
#[derive(Clone)]
pub struct TaskQueryView<'a> {
    /// Complete history and query identity supplying these files.
    pub cursor: &'a Cursor,
    /// Canonical task identity.
    pub task: &'a Component,
    #[cfg(feature = "tui")]
    pub(crate) source: &'a Arc<Query>,
    #[cfg(feature = "tui")]
    selection: &'a FileQueryView,
    files: &'a BTreeMap<FileKey, HeadFile>,
    selected: std::collections::btree_map::Range<'a, FileKey, HeadFile>,
}

/// One model stream, including its exact chunk watermark.
#[derive(Clone, Copy)]
pub struct TaskTextView<'a> {
    /// Model turn/attempt identity.
    pub turn: u64,
    /// Text or reasoning; never mix their independent UTF-8 boundaries.
    pub channel: Channel,
    /// Ordered immutable segments, without joining accumulated output.
    pub file: &'a HeadFile,
}

/// One tool folder. Requests, results and output share the selected file index.
#[derive(Clone)]
pub struct ToolQueryView<'a> {
    /// Model turn/attempt identity.
    pub turn: u64,
    /// Runtime operation identity; the request retains the provider's call ID.
    pub call: &'a Component,
    task: TaskQueryView<'a>,
}

impl FileQueryView {
    /// Borrows one task, independently of how many other tasks are selected.
    pub fn task<'a>(&'a self, task: &'a Component) -> TaskQueryView<'a> {
        let range = Query::Task(task.clone())
            .ranges()
            .pop()
            .expect("one task range");
        TaskQueryView {
            cursor: self.cursor(),
            task,
            #[cfg(feature = "tui")]
            source: self.source(),
            #[cfg(feature = "tui")]
            selection: self,
            files: self.files(),
            selected: self.files().range(range),
        }
    }
}
impl ClientViews {
    /// Projects a task from the exact selected volume/query cut.
    pub fn task<'a>(
        &'a self,
        selection: &([u8; 32], [u8; 32]),
        task: &'a Component,
    ) -> Option<TaskQueryView<'a>> {
        Some(self.selection(selection)?.task(task))
    }
}
impl<'a> TaskQueryView<'a> {
    #[cfg(feature = "tui")]
    pub(crate) fn selection(&self) -> &'a FileQueryView {
        self.selection
    }
    #[cfg(feature = "tui")]
    pub(crate) fn file(&self, key: &FileKey) -> Option<&'a HeadFile> {
        match key {
            FileKey::Task { task, .. } if task == self.task => self.files.get(key),
            _ => None,
        }
    }
    #[cfg(feature = "tui")]
    pub(crate) fn text_from(
        &self,
        from: Option<&FileKey>,
    ) -> impl Iterator<Item = TaskTextView<'a>> + use<'a> {
        let (mut lower, upper) = Query::Task(self.task.clone())
            .ranges()
            .pop()
            .expect("task range");
        if let Some(from) = from {
            lower = std::ops::Bound::Included(from.clone());
        }
        self.files.range((lower, upper)).filter_map(|(_, file)| {
            let FileKey::Task { file: kind, .. } = &file.key else {
                return None;
            };
            let (turn, channel) = match kind {
                TaskFile::TextHead { turn, channel } => (*turn, *channel),
                TaskFile::Chunk { turn, .. } => (*turn, file.channel?),
                _ => return None,
            };
            Some(TaskTextView {
                turn,
                channel,
                file,
            })
        })
    }
    /// Exact task files in numeric identity order, retaining canonical metadata.
    pub fn files(&self) -> impl Iterator<Item = &'a HeadFile> + use<'a> {
        self.selected.clone().map(|(_, file)| file)
    }
    /// Selected text/reasoning heads, or raw chunks from an ordinary query.
    /// A raw chunk's identity remains in `file.key`; heads include through_sequence.
    pub fn text(&self) -> impl Iterator<Item = TaskTextView<'a>> + use<'a> {
        self.files().filter_map(|file| {
            let FileKey::Task { file: kind, .. } = &file.key else {
                unreachable!()
            };
            let (turn, channel) = match kind {
                TaskFile::TextHead { turn, channel } => (*turn, *channel),
                TaskFile::Chunk { turn, .. } => (*turn, file.channel?),
                _ => return None,
            };
            Some(TaskTextView {
                turn,
                channel,
                file,
            })
        })
    }
    /// Unique tool folders, including a retained output tail without its request.
    pub fn tools(&self) -> impl Iterator<Item = ToolQueryView<'a>> + use<'a> {
        let task = self.clone();
        let mut previous = None;
        self.files().filter_map(move |head| {
            let FileKey::Task {
                file: TaskFile::Tool { turn, call, .. },
                ..
            } = &head.key
            else {
                return None;
            };
            let identity = (*turn, call);
            if previous == Some(identity) {
                return None;
            }
            previous = Some(identity);
            Some(ToolQueryView {
                turn: *turn,
                call,
                task: task.clone(),
            })
        })
    }
    /// Per-turn completion files retain the producer's canonical metadata schema.
    /// No last-step replacement or renderer-specific usage schema is introduced.
    pub fn completions(&self) -> impl Iterator<Item = (u64, &'a HeadFile)> + use<'a> {
        self.files().filter_map(|file| match &file.key {
            FileKey::Task {
                file: TaskFile::Completion(turn),
                ..
            } => Some((*turn, file)),
            _ => None,
        })
    }
}
impl<'a> ToolQueryView<'a> {
    #[cfg(feature = "tui")]
    pub(crate) fn source(&self) -> &'a Arc<Query> {
        self.task.source
    }
    #[cfg(feature = "tui")]
    pub(crate) fn file(&self, key: &FileKey) -> Option<&'a HeadFile> {
        self.task.file(key).filter(|file| matches!(&file.key,
            FileKey::Task { file: TaskFile::Tool { turn, call, .. }, .. } if *turn == self.turn && call == self.call))
    }
    #[cfg(feature = "tui")]
    pub(crate) fn output_from(
        &self,
        stderr: bool,
        from: Option<&FileKey>,
    ) -> impl Iterator<Item = &'a HeadFile> + use<'a> {
        let leaf = |sequence| {
            if stderr {
                ToolFile::Stderr(sequence)
            } else {
                ToolFile::Stdout(sequence)
            }
        };
        self.task
            .files
            .range(from.cloned().unwrap_or_else(|| self.key(leaf(0)))..=self.key(leaf(u64::MAX)))
            .map(|(_, file)| file)
    }
    fn key(&self, file: ToolFile) -> FileKey {
        FileKey::Task {
            task: self.task.task.clone(),
            file: TaskFile::Tool {
                turn: self.turn,
                call: self.call.clone(),
                file,
            },
        }
    }
    /// The exact canonical ToolInvocation file, including name and arguments.
    pub fn request(&self) -> Option<&'a HeadFile> {
        self.task.files.get(&self.key(ToolFile::Request))
    }
    /// The exact canonical tool result; absence means it has not been observed.
    pub fn result(&self) -> Option<&'a HeadFile> {
        self.task.files.get(&self.key(ToolFile::Result))
    }
    fn range(
        &self,
        first: ToolFile,
        last: ToolFile,
    ) -> impl Iterator<Item = &'a HeadFile> + use<'a> {
        self.task
            .files
            .range(self.key(first)..=self.key(last))
            .map(|(_, file)| file)
    }
    /// Provider argument deltas, in numeric sequence order.
    pub fn arguments(&self) -> impl Iterator<Item = &'a HeadFile> + use<'a> {
        self.range(ToolFile::Arguments(0), ToolFile::Arguments(u64::MAX))
    }
    /// Standard output deltas, in numeric sequence order.
    pub fn stdout(&self) -> impl Iterator<Item = &'a HeadFile> + use<'a> {
        self.range(ToolFile::Stdout(0), ToolFile::Stdout(u64::MAX))
    }
    /// Standard error deltas, with an independent numeric sequence.
    pub fn stderr(&self) -> impl Iterator<Item = &'a HeadFile> + use<'a> {
        self.range(ToolFile::Stderr(0), ToolFile::Stderr(u64::MAX))
    }
}

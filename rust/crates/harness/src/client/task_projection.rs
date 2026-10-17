//! Incremental folds over immutable task segments and their sequence cursors.
use super::{TaskFileQueryView, TaskFileViews};
use crate::task_volume::{Channel, FileKey, FileValue, TaskFile, ToolFile};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct Fold {
    streams: BTreeMap<(u64, crate::OperationId, Channel), Tail>,
    tools: BTreeMap<(u64, String, bool), Tail>,
    results: BTreeSet<u64>,
    streamed: bool,
}
#[derive(Clone, Debug, Default, PartialEq)]
struct Tail {
    through: Option<u64>,
    pending: Vec<u8>,
    pending_text: usize,
    frozen: bool,
}
impl Tail {
    // Each accepted byte is visited once, plus at most three UTF-8 boundary bytes.
    fn append(&mut self, sequence: u64, bytes: &[u8], target: &mut String) -> usize {
        if self.frozen || self.through.is_some_and(|through| sequence <= through) {
            return 0;
        }
        self.through = Some(sequence);
        target.truncate(target.len().saturating_sub(self.pending_text));
        self.pending_text = 0;
        let mut input = std::mem::take(&mut self.pending);
        input.extend_from_slice(bytes);
        let work = input.len();
        let mut at = 0;
        while at < input.len() {
            match std::str::from_utf8(&input[at..]) {
                Ok(text) => {
                    target.push_str(text);
                    break;
                }
                Err(error) => {
                    let valid = at + error.valid_up_to();
                    target.push_str(
                        std::str::from_utf8(&input[at..valid]).expect("validated prefix"),
                    );
                    target.push('\u{fffd}');
                    if let Some(length) = error.error_len() {
                        at = valid + length;
                    } else {
                        self.pending.extend_from_slice(&input[valid..]);
                        self.pending_text = '\u{fffd}'.len_utf8();
                        break;
                    }
                }
            }
        }
        work
    }
}
fn owner(task: &crate::task_volume::Component) -> String {
    format!("AGGREGATE_KIND_TASK:{}", task.as_str())
}

pub(super) fn snapshot(views: &mut TaskFileViews, files: &TaskFileQueryView) {
    let previous = views
        .queries
        .get(&(files.cursor().volume, files.cursor().query));
    let owners: BTreeSet<_> = previous
        .into_iter()
        .chain(std::iter::once(files))
        .flat_map(|query| query.files().keys())
        .filter_map(|key| match key {
            FileKey::Task {
                task,
                file: TaskFile::Chunk { .. } | TaskFile::ExecutionResult(_) | TaskFile::Tool { .. },
            } => Some(owner(task)),
            _ => None,
        })
        .collect();
    for key in owners {
        views.transcripts.remove(&key);
        views.tools.remove(&key);
        views.task_folds.remove(&key);
    }
    // The source snapshot/follow order comes from durable publication positions.
    // File keys identify streams; per-turn ordinals do not order different streams.
    for file in files.ordered_files().filter(|file| {
        !matches!(
            file.key,
            FileKey::Task {
                file: TaskFile::ExecutionResult(_),
                ..
            }
        )
    }) {
        append(views, file);
    }
    let mut results: Vec<_> = files
        .files()
        .values()
        .filter_map(|file| super::command_output::text(file).map(|(sequence, _)| (sequence, file)))
        .collect();
    results.sort_by_key(|(sequence, _)| *sequence);
    for (_, file) in results {
        append(views, file);
    }
}

pub(super) fn projected_task(key: &FileKey) -> Option<crate::task_volume::Component> {
    match key {
        FileKey::Task {
            task,
            file:
                TaskFile::Chunk { .. }
                | TaskFile::ExecutionResult(_)
                | TaskFile::Tool {
                    file: ToolFile::Stdout(_) | ToolFile::Stderr(_),
                    ..
                },
        } => Some(task.clone()),
        _ => None,
    }
}

// Eviction is a bounded query-window replacement, not another text delta.
// Rebuild only affected tasks, visiting their retained segments once.
pub(super) fn replace_tasks(
    views: &mut TaskFileViews,
    tasks: BTreeSet<crate::task_volume::Component>,
    mut files: Vec<FileValue>,
) {
    for task in tasks {
        let key = owner(&task);
        views.transcripts.remove(&key);
        views.tools.remove(&key);
        views.task_folds.remove(&key);
    }
    files.sort_by_cached_key(|file| {
        super::command_output::text(file).map_or((0, 0), |(sequence, _)| (1, sequence))
    });
    for file in files {
        append(views, &file);
    }
}

pub(super) fn append(views: &mut TaskFileViews, file: &FileValue) -> usize {
    let FileKey::Task { task, file: kind } = &file.key else {
        return 0;
    };
    let key = owner(task);
    match kind {
        TaskFile::Chunk {
            turn,
            execution,
            sequence,
        } => {
            let Some(channel) = file.channel else {
                return 0;
            };
            let fold = views.task_folds.entry(key.clone()).or_default();
            let tail = fold
                .streams
                .entry((*turn, *execution, channel))
                .or_default();
            if tail.frozen || tail.through.is_some_and(|through| *sequence <= through) {
                return 0;
            }
            let transcript = views.transcripts.entry(key).or_default();
            let start = if channel == Channel::Reasoning {
                &mut transcript.reasoning_start
            } else {
                &mut transcript.file_start
            };
            start.get_or_insert_with(|| file.key.clone());
            let target = if channel == Channel::Reasoning {
                &mut transcript.reasoning
            } else {
                &mut transcript.streaming_text
            };
            if tail.through.is_none() && !target.is_empty() {
                target.push_str("\n\n");
            }
            if channel == Channel::Text {
                fold.streamed = true;
            }
            transcript.streaming_turn = Some(crate::OperationId::from_bytes(
                u128::from(*turn).to_be_bytes(),
            ));
            let work = tail.append(*sequence, &file.bytes, target);
            if channel == Channel::Text {
                transcript.text_pending = tail.pending_text;
            } else {
                transcript.reasoning_pending = tail.pending_text;
            }
            work
        }
        TaskFile::Tool {
            turn,
            call,
            file: leaf @ (ToolFile::Stdout(_) | ToolFile::Stderr(_)),
        } => {
            let (sequence, stderr) = match leaf {
                ToolFile::Stdout(n) => (*n, false),
                ToolFile::Stderr(n) => (*n, true),
                _ => unreachable!(),
            };
            let tail = views
                .task_folds
                .entry(key.clone())
                .or_default()
                .tools
                .entry((*turn, call.as_str().to_owned(), stderr))
                .or_default();
            let tool = views
                .tools
                .entry(key)
                .or_default()
                .entry(call.as_str().to_owned())
                .or_default();
            let crate::model::ToolResultContent::Parts { parts } = &mut tool.output else {
                return 0;
            };
            // Keep each channel's UTF-8 boundary independent even when stdout
            // and stderr publications interleave. Fixed slots also make reopen
            // independent of the publication order between the two streams.
            while parts.len() < 2 {
                parts.push(crate::model::ModelDataPart::Text {
                    text: String::new(),
                });
            }
            let Some(crate::model::ModelDataPart::Text { text }) =
                parts.get_mut(usize::from(stderr))
            else {
                return 0;
            };
            tail.append(sequence, &file.bytes, text)
        }
        TaskFile::ExecutionResult(_) => {
            let Some((sequence, text)) = super::command_output::text(file) else {
                return 0;
            };
            let fold = views.task_folds.entry(key.clone()).or_default();
            if fold.streamed || !fold.results.insert(sequence) {
                return 0;
            }
            let transcript = views.transcripts.entry(key).or_default();
            if !transcript.streaming_text.is_empty() {
                transcript.streaming_text.push_str("\n\n");
            }
            transcript.streaming_text.push_str(&text);
            transcript.streaming_turn = Some(crate::OperationId::from_bytes(
                u128::from(sequence).to_be_bytes(),
            ));
            text.len()
        }
        TaskFile::Completion(turn) => {
            if let Some(fold) = views.task_folds.get_mut(&key) {
                for channel in [Channel::Text, Channel::Reasoning] {
                    let latest = fold
                        .streams
                        .keys()
                        .rev()
                        .find_map(|(turn, execution, found)| {
                            (*found == channel).then_some((*turn, *execution))
                        });
                    if let Some((latest_turn, execution)) = latest
                        && latest_turn == *turn
                        && let Some(tail) = fold.streams.get_mut(&(*turn, execution, channel))
                    {
                        tail.frozen = true;
                        if let Some(transcript) = views
                            .transcripts
                            .get_mut(&key)
                            .filter(|_| latest.is_some_and(|(latest, _)| latest == *turn))
                        {
                            if channel == Channel::Text {
                                transcript.text_pending = 0;
                            } else {
                                transcript.reasoning_pending = 0;
                            }
                        }
                    }
                }
            }
            0
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests;

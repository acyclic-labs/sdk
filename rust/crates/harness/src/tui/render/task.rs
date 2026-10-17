use super::{Line, StreamText, archive::Archive, tool};
use crate::{
    client::TaskQueryView,
    task_volume::{Channel, Component, FileKey, HeadFile, Query, TaskFile},
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Default)]
struct ToolRows {
    request: Option<HeadFile>,
    result: Option<HeadFile>,
    stdout: StreamText,
    stderr: StreamText,
    out: Vec<Line<'static>>,
    err: Vec<Line<'static>>,
    rows: Vec<Line<'static>>,
}
#[derive(Default)]
pub(super) struct TaskRows {
    identity: Option<(Arc<Query>, Component)>,
    stream: StreamText,
    reasoning: StreamText,
    text: Vec<Line<'static>>,
    reasoning_rows: Vec<Line<'static>>,
    reasoning_changed: usize,
    tools: BTreeMap<(u64, Component), ToolRows>,
    fallback: Archive,
    rows: Vec<Line<'static>>,
    status: Option<HeadFile>,
    state: serde_json::Value,
    completions: Vec<HeadFile>,
    usage: serde_json::Value,
    hidden: bool,
    details: bool,
}
fn metadata(file: &HeadFile) -> Option<serde_json::Value> {
    let [bytes] = file.segments.as_slice() else {
        return None;
    };
    serde_json::from_slice(bytes).ok()
}
impl TaskRows {
    pub(super) fn rows(&self) -> &[Line<'static>] {
        &self.rows
    }
    pub(super) fn reasoning(&self) -> (usize, &Vec<Line<'static>>) {
        (self.reasoning_changed, &self.reasoning_rows)
    }
    pub(super) fn busy(&self) -> bool {
        self.state["state"] == "active" || self.state["state"] == "running"
    }
    pub(super) fn stopped(&self) -> bool {
        self.state["state"] == "cancelled"
    }
    pub(super) fn usage(&self) -> String {
        if self.usage.is_null() {
            String::new()
        } else {
            self.usage.to_string()
        }
    }
    pub(super) fn notices(&self) -> Vec<Line<'static>> {
        self.state["reason"]
            .as_str()
            .map(|text| vec![Line::raw(text.escape_debug().to_string())])
            .unwrap_or_default()
    }
    #[cfg(test)]
    pub(super) fn work(&self) -> usize {
        self.stream.work + self.reasoning.work + self.fallback.work()
    }
    pub(super) fn update(
        &mut self,
        task: Option<TaskQueryView<'_>>,
        details: bool,
        hidden: bool,
    ) -> usize {
        let Some(task) = task else {
            let changed = if self.identity.is_some() {
                0
            } else {
                self.rows.len()
            };
            *self = Self::default();
            return changed;
        };
        let mut changed = self.rows.len();
        if !self
            .identity
            .as_ref()
            .is_some_and(|(source, owner)| Arc::ptr_eq(source, task.source) && owner == task.task)
        {
            *self = Self {
                identity: Some((task.source.clone(), task.task.clone())),
                ..Self::default()
            };
            changed = 0;
        }
        if self.hidden != hidden {
            self.hidden = hidden;
            changed = 0;
        }
        let details_changed = self.details != details;
        self.details = details;
        let text_changed = self.stream.update(&task, Channel::Text);
        self.text.truncate(text_changed);
        self.text.extend(self.stream.rows(text_changed));
        changed = changed.min(text_changed);
        let reasoning_changed = self.reasoning.update(&task, Channel::Reasoning);
        self.reasoning_changed = reasoning_changed;
        self.reasoning_rows.truncate(reasoning_changed);
        self.reasoning_rows
            .extend(self.reasoning.rows(reasoning_changed));
        let status = task.files().find(|file| {
            matches!(
                file.key,
                FileKey::Task {
                    file: TaskFile::Status,
                    ..
                }
            )
        });
        if self.status.as_ref() != status {
            self.state = status.and_then(metadata).unwrap_or_default();
            self.status = status.cloned();
        }
        let completions: Vec<_> = task.completions().map(|(_, file)| file).collect();
        if !self.completions.iter().eq(completions.iter().copied()) {
            self.usage = serde_json::Value::Null;
            for value in completions.iter().filter_map(|file| metadata(file)) {
                if let Err(error) = crate::model::accumulate_usage(&mut self.usage, value) {
                    self.usage = serde_json::json!({"error":error.to_string()});
                    break;
                }
            }
            self.completions = completions.into_iter().cloned().collect();
        }
        let mut retained = std::collections::BTreeSet::new();
        let mut tool_changed = false;
        for tool in task.tools() {
            let key = (tool.turn, tool.call.clone());
            retained.insert(key.clone());
            let cached = self.tools.entry(key).or_default();
            let stdout = cached.stdout.update_tool(&tool, false, 20);
            let stderr = cached
                .stderr
                .update_tool(&tool, true, 20 - cached.stdout.tool_lines());
            let request = tool.request();
            let result = tool.result();
            let metadata_changed =
                cached.request.as_ref() != request || cached.result.as_ref() != result;
            if metadata_changed
                || details_changed
                || stdout < cached.out.len()
                || cached.stdout.len() != cached.out.len()
                || stderr < cached.err.len()
                || cached.stderr.len() != cached.err.len()
                || cached.rows.is_empty()
            {
                cached.out.truncate(stdout);
                cached.out.extend(cached.stdout.rows(stdout));
                cached.err.truncate(stderr);
                cached.err.extend(cached.stderr.rows(stderr));
                if metadata_changed || cached.rows.is_empty() {
                    cached.rows.clear();
                    cached.rows.push(tool::heading(request, result, tool.call));
                } else {
                    cached.rows.truncate(1);
                }
                if details {
                    if tool.stdout().next().is_some() || tool.stderr().next().is_some() {
                        cached
                            .rows
                            .extend(cached.out.iter().chain(&cached.err).take(20).cloned());
                    } else {
                        cached.rows.extend(tool::result_rows(result));
                    }
                }
                cached.request = request.cloned();
                cached.result = result.cloned();
                tool_changed = true;
            }
        }
        let previous = self.tools.len();
        self.tools.retain(|key, _| retained.contains(key));
        tool_changed |= previous != self.tools.len();
        let streamed = task.text().any(|text| text.channel == Channel::Text);
        // Command text is a fallback only when the canonical task has no chunks.
        let fallback = if !streamed {
            // This view is backed by the same selected file index, never a second transcript.
            self.fallback.update(Some((
                task.selection(),
                crate::client::OutputOwner::Task(task.task.clone()),
            )))
        } else {
            self.fallback.update(None)
        };
        if !streamed {
            changed = changed.min(fallback);
        }
        let text = if streamed {
            &self.text
        } else {
            self.fallback.rows()
        };
        if tool_changed {
            changed = changed.min(text.len());
        }
        if hidden {
            self.rows.clear();
            return 0;
        }
        // Only the changed Markdown suffix and tool tail enter the render cache.
        changed = changed.min(text.len());
        self.rows.truncate(changed);
        self.rows.extend_from_slice(&text[changed..]);
        self.rows.extend(
            self.tools
                .values()
                .flat_map(|tool| tool.rows.iter().cloned()),
        );
        changed
    }
}

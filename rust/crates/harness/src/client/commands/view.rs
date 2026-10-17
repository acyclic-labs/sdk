//! Rendering and form consumption over the followed command and private files.
use super::*;
use crate::{
    client::CommandViewFiles,
    task_volume::{Channel, FileKey, Query, TaskFile, ToolFile},
};
use acyclic_stream::StreamProvider;
use serde_json::{Value, json};
use std::fmt::Write as _;

/// Render an immutable command outcome, preserving admission identities used by
/// installed clients. The terminal adapter applies its own text encoding.
pub fn render_command_record(value: &Value) -> String {
    let value = value.get("ok").unwrap_or(value);
    if let Some(text) = value["rendered"].as_str() {
        return text.to_owned();
    }
    let mut text = String::new();
    if let (Some(task), Some(operation)) = (
        value["task"].as_str(),
        value["admission_operation"].as_str(),
    ) {
        text.push_str(&format!(
            "queued task={task} admission-operation={operation}\n"
        ));
    }
    text.push_str(&format!("command-json={value}\n"));
    text
}
impl<P: StreamProvider + ?Sized> CommandSession<P> {
    /// Private query selections needed by the current file-backed layout.
    pub fn view_queries(&self) -> Result<Vec<Query>> {
        let layout = self
            .state(CommandFileRoot::Session, "layout.json")?
            .unwrap_or(json!({}));
        let mut queries = Vec::new();
        if layout["tree"] == true {
            queries.extend([Query::TaskOverviews, Query::Agents]);
        }
        if let Some(task) = self.selected_task()? {
            let settings = self
                .state(CommandFileRoot::Config, "settings.json")?
                .unwrap_or(json!({}));
            queries.push(Query::TaskView {
                task: crate::task_volume::Component::new(task.to_string()).map_err(storage)?,
                recent: 16,
                reasoning: settings["reasoning"] == true,
                details: settings["tool_details"] == true,
                terminal: layout["terminal"] == true,
            });
        }
        match layout["open_view"].as_str() {
            Some("plan") => queries.push(Query::Plans),
            Some("goal") => queries.push(Query::File(FileKey::Goal)),
            _ => {}
        }
        Ok(queries)
    }

    /// Render the actual selected content. Both headless and terminal clients
    /// use this fold; reasoning, tool detail and terminal flags affect its rows.
    pub fn render_view(&self, documents: &CommandViewFiles) -> Result<String> {
        let layout = self
            .state(CommandFileRoot::Session, "layout.json")?
            .unwrap_or(json!({}));
        let settings = self
            .state(CommandFileRoot::Config, "settings.json")?
            .unwrap_or(json!({}));
        let task_settings = self
            .state(CommandFileRoot::Task, "settings.json")?
            .unwrap_or(json!({}));
        let mut out = String::new();
        let view = layout["open_view"].as_str();
        match view {
            Some("history") => {
                let page = layout["history_page"].as_u64().unwrap_or(0);
                match usize::try_from(page)
                    .ok()
                    .map(|page| self.output_page(page))
                    .transpose()?
                    .flatten()
                {
                    Some(value) => out.push_str(&render_command_record(&value)),
                    None => writeln!(out, "History page {page} is unavailable.").unwrap(),
                }
            }
            Some("shortcuts") => {
                for spec in self.catalog().commands() {
                    writeln!(
                        out,
                        "/{} — {}{}",
                        spec.name,
                        spec.description,
                        if spec.aliases.is_empty() {
                            String::new()
                        } else {
                            format!(" ({})", spec.aliases.join(", "))
                        }
                    )
                    .unwrap();
                }
            }
            Some("theme") => {
                writeln!(
                    out,
                    "Theme picker — type a name; /shortcuts closes the picker"
                )
                .unwrap();
                for name in &self.catalog().themes {
                    writeln!(out, "{name}").unwrap();
                }
                if self.catalog().themes.is_empty() {
                    writeln!(out, "No themes configured in commands/themes.json").unwrap();
                }
            }
            Some("model") => {
                writeln!(
                    out,
                    "Model: {}",
                    task_settings["model"]
                        .as_str()
                        .or(settings["model"].as_str())
                        .unwrap_or("configured default")
                )
                .unwrap();
                writeln!(out, "Select with /model <name>").unwrap();
                if let Some(models) = settings["models"].as_array() {
                    for model in models {
                        writeln!(out, "{model}").unwrap();
                    }
                }
            }
            Some("mcp") => {
                let tools = self
                    .state(CommandFileRoot::Config, "tools.json")?
                    .unwrap_or(json!([]));
                writeln!(
                    out,
                    "Registered tools and MCP diagnostics
{tools}"
                )
                .unwrap();
            }
            Some("feedback") => {
                if layout["view_input"] == "feedback" {
                    writeln!(out, "Feedback — type your report; /shortcuts cancels").unwrap();
                } else if let Some(saved) = self.state(CommandFileRoot::Session, "feedback.json")? {
                    writeln!(
                        out,
                        "Feedback saved locally: {}",
                        saved["text"].as_str().unwrap_or("")
                    )
                    .unwrap();
                }
            }
            Some("find") => {
                if layout["view_input"] == "find" {
                    writeln!(
                        out,
                        "Find in transcript — type a search; /shortcuts cancels"
                    )
                    .unwrap();
                }
            }
            _ => {}
        }
        if matches!(
            view,
            Some("shortcuts" | "theme" | "model" | "mcp" | "feedback" | "history")
        ) {
            if out.len() as u64 > self.view_maximum_bytes() {
                return Err(Error::Invalid("view exceeds display byte bound".into()));
            }
            return Ok(out);
        }
        writeln!(
            out,
            "reasoning={} tool-details={} diff-mode={} tree={} terminal={} theme={} mode={}",
            settings["reasoning"] == true,
            settings["tool_details"] == true,
            settings["diff_mode"].as_str().unwrap_or("auto"),
            layout["tree"] == true,
            layout["terminal"] == true,
            self.state(CommandFileRoot::Config, "theme.json")?
                .and_then(|value| value["name"].as_str().map(str::to_owned))
                .unwrap_or_default(),
            task_settings["agent_mode"].as_str().unwrap_or("code")
        )
        .unwrap();
        let search = self
            .state(CommandFileRoot::Session, "search.json")?
            .unwrap_or(json!({}));
        let needle = search["text"].as_str().unwrap_or("").to_lowercase();
        let mut tree_tasks = BTreeSet::new();
        for fold in &documents.0 {
            for file in fold.files().values() {
                let text = std::str::from_utf8(&file.segments[0])
                    .map_err(|_| Error::Unsupported("view file is not UTF-8".into()))?;
                match &file.key {
                    FileKey::Plan(_) | FileKey::Goal if matches!(view, Some("plan" | "goal")) => {
                        writeln!(
                            out,
                            "{}
{text}",
                            file.key.path()
                        )
                        .unwrap();
                    }
                    FileKey::Agent(_) if layout["tree"] == true => {
                        writeln!(out, "agent {} {text}", file.key.path()).unwrap();
                    }
                    FileKey::Task { task, file: kind } => {
                        if layout["tree"] == true && tree_tasks.insert(task.clone()) {
                            writeln!(out, "task {}", task.as_str()).unwrap();
                        }
                        if layout["tree"] == true && *kind == TaskFile::Status {
                            writeln!(out, "task {} {text}", task.as_str()).unwrap();
                        }
                        if self
                            .selected_task()?
                            .is_none_or(|selected| selected.to_string() != task.as_str())
                        {
                            continue;
                        }
                        let visible = match kind {
                            TaskFile::ExecutionResult(_) => {
                                let outcome: Value = serde_json::from_str(text)
                                    .map_err(|error| Error::Invalid(error.to_string()))?;
                                let result = &outcome["ok"];
                                if layout["terminal"] == true {
                                    for key in ["stdout", "stderr"] {
                                        if let Some(text) = result[key].as_str() {
                                            writeln!(out, "{text}").unwrap();
                                        }
                                    }
                                }
                                if settings["tool_details"] == true {
                                    writeln!(out, "{result}").unwrap();
                                }
                                false
                            }
                            TaskFile::Request(_) => {
                                let request: Value = serde_json::from_str(text)
                                    .map_err(|error| Error::Invalid(error.to_string()))?;
                                if request["kind"] == "prompt" {
                                    if let Some(prompt) = request["text"].as_str() {
                                        for line in prompt.lines() {
                                            if view != Some("find")
                                                || !needle.is_empty()
                                                    && line.to_lowercase().contains(&needle)
                                            {
                                                writeln!(out, "{line}").unwrap();
                                            }
                                        }
                                    }
                                }
                                false
                            }
                            TaskFile::TextHead {
                                channel: Channel::Text,
                                ..
                            }
                            | TaskFile::TurnRequest(_) => true,
                            TaskFile::TextHead {
                                channel: Channel::Reasoning,
                                ..
                            } => settings["reasoning"] == true,
                            TaskFile::Chunk { .. } => {
                                file.channel != Some(Channel::Reasoning)
                                    || settings["reasoning"] == true
                            }
                            TaskFile::Tool {
                                file: ToolFile::Request | ToolFile::Result | ToolFile::Arguments(_),
                                ..
                            } => settings["tool_details"] == true,
                            TaskFile::Tool {
                                file: ToolFile::Stdout(_) | ToolFile::Stderr(_),
                                ..
                            } => layout["terminal"] == true,
                            _ => false,
                        };
                        if visible {
                            for line in text.lines() {
                                if view != Some("find")
                                    || !needle.is_empty() && line.to_lowercase().contains(&needle)
                                {
                                    writeln!(out, "{line}").unwrap();
                                }
                            }
                        }
                    }
                    _ => {}
                }
                if out.len() as u64 > self.view_maximum_bytes() {
                    return Err(Error::Invalid("view exceeds display byte bound".into()));
                }
            }
        }
        if out.len() as u64 > self.view_maximum_bytes() {
            return Err(Error::Invalid("view exceeds display byte bound".into()));
        }
        Ok(out)
    }
}
fn storage(error: crate::task_volume::Error) -> Error {
    Error::Storage(error.to_string())
}

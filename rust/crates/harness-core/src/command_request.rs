//! Typed client command requests and resolutions.
//!
//! Clients produce these values and the filesystem execution journal reads the
//! published cancellation requests back, so they live below both; the
//! `acyclic-harness` facade re-exports them from `client`.
use crate::{Error, OperationId, Result, context::PinnedContextPath, conversation::FileRef};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// Exact composer range and immutable file selected by an `@` mention.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMention {
    /// Inclusive UTF-8 byte offset of `@`.
    pub start: usize,
    /// Exclusive UTF-8 byte offset, including closing quotes when present.
    pub end: usize,
    /// Owner-verified immutable content reference.
    pub file: FileRef,
}

/// Typed client requests emitted by built-in commands.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommandRequest {
    /// Cancel the captured admitted task. Published as an immutable request file.
    Cancel {
        /// Exact task; later selection changes cannot retarget this request.
        task: crate::TaskId,
    },
    /// Submit an immutable replacement of an earlier user request as a new branch.
    Replace {
        /// Original prompt admission, retained for provenance.
        message: crate::OperationId,
        /// Replacement prompt; never interpreted as a slash command.
        text: String,
    },
    /// Exit the client.
    Quit,
    /// Create a new root conversation.
    New,
    /// Delete the selected agent, after confirmation.
    Delete,
    /// Toggle paused state for the selected subtree.
    TogglePause,
    /// Pause the declared task set.
    Pause {
        /// Subtree or all conversation trees.
        scope: CommandTaskScope,
    },
    /// Resume the declared task set.
    Resume {
        /// Subtree or all conversation trees.
        scope: CommandTaskScope,
    },
    /// Cycle the selected agent's mode.
    CycleMode,
    /// Clear conversations, after confirmation.
    ClearAll,
    /// Export diagnostics.
    Export,
    /// Select an exact configured theme name.
    Theme {
        /// Canonical name from the caller's theme data.
        name: String,
    },
    /// Compact with optional summary guidance.
    Compact {
        /// User-supplied summary guidance.
        instructions: String,
    },
    /// Change the model through the harness request owner.
    Model {
        /// Provider/model identifier.
        model: String,
    },
}

/// Task control commands distinguish the current subtree from every tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandTaskScope {
    /// The selected task and its descendants.
    Subtree,
    /// All conversation trees.
    All,
}

/// Direct tools receive structured arguments without a model turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandTool {
    /// Execute a shell command through the bash tool owner.
    Bash,
    /// Fork/spawn an agent through the task owner.
    Spawn,
    /// Search the web through the configured search owner.
    WebSearch,
    /// Fetch a URL through the configured fetch owner.
    UrlFetch,
}

impl CommandTool {
    #[doc(hidden)]
    pub fn input(self, arguments: &str) -> serde_json::Value {
        let key = match self {
            Self::Bash => "command",
            Self::Spawn => "task",
            Self::WebSearch => "query",
            Self::UrlFetch => "url",
        };
        serde_json::json!({key: arguments})
    }
}

/// View selected without submitting model text.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandQuery {
    /// Catalog-generated shortcuts/help overlay.
    Shortcuts,
    /// Toggle reasoning visibility.
    Reasoning,
    /// Toggle tool detail visibility.
    Details,
    /// Toggle the agent tree.
    Tree,
    /// Toggle the embedded terminal.
    Terminal,
    /// Search the transcript.
    Find,
    /// Select the next task needing attention.
    Attention,
    /// Open the theme picker.
    Theme,
    /// Open the feedback overlay.
    Feedback,
    /// Current model and available models.
    Model,
    /// Captured workspace changes against each volume's base.
    Diff,
    /// Markdown files in the private plans/ directory.
    Plan,
    /// Private GOAL.md.
    Goal,
    /// MCP servers/tools.
    Mcp,
}

/// Complete composer classification; renderers do not parse slash syntax.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommandResolution {
    /// An installed SDK operation, consumed from an ordinary request file.
    Sdk {
        /// Exact SDK operation from the catalog.
        command: String,
        /// Unmodified argument text, normally a pinned request-file location.
        arguments: String,
    },
    /// A destructive client operation awaiting explicit confirmation.
    Confirm {
        /// Operation to admit only after confirmation.
        request: CommandRequest,
    },
    /// A direct structured tool call, bypassing the model.
    Tool {
        /// Tool routed to its configured owner.
        tool: CommandTool,
        /// Exact arguments, with no prompt/template interpretation.
        input: serde_json::Value,
    },
    /// Ordinary text or a resolved prompt template.
    Prompt {
        /// Text ready for mention resolution and submission.
        text: String,
        /// Pins the template/skill revision used to produce the text.
        source: Option<PinnedContextPath>,
        /// Pinned files resolved in the declared workspace, in mention order.
        mentions: Vec<FileMention>,
    },
    /// A typed harness operation.
    Submit {
        /// Operation to admit.
        request: CommandRequest,
    },
    /// A live view to render.
    Select {
        /// View identifier.
        query: CommandQuery,
    },
}

mod model_input;

impl CommandResolution {
    #[doc(hidden)]
    pub fn from_request(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes)
            .or_else(|_| serde_json::from_slice(bytes).map(|request| Self::Submit { request }))
            .map_err(codec)
    }
}

/// Explicit bindings to the caller's authorized file-query roots.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandFileRoot {
    /// User config volume for shared presentation preferences.
    Config,
    /// Current UI session's directory, isolated from other clients.
    Session,
    /// Selected task's protected namespace in the agent's private volume.
    Task,
}

/// Read-only projection of files from the three authorized live-query roots.
pub type CommandFileSnapshot = BTreeMap<CommandFileRoot, BTreeMap<String, Value>>;

/// Ordinary file publication data. The FS client applies its existing generation
/// precondition and authority checks; this is not a separate submit protocol.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandFileChange {
    /// Stable publication identity reused by the filesystem client's retries.
    pub operation: OperationId,
    /// Caller-bound volume/directory, never an implicit host path.
    pub root: CommandFileRoot,
    /// Path relative to that root.
    pub path: String,
    /// JSON file contents to publish.
    pub contents: Value,
    /// Request files are immutable creations; settings replace their query cut.
    pub create_only: bool,
}

impl CommandResolution {
    #[doc(hidden)]
    pub fn file_inputs(&self) -> &'static [(CommandFileRoot, &'static str)] {
        use CommandFileRoot::{Config, Session, Task};
        match self {
            Self::Submit {
                request: CommandRequest::Model { .. } | CommandRequest::CycleMode,
            } => &[(Task, "settings.json")],
            Self::Submit {
                request: CommandRequest::Theme { .. },
            } => &[(Config, "theme.json")],
            Self::Select {
                query: CommandQuery::Reasoning | CommandQuery::Details | CommandQuery::Diff,
            } => &[(Config, "settings.json")],
            Self::Select {
                query: CommandQuery::Attention,
            } => &[
                (Session, "layout.json"),
                (Config, "attention.json"),
                (Session, "attention.json"),
            ],
            Self::Submit {
                request: CommandRequest::Quit,
            }
            | Self::Confirm { .. }
            | Self::Select { .. } => &[(Session, "layout.json")],
            Self::Sdk { command, .. } if command == "back" => &[(Session, "layout.json")],
            _ => &[],
        }
    }

    /// Produce file changes from the current config-volume query snapshot.
    /// Callers publish with that cut's generation, then render the resulting
    /// query. No UI selection or setting is committed in an in-memory store.
    pub fn file_changes(
        &self,
        snapshot: &CommandFileSnapshot,
        operation: OperationId,
    ) -> Result<Vec<CommandFileChange>> {
        let config = |root, path: &str, contents| {
            vec![CommandFileChange {
                operation,
                root,
                path: path.into(),
                contents,
                create_only: false,
            }]
        };
        let publish_request = |root, contents| {
            vec![CommandFileChange {
                operation,
                root,
                path: format!("requests/{operation}.json"),
                contents,
                create_only: true,
            }]
        };
        match self {
            Self::Submit {
                request: CommandRequest::Model { model },
            } => {
                let mut settings = document(snapshot, CommandFileRoot::Task, "settings.json")?;
                settings.insert("model".into(), json!(model));
                return Ok(config(
                    CommandFileRoot::Task,
                    "settings.json",
                    settings.into(),
                ));
            }
            Self::Submit {
                request: CommandRequest::Theme { name },
            } => {
                let mut theme = document(snapshot, CommandFileRoot::Config, "theme.json")?;
                theme.insert("name".into(), json!(name));
                return Ok(config(CommandFileRoot::Config, "theme.json", theme.into()));
            }
            Self::Submit {
                request: CommandRequest::CycleMode,
            } => {
                let mut settings = document(snapshot, CommandFileRoot::Task, "settings.json")?;
                cycle(&mut settings, "agent_mode", "agent_modes")?;
                return Ok(config(
                    CommandFileRoot::Task,
                    "settings.json",
                    settings.into(),
                ));
            }
            Self::Submit {
                request: CommandRequest::Quit,
            } => {
                let mut layout = document(snapshot, CommandFileRoot::Session, "layout.json")?;
                layout.insert("exit_requested".into(), json!(true));
                return Ok(config(
                    CommandFileRoot::Session,
                    "layout.json",
                    layout.into(),
                ));
            }
            Self::Submit { request } => {
                let root = if matches!(
                    request,
                    CommandRequest::Pause {
                        scope: CommandTaskScope::All
                    } | CommandRequest::Resume {
                        scope: CommandTaskScope::All
                    } | CommandRequest::New
                        | CommandRequest::ClearAll
                        | CommandRequest::Export
                ) {
                    CommandFileRoot::Session
                } else {
                    CommandFileRoot::Task
                };
                return Ok(publish_request(
                    root,
                    serde_json::to_value(request).map_err(codec)?,
                ));
            }
            Self::Sdk { command, arguments } if command == "back" => {
                let back = arguments.parse::<u64>().map_err(|_| {
                    Error::Invalid(
                        "display page unavailable; request an explicit archival page".into(),
                    )
                })?;
                let mut layout = document(snapshot, CommandFileRoot::Session, "layout.json")?;
                layout.insert("history_page".into(), json!(back));
                layout.insert("open_view".into(), json!("history"));
                layout.insert("view_request".into(), json!(operation));
                return Ok(config(
                    CommandFileRoot::Session,
                    "layout.json",
                    layout.into(),
                ));
            }
            Self::Sdk { .. } => {
                return Ok(publish_request(
                    CommandFileRoot::Session,
                    serde_json::to_value(self).map_err(codec)?,
                ));
            }
            Self::Tool { .. } | Self::Prompt { .. } => {
                return Ok(publish_request(
                    CommandFileRoot::Task,
                    serde_json::to_value(self).map_err(codec)?,
                ));
            }
            Self::Confirm { request } => {
                let mut layout = document(snapshot, CommandFileRoot::Session, "layout.json")?;
                let mut confirmation = json!({"request":request,"operation":operation});
                if matches!(request, CommandRequest::Delete) {
                    let task = layout
                        .get("selected_task")
                        .and_then(Value::as_str)
                        .ok_or_else(|| Error::Invalid("delete requires a selected task".into()))?;
                    crate::TaskId::parse(task)?;
                    confirmation["task"] = json!(task);
                }
                layout.insert("confirmation".into(), confirmation);
                return Ok(config(
                    CommandFileRoot::Session,
                    "layout.json",
                    layout.into(),
                ));
            }
            Self::Select {
                query: query @ (CommandQuery::Reasoning | CommandQuery::Details),
            } => {
                let mut settings = document(snapshot, CommandFileRoot::Config, "settings.json")?;
                toggle(
                    &mut settings,
                    if *query == CommandQuery::Reasoning {
                        "reasoning"
                    } else {
                        "tool_details"
                    },
                )?;
                return Ok(config(
                    CommandFileRoot::Config,
                    "settings.json",
                    settings.into(),
                ));
            }
            Self::Select {
                query: query @ (CommandQuery::Tree | CommandQuery::Terminal),
            } => {
                let mut layout = document(snapshot, CommandFileRoot::Session, "layout.json")?;
                toggle(
                    &mut layout,
                    if *query == CommandQuery::Tree {
                        "tree"
                    } else {
                        "terminal"
                    },
                )?;
                return Ok(config(
                    CommandFileRoot::Session,
                    "layout.json",
                    layout.into(),
                ));
            }
            Self::Select {
                query: CommandQuery::Diff,
            } => {
                let mut settings = document(snapshot, CommandFileRoot::Config, "settings.json")?;
                // The choices are configuration data, shared with the renderer.
                cycle(&mut settings, "diff_mode", "diff_modes")?;
                return Ok(config(
                    CommandFileRoot::Config,
                    "settings.json",
                    settings.into(),
                ));
            }
            Self::Select {
                query: CommandQuery::Attention,
            } => {
                let mut layout = document(snapshot, CommandFileRoot::Session, "layout.json")?;
                let shared = document(snapshot, CommandFileRoot::Config, "attention.json")?;
                let attention = if shared.contains_key("tasks") {
                    shared
                } else {
                    document(snapshot, CommandFileRoot::Session, "attention.json")?
                };
                let candidates = match attention.get("tasks") {
                    None => &[][..],
                    Some(Value::Array(tasks)) => tasks.as_slice(),
                    _ => return Err(Error::Invalid("attention query requires tasks".into())),
                };
                if candidates.is_empty() {
                    return Ok(Vec::new());
                }
                for candidate in candidates {
                    crate::TaskId::parse(candidate.as_str().ok_or_else(|| {
                        Error::Invalid("attention tasks must be task IDs".into())
                    })?)?;
                }
                let current = layout.get("selected_task");
                let next = candidates
                    .iter()
                    .position(|task| Some(task) == current)
                    .map_or(0, |index| (index + 1) % candidates.len());
                layout.insert("selected_task".into(), candidates[next].clone());
                return Ok(config(
                    CommandFileRoot::Session,
                    "layout.json",
                    layout.into(),
                ));
            }
            Self::Select { query } => {
                let mut layout = document(snapshot, CommandFileRoot::Session, "layout.json")?;
                if *query == CommandQuery::Find {
                    layout.insert("search_query".into(), json!(""));
                    layout.insert("search_index".into(), json!(0));
                }
                layout.insert(
                    "view_input".into(),
                    if matches!(
                        query,
                        CommandQuery::Find | CommandQuery::Theme | CommandQuery::Feedback
                    ) {
                        serde_json::to_value(query).map_err(codec)?
                    } else {
                        Value::Null
                    },
                );
                layout.insert("view_request".into(), json!(operation));
                layout.insert(
                    "open_view".into(),
                    serde_json::to_value(query).map_err(codec)?,
                );
                return Ok(config(
                    CommandFileRoot::Session,
                    "layout.json",
                    layout.into(),
                ));
            }
        }
    }
}

fn codec(error: serde_json::Error) -> Error {
    Error::Invalid(format!("command file codec: {error}"))
}

fn document(
    snapshot: &CommandFileSnapshot,
    root: CommandFileRoot,
    path: &str,
) -> Result<Map<String, Value>> {
    match snapshot.get(&root).and_then(|files| files.get(path)) {
        None => Ok(Map::new()),
        Some(Value::Object(object)) => Ok(object.clone()),
        _ => Err(Error::Invalid(format!("{path} must be a JSON object"))),
    }
}

fn toggle(document: &mut Map<String, Value>, key: &str) -> Result<()> {
    let current = match document.get(key) {
        None => false,
        Some(Value::Bool(current)) => *current,
        _ => return Err(Error::Invalid(format!("{key} must be boolean"))),
    };
    document.insert(key.into(), json!(!current));
    Ok(())
}

fn cycle(document: &mut Map<String, Value>, key: &str, choices: &str) -> Result<()> {
    let values = document
        .get(choices)
        .and_then(Value::as_array)
        .filter(|values| !values.is_empty() && values.iter().all(Value::is_string))
        .ok_or_else(|| Error::Invalid(format!("{choices} requires nonempty string choices")))?;
    let distinct: BTreeSet<_> = values.iter().filter_map(Value::as_str).collect();
    if distinct.len() != values.len() {
        return Err(Error::Invalid(format!(
            "{choices} contains duplicate choices"
        )));
    }
    let next = values
        .iter()
        .position(|value| Some(value) == document.get(key))
        .map_or(0, |index| (index + 1) % values.len());
    let next = values[next].clone();
    document.insert(key.into(), next);
    Ok(())
}

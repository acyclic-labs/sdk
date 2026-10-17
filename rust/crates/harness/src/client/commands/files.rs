use super::*;
use crate::OperationId;
use serde_json::{Map, Value, json};

impl CommandCatalog {
    /// Decode a bounded catalog-file snapshot; duplicate names/aliases fail.
    pub fn from_bytes(bytes: &[u8], maximum_bytes: u64) -> Result<Self> {
        if maximum_bytes == 0 || bytes.len() as u64 > maximum_bytes {
            return Err(Error::Invalid("catalog file exceeds byte bound".into()));
        }
        let specs: Vec<CommandSpec> = serde_json::from_slice(bytes)
            .map_err(|error| Error::Invalid(format!("invalid command catalog: {error}")))?;
        let mut names = name_index::NameIndex::default();
        let mut commands = BTreeMap::new();
        for mut spec in specs {
            spec.name.make_ascii_lowercase();
            for alias in &mut spec.aliases {
                alias.make_ascii_lowercase();
            }
            for name in std::iter::once(&spec.name).chain(&spec.aliases) {
                validate_name(name)?;
                if names.insert(name, &spec.name).is_some() {
                    return Err(Error::Conflict("duplicate command or alias".into()));
                }
            }
            if spec.description.trim().is_empty() || spec.description.contains('\0') {
                return Err(Error::Invalid("invalid command description".into()));
            }
            let expected = match &spec.action {
                CommandAction::Builtin {
                    command: BuiltinCommand::Compact,
                } => CommandArguments::Text,
                CommandAction::Builtin {
                    command: BuiltinCommand::Model,
                } => CommandArguments::Word,
                CommandAction::Builtin { .. }
                | CommandAction::Request { .. }
                | CommandAction::Select { .. } => CommandArguments::None,
                CommandAction::Tool { .. } => CommandArguments::RequiredText,
                CommandAction::Theme
                | CommandAction::Sdk { .. }
                | CommandAction::Prompt { .. }
                | CommandAction::Template { .. }
                | CommandAction::Skill { .. } => CommandArguments::Text,
            };
            if spec.arguments != expected {
                return Err(Error::Invalid(
                    "command argument shape disagrees with action".into(),
                ));
            }
            if let CommandAction::Request {
                request,
                confirmation,
            } = &spec.action
            {
                if *confirmation
                    != matches!(request, CommandRequest::Delete | CommandRequest::ClearAll)
                {
                    return Err(Error::Invalid(
                        "confirmation is required only for delete/clearall".into(),
                    ));
                }
            }
            match &spec.action {
                CommandAction::Sdk { command } => validate_name(command)?,
                CommandAction::Template { file, body, .. } => {
                    if !matches!(file, crate::task_volume::FileKey::Input(_)) || body.contains('\0')
                    {
                        return Err(Error::Invalid("invalid file template".into()));
                    }
                }
                CommandAction::Prompt { source, .. } => source.validate(&mut None)?,
                CommandAction::Skill { skill } => skill.validate(&mut None)?,
                _ => {}
            }
            commands.insert(spec.name.clone(), spec);
        }
        Ok(Self {
            commands,
            names,
            themes: Vec::new(),
        })
    }

    /// Load the authoritative command-definition file at the live query's cut.
    /// Bootstrap/default installation and publication belong to the FS client.
    pub async fn from_file(
        source: &PinnedContextPath,
        reader: &dyn ContentResidencyVerifier,
        maximum_bytes: u64,
    ) -> Result<Self> {
        let (_, bytes) = source.read(reader, maximum_bytes).await?;
        Self::from_bytes(&bytes, maximum_bytes)
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
    pub(super) fn file_inputs(&self) -> &'static [(CommandFileRoot, &'static str)] {
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

use super::{Error, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[cfg(test)]
#[path = "../../tests/task_volume/ranges.rs"]
mod tests;

/// Task namespace and packed-segment format revision.
pub const SCHEMA: u32 = 2;

/// A portable, canonical namespace component.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Component(Arc<str>);

impl Component {
    /// Validates a task, request, tool, principal or mount identity.
    pub fn new(value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref();
        if value.is_empty()
            || value.len() > 255
            || value == "."
            || value == ".."
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        {
            return Err(Error::InvalidPath);
        }
        Ok(Self(Arc::from(value)))
    }

    /// Canonical component text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Component {
    type Error = Error;
    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}
impl From<Component> for String {
    fn from(value: Component) -> Self {
        value.0.to_string()
    }
}

/// Authorization subject; the host applies its volume policy before publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Owner {
    /// Authenticated submitter.
    Caller,
    /// Agent with write access to its private volume.
    Agent,
    /// Authenticated approval principal.
    Approver,
    /// Fenced harness execution owner.
    Harness,
}

/// Tool records share the same immutable segment storage as assistant chunks.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum ToolFile {
    /// Completed, validated tool invocation.
    Request,
    /// Ordered argument fragments, before request validation.
    Arguments(u64),
    /// Ordered standard output.
    Stdout(u64),
    /// Ordered standard error.
    Stderr(u64),
    /// Terminal tool outcome.
    Result,
}

/// Files below one task, with numeric sequence identities rather than path strings.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum TaskFile {
    /// Immutable submitted command, including cancellation and other intents.
    Request(Component),
    /// Current task status.
    Status,
    /// Immutable model turn input; turn numbers include retries/attempts.
    TurnRequest(u64),
    /// One immutable stream chunk.
    Chunk {
        /// Turn/attempt identity.
        turn: u64,
        /// Assigned monotonically increasing chunk identity.
        sequence: u64,
    },
    /// Read-only materialized text head, assembled from immutable chunk slices.
    TextHead {
        /// Turn/attempt identity.
        turn: u64,
        /// Assistant channel represented by this head.
        channel: Channel,
    },
    /// Terminal turn outcome.
    Completion(u64),
    /// Tool lifecycle file.
    Tool {
        /// Turn/attempt identity.
        turn: u64,
        /// Provider tool call identity.
        call: Component,
        /// Tool lifecycle location.
        file: ToolFile,
    },
    /// Immutable operation and argument binding to be approved.
    ApprovalRequest(Component),
    /// Authenticated decision about that immutable binding.
    ApprovalDecision {
        /// Immutable approval request identity.
        request: Component,
        /// Authenticated decision maker.
        principal: Component,
    },
    /// Notification with stable identity.
    Notification(Component),
    /// Pinned instruction and attachment provenance.
    Context(Component),
}

impl TaskFile {
    fn turn(&self) -> Option<u64> {
        match self {
            Self::TurnRequest(n) | Self::Completion(n) => Some(*n),
            Self::Chunk { turn, .. } | Self::TextHead { turn, .. } | Self::Tool { turn, .. } => {
                Some(*turn)
            }
            _ => None,
        }
    }

    fn path(&self) -> String {
        match self {
            Self::Request(id) => format!("requests/{}.json", id.as_str()),
            Self::Status => "status.json".into(),
            Self::TurnRequest(n) => format!("turns/{n}/request.json"),
            Self::Chunk { turn, sequence } => format!("turns/{turn}/chunks/{sequence}"),
            Self::TextHead { turn, channel } => format!(
                "turns/{turn}/{}",
                match channel {
                    Channel::Text => "text.md",
                    Channel::Reasoning => "reasoning.md",
                }
            ),
            Self::Completion(n) => format!("turns/{n}/completion.json"),
            Self::Tool { turn, call, file } => {
                let leaf = match file {
                    ToolFile::Request => "request.json".into(),
                    ToolFile::Arguments(n) => format!("arguments/{n}"),
                    ToolFile::Stdout(n) => format!("stdout/{n}"),
                    ToolFile::Stderr(n) => format!("stderr/{n}"),
                    ToolFile::Result => "result.json".into(),
                };
                format!("turns/{turn}/tools/{}/{leaf}", call.as_str())
            }
            Self::ApprovalRequest(id) => format!("approvals/{}/request.json", id.as_str()),
            Self::ApprovalDecision { request, principal } => format!(
                "approvals/{}/decisions/{}.json",
                request.as_str(),
                principal.as_str()
            ),
            Self::Notification(id) => format!("notifications/{}.json", id.as_str()),
            Self::Context(id) => format!("context/{}/manifest.json", id.as_str()),
        }
    }
}

/// Canonical logical input source path, fenced to commands/ or workspace/.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct InputPath(String);

impl TryFrom<String> for InputPath {
    type Error = Error;
    fn try_from(path: String) -> Result<Self> {
        let mut parts = path.split('/');
        if path.len() > 4096
            || !matches!(parts.next(), Some("commands" | "workspace" | "config"))
            || !path.contains('/')
            || parts.any(|p| {
                p.is_empty()
                    || p == "."
                    || p == ".."
                    || p.chars().any(|c| c.is_control() || matches!(c, '\\' | ':'))
            })
        {
            return Err(Error::InvalidPath);
        }
        Ok(Self(path))
    }
}
impl From<InputPath> for String {
    fn from(path: InputPath) -> Self {
        path.0
    }
}

/// Logical private-volume file identity; chunk keys retain no full path string.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum FileKey {
    /// Ordinary goal Markdown with active/blocked/done frontmatter.
    Goal,
    /// Editable command definitions and workspace files followed by UI queries.
    Input(InputPath),
    /// Ordinary editable Markdown in the private volume's plans folder.
    Plan(Component),
    /// Child fork mount declaration with lineage and capability references.
    Agent(Component),
    /// Protected task namespace.
    Task {
        /// Task identity in the private volume.
        task: Component,
        /// Typed task-relative file location.
        file: TaskFile,
    },
}

impl FileKey {
    pub(super) fn sequence_range(&self) -> Option<(Self, Self, u64)> {
        let Self::Task { task, file } = self else {
            return None;
        };
        let (first, last, sequence) = match file {
            TaskFile::Chunk { turn, sequence } => (
                TaskFile::Chunk {
                    turn: *turn,
                    sequence: 0,
                },
                TaskFile::Chunk {
                    turn: *turn,
                    sequence: u64::MAX,
                },
                *sequence,
            ),
            TaskFile::Tool { turn, call, file } => {
                let (first, last, sequence) = match file {
                    ToolFile::Arguments(n) => {
                        (ToolFile::Arguments(0), ToolFile::Arguments(u64::MAX), *n)
                    }
                    ToolFile::Stdout(n) => (ToolFile::Stdout(0), ToolFile::Stdout(u64::MAX), *n),
                    ToolFile::Stderr(n) => (ToolFile::Stderr(0), ToolFile::Stderr(u64::MAX), *n),
                    _ => return None,
                };
                (
                    TaskFile::Tool {
                        turn: *turn,
                        call: call.clone(),
                        file: first,
                    },
                    TaskFile::Tool {
                        turn: *turn,
                        call: call.clone(),
                        file: last,
                    },
                    sequence,
                )
            }
            _ => return None,
        };
        Some((
            Self::Task {
                task: task.clone(),
                file: first,
            },
            Self::Task {
                task: task.clone(),
                file: last,
            },
            sequence,
        ))
    }

    /// Parses a logical filesystem write/read location, rejecting aliases.
    pub fn parse(path: &str) -> Result<Self> {
        if path.len() > 4096 {
            return Err(Error::InvalidPath);
        }
        let parts: Vec<_> = path.split('/').collect();
        let id = |s: &str| Component::new(s);
        let json_id = |s: &str| Component::new(s.strip_suffix(".json").ok_or(Error::InvalidPath)?);
        let number = |s: &str| -> Result<u64> {
            let n = s.parse::<u64>().map_err(|_| Error::InvalidPath)?;
            if n.to_string() != s {
                return Err(Error::InvalidPath);
            }
            Ok(n)
        };
        match parts.as_slice() {
            ["commands" | "workspace" | "config", ..] => {
                return Ok(Self::Input(InputPath::try_from(path.to_owned())?));
            }
            ["GOAL.md"] => return Ok(Self::Goal),
            ["plans", name] => {
                return Ok(Self::Plan(id(name
                    .strip_suffix(".md")
                    .ok_or(Error::InvalidPath)?)?));
            }
            [".volumes", "agents", reference] => return Ok(Self::Agent(id(reference)?)),
            _ => {}
        }
        let [".tasks", task, rest @ ..] = parts.as_slice() else {
            return Err(Error::InvalidPath);
        };
        let file = match rest {
            ["requests", request] => TaskFile::Request(json_id(request)?),
            ["status.json"] => TaskFile::Status,
            ["turns", n, "request.json"] => TaskFile::TurnRequest(number(n)?),
            ["turns", n, "completion.json"] => TaskFile::Completion(number(n)?),
            ["turns", n, "text.md"] => TaskFile::TextHead {
                turn: number(n)?,
                channel: Channel::Text,
            },
            ["turns", n, "reasoning.md"] => TaskFile::TextHead {
                turn: number(n)?,
                channel: Channel::Reasoning,
            },
            ["turns", n, "chunks", seq] => TaskFile::Chunk {
                turn: number(n)?,
                sequence: number(seq)?,
            },
            ["turns", n, "tools", call, rest @ ..] => {
                let file = match rest {
                    ["request.json"] => ToolFile::Request,
                    ["result.json"] => ToolFile::Result,
                    ["arguments", seq] => ToolFile::Arguments(number(seq)?),
                    ["stdout", seq] => ToolFile::Stdout(number(seq)?),
                    ["stderr", seq] => ToolFile::Stderr(number(seq)?),
                    _ => return Err(Error::InvalidPath),
                };
                TaskFile::Tool {
                    turn: number(n)?,
                    call: id(call)?,
                    file,
                }
            }
            ["approvals", request, "request.json"] => TaskFile::ApprovalRequest(id(request)?),
            ["approvals", request, "decisions", principal] => TaskFile::ApprovalDecision {
                request: id(request)?,
                principal: json_id(principal)?,
            },
            ["notifications", notification] => TaskFile::Notification(json_id(notification)?),
            ["context", revision, "manifest.json"] => TaskFile::Context(id(revision)?),
            _ => return Err(Error::InvalidPath),
        };
        Ok(Self::Task {
            task: id(task)?,
            file,
        })
    }

    /// Materializes a path only at a filesystem or wire boundary.
    pub fn path(&self) -> String {
        match self {
            Self::Input(path) => path.0.clone(),
            Self::Goal => "GOAL.md".into(),
            Self::Plan(id) => format!("plans/{}.md", id.as_str()),
            Self::Agent(id) => format!(".volumes/agents/{}", id.as_str()),
            Self::Task { task, file } => format!(".tasks/{}/{}", task.as_str(), file.path()),
        }
    }

    /// Owner to pass to the existing volume authorization decision.
    pub fn owner(&self) -> Owner {
        match self {
            Self::Input(path) if path.0.starts_with("config/") => Owner::Caller,
            Self::Goal | Self::Plan(_) | Self::Input(_) => Owner::Agent,
            Self::Task {
                file: TaskFile::Request(_),
                ..
            } => Owner::Caller,
            Self::Task {
                file: TaskFile::ApprovalDecision { .. },
                ..
            } => Owner::Approver,
            _ => Owner::Harness,
        }
    }

    pub(super) fn mutable(&self) -> bool {
        matches!(
            self,
            Self::Goal
                | Self::Input(_)
                | Self::Plan(_)
                | Self::Task {
                    file: TaskFile::Status,
                    ..
                }
        )
    }
}

/// Folder selectors have bounded matching cost and allocate no per-chunk paths.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Query {
    /// Entire private task volume.
    Root,
    /// Command definitions and workspace files used by the input UI.
    Inputs,
    /// Declarative UI settings, keymap and theme files.
    Config,
    /// Editable Markdown files in the private volume's plans folder.
    Plans,
    /// All task records.
    Tasks,
    /// One task subtree.
    Task(Component),
    /// Immutable task requests.
    Requests(Component),
    /// All model turns and their tool subtrees.
    Turns(Component),
    /// One model turn, including its tools.
    Turn {
        /// Task identity.
        task: Component,
        /// Turn/attempt identity.
        turn: u64,
    },
    /// Assistant chunks in numeric sequence order in a snapshot.
    Chunks {
        /// Task identity.
        task: Component,
        /// Turn/attempt identity.
        turn: u64,
    },
    /// Tool invocations in one turn.
    Tools {
        /// Task identity.
        task: Component,
        /// Turn/attempt identity.
        turn: u64,
    },
    /// One invocation subtree.
    Tool {
        /// Task identity.
        task: Component,
        /// Turn/attempt identity.
        turn: u64,
        /// Provider tool call identity.
        call: Component,
    },
    /// Approval requests and decisions.
    Approvals(Component),
    /// Notifications for one task.
    Notifications(Component),
    /// Child mount declarations.
    Agents,
    /// One exact logical file, including goal, plan, status and mount declarations.
    File(FileKey),
}

impl Query {
    pub(super) fn ranges(&self) -> Vec<(std::ops::Bound<FileKey>, std::ops::Bound<FileKey>)> {
        use std::ops::Bound::{Included, Unbounded};
        let range = |task: &Component, first: TaskFile, last: TaskFile| {
            let bound = |file| {
                Included(FileKey::Task {
                    task: task.clone(),
                    file,
                })
            };
            (bound(first), bound(last))
        };
        let minimum = Component(Arc::from(""));
        let maximum = Component(Arc::from("\u{10ffff}"));
        let chunks = |task: &Component, turn: u64| {
            range(
                task,
                TaskFile::Chunk { turn, sequence: 0 },
                TaskFile::Chunk {
                    turn,
                    sequence: u64::MAX,
                },
            )
        };
        let tools = |task: &Component, turn: u64, first: &Component, last: &Component| {
            range(
                task,
                TaskFile::Tool {
                    turn,
                    call: first.clone(),
                    file: ToolFile::Request,
                },
                TaskFile::Tool {
                    turn,
                    call: last.clone(),
                    file: ToolFile::Result,
                },
            )
        };
        vec![match self {
            Self::Config => (
                Included(FileKey::Input(InputPath("config/".into()))),
                Included(FileKey::Input(InputPath("config/\u{10ffff}".into()))),
            ),
            Self::Inputs => (
                Included(FileKey::Input(InputPath(String::new()))),
                Included(FileKey::Input(InputPath("\u{10ffff}".into()))),
            ),
            Self::Plans => (
                Included(FileKey::Plan(minimum)),
                Included(FileKey::Plan(maximum)),
            ),
            Self::Agents => (
                Included(FileKey::Agent(minimum)),
                Included(FileKey::Agent(maximum)),
            ),
            Self::Turn { task, turn } => {
                return vec![
                    range(
                        task,
                        TaskFile::TurnRequest(*turn),
                        TaskFile::TurnRequest(*turn),
                    ),
                    chunks(task, *turn),
                    range(
                        task,
                        TaskFile::Completion(*turn),
                        TaskFile::Completion(*turn),
                    ),
                    tools(task, *turn, &minimum, &maximum),
                ];
            }
            Self::Chunks { task, turn }
            | Self::File(FileKey::Task {
                task,
                file: TaskFile::TextHead { turn, .. },
            }) => chunks(task, *turn),
            Self::Tools { task, turn } => tools(task, *turn, &minimum, &maximum),
            Self::Tool { task, turn, call } => tools(task, *turn, call, call),
            Self::Requests(task) => {
                range(task, TaskFile::Request(minimum), TaskFile::Request(maximum))
            }
            Self::Turns(task) => range(
                task,
                TaskFile::TurnRequest(0),
                TaskFile::Tool {
                    turn: u64::MAX,
                    call: maximum,
                    file: ToolFile::Result,
                },
            ),
            Self::Approvals(task) => range(
                task,
                TaskFile::ApprovalRequest(minimum),
                TaskFile::ApprovalDecision {
                    request: maximum.clone(),
                    principal: maximum,
                },
            ),
            Self::Notifications(task) => range(
                task,
                TaskFile::Notification(minimum),
                TaskFile::Notification(maximum),
            ),
            Self::Task(task) => range(task, TaskFile::Request(minimum), TaskFile::Context(maximum)),
            Self::File(key) => (Included(key.clone()), Included(key.clone())),
            _ => (Unbounded, Unbounded),
        }]
    }

    /// Matches typed keys without rendering paths or decoding payload bytes.
    pub fn matches(&self, key: &FileKey) -> bool {
        match (self, key) {
            (Self::Root, _)
            | (Self::Plans, FileKey::Plan(_))
            | (Self::Agents, FileKey::Agent(_))
            | (Self::Tasks, FileKey::Task { .. }) => true,
            (Self::Inputs, FileKey::Input(path)) => !path.0.starts_with("config/"),
            (Self::Config, FileKey::Input(path)) => matches!(
                path.0.as_str(),
                "config/settings.json" | "config/keymap.json" | "config/theme.json"
            ),
            (Self::Turns(t), FileKey::Task { task, file }) => t == task && file.turn().is_some(),
            (Self::Turn { task: t, turn }, FileKey::Task { task, file }) => {
                t == task && file.turn() == Some(*turn)
            }
            (
                Self::File(FileKey::Task {
                    task: t,
                    file: TaskFile::TextHead { turn, .. },
                }),
                FileKey::Task {
                    task,
                    file: TaskFile::Chunk { turn: n, .. },
                },
            )
            | (
                Self::Chunks { task: t, turn },
                FileKey::Task {
                    task,
                    file: TaskFile::Chunk { turn: n, .. },
                },
            )
            | (
                Self::Tools { task: t, turn },
                FileKey::Task {
                    task,
                    file: TaskFile::Tool { turn: n, .. },
                },
            ) => t == task && turn == n,
            (
                Self::Tool {
                    task: t,
                    turn,
                    call,
                },
                FileKey::Task {
                    task,
                    file:
                        TaskFile::Tool {
                            turn: n, call: c, ..
                        },
                },
            ) => t == task && turn == n && call == c,
            (Self::Task(t), FileKey::Task { task, .. })
            | (
                Self::Requests(t),
                FileKey::Task {
                    task,
                    file: TaskFile::Request(_),
                },
            )
            | (
                Self::Approvals(t),
                FileKey::Task {
                    task,
                    file: TaskFile::ApprovalRequest(_) | TaskFile::ApprovalDecision { .. },
                },
            )
            | (
                Self::Notifications(t),
                FileKey::Task {
                    task,
                    file: TaskFile::Notification(_),
                },
            ) => t == task,
            (Self::File(wanted), actual) => wanted == actual,
            _ => false,
        }
    }

    pub(super) fn matches_file(&self, file: &super::FileValue) -> bool {
        self.matches(&file.key)
            && match self {
                Self::File(FileKey::Task {
                    file: TaskFile::TextHead { channel, .. },
                    ..
                }) => file.channel == Some(*channel),
                _ => true,
            }
    }

    /// Resolves a canonical folder selection at the wire boundary.
    pub fn folder(path: &str) -> Result<Self> {
        if path.len() > 4096 {
            return Err(Error::InvalidPath);
        }
        let parts: Vec<_> = path.split('/').collect();
        let component = |s: &str| Component::new(s);
        let number = |s: &str| -> Result<u64> {
            let n = s.parse::<u64>().map_err(|_| Error::InvalidPath)?;
            if n.to_string() != s {
                return Err(Error::InvalidPath);
            }
            Ok(n)
        };
        Ok(match parts.as_slice() {
            [""] => Self::Root,
            ["plans"] => Self::Plans,
            ["config"] => Self::Config,
            [".volumes", "agents"] => Self::Agents,
            [".tasks"] => Self::Tasks,
            [".tasks", t] => Self::Task(component(t)?),
            [".tasks", t, "requests"] => Self::Requests(component(t)?),
            [".tasks", t, "turns"] => Self::Turns(component(t)?),
            [".tasks", t, "approvals"] => Self::Approvals(component(t)?),
            [".tasks", t, "notifications"] => Self::Notifications(component(t)?),
            [".tasks", t, "turns", n] => Self::Turn {
                task: component(t)?,
                turn: number(n)?,
            },
            [".tasks", t, "turns", n, "chunks"] => Self::Chunks {
                task: component(t)?,
                turn: number(n)?,
            },
            [".tasks", t, "turns", n, "tools"] => Self::Tools {
                task: component(t)?,
                turn: number(n)?,
            },
            [".tasks", t, "turns", n, "tools", c] => Self::Tool {
                task: component(t)?,
                turn: number(n)?,
                call: component(c)?,
            },
            _ => Self::File(FileKey::parse(path)?),
        })
    }
}

/// Assistant channel; tools derive their channel from their logical key.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum Channel {
    /// Visible assistant Markdown.
    Text,
    /// Provider reasoning content.
    Reasoning,
}

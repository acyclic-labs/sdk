use super::{ChildFile, Error, PlanName, Result};
use serde::{Deserialize, Serialize};

/// Task namespace and packed-segment format revision.
pub const SCHEMA: u32 = 4;

#[path = "component.rs"]
mod component;
pub use component::Component;

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
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
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
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub enum TaskFile {
    /// Immutable submitted command, including cancellation and other intents.
    Request(Component),
    /// Host-owned dispatch claim for an immutable request.
    RequestStarted(Component),
    /// Host-owned completed response retained before terminal publication.
    RequestReady(Component),
    /// Host-owned terminal outcome of an immutable request.
    RequestResult(Component),
    /// Current task status.
    Status,
    /// Immutable dispatch marker for one submitted command request.
    ExecutionClaim(Component),
    /// Immutable outcome of that dispatch (absence after a claim is uncertain).
    ExecutionResult(Component),
    /// Immutable selection made by one child wait operation.
    ChildWait(Component),
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
    pub(super) fn turn(&self) -> Option<u64> {
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
            Self::RequestStarted(id) => format!("requests/{}/started.json", id.as_str()),
            Self::RequestReady(id) => format!("requests/{}/ready.json", id.as_str()),
            Self::RequestResult(id) => format!("requests/{}/result.json", id.as_str()),
            Self::Status => "status.json".into(),
            Self::ExecutionClaim(id) => format!("executions/{}/claim.json", id.as_str()),
            Self::ExecutionResult(id) => format!("executions/{}/result.json", id.as_str()),
            Self::ChildWait(id) => format!("child-waits/{}.json", id.as_str()),
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

#[path = "input_path.rs"]
mod input_path;
pub use input_path::InputPath;

/// Logical private-volume file identity; chunk keys retain no full path string.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum FileKey {
    /// UI-session requests and their immutable execution files.
    Session {
        /// Client-selected session identity, distinct from a task identity.
        session: Component,
        /// Request, claim or result file in this session.
        file: TaskFile,
    },
    /// Ordinary user/config/session files, authorized by their containing volume.
    Ui {
        /// Caller-bound config, session or task settings directory.
        directory: Component,
        /// Bounded canonical file name, such as commands.json or layout.json.
        name: Component,
    },
    /// Existing host request head, selected from its authorized journal volume.
    RequestHead,
    /// Ordinary goal Markdown with active/blocked/done frontmatter.
    Goal,
    /// Editable command definitions and workspace files followed by UI queries.
    Input(InputPath),
    /// Ordinary editable Markdown in the private volume's plans folder.
    Plan(PlanName),
    /// Child fork mount declaration with lineage and capability references.
    Agent(Component),
    /// Parent-owned child request, message or lifecycle intent.
    Child {
        /// Canonical reference, scoped to this parent volume.
        reference: Component,
        /// File below the child directory.
        file: ChildFile,
    },
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
            ["inputs", "current.json"] => return Ok(Self::RequestHead),
            [".ui", directory, name] => {
                return Ok(Self::Ui {
                    directory: id(directory)?,
                    name: id(name)?,
                });
            }
            [
                "commands" | "workspace" | "config" | ".claude" | ".codex" | "skills",
                ..,
            ] => {
                return Ok(Self::Input(InputPath::try_from(path.to_owned())?));
            }
            ["GOAL.md"] => return Ok(Self::Goal),
            ["agents", reference, rest @ ..] => {
                return Ok(Self::Child {
                    reference: id(reference)?,
                    file: ChildFile::parse(rest)?,
                });
            }
            ["plans", name] => {
                return Ok(Self::Plan(PlanName::new(
                    name.strip_suffix(".md").ok_or(Error::InvalidPath)?,
                )?));
            }
            [".volumes", "agents", reference] => return Ok(Self::Agent(id(reference)?)),
            _ => {}
        }
        let (session, task, rest) = match parts.as_slice() {
            [".tasks", task, rest @ ..] => (false, task, rest),
            [".sessions", session, rest @ ..] => (true, session, rest),
            _ => return Err(Error::InvalidPath),
        };
        let file = match rest {
            ["requests", request] => TaskFile::Request(json_id(request)?),
            ["requests", request, "started.json"] => TaskFile::RequestStarted(id(request)?),
            ["requests", request, "ready.json"] => TaskFile::RequestReady(id(request)?),
            ["requests", request, "result.json"] => TaskFile::RequestResult(id(request)?),
            ["status.json"] => TaskFile::Status,
            ["executions", request, "claim.json"] => TaskFile::ExecutionClaim(id(request)?),
            ["executions", request, "result.json"] => TaskFile::ExecutionResult(id(request)?),
            ["child-waits", name] => {
                TaskFile::ChildWait(id(name.strip_suffix(".json").ok_or(Error::InvalidPath)?)?)
            }
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
        if session {
            if !matches!(
                file,
                TaskFile::Request(_) | TaskFile::ExecutionClaim(_) | TaskFile::ExecutionResult(_)
            ) {
                return Err(Error::InvalidPath);
            }
            return Ok(Self::Session {
                session: id(task)?,
                file,
            });
        }
        Ok(Self::Task {
            task: id(task)?,
            file,
        })
    }

    /// Materializes a path only at a filesystem or wire boundary.
    pub fn path(&self) -> String {
        match self {
            Self::Session { session, file } => {
                format!(".sessions/{}/{}", session.as_str(), file.path())
            }
            Self::Ui { directory, name } => format!(".ui/{}/{}", directory.as_str(), name.as_str()),
            Self::RequestHead => "inputs/current.json".into(),
            Self::Input(path) => path.0.clone(),
            Self::Goal => "GOAL.md".into(),
            Self::Child { reference, file } => {
                format!("agents/{}/{}", reference.as_str(), file.path())
            }
            Self::Plan(id) => format!("plans/{}.md", id.as_str()),
            Self::Agent(id) => format!(".volumes/agents/{}", id.as_str()),
            Self::Task { task, file } => format!(".tasks/{}/{}", task.as_str(), file.path()),
        }
    }

    /// Owner to pass to the existing volume authorization decision.
    pub fn owner(&self) -> Owner {
        match self {
            Self::Input(path) if path.0.starts_with("config/") => Owner::Caller,
            Self::Ui { .. } => Owner::Caller,
            Self::Session {
                file: TaskFile::Request(_),
                ..
            } => Owner::Caller,
            Self::Child {
                file: ChildFile::Status | ChildFile::Delivered | ChildFile::Result(_),
                ..
            } => Owner::Harness,
            Self::Goal | Self::Plan(_) | Self::Input(_) | Self::Child { .. } => Owner::Agent,
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

    pub(crate) fn mutable(&self) -> bool {
        matches!(
            self,
            Self::Ui { .. }
                | Self::RequestHead
                | Self::Goal
                | Self::Child {
                    file: ChildFile::Control,
                    ..
                }
                | Self::Child {
                    file: ChildFile::Status | ChildFile::Delivered,
                    ..
                }
                | Self::Input(_)
                | Self::Plan(_)
                | Self::Task {
                    file: TaskFile::Status,
                    ..
                }
        )
    }
}

#[path = "query.rs"]
mod query;
pub use query::Query;

/// Assistant channel; tools derive their channel from their logical key.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum Channel {
    /// Visible assistant Markdown.
    Text,
    /// Provider reasoning content.
    Reasoning,
}

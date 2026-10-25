use super::{
    ChildFile, Component, Error, FileKey, InputPath, PlanName, Result, TaskFile, ToolFile,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Folder selectors have bounded matching cost and allocate no per-chunk paths.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub enum Query {
    /// Active command inbox plus a bounded recent outcome window. Historical
    /// request/claim/result files remain addressable by exact file queries.
    CommandSession {
        /// UI session whose settings, inbox and outcomes are selected.
        session: Component,
        /// Maximum number of completed outcomes retained in the active view.
        recent: u32,
    },
    /// One ordinary UI/config directory.
    UiDirectory(Component),
    /// One UI session's requests and immutable execution records.
    Session(Component),
    /// One admitted task and its caller session, without unrelated history.
    TaskSession {
        /// Admitted task identity.
        task: Component,
        /// Caller session identity.
        session: Component,
    },
    /// One request/execution folder in a UI session.
    SessionFiles {
        /// UI session identity.
        session: Component,
        /// Select execution files; otherwise select inbox request files.
        executions: bool,
    },
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
    /// Recent display files, excluding request and dispatch-claim archives.
    TaskView {
        /// Selected task.
        task: Component,
        /// Recent files retained from each visible content class.
        recent: u32,
        /// Include provider reasoning.
        reasoning: bool,
        /// Include tool details.
        details: bool,
        /// Include terminal output.
        terminal: bool,
    },
    /// Task lifecycle records, excluding task archives.
    TaskOverviews,
    /// Immutable task requests.
    Requests(Component),
    /// Immutable dispatch claims and outcomes for submitted requests.
    Executions(Component),
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
    /// Open requests only; settled requests leave the derived index.
    PendingApprovals(Component),
    /// One exact approval request and its decisions, independent of task history.
    Approval {
        /// Original task identity.
        task: Component,
        /// Exact interaction identity.
        request: Component,
    },
    /// Notifications for one task.
    Notifications(Component),
    /// Active child execution acknowledgements; archive history is retained.
    ActiveAgents,
    /// Child mount declarations.
    Agents,
    /// All child requests, inbox files and lifecycle intents.
    Children,
    /// One child subtree.
    Child(Component),
    /// One exact logical file, including goal, plan, status and mount declarations.
    File(FileKey),
}

impl Query {
    #[doc(hidden)]
    pub fn ranges(&self) -> Vec<(std::ops::Bound<FileKey>, std::ops::Bound<FileKey>)> {
        let minimum = Component(Arc::from(""));
        let maximum = Component(Arc::from("\u{10ffff}"));
        if let Self::Approval { task, request } = self {
            return vec![
                range(
                    task,
                    TaskFile::ApprovalRequest(request.clone()),
                    TaskFile::ApprovalRequest(request.clone()),
                ),
                range(
                    task,
                    TaskFile::ApprovalDecision {
                        request: request.clone(),
                        principal: minimum,
                    },
                    TaskFile::ApprovalDecision {
                        request: request.clone(),
                        principal: maximum,
                    },
                ),
            ];
        }
        if let Self::TaskSession { task, session } = self {
            let mut ranges = Self::Session(session.clone()).ranges();
            ranges.push(range(
                task,
                TaskFile::Request(minimum),
                TaskFile::Context(maximum),
            ));
            return ranges;
        }
        if let Some(range) = self.root_range(&minimum, &maximum) {
            return vec![range];
        }
        if let Self::Turn { task, turn } = self {
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
        vec![self.task_range(minimum, maximum)]
    }

    fn task_range(&self, minimum: Component, maximum: Component) -> FileRange {
        use std::ops::Bound::{Included, Unbounded};
        match self {
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
            Self::Executions(task) => range(
                task,
                TaskFile::ExecutionClaim(minimum),
                TaskFile::ExecutionResult(maximum),
            ),
            Self::Turns(task) => range(
                task,
                TaskFile::TurnRequest(0),
                TaskFile::Tool {
                    turn: u64::MAX,
                    call: maximum,
                    file: ToolFile::Result,
                },
            ),
            Self::Approvals(task) | Self::PendingApprovals(task) => range(
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
            Self::Task(task) | Self::TaskView { task, .. } => {
                range(task, TaskFile::Request(minimum), TaskFile::Context(maximum))
            }
            Self::File(key) => (Included(key.clone()), Included(key.clone())),
            _ => (Unbounded, Unbounded),
        }
    }

    fn root_range(&self, minimum: &Component, maximum: &Component) -> Option<FileRange> {
        use std::ops::Bound::{Excluded, Included};
        Some(match self {
            Self::UiDirectory(directory) => (
                Included(FileKey::Ui {
                    directory: directory.clone(),
                    name: minimum.clone(),
                }),
                Included(FileKey::Ui {
                    directory: directory.clone(),
                    name: maximum.clone(),
                }),
            ),
            Self::SessionFiles {
                session,
                executions,
            } => {
                let (first, last) = if *executions {
                    (
                        TaskFile::ExecutionClaim(minimum.clone()),
                        TaskFile::ExecutionResult(maximum.clone()),
                    )
                } else {
                    (
                        TaskFile::Request(minimum.clone()),
                        TaskFile::Request(maximum.clone()),
                    )
                };
                (
                    Included(FileKey::Session {
                        session: session.clone(),
                        file: first,
                    }),
                    Included(FileKey::Session {
                        session: session.clone(),
                        file: last,
                    }),
                )
            }
            Self::Session(session) => (
                Included(FileKey::Session {
                    session: session.clone(),
                    file: TaskFile::Request(minimum.clone()),
                }),
                Included(FileKey::Session {
                    session: session.clone(),
                    file: TaskFile::ExecutionResult(maximum.clone()),
                }),
            ),
            Self::Config => (
                Included(FileKey::Input(InputPath("config/".into()))),
                Included(FileKey::Input(InputPath("config/\u{10ffff}".into()))),
            ),
            Self::Inputs => (
                Included(FileKey::Input(InputPath(String::new()))),
                Included(FileKey::Input(InputPath("\u{10ffff}".into()))),
            ),
            Self::Plans => (
                Included(FileKey::Plan(PlanName(Arc::from("")))),
                Excluded(FileKey::Agent(minimum.clone())),
            ),
            Self::Children => (
                Included(FileKey::Child {
                    reference: minimum.clone(),
                    file: ChildFile::Request,
                }),
                Included(FileKey::Child {
                    reference: maximum.clone(),
                    file: ChildFile::Result(u64::MAX),
                }),
            ),
            Self::Child(reference) => (
                Included(FileKey::Child {
                    reference: reference.clone(),
                    file: ChildFile::Request,
                }),
                Included(FileKey::Child {
                    reference: reference.clone(),
                    file: ChildFile::Result(u64::MAX),
                }),
            ),
            Self::ActiveAgents => (
                Included(FileKey::Child {
                    reference: minimum.clone(),
                    file: super::super::ChildFile::Request,
                }),
                Included(FileKey::Child {
                    reference: maximum.clone(),
                    file: super::super::ChildFile::Result(u64::MAX),
                }),
            ),
            Self::Agents => (
                Included(FileKey::Agent(minimum.clone())),
                Included(FileKey::Agent(maximum.clone())),
            ),
            _ => return None,
        })
    }

    /// Matches typed keys without rendering paths or decoding payload bytes.
    pub fn matches(&self, key: &FileKey) -> bool {
        match self {
            Self::Root => true,
            Self::TaskSession {
                task: selected_task,
                session: selected_session,
            } => match key {
                FileKey::Task { task, .. } => {
                    selected_task == task && super::super::recent::kind(key).is_some()
                }
                FileKey::Session { session, .. } => selected_session == session,
                _ => false,
            },
            Self::UiDirectory(selected) => {
                matches!(key, FileKey::Ui { directory, .. } if selected == directory)
            }
            Self::CommandSession { session, .. } => {
                super::super::command_query::matches(session, key)
            }
            Self::SessionFiles {
                session: selected,
                executions,
            } => {
                matches!(key, FileKey::Session { session, file } if selected == session && if *executions {
                matches!(file, TaskFile::ExecutionClaim(_) | TaskFile::ExecutionResult(_))
            } else { matches!(file, TaskFile::Request(_)) })
            }
            Self::Session(selected) => {
                matches!(key, FileKey::Session { session, .. } if selected == session)
            }
            Self::ActiveAgents => matches!(
                key,
                FileKey::Child {
                    file: super::super::ChildFile::Status,
                    ..
                }
            ),
            Self::Plans => matches!(key, FileKey::Plan(_)),
            Self::Agents => matches!(key, FileKey::Agent(_)),
            Self::Tasks => matches!(key, FileKey::Task { .. }),
            Self::Children => matches!(key, FileKey::Child { .. }),
            Self::Child(wanted) => {
                matches!(key, FileKey::Child { reference, .. } if wanted == reference)
            }
            Self::Inputs => matches!(key, FileKey::Input(path) if !path.0.starts_with("config/")),
            Self::Config => {
                matches!(key, FileKey::Input(path) if matches!(path.0.as_str(), "config/settings.json" | "config/keymap.json" | "config/theme.json"))
            }
            Self::TaskOverviews => matches!(
                key,
                FileKey::Task {
                    file: TaskFile::Status,
                    ..
                }
            ),
            Self::File(FileKey::Task {
                task: selected,
                file:
                    TaskFile::TextHead {
                        turn: selected_turn,
                        ..
                    },
            }) => {
                matches!(key, FileKey::Task { task, file: TaskFile::Chunk { turn, .. } } if selected == task && selected_turn == turn)
            }
            Self::File(wanted) => wanted == key,
            _ => self.matches_task(key),
        }
    }

    fn matches_task(&self, key: &FileKey) -> bool {
        let FileKey::Task { task, file } = key else {
            return false;
        };
        match self {
            Self::Task(selected) => selected == task,
            Self::TaskView {
                task: selected,
                reasoning,
                details,
                terminal,
                ..
            } => {
                selected == task
                    && super::super::recent::kind(key).is_some_and(|kind| match kind {
                        3 => *reasoning,
                        4 => *details,
                        5 => *terminal,
                        6 => *details || *terminal,
                        _ => true,
                    })
            }
            Self::Turns(selected) => selected == task && file.turn().is_some(),
            Self::Turn {
                task: selected,
                turn,
            } => selected == task && file.turn() == Some(*turn),
            Self::Chunks {
                task: selected,
                turn: selected_turn,
            } => {
                matches!(file, TaskFile::Chunk { turn, .. } if selected == task && selected_turn == turn)
            }
            Self::Tools {
                task: selected,
                turn: selected_turn,
            } => {
                matches!(file, TaskFile::Tool { turn, .. } if selected == task && selected_turn == turn)
            }
            Self::Tool {
                task: selected,
                turn: selected_turn,
                call: selected_call,
            } => {
                matches!(file, TaskFile::Tool { turn, call, .. } if selected == task && selected_turn == turn && selected_call == call)
            }
            Self::Approval {
                task: selected,
                request: selected_request,
            } => {
                selected == task
                    && match file {
                        TaskFile::ApprovalRequest(request)
                        | TaskFile::ApprovalDecision { request, .. } => selected_request == request,
                        _ => false,
                    }
            }
            Self::Executions(selected) => {
                selected == task
                    && matches!(
                        file,
                        TaskFile::ExecutionClaim(_) | TaskFile::ExecutionResult(_)
                    )
            }
            Self::Requests(selected) => selected == task && matches!(file, TaskFile::Request(_)),
            Self::Approvals(selected) | Self::PendingApprovals(selected) => {
                selected == task
                    && matches!(
                        file,
                        TaskFile::ApprovalRequest(_) | TaskFile::ApprovalDecision { .. }
                    )
            }
            Self::Notifications(selected) => {
                selected == task && matches!(file, TaskFile::Notification(_))
            }
            _ => false,
        }
    }
    pub(crate) fn matches_file(&self, file: &super::super::FileValue) -> bool {
        self.matches(&file.key)
            && match self {
                Self::TaskView { reasoning, .. } => {
                    *reasoning || file.channel != Some(super::Channel::Reasoning)
                }
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
            ["agents"] => Self::Children,
            ["agents", reference] => Self::Child(component(reference)?),
            [".ui", directory] => Self::UiDirectory(component(directory)?),
            [""] => Self::Root,
            ["plans"] => Self::Plans,
            ["config"] => Self::Config,
            [".sessions", session] => Self::Session(component(session)?),
            [".sessions", session, folder] if matches!(*folder, "requests" | "executions") => {
                Self::SessionFiles {
                    session: component(session)?,
                    executions: *folder == "executions",
                }
            }
            [".volumes", "agents"] => Self::Agents,
            [".tasks"] => Self::Tasks,
            [".tasks", t] => Self::Task(component(t)?),
            [".tasks", t, "requests"] => Self::Requests(component(t)?),
            [".tasks", t, "executions"] => Self::Executions(component(t)?),
            [".tasks", t, "turns"] => Self::Turns(component(t)?),
            [".tasks", t, "approvals"] => Self::Approvals(component(t)?),
            [".tasks", t, "approvals", "open"] => Self::PendingApprovals(component(t)?),
            [".tasks", t, "approvals", r] => Self::Approval {
                task: component(t)?,
                request: component(r)?,
            },
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

#[cfg(test)]
#[path = "../../../harness/tests/task_volume/ranges.rs"]
mod tests;

impl Query {
    pub(crate) fn snapshot_matches_file(&self, file: &super::super::FileValue) -> bool {
        self.matches_file(file)
            && (*self != Self::ActiveAgents
                || super::super::ChildStatus::parse(&file.bytes)
                    .is_ok_and(|status| status.active()))
    }
}

type FileRange = (std::ops::Bound<FileKey>, std::ops::Bound<FileKey>);

fn range(task: &Component, first: TaskFile, last: TaskFile) -> FileRange {
    let bound = |file| {
        std::ops::Bound::Included(FileKey::Task {
            task: task.clone(),
            file,
        })
    };
    (bound(first), bound(last))
}
fn chunks(task: &Component, turn: u64) -> FileRange {
    range(
        task,
        TaskFile::Chunk { turn, sequence: 0 },
        TaskFile::Chunk {
            turn,
            sequence: u64::MAX,
        },
    )
}
fn tools(task: &Component, turn: u64, first: &Component, last: &Component) -> FileRange {
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
}

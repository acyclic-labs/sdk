use serde::{Deserialize, Serialize};

/// Native coding host whose lifecycle is being attached.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExternalHost {
    /// OpenAI Codex.
    Codex,
    /// Anthropic Claude Code.
    ClaudeCode,
    /// Pi's extension API.
    Pi,
}

impl ExternalHost {
    /// Host identity used in request IDs and hook manifests.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::ClaudeCode => "claude-code",
            Self::Pi => "pi",
        }
    }
}

/// One native hook and its shared request action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HookSpec {
    /// Native event emitted by this host.
    pub event: &'static str,
    /// Task request selected by the hook.
    pub action: HookAction,
}

impl HookSpec {
    /// Native hook deadline in seconds, including the terminal-hook budget.
    #[must_use]
    pub const fn timeout_seconds(self) -> u64 {
        if matches!(self.action, HookAction::Detach) {
            3
        } else {
            120
        }
    }
}

/// Finite request vocabulary shared by all host manifests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HookAction {
    /// Attach the native session to a task.
    Attach,
    /// Run a tool through the harness.
    RunTool,
    /// Request cancellation of one native tool invocation.
    CancelTool,
    /// Record the native tool completion.
    ToolFinished,
    /// Fork a child task.
    Fork,
    /// Join a child task.
    Join,
    /// Detach the native session.
    Detach,
}

const NATIVE_HOOKS: &[HookSpec] = &[
    HookSpec {
        event: "SessionStart",
        action: HookAction::Attach,
    },
    HookSpec {
        event: "PreToolUse",
        action: HookAction::RunTool,
    },
    HookSpec {
        event: "PostToolUse",
        action: HookAction::ToolFinished,
    },
    HookSpec {
        event: "SubagentStart",
        action: HookAction::Fork,
    },
    HookSpec {
        event: "SubagentStop",
        action: HookAction::Join,
    },
    HookSpec {
        event: "SessionEnd",
        action: HookAction::Detach,
    },
];
const CLAUDE_HOOKS: &[HookSpec] = &[HookSpec {
    event: "PostToolUseFailure",
    action: HookAction::ToolFinished,
}];
const PI_HOOKS: &[HookSpec] = &[
    HookSpec {
        event: "session_start",
        action: HookAction::Attach,
    },
    HookSpec {
        event: "tool_call",
        action: HookAction::RunTool,
    },
    HookSpec {
        event: "tool_cancel",
        action: HookAction::CancelTool,
    },
    HookSpec {
        event: "tool_result",
        action: HookAction::ToolFinished,
    },
    HookSpec {
        event: "subagent_start",
        action: HookAction::Fork,
    },
    HookSpec {
        event: "subagent_stop",
        action: HookAction::Join,
    },
    HookSpec {
        event: "session_shutdown",
        action: HookAction::Detach,
    },
];

/// Catalog consumed by native hook adapters and manifest generation.
pub fn hooks(host: ExternalHost) -> impl Iterator<Item = &'static HookSpec> {
    let (common, extra) = match host {
        ExternalHost::Codex => (NATIVE_HOOKS, &[][..]),
        ExternalHost::ClaudeCode => (NATIVE_HOOKS, CLAUDE_HOOKS),
        ExternalHost::Pi => (PI_HOOKS, &[][..]),
    };
    common.iter().chain(extra)
}

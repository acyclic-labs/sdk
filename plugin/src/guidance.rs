//! Host-specific agent guidance.

pub(crate) const AGENT_GUIDANCE: &str = "Acyclic gives native subagents independent, recursively forked workspace contexts. Supported filesystem tools are routed to the child's mounted workspace; never target a parent's original path. Use `acyclic git` for history inside managed workspaces; bare `git` is unrelated. Run each `acyclic ...` command as its own shell invocation, without shell operators or unrelated commands. Children appear under `agents/...`: inspect with `acyclic agents` and `acyclic git diff <ref>`, merge a direct child with `acyclic git merge <ref>`, and remove an unwanted subtree with `acyclic discard <ref>`. Start independent or dependent work speculatively as soon as you can state its assumptions; reconcile, merge, or restart when upstream changes. For debugging, freely add logs, probes, and tests in a child, then normally discard it after confirming the issue. For exploration, run hypotheses in parallel and merge only useful results. Descendants publish upward one parent at a time.";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChildWorkspaceSupport {
    RecursiveToolRouting,
    RootLifecycleOnly,
    CliOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HostAdapterProfile {
    pub(crate) child_workspaces: ChildWorkspaceSupport,
    pub(crate) stable_child_identity: bool,
}

pub(crate) fn host_adapter_profile(host: &str) -> HostAdapterProfile {
    match host {
        "codex" | "claude-code" => HostAdapterProfile {
            child_workspaces: ChildWorkspaceSupport::RecursiveToolRouting,
            stable_child_identity: true,
        },
        "copilot" => HostAdapterProfile {
            child_workspaces: ChildWorkspaceSupport::RootLifecycleOnly,
            stable_child_identity: false,
        },
        _ => HostAdapterProfile {
            child_workspaces: ChildWorkspaceSupport::CliOnly,
            stable_child_identity: false,
        },
    }
}

pub(crate) fn capability_guidance(host: &str) -> &'static str {
    let profile = host_adapter_profile(host);
    match (profile.child_workspaces, profile.stable_child_identity) {
        (ChildWorkspaceSupport::RecursiveToolRouting, true) => {
            "Recursive lifecycle routing is active. This claim covers recognized tool routing, not process-level confinement; full confinement requires a passing host/platform escape qualification."
        }
        (ChildWorkspaceSupport::RootLifecycleOnly, false) => {
            "Only root lifecycle routing is available because this host does not provide a stable child identity. Native child workspace routing is not claimed."
        }
        (ChildWorkspaceSupport::CliOnly, false) => {
            "Child work is CLI-only because this host does not expose a correlated native-subagent lifecycle. Native child workspace routing or confinement is not claimed."
        }
        _ => "This adapter has an invalid capability profile and cannot route child work.",
    }
}

pub(crate) fn guidance_for(host: &str) -> String {
    if host_adapter_profile(host).child_workspaces == ChildWorkspaceSupport::RecursiveToolRouting {
        format!("{AGENT_GUIDANCE} {}", capability_guidance(host))
    } else {
        format!(
            "Acyclic provides a Git-shaped local history CLI without intercepting bare `git`. Use `acyclic git` from the current workspace. {}",
            capability_guidance(host)
        )
    }
}

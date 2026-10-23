# Native host workspace coordination

Claude Code, Codex and Pi own their agents, models, context, tools and
permissions. Acyclic hooks select and capture shared harness workspaces.
GraphCoder owns the shared task tree, filesystem history and merge operation.
The plugin contains no agent executor or background service.

Install the package and start GraphCoder **outside** the host sandbox, from
the checkout the native host will use:

```sh
npm install -g @acyclic-labs/plugin
graphcoder --host-workspace --host-state-directory /path/to/explicit/state
acyclic install codex
```

Supported installer targets are `codex`, `claude-code` and `pi`. Codex must be
at least **0.164.0**; installation and `doctor codex` refuse older or unknown
versions. Pi targets `@earendil-works/pi-coding-agent` **1.1.0**. Claude hooks
require native `WorktreeCreate` support. Use `--plugin-root PATH` to select
package assets and `--settings PATH` to select a Claude settings file or Pi
extension file. Native host configuration directories remain host-owned.

The installed hooks declare their access through these workspace paths:

| Path | Use |
| --- | --- |
| `.acyclic-sdk/host-files/owner.json` | Read the existing GraphCoder endpoint |
| `.acyclic-sdk/host-files/requests/` | Publish workspace requests |
| `.acyclic-sdk/host-files/results/` | Read and retire disposable results |
| `.volumes/agents/` | Native child working views |

These paths are inside the host's existing writable workspace. Installation
adds no writable sandbox roots or state-directory permissions. Keep Codex's
workspace-write sandbox and Claude's native permissions enabled. A CLI call
inside the sandbox never opens harness state, starts GraphCoder or creates
service identity or lifecycle lock files. Start GraphCoder first; an absent
endpoint produces an error. Its durable request/claim/result files remain in
the harness even when disposable workspace projections are removed.

```sh
acyclic agents
acyclic fs log
acyclic --task TASK_ID fs merge agents/CHILD_REF
acyclic --task TASK_ID capture
acyclic cancel TASK_ID
acyclic reopen TASK_ID
```

Native hosts stop their own agents and tools before cancellation or capture.
Pending native writes block capture and merge. Reopening retains task history;
it does not start a model or an agent. Pi's extension delegates subagent
sessions to Pi's own SDK and uses its session manager for context forks.
Claude's worktree hook returns the shared child view as the native cwd.
Codex child filesystem inputs are routed through lifecycle hooks; native
session-cwd selection and sandbox acceptance require real-host qualification.

The separate [identity package](identity-mcp.md) retains its existing command
surface. Workspace coordination does not require or inspect host credentials.

Source changes and manifest discovery alone do not establish cross-platform
qualification. The plugin e2e lane owns sandbox-enabled Linux, macOS and
Windows host acceptance, with results in `Q:/lanes/plugin-e2e/`.

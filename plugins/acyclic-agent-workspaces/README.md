# Acyclic Agent Workspaces

This Codex plugin gives each native subagent a recursive, isolated Acyclic SDK
workspace without changing Codex's normal `spawn_agent` interface. Filesystem
tools are redirected through a durable operation window, and only three
publication decisions are model-visible:

- `agent_changes(agent, path?)`
- `agent_merge(agent)`
- `agent_discard(agent)`

The MCP process owns live native mounts. `acyclic-fs` implements lineage,
direct-parent authorization, barriers, rebasing, merge publication,
compatibility history, and recovery. The plugin persists only the short Codex
spawn/turn-to-workspace association required by its lifecycle hooks.

## Platform requirements

Use the plugin artifact built for the exact operating-system and CPU target.
The packaged artifact has no Rust, Cargo, Node.js, or SDK checkout dependency.
Windows MSVC artifacts statically link the C runtime, so they do not require a
separate Visual C++ Redistributable installation.

- Linux requires a usable `/dev/fuse`; `fusermount3` or `fusermount` is also
  required for crash recovery.
- macOS requires the built-in `/sbin/mount_nfs` and `/sbin/umount` utilities.
- Windows requires Projected File System (`ProjectedFSLib.dll`). Enable the
  optional **Windows Projected File System** feature if it is absent.
- Other operating systems are not supported by the native mount backend.

The checked-in source configuration is a repository-development fallback. It
requires this SDK checkout, Rust 1.94 or later, Cargo, and the platform
facilities above. The control server deliberately builds against the current
workspace `acyclic-fs` source so its compatibility semantics cannot drift from
the `acyclic` CLI being integrated. Its first startup can take several minutes.
Use a packaged artifact when testing the child-visible `acyclic` CLI.

## Build a production artifact

From the repository root, build the current host target:

```text
python plugins/acyclic-agent-workspaces/scripts/package.py
```

The script uses the committed control lockfile, builds a release executable,
builds the workspace's `acyclic-cli` package and `acyclic` binary target, and
writes a staged local marketplace at
`plugins/acyclic-agent-workspaces/dist/marketplace`, plus a deterministic ZIP
and its SHA-256 file under `plugins/acyclic-agent-workspaces/dist/`. The Git-façade workstream owns
that CLI target; packaging intentionally fails if it is absent. For integration
testing, `--acyclic-binary <path>` can supply an already-built target. Pass
`--target <rust-triple>` after installing the corresponding Rust target to
cross-compile. Native mount dependencies can make native builds on each target
simpler than cross builds. The override does not bypass artifact validation.

Validate the staged directory in an empty, artifact-only temporary directory:

```text
python plugins/acyclic-agent-workspaces/scripts/validate-package.py \
  plugins/acyclic-agent-workspaces/dist/marketplace
```

The validator copies only the staged artifact, resolves the plugin through its
local marketplace metadata, rejects Cargo or repository paths in its runtime
configuration, and performs MCP `initialize`, `tools/list`, and root
`SessionStart` calls against the packaged executable. It also requires
`bin/acyclic` (or `bin/acyclic.exe`), verifies its help exposes the `git`
namespace, and rejects any packaged `git` executable.

## Install from the local marketplace

1. For normal use, extract the target-specific ZIP. Its
   `.agents/plugins/marketplace.json` points to the self-contained plugin under
   `plugins/acyclic-agent-workspaces`.
2. Register the extracted, absolute marketplace root and install the plugin:

   ```text
   codex plugin marketplace add <extracted-marketplace-root>
   codex plugin add acyclic-agent-workspaces@acyclic-sdk
   ```

3. In Codex plugin settings, confirm that **Acyclic Agent Workspaces** is
   enabled. `codex plugin list` shows the configured marketplace and plugin.
4. Review the displayed hook definitions before trusting them. The hooks run on
   session, prompt, tool, and subagent lifecycle events and can affect every
   filesystem-capable tool call in that session.
5. Start a new Codex task in a disposable Git checkout. The MCP server starts
   automatically when the plugin is enabled; no daemon needs to be launched.

The repository's raw plugin directory is development-only. The checked-in
marketplace points only to `dist/marketplace`, which does not exist until the
production artifact is built. The Cargo fallback uses the current workspace
`acyclic-fs`, so copying the raw plugin into a cache is not a supported
installation. Only the generated marketplace is installable: its plugin is
self-contained, starts the bundled executable directly, and never invokes
Cargo or depends on repository source paths. The artifact validator copies that
plugin alone into an isolated cache layout before starting it.

## Child command integration contract

The packager keeps the control plane at
`libexec/acyclic-agent-workspaces-control` and the agent-callable umbrella CLI
at `bin/acyclic`. The companion lifecycle implementation must add that `bin`
directory to the authenticated child environment without adding a `git`
executable or changing root-task PATH. The canonical compatibility command is:

```text
acyclic git <git-style argv...>
```

The release gate is that bare `git` resolves to the user's system Git in both
root and child environments. The `acyclic` CLI is an extensible umbrella for
future subcommands; its `git` namespace must be a thin caller of the SDK
compatibility/core APIs rather than a second Git implementation. The CLI
implementation is owned by `rust/crates/cli`; authenticated child context,
removal of the legacy bare-`git` interception, and PATH injection are owned by
the lifecycle integration. This packaging layer only builds and places their
outputs and never rewrites Git behavior. Do not release an artifact until the
CLI and lifecycle workstreams pass their child-command integration tests.

The lifecycle integration gives managed child commands only the opaque IPC
capability contract:
`ACYCLIC_CONTROL_ENDPOINT`, `ACYCLIC_WORKSPACE_TOKEN`, and
`ACYCLIC_CONTEXT_VERSION=1`. The endpoint is a child-visible Unix-domain socket
URI or Windows named-pipe URI. The random bearer token is scoped and rotated by
the control plane for one live session, agent route, workspace, and route epoch.
The CLI submits `{version, token, command, argv}`; the control plane resolves
workspace identities and invokes existing compatibility APIs. Neither the CLI
nor child environment receives `PLUGIN_DATA`, storage paths, signing secrets,
or direct write authority. Missing, stale, revoked, mismatched, and cross-child
capabilities fail closed.

## First-use verification

After starting a new task, ask Codex to spawn a subagent that creates a harmless
file. Before publication, confirm the file appears in `agent_changes` but not in
the parent checkout. Then call `agent_merge` and confirm it appears in the
parent. Repeat with another file and `agent_discard`; the discarded file must
never appear in the parent. Finally run system `git status` in the parent and
confirm that only the deliberately merged file is reported.

The plugin fails closed when a filesystem tool cannot be associated with a
known subagent turn. Unknown agent kinds and filesystem-capable tools without a
stable caller turn are rejected. Arbitrary shell tools are denied unless the
host attests that it established a process namespace rooted at the child;
structured filesystem tools and SDK Git compatibility commands remain
available. Shell expansion and hard-coded parent-root paths are also rejected.
The root checkout and its system Git repository are never replaced.

Inside a child, bare `git` is the ordinary system Git and is not intercepted or
replaced by this plugin. Use `acyclic git` for the SDK façade. Compatibility
`HEAD` is the latest explicit commit, the live SDK generation is the
automatically staged working copy, and transport or object-database commands
fail explicitly. No hidden Git object store is used.

## Disable, upgrade, and uninstall

- **Disable:** turn the plugin off in Codex plugin settings, then start a new
  task. Existing tasks can retain their already-started MCP process and hooks.
- **Upgrade:** build or obtain an artifact with a newer manifest version, verify
  its checksum, replace the local source, and run
  `codex plugin add acyclic-agent-workspaces@acyclic-sdk` again. Review hooks and
  start a new task. Keep `PLUGIN_DATA` intact to preserve recoverable workspace
  metadata.
- **Uninstall:** uninstall the plugin in Codex settings and start a new task.
  After no plugin task is running, remove its local source. Delete its plugin
  data only when no unpublished child workspace needs inspection or recovery;
  deleting that data is irreversible.

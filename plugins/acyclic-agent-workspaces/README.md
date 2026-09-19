# Acyclic Agent Workspaces

This Codex plugin gives every native subagent a recursive SDK workspace. Codex's
normal `spawn_agent` interface stays unchanged. Filesystem tools are redirected
through a durable operation window, and only three decisions are model-visible:

- `agent_changes(agent, path?)`
- `agent_merge(agent)`
- `agent_discard(agent)`

The MCP process owns live native mounts. Lineage, direct-parent authorization,
barriers, rebasing, merge publication, compatibility history, and recovery are
implemented by `acyclic-fs`; the plugin persists only the short Codex
spawn/turn-to-workspace association required by lifecycle hooks.

The plugin fails closed when a filesystem tool cannot be associated with a
known subagent turn. The first release supports Codex-native subagents only;
unknown agent kinds and filesystem-capable tools without a stable caller turn
are rejected. Linux uses FUSE, macOS uses the native loopback mount, and Windows
uses a redirected ProjFS child directory. Arbitrary shell tools fail closed
unless the host explicitly attests that it established a process namespace
rooted at the child; structured filesystem tools and SDK Git compatibility
commands remain available. Shell
expansion and hard-coded parent-root paths are also rejected. The root checkout
and its system Git repository are never replaced by the plugin.

Inside a child, a standalone `git` command is intercepted by the SDK's
Git-compatible library. Compatibility `HEAD` is the latest explicit commit, the
live SDK generation is the automatically staged working copy, and transport or
object-database commands fail explicitly. No system Git process or hidden Git
object store is used.

For development, the MCP configuration builds and starts the single Rust
control executable through Cargo. Installed hooks must be reviewed and trusted
in Codex before they run.

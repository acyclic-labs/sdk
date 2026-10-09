# Acyclic

Acyclic gives supported native coding-agent subagents independent, recursively
forked distributed-filesystem workspaces. Recognized filesystem tools are
routed into the child mount; process-level confinement is a separate,
host-and-platform-qualified capability. It adds a familiar local-history interface
through `acyclic git`; it never intercepts bare `git` and never changes the
filesystem SDK's fork, lease, conflict, merge, or recovery semantics.

## Install

The provenance-attested npm package installs a checksum-verified platform
binary and the Codex plugin assets:

```sh
npm install -g @acyclic-labs/plugin
acyclic install codex
```

Rust users can install the standalone executable from crates.io:

```sh
cargo install --locked acyclic-plugin
acyclic --help
```

The Cargo installation provides the standalone CLI, service, hook runner, and
MCP bridge. The npm package remains the distribution that also carries the
Codex plugin, marketplace assets, and native qualification receipts for tagged
release binaries. A source-built Cargo binary remains uncertified until that
exact executable completes the native-mount qualification.

Native hooks and the npm-installed `acyclic` command both run the installed
Acyclic binary directly: installation verifies it against the release checksum
once, and no interpreter starts on each run. Install scripts must be enabled.
Codex runs command hooks through the session shell (`PowerShell` on Windows), so
the Codex plugin delivers its per-prompt, per-tool, and subagent hooks to
`acyclic __mcp`, a hook-only MCP server that Codex keeps connected for each
thread and that lists no tools to the model; only `SessionStart`, which starts
the service, and `SessionEnd` remain command hooks. Acyclic adds no
model-visible MCP server or second command implementation to shell-capable
hosts. Codex plugins do not inject
arbitrary executables into `PATH`, so install the npm package globally when
humans or agent shell commands need the `acyclic` executable.

`acyclic install <host>` changes only per-user host
configuration. Pass `--project` explicitly before Acyclic may create project
configuration. No `init` command or repository marker exists.

Supported target names are `codex`, `claude-code`, `cursor`, `copilot`,
`opencode`, `vscode`, `claude-desktop`, `copilot-cloud`, and `pydantic-ai`.
`acyclic install --detected` installs detected local targets. Process
confinement is certified only after that host/platform's escape suite passes;
native-mount qualification alone does not make that claim. MCP-only and
provisional targets say so at session start.

## Use

Normal commands and filesystem tools are transparently redirected to a child
mount. Acyclic itself has this public command surface:

```text
acyclic git <git-style argv...>
acyclic agents
acyclic discard agents/<ref>
acyclic install <host> [--project]
acyclic install --detected
acyclic uninstall <host> [--purge]
acyclic doctor [--json]
```

`acyclic install pi [--project]` installs the initial Pi diagnostics extension.
Pi owns its context, compaction and model retries. This source slice exposes
`/acyclic-doctor`; recursive workspace execution and host qualification remain
pending. `acyclic uninstall pi` restores the owned settings and retains cached,
pinned extension assets. Modified settings are preserved and reported for manual
resolution. The current API target is Pi 1.1.0; see [Pi qualification](pi/README.md).

`acyclic doctor` reports the packaged binary identity, plugin/cache version,
marketplace and hook assets, service and durable recovery state, native mount
backend, CLI availability, and an exact-binary-bound live
platform-certification receipt. Its human and JSON forms are rendered from the
same stable check result. Uninstall drains the service and keeps durable
workspace/recovery state by default; `--purge` also removes that state
explicitly.

Use `acyclic git status`, `diff`, `commit`, `switch`, `merge`, `rebase`, and
the other documented local porcelain inside a managed workspace. Child refs
are visible as `agents/...`; only a direct parent may merge or discard a child,
and descendants publish upward one level at a time. Bare `git` continues to use
the checkout's real `.git` repository and is intentionally absent in child
mounts.

If a merge projects text conflict markers, edit the mounted files, declare each
resolved path with `acyclic git add <path>`, and run
`acyclic git merge --continue`; use `acyclic git merge --abort` to restore the
exact pre-merge workspace. Non-text conflicts are reported with typed paths and
kinds rather than being flattened into text and must likewise be declared.

The plugin keeps its durable state in the per-user `state-v5` namespace.

With `ACYCLIC_LOG` enabled, the background service writes to `logs/service.log`
in that namespace, or to `ACYCLIC_LOG_FILE` when supplied (`{pid}` is still
expanded). Each destination retains at most 4 MiB in the active log and 4 MiB
in `service.log.1` (or the supplied filename plus `.1`). The active file's lock
serializes cooperating writers, including aliases. Oversize records and filesystem failures
discard diagnostic output; logging never writes to JSON-RPC standard output.
Rollover copies through one bounded `.1.next` staging file and publishes the
archive only after copying succeeds. Transient logical retention is at most
12 MiB per destination; restart removes the reserved staging entry. Logs are
best effort: individual writes can be partial and no power-loss durability is
promised. A failed archive copy preserves both published files.
Oversized existing files and invalid destinations are rejected at startup.
Explicit PID destinations retain independently.
Explicit foreground file logging uses the same bound and lock. Foreground
stderr logging and Chrome trace output keep their existing behavior.
Log destinations are trusted host configuration: reserve the active and
archive/staging names, and keep the active file in place while writers run.

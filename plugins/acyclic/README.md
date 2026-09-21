# Acyclic

Acyclic gives native coding-agent subagents isolated, recursively forked
distributed-filesystem workspaces. It adds a familiar local-history interface
through `acyclic git`; it never intercepts bare `git` and never changes the
filesystem SDK's fork, lease, conflict, merge, or recovery semantics.

## Install

The provenance-attested npm package installs a checksum-verified platform
binary and the Codex plugin assets:

```sh
npm install -g @acyclic-labs/plugin
acyclic install codex
```

`acyclic install <host>` changes only per-user host
configuration. Pass `--project` explicitly before Acyclic may create project
configuration. No `init` command or repository marker exists.

Supported target names are `codex`, `claude-code`, `cursor`, `copilot`,
`opencode`, `vscode`, `claude-desktop`, `copilot-cloud`, and `pydantic-ai`.
`acyclic install --detected` installs detected local targets. Full isolation is
certified only after that host/platform's escape suite passes; MCP-only and
provisional targets say so at session start.

## Use

Normal commands and filesystem tools are transparently redirected to a child
mount. Acyclic itself has six public command shapes:

```text
acyclic git <git-style argv...>
acyclic agents
acyclic discard agents/<ref>
acyclic install <host> [--project]
acyclic install --detected
acyclic uninstall <host>
```

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

The first v2 release uses a fresh per-user state namespace. It neither migrates
nor deletes prior plugin state. Remove old state manually only after confirming
that no older Acyclic installation still needs it.

# Acyclic

Acyclic gives supported native coding-agent subagents independent, recursively
forked distributed-filesystem workspaces. Recognized filesystem tools are
routed into the child mount; process-level confinement is a separate,
host-and-platform-qualified capability. It adds a familiar local-history interface
through `acyclic-plugin git`; it never intercepts bare `git` and never changes the
filesystem SDK's fork, lease, conflict, merge, or recovery semantics.

## Install

The provenance-attested npm package installs a checksum-verified platform
binary and the Codex plugin assets:

```sh
npm install -g @acyclic-labs/plugin
acyclic-plugin install codex
```

Rust users can install the standalone executable from crates.io:

```sh
cargo install --locked acyclic-plugin
acyclic --help
```

Cargo names the executable `acyclic`, while the npm package and the guidance
Acyclic gives agents call it `acyclic-plugin`. Link that name to it so agents
can run it:

```sh
ln -s "$(command -v acyclic)" "$(dirname "$(command -v acyclic)")/acyclic-plugin"
```

On Windows, in PowerShell:

```powershell
$acyclic = (Get-Command acyclic).Source
New-Item -ItemType HardLink -Path (Join-Path (Split-Path $acyclic) "acyclic-plugin.exe") -Target $acyclic
```

The Cargo installation provides the standalone CLI, service, hook runner, and
MCP bridge. The npm package remains the distribution that also carries the
Codex plugin, marketplace assets, and native qualification receipts for tagged
release binaries. A source-built Cargo binary remains uncertified until that
exact executable completes the native-mount qualification.

Native hooks and the npm-installed `acyclic-plugin` command both run the installed
Acyclic binary directly: installation verifies it against the release checksum
once, and no interpreter starts on each run. When the package manager skipped
install scripts, as npm 12 does by default, the command's first run completes
the installation.
Codex runs command hooks through the session shell (`PowerShell` on Windows), so
the Codex plugin delivers its per-prompt, per-tool, and subagent hooks to
`acyclic __mcp`, a hook-only MCP server that Codex keeps connected for each
thread and that lists no tools to the model; only `SessionStart`, which starts
the service, and `SessionEnd` remain command hooks. Acyclic adds no
workspace MCP server or second command implementation to shell-capable
hosts. The separate [identity MCP server](identity-mcp.md) exposes customer
login and identity operations with `acyclic-plugin identity-mcp`. Codex plugins do not inject
arbitrary executables into `PATH`, so install the npm package globally when
humans or agent shell commands need the `acyclic-plugin` command.

`acyclic-plugin install <host>` changes only per-user host
configuration. Pass `--project` explicitly before Acyclic may create project
configuration. No `init` command or repository marker exists.

Supported target names are `codex`, `claude-code`, `cursor`, `copilot`,
`opencode`, `vscode`, `claude-desktop`, `copilot-cloud`, `pydantic-ai`, and `pi`.
`acyclic-plugin install --detected` installs detected local targets. Process
confinement is certified only after that host/platform's escape suite passes;
native-mount qualification alone does not make that claim. MCP-only and
provisional targets say so at session start.

## Use

Normal commands and filesystem tools are transparently redirected to a child
mount. Acyclic itself has this public command surface:

```text
acyclic-plugin git <git-style argv...>
acyclic-plugin agents
acyclic-plugin discard agents/<ref>
acyclic-plugin install <host> [--project]
acyclic-plugin install --detected
acyclic-plugin uninstall <host> [--purge]
acyclic-plugin doctor [--json]
acyclic-plugin login [--json]
acyclic-plugin signup --agent [--json]
acyclic-plugin whoami [--json]
acyclic-plugin orgs [--json]
acyclic-plugin refresh [--json]
acyclic-plugin logout [--json]
acyclic-plugin bearer --org ID --output NEW_FILE [--json]
acyclic-plugin identity-mcp
```

Set `ACYCLIC_API_URL` to the public Edge origin, or pass `--endpoint ORIGIN`
to each identity command. `acyclic-plugin signup --agent` generates an Ed25519 key,
solves the server hashcash challenge, and saves an agent registration. It
requires new or pending agent credentials. `refresh` renews agent-only certificates with
a key signature. `login` upgrades an agent through device approval; identity
and org reads require the resulting session. See the
[wire contract](identity-api.md) for the pending signup retry contract.
`acyclic-plugin login` prints a device approval URL and
code on stderr, waits for approval, and saves the session. With `--json`, stdout
contains one JSON result or error; device approval instructions on stderr are
also JSON. Identity results never include access or refresh tokens.
`acyclic-plugin logout` revokes the OAuth session and clears local session tokens,
preserving agent keys. Failed revocation retains credentials for retry.

Use `bearer --org ID --output NEW_FILE` after login to mint a short-lived organization bearer. The new output file contains the token; stdout contains only its account, expiry and filename. Existing files and credential sidecar names are refused. Unix output has mode 0600; on Windows use a private directory with its inherited ACL.

The credential file defaults to `~/.acyclic/identity.json`. Override it with
`ACYCLIC_IDENTITY_FILE` or `--credentials PATH`. Filenames ending in `.lock` or
`.next` are reserved for credential sidecars. Existing Windows short filenames
resolve to the same lock as their long name. It is bound to its API origin,
written atomically, and locked across identity operations so two processes
cannot reuse a rotating refresh token. Unix files have mode 0600; on Windows,
use a private user directory with its inherited ACL. `whoami` and `orgs`
refresh expiring access tokens automatically. A failed or ambiguous refresh
clears the local session; sign in again rather than replaying a refresh token.

`acyclic-plugin install pi [--project]` installs the initial Pi diagnostics extension.
Pi owns its context, compaction and model retries. This source slice exposes
`/acyclic-doctor` and recursive-agent tools; host qualification remains
pending. `acyclic-plugin uninstall pi` restores the owned settings and retains cached,
pinned extension assets. Modified settings are preserved and reported for manual
resolution. The current API target is Pi 1.1.0; see [Pi qualification](pi/README.md).

`acyclic-plugin doctor` reports the packaged binary identity, plugin/cache version,
marketplace and hook assets, service and durable recovery state, native mount
backend, CLI availability, and an exact-binary-bound live
platform-certification receipt. Its human and JSON forms are rendered from the
same stable check result. Uninstall drains the service and keeps durable
workspace/recovery state by default; `--purge` also removes that state
explicitly.

Use `acyclic-plugin git status`, `diff`, `commit`, `switch`, `merge`, `rebase`, and
the other documented local porcelain inside a managed workspace. Child refs
are visible as `agents/...`; only a direct parent may merge or discard a child,
and descendants publish upward one level at a time. Bare `git` continues to use
the checkout's real `.git` repository and is intentionally absent in child
mounts.

If a merge projects text conflict markers, edit the mounted files, declare each
resolved path with `acyclic-plugin git add <path>`, and run
`acyclic-plugin git merge --continue`; use `acyclic-plugin git merge --abort` to restore the
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

# Acyclic CLI

The `acyclic` executable runs the GraphCoder preset with `acyclic run`, serves the
shared harness with `acyclic serve ADDRESS`, and reopens an existing agent with
`acyclic attach AGENT`. All three use the same frontend library and SDK client.
`acyclic run --help` lists execution, provider and output options.
The thin `graphcoder` executable is `acyclic run --template graphcoder`: with
terminal input and output it opens the same TUI as `acyclic attach`; with pipes
it runs headless. It forwards options to the shared parser. Submit a headless task
with `-p 'TASK'`, or use `acyclic exec --operation OPERATION` with JSONL input.
With no task, closing stdin exits; unexpected text is refused with a recovery
command. Text stdin otherwise answers pending approvals.
`ACYCLIC_MODEL` selects the model when `--model` is absent. `ACYCLIC_LOG` enables
diagnostics with explicit `--diagnostic-records` and `--diagnostic-record-bytes`
storage; use `off` to disable text diagnostics. `--default-config` ignores the
environment model and `ACYCLIC_CATALOG` selection. `ACYCLIC_CATALOG` selects the
provider catalog when `--catalog` is absent; neither variable contains credentials.

## Native host workspace coordination

Claude Code, Codex, Pi, OpenCode and Antigravity run their own sessions, providers, tools, permissions
and agent loops. Acyclic coordinates isolated workspaces, capture and merges
through an already-running Acyclic harness.

Root coordination and capture use the existing checkout and work without
`/dev/fuse`, including in a stock Linux container. Native hosts retain their own
configuration and session directories. Managed children project forked volumes
and require the platform's native mount provider when their views open.

Embedders select the host capability namespace with
`LocalHarnessConfig::native_mount_probe(NativeMountProbe::new(root))`.
The one probe opens `root/dev/fuse` on Linux; it also inspects mount authority
in that namespace. `plugin/examples/workspace-probe.rs` embeds the same owner
with that explicit root. The no-FUSE qualification supplies an empty temporary
root and drives both `__workspace` and the headless client against it, then
repeats those clients in stock Debian Docker with no FUSE device.

Start the shared workspace owner outside the host sandbox, from the workspace:

```sh
acyclic run --host-workspace --host-state-directory /path/to/explicit/state
acyclic install codex
acyclic host codex -- --sandbox workspace-write --ask-for-approval never
```

Codex requires **0.164.0 or later**; install and doctor reject older or unknown
versions. Pi targets **1.1.0**. Use `--plugin-root PATH` for package assets and
`--settings PATH` for a Claude settings file or Pi extension file.

OpenCode installs one extension in `OPENCODE_CONFIG_DIR/plugins/acyclic.js`
(otherwise the native XDG configuration directory). `--settings PATH` selects
that extension file explicitly. Antigravity (`agy`) installs a complete package
through its native plugin manager into the launching HOME profile.
`acyclic install agy` does not accept `--settings`; native session launch still
accepts AGY's `--settings` file or inline JSON.
Both adapters share the workspace owner and native process supervision; the
host continues to own its tools, model loop and permissions. The retained native
controls target OpenCode 1.19.5 and Antigravity 1.5.3.

Without `--settings`, installation follows `CLAUDE_CONFIG_DIR` or
`PI_CODING_AGENT_DIR`, falling back to the native default when the selector is
unset or empty. Use an absolute configuration directory. Selecting a profile
does not supply authentication: each host retains its own login and refresh
lifecycle. Pi uses its own native OAuth login; `CODEX_HOME` does not select it.

The plugin's `spawn` tool starts a separate native host process in the admitted
lazy view under `<workspace>/.volumes/agents/`: `codex exec`, `claude -p`, or
`pi --mode json -p`. Native instruction/config discovery and tools run there.
OpenCode children use `opencode run --format json --dir VIEW`; AGY children use
stream-JSON input and output in that view. Both inherit the launching environment,
including provider credentials and endpoint selectors, without an environment
allowlist. Native configuration and session storage remain owned by the host.
The plugin disables native built-in delegation so children use harness forks.

Codex and Claude permission hooks consult the harness approval chain only for
the plugin's exact `spawn` tool. Other tools retain the host's permission
handling. Fork, merge and filesystem commit use the workspace boundary; the
plugin does not intercept native shell, read, write or edit permission prompts.

The root launcher forwards the same explicit native policy arguments to managed
children. Spawn cannot override them. Codex requires explicit `--sandbox` and
`--ask-for-approval`; Claude requires explicit `--settings` and
`--permission-mode`. For Codex and Claude, the launcher records its already
writable workspace with native `--add-dir` on both parent and child. Changing
the child's cwd therefore retains the parent's workspace boundary, including
the coordination mailbox; this adds no path outside the parent's workspace.
Launch Claude with its native plugin directory enabled:

```sh
acyclic host claude-code -- --model claude-haiku-5-5 \
  --settings /path/to/policy.json --permission-mode default --plugin-dir /path/to/plugin/claude
acyclic host pi --
```

Harnesses start a headless root with `acyclic host HOST --prompt TEXT -- POLICY_ARGS`.
This is supported for every host family and uses the same policy resolution as
managed children, including AGY's relative file permissions and `..` refusal:

```sh
acyclic host agy --prompt 'Complete the task' -- --sandbox --mode accept-edits --model gemini-2.5-flash --settings '{"permissions":{"allow":["write_file(.)"]}}'
acyclic host opencode --prompt 'Complete the task' -- --model provider/model
acyclic host claude-code --prompt 'Complete the task' -- --settings /path/to/policy.json --permission-mode default
acyclic host codex --prompt 'Complete the task' -- --sandbox workspace-write --ask-for-approval never
acyclic host pi --prompt 'Complete the task' --
```

Headless output is the native JSON event stream, preceded by versioned
`acyclic.host_context` metadata. Root output and managed conversation event logs
retain the explicit `--model` selection when a native event omits its model;
reported native model names take precedence. Select `--model` explicitly for
cost attribution; the launcher does not guess a host's configured default.

OpenCode 1.19.5 [requests and resets MCP progress](https://github.com/anomalyco/opencode/blob/v1.19.5/packages/opencode/src/mcp/catalog.ts).
Parked waits emit progress only when the child's native output advances.
A quiet child needs no keepalive: the per-server `timeout` uses the maximum
native JS timer delay, 2,147,483,647 ms (about 24.9 days), to protect an otherwise
event-driven wait. No timer generates progress and wait returns only its result.

For writing AGY swarms, select the native `accept-edits` mode explicitly:

```sh
acyclic install agy --host-version 1.5.3
acyclic host agy -- --sandbox --mode accept-edits
acyclic install opencode
acyclic host opencode -- --model provider/model
```

Use a versioned native archive instead of a latest-only installation script.
The qualified Linux x86_64 release is [Antigravity 1.5.3](https://storage.googleapis.com/antigravity-public/antigravity-cli/1.5.3-6604829099950080/linux-x64/cli_linux_x64.tar.gz),
with SHA-512 `e363e1b17daaac2589f18e6973429c039fd5a5e5c15e9c0852fa349836f3992b10fe84732bc8d8c8208e149f0304f15aba9dec696d12efeed03917492c0580eb`.
The publisher's [release manifest](https://antigravity-cli-auto-updater-974169037036.us-central1.run.app/manifests/linux_amd64.json)
names the versioned URL and SHA-512 digest. Verify the downloaded archive before
execution. Plugin installation checks the requested `--host-version`, records
`host-version.json` in the installed plugin, and verifies the native version
again after installation. This installs the plugin; it does not replace the
native executable.

AGY `--print-timeout DURATION` is inherited unchanged. When no deadline is
selected, both root and managed children receive native `--print-timeout 0`,
which waits for completion without a deadline. The plugin blocks AGY
`invoke_subagent` and `define_subagent`, OpenCode Task, and Claude Agent/Task;
Codex installation and the shared launcher select `agents.enabled=false` and disable the overriding multi-agent v2 feature.
Delegation uses the installed coordination tools. Each managed child's harness
transcript starts with versioned `acyclic.host_context` metadata containing its
AgentPath and admitted cwd, before the first native event.

For AGY terminal writes, select a native session policy containing
`"permissions":{"allow":["write_file(.)"]}` using `--settings` (a JSON file
or inline JSON). The shared launcher resolves relative `read_file` and
`write_file` targets in all three native permission lists (`allow`, `ask`,
`deny`) against the current admitted view before each invocation. Absolute
targets, wildcard and home references, execution mode and unrelated settings
retain their native meanings. It adds no permission grants. The parent policy
is launched with absolute targets resolved against its admitted cwd: `.` grants
exactly that directory, never its parent. The inherited policy keeps relative
targets so each child resolves them against its own admitted view. Relative
targets containing `..` are refused.

AGY's `plan` mode is for investigation and planning; `accept-edits` permits file
edits in the workspace while retaining native terminal permissions. Headless MCP
coordination also needs the native policy to allow `mcp(acyclic_sessions/spawn)`
and `mcp(acyclic_sessions/wait)`. The launcher forwards the selected mode and
settings to every managed child. MCP rules name individual tools literally;
use separate `mcp(acyclic_sessions/TOOL)` rules for spawn, send, wait, cancel,
archive and resume, rather than a regular expression or pipe-separated name.

Use the same absolute per-trial HOME for AGY installation and launch; on Windows
select the same USERPROFILE too. Its native profile is
`$HOME/.gemini/antigravity-cli`. Keep provider environment variables when selecting
a fresh profile. For ADC, a credential file selected by
`GOOGLE_APPLICATION_CREDENTIALS` must remain reachable outside agent volumes;
a fresh HOME does not copy a login or default credential file.

For OpenCode, select an absolute `OPENCODE_CONFIG_DIR` before installation and
launch. Put the native `opencode.json` provider configuration there, including
`provider.options.baseURL`; managed children inherit that directory while their
cwd changes to the admitted view. Select per-trial XDG data, state and cache
directories too: the configuration selector alone does not isolate native session
storage. The shared owner retains each native session ID and passes it back to
that host on resume, independently of the view directory's lifetime.

Direct native sessions continue to work normally. Managed spawn requires the
launcher context; host metadata does not provide the effective security policy,
so the plugin does not guess it. Native configuration directories remain owned
by the native host. The plugin never reads credentials or native transcripts.
Pi's native CLI receives its saved native session file when resuming.

Pi provider extensions that remove authentication bootstrap from the shell
environment can supply it to managed children through the native event bus.
The shim emits `acyclic:coordination-environment` with its private environment
view immediately before starting the coordination bridge. A provider's
synchronous handler may add its retained bootstrap to that view without
restoring it to `process.env`. This event stays within trusted host extensions;
it is not a model-visible tool or a guest command environment.

The coordination tools are `spawn`, `send`, `wait`, `cancel`, `archive` and
`resume`. Spawn takes a parent-local `reference`; existing children can also be
addressed by their full canonical AgentPath. Only the child's immediate parent
can coordinate it. Spawn/send/resume also take a `prompt`. They use the same
child request, inbox, control, status, delivered
and result files as GraphCoder. Spawn and send return the prompt's committed
`id` and `inbox`; wait answers through the harness's shared committed reply
once that prompt's turn settles, is dropped, or can no longer run.
Conversation content and native event logs may be returned as a `FileRef`.
This is an object reference to bytes in the named volume. Resolve it through
that volume when needed, preserving lazy access to large content and logs.
Cancel stops and reaps the native child process tree before capturing partial
writes. Resume re-launches the saved native session in the recreated view.
Archive freezes the shared child reference permanently, including replay of its
original spawn prompt. Spawn a new reference for further work. A child returns
its result to its parent; the parent merges that child's changes.

The MCP server process owns the native children it spawns. Closing its input
interrupts and reaps its running children; death of that process also contains
them through the runtime's process custody. Losing this producer rejects its
outstanding MCP calls. A fresh connection fences the dead owner's process trees
and settles orphaned turns as cancelled before running queued inputs. The
GraphCoder workspace owner retains coordination state, not those process trees.

Capture happens at native turn/session boundaries and child process exit.
Failed or cancelled native tools leave their filesystem state capturable;
there is no per-tool pending-write ledger or pre/post hook pairing.

```sh
acyclic agents
acyclic fs log
acyclic --task TASK_ID fs merge agents/CHILD_REF
```

Native coordination and custody files live in the external owner state directory:

| Path relative to owner state | Use |
| --- | --- |
| `host-files/owner.json` | Identify the running GraphCoder owner |
| `host-files/requests/` | Drop request records, withdrawn once answered |
| `host-files/status/` | Await immutable replies keyed by operation; withdrawn input without a reply is indeterminate |
| `host-processes/` | Native process custody and exclusive ownership |
| `.volumes/agents/` | Admitted native child working views |

The owner announces its absolute carrier directory in the
`workspace-owner-ready` JSONL event. Set `ACYCLIC_HOST_FILES_DIRECTORY` to
that directory for native clients. Commands never search the checkout for
coordination files or start an owner. `acyclic host` grants native hosts the
three specific directories above for coordination, custody and child views;
owner identities and signing custody stay outside those grants. When launching
a sandboxed host directly, admit those same three directories through its
native writable-root configuration. Keep native sandbox and permissions enabled.

The carrier uses protocol v4. Run matching owner and plugin versions with fresh
external state; old coordination projections are refused and are not migrated.
External symlinked carriers are rejected before publishing a request.
`PLUGIN_DATA`, `CLAUDE_PLUGIN_DATA` and `XDG_STATE_HOME` do not select the
carrier. Installation changes native configuration; coordination commands use
only the explicitly admitted external owner directories.

Pi 1.1 has [no built-in sandbox or permission layer](https://github.com/agentic-labs/pi-mono).
It inherits the launching
process's restrictions. The plugin preserves those restrictions; outside-write
denial for Pi must be verified under the user's external sandbox.

The no-model command contract uses fixture profiles and the real native tool:

```sh
node plugin/tests/hosts/sandbox-commands.mjs --host codex \
  --binary /path/to/package/plugin/bin/acyclic --graphcoder /path/to/graphcoder \
  --native /path/to/codex --package /path/to/package/plugin --scratch /path/to/scratch
```

Select `pi` for native RPC Bash and extension capture; that case explicitly
reports Pi's unrestricted default. Each case checks agents, log, capture,
merge, discard and the hook command, with an external-data mailbox negative
control. The coordinator runs real Claude Code sandbox qualification after
merge, with `--model claude-haiku-5-5`. These probes complement that live matrix.

On Linux, `--host pi --pi-sandbox-codex /path/to/codex` launches native Pi under
that host's existing workspace-write sandbox. The same command contract then
requires outside writes to fail. This tests inherited restrictions without
adding a sandbox implementation to Pi or the plugin.

The separate [identity package](identity-mcp.md) retains its command surface.

The probe-only resource check exercises each production host adapter with
stand-in native sessions held alive at the N/2N samples:

```sh
node plugin/tests/hosts/shared-mount-scale.mjs ACYCLIC GRAPHCODER codex
```

Select `claude-code` or `pi` for the other adapters. This Acyclic build enables
the harness `test-support` feature for its OS resource query. The instrumented
executable is a test fixture. The oracle
shares the harness's descriptor/thread bounds and requires zero mount growth;
it samples the shared mount owner, excluding descendant host-runtime resources.
Each child acknowledges the hold barrier before and after each sample; the
same N sessions remain alive through the 2N sample. Releasing and waiting for
children before sampling must fail that barrier. The per-child-mount
counterfactual must fail the same resource check. The real-host e2e
owner runs the corresponding scale and sandbox matrix with native sessions.

The canonical scripted qualifier also exposes one selected scenario to an
acceptance caller that already owns its admitted artifacts, process scope,
fixture environment, output directory and cancellation:

```js
import {scriptedScenario, runScriptedScenario} from './tests/hosts/qualify-scripted.mjs';

const scenario = scriptedScenario(artifacts, codex, 'managed:lifecycle');
const receipt = await runScriptedScenario({
  scenario, processes, directory, env, cwd, expired,
});
```

This call runs exactly one scenario. It creates no process scope, profile or
timer, and does not stage artifacts again. The returned
`acyclic.plugin.scripted-scenario/v1` receipt records the selection, exit status,
elapsed seconds, expiry and acceptance; `NAME.log` is created exclusively in
the caller's output directory. Callers retain that receipt with their source,
artifact and host admission proofs.

Supported selections and required inputs are:

| Selection | Admitted executables |
| --- | --- |
| `managed:lifecycle`, `managed:recursive`, `managed:resume` | Acyclic, GraphCoder and Codex 0.164.0 or later |
| `managed:bounded-framing` | Acyclic only |
| `scale:codex`, `scale:claude-code`, `scale:pi` | Acyclic with resource instrumentation |

For standalone framing, the same qualifier stages only its required binary
from Cargo's JSON output and uses the shared fixture and case-budget owners:

```sh
node plugin/tests/hosts/qualify-scripted.mjs CARGO_JSON FRESH_OUTPUT managed:bounded-framing
```

Managed-session and scale fixtures currently require Linux POSIX launch tools.
Their captured native initialization records and scripted process behavior
do not establish live model acceptance. Windows generic process transport does
not translate these fixture executables; a Windows fixture launch contract and
macOS selected-case qualification remain separate acceptance work.

Captured Codex version output and its command, package integrity, binary hashes
and capture provenance live in `rust/crates/harness/fixtures/hosts/`. The shared
harness owns the version-admission test. Malformed controls are explicit
mutations of a real capture. The session stand-ins replay captured native
initialization records for all three hosts from `tests/hosts/fixtures/native/`,
with SHA-256 admission and capture provenance. Managed-model acceptance remains
with the coordinator and the real-host owner.

The no-model CLI admission regression uses the actual native Codex 0.146.0 and
0.164.0 executables, checking their output against those captures. Provide
absolute admitted executable paths in `ACYCLIC_TEST_BINARY`,
`ACYCLIC_TEST_CODEX_OLD` and `ACYCLIC_TEST_CODEX_CURRENT`, then run:

```sh
node --test --test-reporter=tap --test-concurrency=1 plugin/tests/hosts/version-admission.test.mjs
```

It checks both install and doctor reject the old host before reading the plugin
package or changing configuration, and supported doctor retains the sandbox
roots. Profiles are fresh and contain no login. Windows execution uses the
shared process host and serial Windows execution.

The no-model MCP framing probe runs all three host entry points without opening
workspace state or native login files:

```sh
node plugin/tests/hosts/mcp-framing.mjs ACYCLIC
```

It checks CRLF, buffered requests and an oversized peer that keeps its input
pipe open without sending a newline. Framing uses the shared harness MCP codec.

## Protected secrets

Use an owner-only bearer file. Put reads the value exactly from stdin or from
`--value-file`; values are never accepted as command-line arguments.

```sh
acyclic secrets list --endpoint https://api.example.com --bearer-file ./bearer --scope production
acyclic secrets put --endpoint https://api.example.com --bearer-file ./bearer --scope production --name API_KEY --kind protected --expected-generation 0 --value-file ./secret-value
acyclic secrets delete --endpoint https://api.example.com --bearer-file ./bearer --scope production --name API_KEY --expected-generation 1
```

`ACYCLIC_API_URL` may supply the endpoint. List includes values for declared
plain rows and metadata for protected rows. Put and delete require the generation
returned by list. A conflict returns a redacted error; list again before choosing
a new mutation. List returns one page. `--page-size N` sets its chunk, and
`--cursor-generation G --after NAME` continues from the page's `nextCursor`. If the
scope changed in between, list fails `listing changed`; restart without a cursor.
There is no account selector, get, reveal or export command.

## Assemble a release package locally

Build the native executable with `node scripts/build-product.mjs`, then assemble
and smoke-test the package using the same entry point as the release workflow:

```sh
node scripts/assemble-plugin-release.mjs --binary linux-x64-gnu=target/dist/acyclic --out release
```

Run from the repository root with Node.js, npm and GNU tar available. Select the
actual target and binary path for your build; cross builds include the target
triple under `target/`. Repeat `--binary target=path` to bundle more platforms.
The bundle must include the assembly host's target so it can be installed and
executed. Assembly uses a fresh npm cache and prefix, installs offline, and runs
`acyclic-plugin --help` and `--version` before publishing either archive.
The resulting npm archive is `release/acyclic-plugin-npm.tgz`; the marketplace
archive is `release/acyclic-plugin.tar.gz`.

With `ACYCLIC_TEST_BINARY` set to the absolute path of that executable, run
`node --test scripts/assemble-plugin-release.test.mjs` to check publication and
temporary installation cleanup with both valid and truncated native bytes.

Universal releases additionally use `--require-universal --certification DIR`,
where `DIR` contains the original `linux-fuse.json`, `macos-nfs.json` and
`windows-projfs.json` qualification receipts. The existing validator checks each
receipt against the bundled executable before packaging.

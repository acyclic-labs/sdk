# Native host workspace coordination

Claude Code, Codex and Pi run their own sessions, providers, tools, permissions
and agent loops. Acyclic coordinates isolated workspaces, capture and merges
through an already-running GraphCoder harness.

Start GraphCoder outside the host sandbox, from the workspace:

```sh
graphcoder --host-workspace --host-state-directory /path/to/explicit/state
acyclic install codex
acyclic host codex -- --sandbox workspace-write --ask-for-approval never
```

Codex requires **0.164.0 or later**; install and doctor reject older or unknown
versions. Pi targets **1.1.0**. Use `--plugin-root PATH` for package assets and
`--settings PATH` for a Claude settings file or Pi extension file.

The plugin's `spawn` tool starts a separate native host process in the admitted
lazy view under `<workspace>/.volumes/agents/`: `codex exec`, `claude -p`, or
`pi --mode json -p`. Native instruction/config discovery and tools run there.
Native built-in subagents keep their host behavior and **are not harness forks**.

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

Direct native sessions continue to work normally. Managed spawn requires the
launcher context; host metadata does not provide the effective security policy,
so the plugin does not guess it. Native configuration directories remain owned
by the native host. The plugin never reads credentials or native transcripts.
Pi's native CLI receives its saved native session file when resuming.

The coordination tools are `spawn`, `send`, `wait`, `cancel`, `archive` and
`resume`. Each takes a parent-local `reference`; spawn/send/resume also take a
`prompt`. They use the same child request, inbox, control, status, delivered
and result files as GraphCoder. Spawn and send return the prompt's committed
`id` and `inbox`; wait answers through the harness's shared committed reply
once that prompt's turn settles, is dropped, or can no longer run.
Cancel stops and reaps the native child process tree before capturing partial
writes. Resume re-launches the saved native session in the recreated view.
Archive freezes the shared child reference permanently.

Capture happens at native turn/session boundaries and child process exit.
Failed or cancelled native tools leave their filesystem state capturable;
there is no per-tool pending-write ledger or pre/post hook pairing.

```sh
acyclic agents
acyclic fs log
acyclic --task TASK_ID fs merge agents/CHILD_REF
```

All sandboxed coordination and custody writes stay within the existing workspace:

| Path | Use |
| --- | --- |
| `.acyclic-sdk/host-files/owner.json` | Discover the running GraphCoder owner |
| `.acyclic-sdk/host-files/requests/` | Drop request records, withdrawn once answered |
| `.acyclic-sdk/host-files/status/` | Await replies in each agent's latest status |
| `.acyclic-sdk/host-processes/` | Native process custody and exclusive ownership |
| `.volumes/agents/` | Admitted native child working views |

Install declares workspace access without adding sandbox roots. Commands never
start a service or write state-directory identities or lifecycle locks. Keep the
host sandbox and permissions enabled. GraphCoder must already be running.

Codex exports [`PLUGIN_DATA`](https://developers.openai.com/plugins/build/plugins)
to plugin hooks, and Claude exports
[`CLAUDE_PLUGIN_DATA`](https://code.claude.com/docs/en/plugins-reference)
to hooks and MCP servers. These are persistent plugin
directories, not a grant for commands run by the model's sandboxed shell. The
coordination client derives every mutable path from the admitted workspace,
regardless of either variable or `XDG_STATE_HOME`. External or symlinked mailbox
directories are rejected before publishing a request. Installation changes native
configuration outside the sandbox; ordinary coordination commands never do.

Pi 1.1 has [no built-in sandbox or permission layer](https://github.com/agentic-labs/pi-mono).
It inherits the launching
process's restrictions. The plugin preserves those restrictions; outside-write
denial for Pi must be verified under the user's external sandbox.

The no-model command contract uses fixture profiles and the real native tool:

```sh
python3 plugin/tests/hosts/sandbox-commands.py --host codex \
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
python3 plugin/tests/hosts/shared-mount-scale.py ACYCLIC GRAPHCODER codex
```

Select `claude-code` or `pi` for the other adapters. GraphCoder must be built
with the harness `test-support` feature for its OS resource query. These
instrumented binaries are test fixtures, not the shipping package. The oracle
shares the harness's descriptor/thread bounds and requires zero mount growth;
it samples the shared mount owner, excluding descendant host-runtime resources.
Each child acknowledges the hold barrier before and after each sample; the
same N sessions remain alive through the 2N sample. Releasing and waiting for
children before sampling must fail that barrier. The per-child-mount
counterfactual must fail the same resource check. The real-host e2e
owner runs the corresponding scale and sandbox matrix with native sessions.

The plugin e2e lane owns sandbox-ON cross-OS real-host acceptance, recorded in
`Q:/lanes/plugin-e2e/`. Source checks and no-model probes alone do not establish
native model-session acceptance.

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
| `scale:codex`, `scale:claude-code`, `scale:pi` | Acyclic and GraphCoder with resource instrumentation |

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
shared process host and the fleet's serial Windows runner.

The no-model MCP framing probe runs all three host entry points without opening
workspace state or native login files:

```sh
python3 plugin/tests/hosts/mcp-framing.py ACYCLIC
```

It checks CRLF, buffered requests and an oversized peer that keeps its input
pipe open without sending a newline. Framing uses the shared harness MCP codec.

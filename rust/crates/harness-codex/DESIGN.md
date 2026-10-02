# Codex harness: design and build plan

Codex becomes a second agent loop that a swarm can choose (`harness: "codex"`) in place of our recursive
decomposition. It is a new `acyclic_harness::Executor` in this crate. Nothing in `StockExecutor` changes, and
the recursive harness stays the default everywhere. The original design page is the claude.ai artifact
"Codex Harness" (KefVbgHYDeouUiTxrgoPEa), with a copy at `~/.claude/plans/codex-harness.html`. This file
supersedes it at task level: it adds what a live Codex 0.155.1 run and the current code turned up, and it
defines a verification gate for every task.

Status, 2026-10-02:

| Phase | State |
| --- | --- |
| A sdk | A1–A8 built. 39 tests pass, and the 3 e2e tests pass against the real Codex 0.155.1 locally; CI runs them on Linux and macOS. |
| B cloud | B1 is done (`3d4ff424`, unrebased). B2–B7 have gates written. |
| C eval-swarm | Gate written |
| D website | Gate written |
| E rollout | Planned |

## Decisions

| Question | Decision | Why |
| --- | --- | --- |
| Codex subagents | On (`features.multi_agent = true`) | Codex should really replace the tree; a single-agent baseline is one config flag away |
| Our tools in Codex | Granted registry tools over MCP, minus the recursion tools | Research A/Bs then compare harnesses, not search tools |
| Model path | Metered local proxy | Budgets can stop a run mid-way; the real key never enters Codex |
| Who counts steps | The proxy: one forwarded `/v1/responses` call = one step | Codex's JSONL has no model-call boundaries; the proxy sees every call |
| How a stop reaches Codex | Proxy answers `429` + `{"error":{"type":"insufficient_quota"}}` | Measured: Codex ends the turn after 1 request. A `402` is retried 6 times and a `500` 30 times (`fixtures/…/request-counts.json`) |
| Our MCP server | `required = true` | An optional server that fails is silent on stdout, so the turn would run without our tools |
| Config drift | Always pass `--strict-config` | A Codex upgrade that drops a key fails loudly |
| Release | `publish = false`; cloud pins the sdk rev | Cloud already pins sdk by rev (`managed-agents/Cargo.toml`), so A9 needs no crates.io release |

## What the live probe changed

A real `codex exec --json` 0.155.1 run against a fake Responses server showed these facts. Each one is now
pinned by a test.

1. **Stdin must be closed.** Codex blocks until stdin is closed, even when the prompt is an argument. Pinned by
   `executor::codex_runs_in_a_private_home…`; the fake `codex` exits 97 when stdin is left open.
2. **Usage is cumulative per thread.** `turn.completed.usage` covers the whole thread, so a resumed turn
   reports the sum. `CodexUsage::since` takes the difference, and billing comes from the proxy anyway. Pinned by
   `events::usage_is_cumulative…`.
3. **Retries are not failures.** A top-level `error` event is also used for retry notices
   (`Reconnecting... 1/5`). Only `turn.failed`, or exit with no turn event, ends a turn. Pinned by
   `events::retries_are_notices…`.
4. **`agent_message` and `reasoning` only ever `item.completed`.** The journal mapping must emit
   `ToolStarted` itself for items that never sent `item.started`.
5. **Codex calls home unless told not to.** Without `features.plugins = false` it calls github.com,
   api.github.com and chatgpt.com at startup.
6. **The shell tool is `exec_command {"cmd"}`.** Commands come back wrapped as `<user shell> -lc '…'`.
7. **Resume needs the model again.** `codex exec … -C <ws> resume <thread> <prompt>` works but forgets the
   model, so pass `-m` again. Sessions live in `$CODEX_HOME/sessions/…`, so a `CODEX_HOME` must survive a
   crash. That is why `CodexConfig::state_dir` exists.
8. **A required MCP server that fails leaves stdout empty.** The reason is only on stderr, so the
   executor keeps a 64 KB stderr tail. Pinned by `fixtures::a_required_mcp_server_failure…` and
   `executor::a_failure_before_any_event_reports_stderr`.
9. **MCP tools are hidden by default.** 0.155.1 puts every MCP tool behind its `tool_search` tool, so the
   model never sees ours. `omit_tools_from = ["deferred", "code_mode"]` on our server lists them directly,
   as a `{"type":"namespace","name":"mcp__acyclic","tools":[…]}` entry. The model calls them with
   `function_call{namespace: "mcp__acyclic", name: "acyclic_echo"}`. Found by the e2e test; the config
   snapshot pins it.

## Phase A: this crate

Rules for all of Phase A: workspace lints (no `unwrap`, `expect` or `panic` outside tests), no `unsafe` (so
use `acyclic-native-runtime::spawn_process_tree` for process groups, not `pre_exec`), unix only for process
code, and no change to any other sdk crate.

### A1 Scaffold and public API: done

`src/{lib,executor,meter,proxy,mcp}.rs`. The surface:

- `CodexConfig { binary, model, upstream, workspace, instructions, subagents, max_steps, deadline, state_dir, resume_thread }`
- `CodexExecutor::new(config, tools).with_tool_authority(scope, policy)?.with_meter(m).with_observer(o)`
- `UsageMeter { admit(model) -> MeterVerdict; record(model, &ResponsesUsage) -> MeterVerdict }`
- `ResponsesProxy`, `McpEndpoint`, `CODEX_VERSION = "0.155.1"`, `EXECUTOR_ID = "acyclic.codex.v1"`

Unbuilt parts return `Error::Unsupported("… not built yet (Ax)")`, so a premature consumer fails loudly.

### A2 Metered Responses proxy (`src/proxy.rs`): done

- **Server:** axum on `127.0.0.1:0`, serving `POST /v1/responses` only. Any other path gets 404
  `{"error":{"message":"… /v1/models …"}}` and one log line.
- **Forwarding:** reqwest streaming to `{upstream.base_url}/responses` with `Authorization: Bearer
  <upstream.api_key>`. Codex's dummy key is dropped.
- **Body:** deep-merge `extra_body` into the body: objects merge, and `extra_body` wins on scalars. Codex
  already sends `reasoning: {summary}`, so `extra_body.reasoning.effort` must merge, not replace.
- **Headers:** forward `session-id`, `thread-id` and `x-client-request-id`, and drop `authorization`. Hop-by-hop
  headers are recomputed.
- **SSE:** pass bytes through as they arrive. Parse frames on the side, looking only for `response.completed`
  (meter `usage.input_tokens`, `input_tokens_details.cached_tokens`, `output_tokens`) and `response.failed`
  (no usage).
- **Order of checks per call:** step cap, then `meter.admit`, then forward. When a call is refused, return 429
  `insufficient_quota` and set `stopped()` to `StepLimit(n)` or `Budget(reason)`. After a `Stop` from
  `record`, every later call is refused.
- **Upstream errors:** 4xx and 5xx pass through unchanged and are not metered. Codex's own retries are capped
  by config (`request_max_retries = 2`, `stream_max_retries = 2`).
- **Gate:** `tests/proxy.rs`, 7 tests: real key, deep merge, frame pass-through under 350 ms, meter stop to 429
  with 1 upstream request, step cap, 404 path, error pass-through, failed SSE not metered.

### A3 MCP endpoint (`src/mcp.rs`): done

- **Server:** hand-rolled JSON-RPC over axum at `/mcp`, protocol `2025-06-18`. It handles `initialize`,
  `notifications/initialized` (202), `tools/list` and `tools/call`, with JSON responses (the rmcp client accepts
  them). The workspace's `rmcp` lacks the streamable-HTTP server feature. Adopt it only if the A8 e2e test
  shows the hand-rolled server is not enough.
- **Auth:** a random per-turn bearer token, sent to Codex through `bearer_token_env_var =
  ACYCLIC_CODEX_MCP_TOKEN`. Anything else gets a 401.
- **Listing:** the definitions whose `tool:call:<name>` the scope grants. Names map `.` to `_` through a
  lookup table built at start, and a collision is an `Error::Invalid` at start.
- **Calls:** grant, then `executor.authorize(Some(scope))`, then input-schema validation, then policy. With
  `RequireApproval` and no interaction route the call is refused. Then `execute`, and the projection is
  capped at `Limits::render_bytes`.
- **Results:** `{content:[{type:text,text:<projection JSON>}], structuredContent:<result>}`. Every refusal or
  failure is `isError: true` with a reason, never a JSON-RPC error.
- **Gate:** `tests/mcp.rs`, 5 tests.

### A4 `CODEX_HOME` config (`src/config.rs`): done

`HomeConfig::render`/`write` produce `config.toml` and `AGENTS.md`. The pinned snapshot and the test that
every key must appear in the strict-config probe both pass.

### A5 Event parser (`src/events.rs`): done

Typed `CodexEvent` and `ItemKind`. Unknown types are kept as `Other`. `Transcript` folds a run. Tests check
that every recorded line maps to a typed event, and cover shell and patch items, cumulative usage, retries,
duplicate keys and unknown types.

### A6 Process control (`src/process.rs`): done

- **Command:** `<binary> exec --json --strict-config --skip-git-repo-check -m <model> -C <workspace>
  [resume <thread>] <prompt>`, run through `spawn_process_tree` with stdin set to null and stdout read line by
  line.
- **Environment:** start from an empty environment, then set `PATH`, `LANG`, `TMPDIR`, `SSL_CERT_FILE` (when
  set), `HOME = <state_dir>/<op>/home`, `CODEX_HOME = <state_dir>/<op>/codex`, `ACYCLIC_CODEX_PROXY_KEY` (a
  random dummy) and `ACYCLIC_CODEX_MCP_TOKEN`. `OPENAI_API_KEY` must never appear.
- **Version check:** run `<binary> --version` once per executor, and refuse anything other than
  `CODEX_VERSION`.
- **Limits:** at the deadline, SIGTERM the process tree, then SIGKILL 10 s later. Return
  `Error::Conflict("codex deadline reached …")`. The step cap is the proxy's job (A2).
- **Exit handling:**
  - exit 0 with `turn.completed`: success
  - `turn.failed`: `Error::Invalid(<codex message>)`
  - budget or step stop: `Error::Unauthorized("… budget …")` or `Error::Conflict("executor step limit reached")`
  - no turn event: `Error::Indeterminate(op)`, so the caller may retry and the turn resumes
  - no events at all: the error includes the stderr tail
- **Gate:** `tests/executor.rs`, the 4 tests tagged A6, using the replaying fake `codex`.

### A7 Executor and journal mapping (`src/executor.rs`): done

- **`Started`:** digest of `EXECUTOR_ID`, `CODEX_VERSION`, model, input, `subagents`, `max_steps`, the
  instructions, the granted tool definitions, and the policy identity. Ports and tokens are excluded. An
  existing `Started` with another digest is `Error::Conflict`.
- **Thread id:** on `thread.started`, append `Model{step: 0, Completed{metadata:{codex:{thread_started:<id>}}}}`.
  `ExecutionEvent` has no free-form record, and this keeps the harness contract unchanged.
- **Per proxied call (step n ≥ 1):** `ModelStarted{n, digest(request body)}`, then
  `Model{n, Completed{usage}}`. The proxy reports each call to the executor through a channel.
- **Assistant text:** `agent_message` becomes `Model{n, Content{delta: text}}`.
- **Tool items:** `command_execution`, `file_change`, `mcp_tool_call` and `collab_tool_call` become
  `ToolStarted{n, call_id: item.id, invocation: staged item}`, then `ToolCompleted{…, result, projection: staged
  item}`, or `ToolFailed{ExecutorRejected}` when the status is `failed` or `declined`. `ToolStarted` is
  synthesised if no `item.started` arrived.
- **Completion marker:** `Model{n, Completed{metadata:{harness:"codex", codex_version, thread_id, usage}}}` is
  the last record.
- **Staging:** canonical JSON (re-implemented, because the harness helpers are `pub(crate)`). Keys are
  `codex:<kind>:<step>:<id>` and stay at most 256 bytes.
- **Replay:**
  - a completion marker returns the recorded output without starting Codex;
  - `Started` plus a thread id and no marker resumes that thread;
  - `Started` with no thread starts fresh.
- **Output:** `TurnOutput{text: last agent_message, steps: proxied calls, metadata as above}`.
  `resume_thread` set in the config means a follow-up turn: `resume <thread>` with the input as prompt.
- **Gate:** `tests/executor.rs`, the 5 tests tagged A7.

### A8 End-to-end against real Codex: done

- **Tests:** `tests/e2e.rs` has 3 tests. The real pinned binary talks to the scripted `FakeUpstream`
  (MCP echo, then `exec_command`, then answer). A spent budget ends the turn after 1 upstream request, and
  `max_steps = 1` ends it on the step cap. No OpenAI traffic.
- **CI:** `.github/workflows/agent-host-qualification.yml` gains `rust/crates/harness{,-codex}/**` in its path
  filter and a "Run codex executor scenario" step on the Linux and macOS lanes. The crate is
  `#![cfg(unix)]`, so the Windows lane's `cargo test --workspace` still builds.

### A9 Docs and merge

Write the crate README (guarantees, how it differs from `StockExecutor`: no `ContextPipeline` stages, Codex
owns compaction, and the version policy), add a `CHANGELOG.md` line under 0.2.0, open the PR, merge, and
record the merge SHA for B2.

**Phase A done when:** `verify.sh all` shows 0 pending, `verify.sh e2e` passes locally and in CI, and
`cargo test --workspace` is otherwise unchanged.

## Phase B: cloud (`managed-agents`)

**Prerequisites:**

1. **Edit freeze.** `cloud/AGENTS.md` freezes `managed-agents/` unless ramstar explicitly overrides it. Get
   that before editing.
2. **Rebase first.** Rebase `feat/swarm-harness` onto `origin/main`, which is 120 commits ahead. Expect 2
   conflicts:
   - `host/tests/swarm_engine.rs`: keep both the doc comment and the harness test.
   - The `swarm_agent.rs` protocol `use` list: take main's list and add `Harness`.

   Main replaced the workspace flow (SDK generations: `bind_workspace` → run → `publish_workspace` →
   `changes()`), so the Codex arm is written against main.

| Task | Change | Tests |
| --- | --- | --- |
| B2 | `Cargo.toml`: every sdk crate moves to the Phase A merge SHA (main currently pins `42b089f`), plus `acyclic-harness-codex` | `verify-phase.sh B` checks the rev contains the crate |
| B3 | `bundle-graphcoder/src/swarm.rs:553` `swarm_executor(…, harness) -> Arc<dyn Executor>`. The Acyclic arm is today's body unchanged. The Codex arm uses `swarm_registry(host, None, …)`, which with no swarm host already leaves out fork, speculate, ask, reply and delegation, plus `registry_authority`, with `SwarmContextStage::system_prompt` as instructions | New: `the_codex_registry_has_no_recursion_tools`; existing `the_model_is_offered_every_tool_the_swarm_registered` unchanged |
| B4 | `worker/src/swarm_agent.rs`: drop the refusal at `:1279`. `CodexMeter: UsageMeter`: `admit` = `reporter.proceed()`; `record` = `ModelPrice::cost(prompt, cached, completion)` → `reporter.spend` + a `model.call` span with today's attrs. `CodexObserver` maps: `command_execution` → `tool.exec` with tool `codex.shell`; `file_change` → `codex.edit`; `mcp_tool_call` → the registry name (`acyclic.web`); `collab_tool_call` → log line `codex subagent: …`; `agent_message` → `assistant: …` + `reporter.summary`. Inbox continuation sets `resume_thread` from `TurnOutput.metadata.thread_id`. `state_dir` is outside `/workspace`, so it never lands in the patch | New: `codex_meter_prices_like_metered_provider`, `codex_observer_emits_tool_exec_spans`, `a_codex_commit_still_diffs_against_base` (Codex may `git commit`) |
| B5 | Host: `OPENAI_SECRET = "openai"` from `openai_key_env` (default `OPENAI_API_KEY`); secret refs include it when `harness == codex`; `api.openai.com` joins `allow_domains` when any catalog model lists codex. Codex plus per-swarm BYOK `inference` is refused (the gateway speaks Chat Completions only; see Open) | New: `a_codex_swarm_gets_the_openai_secret`, `codex_with_byok_inference_is_refused` |
| B6 | `image/Containerfile` and `worker-cross.Containerfile`: the `@openai/codex-linux-x64@0.155.1` musl binary, checked against the sha512 already in `sdk/plugin/tests/hosts/package-lock.json`, installed to `/usr/local/bin/codex`, no Node. Snapshot `acyclic-worker-codex-<sha>`; never `--replace`, never the prod label | `docker run … codex --version` = 0.155.1 |
| B7 | Probes `codex-baseline`, `codex-web` and `codex-budget` (drafts in `../codex-harness/probes/`), local then Daytona | `verify-phase.sh B-probe [--daytona acyclic-worker-codex-<sha>]` |

**Phase B done when:**
- `verify-phase.sh B` passes (B1 tests, the new tests, `make check`).
- `verify-phase.sh regress` passes (the default path is unchanged).
- `verify-phase.sh B-probe` passes locally and on Daytona: `codex-baseline` and `codex-web` succeed with zero
  fork or delegate spans, and `codex-budget` stops on the budget.

## Phase C: graphcoder eval-swarm

| Task | Change |
| --- | --- |
| C1 | `run.ts:165-182` add `--harness` (`acyclic`, `codex`); request at `:242-254` sends `harness`; `metadata.swarm.harness` (`:350-367`); batch name gains `-codex`. `host.ts:27-39` type gains `harness?` |
| C2 | `e2e.sh host_up --harness codex`: `"harnesses": ["codex"]` on the catalog model (`:166-174`), snapshot `acyclic-worker-codex-<sha>` |
| C3 | `score.ts` adds a `harness` value from `metadata.swarm.harness` to `results.tsv` (today that column is the constant `eval-swarm`, so add a `swarm_harness` column). `compare` groups by label and harness. `audit.ts` per-tier cost: Codex has one tier. `tree.ts` prints "no tree (codex)" |
| C4 | `RUNBOOK.md:270-283`: a Codex column in the harness-differences table and an "A/B procedure" section; fix the stale model-routing rows (`:280`, `:341-352`, `:361`) |

**Gate:** `verify-phase.sh C`, which is `bun test scripts/eval-swarm` plus the wiring checks. Then, by hand, one
`e2e.sh round --harness codex` on 2 questions against a local host.

## Phase D: acyclic-website (after B is live on the host)

| Task | Change |
| --- | --- |
| D1 | `create-swarm.ts:46-56` add `"harness"`; `validateCreateSwarm` (`host.ts:168-228`) copies `harness` when it is `acyclic` or `codex`; `types.ts:207` gets `harness?`; the create response returns the host's `harness` (the CLI's old-host check depends on that echo). Do not attach BYOK `inference` to a codex swarm (see Open) |
| D2 | `dashboard/swarms/+page.svelte`: an "Agent harness" select (Recursive / Codex) next to "How much to split", sent in the body at `:99-106`; the swarm page shows the harness |
| D3 | `mcp/tools.ts` adds `harness` to the `spawn_swarm` schema (`:45-134`, enum) and the allow-list (`:299`), and the handler echoes it; `cli/+page.svelte:160` documents `--harness codex` |

**Gate:** `verify-phase.sh D`. That is the `create-swarm`, `tools`, `handlers` and `server` tests (new:
`harness` accepted and echoed, `claude` rejected, and the schema and validator stay in parity), plus `bun run
check` and `check:cli-snippets`.

## Phase E: rollout and A/B

1. **E1:** a new Daytona host sandbox from the Codex snapshot with one model (`gpt-5.5`) tagged
   `harnesses: ["codex"]`, following the host redeploy runbook (patch `agent_endpoint`, `ACYCLIC_HOST_URL`, the
   `prod-daytona` profile).
2. **E2:** 10 FRAMES, 10 BrowseComp and 2 deep-research tasks on both harnesses, with the same model and
   budget. Compare score, timeouts, web calls, cost and wall time per run. Scale to about 30 questions per
   suite before concluding.
3. **E3:** Codex becomes the default only if it scores at least as well at no more cost. Otherwise it stays
   opt-in. Ship D2 either way.

## Verification at a glance

| Gate | Command | Needs | State today |
| --- | --- | --- | --- |
| A unit | `rust/crates/harness-codex/verify.sh unit` | nothing | green: 39 tests |
| A acceptance | `verify.sh status` | nothing | 0 pending (every acceptance test has been un-ignored) |
| A e2e | `verify.sh e2e` | npm (resolves pinned codex); no API key | green: 3 tests; also in `agent-host-qualification.yml` |
| B | `codex-harness/verify-phase.sh B` | cloud worktree | red (B2 pin missing) |
| B live | `verify-phase.sh B-probe [--daytona S]` | `OPENAI_API_KEY`; billable | not run |
| No regression | `verify-phase.sh regress` | cloud worktree | run before every B merge |
| C | `verify-phase.sh C` | bun | red (no `--harness`) |
| D | `verify-phase.sh D` | bun | red (`harness` not accepted) |

A task is done when its acceptance tests pass under `status`. Their `#[ignore]` is then removed, so `unit` and
CI guard them from that point on.

## Upgrading Codex

1. Bump `plugin/tests/hosts/package.json` and the lock, then `CODEX_VERSION`.
2. Re-record: run `tools/fake-responses-server.mjs` and `tools/record-run.sh <name> <mode>` for every run listed in
   `fixtures/codex-0.155.1/README.md`, into a new `fixtures/codex-<ver>/`.
3. Run `cargo test` (the fixture invariants), then `verify.sh e2e`. Every failure names the design assumption that
   moved.
4. Bump the B6 image binary and its checksum.

## Open

- **BYOK and Codex.** The website attaches per-org BYOK `inference` whenever an org has a key
  (`create-swarm.ts:187-208`), and the gateway only speaks Chat Completions. Choose one:
  - (a) codex swarms run on the platform OpenAI key and ignore BYOK (recommended for the A/B);
  - (b) codex is refused for BYOK orgs;
  - (c) the gateway gains `/responses`.
- **Thread id record.** It is stored as a step-0 `Model` completion (A7). If the harness ever gains a
  checkpoint event, move it there.
- **Model ids.** Catalog keys are bare (`gpt-5.5`). If a swarm's model is provider-prefixed
  (`openai/gpt-5.5`), B3 strips the prefix for Codex.

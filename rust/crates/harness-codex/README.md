# acyclic-harness-codex

An `acyclic_harness::executor::Executor` that runs a whole turn as one OpenAI Codex CLI process
(`codex exec --json`, pinned to 0.155.1). It is a drop-in alternative to `StockExecutor`. Bind it with
`HarnessBuilder::executor`.

| What the executor does | How |
| --- | --- |
| Meters model calls and enforces budgets | Codex calls a local Responses proxy that holds the real key and meters every call through `UsageMeter` |
| Exposes the consumer's tools | A local MCP endpoint offers the granted tools of the `ToolRegistry` |
| Isolates Codex | A private `CODEX_HOME` per operation: approvals off, Codex web search off, plugins off, our role prompt in `AGENTS.md` |
| Enforces limits | Steps are capped at the proxy; the deadline terminates the process tree |
| Journals the turn | Records follow the stock executor's shape, and a crashed turn resumes its Codex thread |

The difference from `StockExecutor`: Codex owns context assembly and compaction, so `ContextPipeline` stages
do not run.

Protected native turns supply an owned `ProcessHost`, admitted `TaskContext`,
and complete `BuiltinMediator` bindings for the pinned exec-server protocol.
`ExecServerAuthority` checks the original task, lease and physical fence before
each dispatch and supplies genuine provider notifications. The private
`CODEX_EXEC_SERVER_URL` is for the Codex process only: its shell environment
policy must exclude that URL and the MCP/proxy secrets. A configured remote
executor has no local-tool fallback. Shutdown kills and drains the original
Codex process group, then awaits endpoint closure and durable dispatch drain.
The generic SDK hooks do not themselves implement a consumer's Root authority
or qualify a live provider.

Durable native machines may retain the exact canonical `Vec<u8>` turn codec
with the existing Rust-generated `tool::schema::input/output` contracts. Decode
`TurnInput` before admission and initialization, and encode only the actual
executor's `TurnOutput`; do not duplicate the nested model/resource schemas.
`TaskAdmissionRecord::runtime_scope` restores the retained effective scope and
extension selection, but does not replace owner authority verification.

See `DESIGN.md` for the design and `verify.sh` for the gates (`verify.sh e2e` runs the real pinned Codex against a scripted upstream, with no API key).

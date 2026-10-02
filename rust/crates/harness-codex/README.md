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

See `DESIGN.md` for the design and `verify.sh` for the gates (`verify.sh e2e` runs the real pinned Codex against a scripted upstream, with no API key).

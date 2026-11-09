# Native host pairs

Concurrent MCP session programs reuse the native process fixture and its captured
completion records, without a live model:

```sh
node plugin/tests/hosts/concurrent-spawn.mjs ACYCLIC GRAPHCODER claude-code 0eaa80
```

Omit the final hex argument to run the retained zero-, one- and two-send
programs. The zero-send program parks a wait before admitting the peer spawn.

`native-calls.mjs`'s `fuzz(bytes, tool)` directs the real initialized MCP client. Bytes
choose the interleaving of two child call chains, up to two queued sends, spawn
replay, duplicate waits and forward/reverse reply reads. All calls are submitted
before responses are consumed. Each native turn holds its physical view until
its peer reaches that turn's socket; there are no clock waits. The oracle checks
original task/prompt identities, replayed view identity, turn counts and shared
wait results, then rejects mutations of those actual replies (four for the
retained replay input). The fixture emits the original call and
reply trace in its result and the scripted qualification receipt. Its selector exposes
`concurrent:codex` and `concurrent:claude-code`; other hosts require captured
completion records before qualification.

After joining the children, a producer-loss control observes a live child held
on its original socket, submits its wait, then kills and joins the MCP server.
The unanswered call must end in the shared client's original pipe/process-close
error. Under the [native process ownership contract](../../README.md), the MCP
server is the spawning owner: its death contains the held child. The original
socket must close without release; a fresh MCP connection fences the orphaned
turn and observes its cancelled result, never a successful answer.
These are process and socket events, without a deadline; the original error
is retained with the trace.

The managed lifecycle, recursive and resume scenarios require the Linux native
Codex sandbox leg. Each first proves a workspace write succeeds and a sibling
write is denied. Their temporary root is under `/var/tmp`, outside the default
`/tmp` and fixture `TMPDIR` write grants. Unavailable sandbox enforcement fails
the capability assertion; none of these scenarios is silently skipped.

The native pair collector uses the same pinned package, shipping plugin install,
workspace owner and native permission path as the installed acceptance cases.
It observes the original process writer, transcript sink, exit and close.
All pairs and the optional workflow share one package staged after admission;
its path stays fixed across the native profile's marketplace registrations.

```sh
node plugin/tests/hosts/native-pairs.mjs HOST GRAPHCODER MOCK_MODEL PLUGIN NEW_OUTPUT PAIRS
```

All executable and output paths are absolute. `E2E_SOURCE` identifies the exact
consumer source; `E2E_ARTIFACT_SOURCE` identifies the admitted executable source.
Use the original artifact/runtime manifest to verify those bindings before
launch. Adjacent pairs use opposite launch orders, with the first order random.
Raw offsets and exact effects remain in each pair directory. The enclosing
process owner must retain its original physical completion receipt separately.

For an approved live pass, append `--live NONSECRET_NATIVE_LOGIN_JSON`. The JSON
contains exactly `provider`, `model` and `profile`: an explicit native provider,
exact approved model ID, and absolute path to an existing exclusive native login
profile outside the fixture root. Provision that profile through the original
host login before running; never put credentials in this file, arguments,
fixtures or job environment. The collector passes the profile path to the host
without reading or copying its credentials. It may install/remove its plugin in
that exclusive profile. Keep unrelated plugins disabled and retain managed deny
settings. Live Claude Code runs on Windows.

| Host | Native login provider |
|---|---|
| codex | openai |
| claude-code | anthropic |
| pi | openai-codex |

Live mode starts no mock process; pass `-` in the unused `MOCK_MODEL` position.
Pi uses its original login provider and an explicit installed extension; its
unsupported keyless mock provider remains a refusal. No model fallback is
selected by the driver. Add `--workflow` after the live configuration path to
run one installed child/wait/merge/resume case after the process pairs. It uses
the existing real filesystem and native-session identity oracle.

These samples measure whole native processes executing the same shell effect.
Input acceptance is not child consumption; transcript write is not read arrival;
EOF, close and file finish differ. Logical warm-turn, fork, wait and merge
latencies remain unmeasured until their original producer boundaries are
available. A whole-workflow duration cannot substitute for those pairs. Failed
or incomplete legs never contribute a latency percentile. A failed campaign
keeps its original captures and stops; repair its named failure before scheduling
another authorized pass.

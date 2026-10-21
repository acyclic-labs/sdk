# Shared deterministic model server

Decision: a standalone test-support crate, independent of Harness internals, shared
by the GraphCoder and plugin e2e lanes. Build before timing the suite:

```
cargo build -p harness-mock-model
harness-mock-model --script script.json --record requests.jsonl
```

The first flushed stdout line is `{"event":"ready","port":12345}`. Bind is always
`127.0.0.1:0`. Subsequent stdout lines are request notifications containing
`event`, `sequence`, `case`, `path`, `step`. Each notification follows a flushed
JSONL record with those fields plus `method`, `body`, `status`. Wait on these
lines; there is no readiness polling or timing delay. Kill/reap the process at
suite end. No credentials or network service are needed.

```json
{"cases":{"example":{"input_tokens":64,"steps":[
  {"when":{"user":"hello"},"text":["Hello ","**world**"],"tools":[
    {"id":"write_1","name":"write","arguments":{"path":"hello.txt","text":"file contents"}}
  ]},
  {"when":{"tool_result":"file contents"},"text":["done"]}
]}}}
```

Catalog `base_url`: `http://127.0.0.1:PORT/example/v1`. Adapters `responses`,
`chat`, `anthropic` select `/responses`, `/chat/completions`, `/messages`.
All emit native streaming SSE, including function argument deltas. Configure
`token_counter` with `path: "count"`, `adapter` matching the provider adapter,
`response_field: "input_tokens"`, `framing_tokens: 0`. Token requests are
recorded but do not consume steps. Native token-count paths work too.
`count_bytes: true` adds serialized request bytes to the count for compaction
cases. A step with explicit `when.path: "count"` overrides automatic counting;
combine `json` and `hold` to suspend preparation until the process is killed.

Each namespace independently consumes the first unused matching step, exactly
once. Optional `when` guards are ANDed: `path` is an exact endpoint suffix;
`user` and `tool_result` are substrings of the last corresponding content;
`contains` is a substring of the serialized request body. Mismatch/exhaustion
returns HTTP 409 and consumes nothing. Unknown namespaces fail. Guards never
match older user messages or tool definitions by accident.

Optional step fields: `status` (HTTP code), `json` (ordinary JSON response for
errors or scripted search/MCP endpoints), `hold` (emit chunks but leave SSE open
until the client cancels). Text and tools default to empty. Scripts reject
unknown fields. Parallel requests share a local executor; consume, append,
flush and notification are one synchronous operation with no shared locks.

Tool argument leaves `{"$tool_result":"/file"}` copy a JSON pointer from the
last actual tool result. Missing pointers fail. Use this for write → edit → read
with runtime-generated file references; no fabricated authority enters GraphCoder.

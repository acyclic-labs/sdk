# GraphCoder native runtime

`graphcoder-runtime` is the native JSON-lines host for the local GraphCoder
composition. It opens `PersistentLocalSwarm` against the supplied durable
root, selects an explicit deterministic model fixture, and maps the public
GraphCoder bridge methods to SDK calls.

The host owns no session journal, conversation history, workspace merge logic,
or effect registry. It returns a typed `unsupported` response while the local
composition is still adding a durable projection for activity, messages,
approvals, changes, file reads, cancellation, and root writeback.

Example (the process reads requests from standard input and writes one response
per line):

```text
graphcoder-runtime --root C:\\work\\graphcoder --model-fixture stage
```

The `echo` and `complete` fixtures produce deterministic text. The `stage`
fixture emits the SDK's `acyclic.stage_file` tool call once, allowing the real
Filesystem-backed harness to exercise a controlled file mutation.

The `blocking` fixture opens one durable approval ticket at startup and holds
its model turn until the public `cancel_session` request arrives. This fixture
is used by the native JSON-lines tests to verify pending approval visibility,
operator authentication, cancellation, and the absence of a false completed
result through the production bridge.

# @acyclic-labs/graphcoder

GraphCoder is a thin local application boundary over Harness. The package
contains a transport-neutral UI state machine and a terminal adapter. Durable
session state, recursive agents, model requests, messages, approvals,
workspace generations, and writeback are supplied by the injected
`GraphCoderTransport`; the UI layer does not create a second storage or
orchestration system.

The same `GraphCoderUi` can drive a native application. `GraphCoderTerminal`
only translates newline-delimited commands into UI operations and JSON
output, so terminal behavior is covered by the same contracts as other hosts.

The command line fixture is deliberately explicit:

```sh
graphcoder --fixture=deterministic "start inspect the repository" list
```

The deterministic fixture is for tests and local demonstrations. A production
host should construct the UI with a Harness-backed transport. Listing sessions
returns summaries only; activity, messages, approvals, change metadata, and
diff bodies are requested separately and lazily. Root writeback requires an
operation-bound approval and the expected workspace generation.

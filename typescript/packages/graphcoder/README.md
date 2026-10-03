# @acyclic-labs/graphcoder

GraphCoder is a thin local application boundary over Harness. The package
contains a transport-neutral UI state machine and a terminal adapter. Durable
session state, recursive agents, model requests, messages, approvals,
workspace generations, and writeback are supplied by the injected
`GraphCoderTransport`; the UI layer does not create a second storage or
orchestration system.

The same `GraphCoderUi` can drive a native application. `GraphCoderTerminal`
from the `@acyclic-labs/graphcoder/terminal` entry point only translates
newline-delimited commands into JSON output, so the generic root entry point
does not import Node readline or process APIs into native applications.

The command line fixture is deliberately explicit:

```sh
graphcoder --fixture=deterministic "start inspect the repository" list
```

The deterministic fixture is for tests and local demonstrations. A production
host should construct the UI with a Harness-backed transport. Listing sessions
returns summaries only; activity, messages, approvals, change metadata, and
diff bodies are requested separately and lazily. Root writeback requires an
operation-bound approval and the expected workspace generation.

After building, the Windows PTY qualification can be run when Python's
`winpty` binding is installed:

```sh
python typescript/packages/graphcoder/test/pty_smoke.py
```

It drives the same command sequence through the headless and interactive
artifact paths and checks the visible lifecycle, approval, lazy file/diff,
and cancellation markers in both transcripts.

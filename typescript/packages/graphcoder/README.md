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

For a native or terminal host, `@acyclic-labs/graphcoder/bridge` provides
`HarnessGraphCoderTransport`. Inject a `GraphCoderBridge` implemented by the
durable local runtime. Each request contains a unique `request_id`, a
snake-case `method`, and only the operation's explicit `params`; responses use
the same ID and encode generations and activity sequences as decimal strings.
The bridge owns session journals, recursive workers, approvals, workspace
generations, and recovery. The TypeScript adapter does not open workers while
listing sessions or hydrate files while reading summaries.

The wire envelope is:

```json
{"request_id":"graphcoder-1","method":"list_sessions","params":{"query":{"limit":32}}}
{"request_id":"graphcoder-1","ok":true,"result":{"items":[],"next":"cursor"}}
```

The runtime methods and parameter names are fixed: `list_sessions(query)`,
`start_session(prompt, model_fixture?)`, `open_session(session_id)`,
`resume_session(session_id)`, `read_activity(session_id, query?)`,
`read_messages(session_id, query?)`, `send_message(session_id, sender_id,
recipient_id, body)`, `list_approvals(session_id, query?)`,
`resolve_approval(approval_id, approved, session_id?)`, `cancel_session(session_id)`,
`list_changes(session_id)`, `read_change(session_id, path, generation)`,
`read_file(session_id, path, generation)`, and
`approve_writeback(session_id, operation_id, expected_generation, approved)`.
Generations and activity sequences are unsigned decimal strings; file bodies
use an array of octets. A failed response is
`{"request_id":"...","ok":false,"error":{"code":"denied","message":"..."}}`.
Session-bound responses include the requested `session_id`; change and file
bodies use `unified_diff` and `media_type`, renamed entries use `old_path`, and
writeback receipts echo the requested operation, session, generation, and
boolean `applied` value. The adapter rejects a response that changes any of
these bindings before exposing it to the UI.
The default JSON-lines line and bridge-envelope bound is 16 MiB. File bodies
have a 64 MiB protocol ceiling; a host that needs larger files must use its
own chunking protocol or explicitly configure a larger line/envelope within
that 64 MiB ceiling.

Hosts consume it by constructing `new HarnessGraphCoderTransport(bridge)` and
passing that transport to `new GraphCoderTerminal(transport, io)` or
`new GraphCoderUi(transport)`. The host may implement `bridge.request` over a
JSON-lines process, native callback, or WASM binding; the package does not
choose or start that execution provider.

Hosts that want the packaged command loop without the deterministic fixture can
call `runCliWithTransport(argv, transport, io)` from the terminal entry point.
Headless commands return status `1` when any command produces an error, and
fixture flags are rejected by this production runner. The fixture-only
`runCli` entry point requires an explicit supported `--fixture` and remains a
test/demo surface.

Node hosts that explicitly own a local runtime executable can use
`@acyclic-labs/graphcoder/node`'s `JsonLineGraphCoderBridge`. It correlates
concurrent requests, rejects pending requests on process errors or EOF, bounds
one line, and reports stderr, malformed lines, unmatched responses, and exit
status through `onDiagnostic`. Pass `executable`, `args`, `cwd`, and `env`
explicitly; an omitted environment is empty and does not inherit host
credentials.

`createNodeGraphCoderConnection(options)` composes that bridge with
`HarnessGraphCoderTransport` and returns both objects. The host must call
`connection.bridge.close()` when the runtime process should stop; process
launch, executable selection, and environment policy remain explicit host
decisions.

After building, the Windows PTY qualification can be run when Python's
`winpty` binding is installed:

```sh
python typescript/packages/graphcoder/test/pty_smoke.py
```

It drives the same command sequence through the headless and interactive
artifact paths and checks the visible lifecycle, approval, lazy file/diff,
and cancellation markers in both transcripts.

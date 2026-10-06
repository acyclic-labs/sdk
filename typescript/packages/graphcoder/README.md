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
graphcoder --fixture=deterministic "start op-demo-1 inspect the repository" list
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
`start_session(prompt, operation_id, model_fixture?)`, `open_session(session_id)`,
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

The durable local dispatcher maps these methods to the existing Harness
composition. `list_sessions` reads `PersistentLocalSwarm.sessions()` only;
`start_session` requires a caller supplied stable `operation_id`, admits that
identity, and calls `run_root(operation, prompt)`; reconnects must resend the
same operation ID. `open_session` reads one lazy descriptor and
hydrates its snapshot on demand; and `resume_session` calls `resume(task)`
before returning that snapshot. A task identity is the stable session and
agent identity for this local protocol, with its direct parent and depth
forming the agent tree. The dispatcher maps `Ready`, `Activating`,
`Completed`, and `Failed` to the public session states and retains an
explicit cancelled state in the local runtime's durable operation journal
when a run is cancelled.

The remaining endpoints must project the owner-owned durable records: activity
and messages come from the task journal and authenticated communication host,
approvals come from the interaction journal, and changes, generations, diffs,
and file bodies come from the Filesystem/Git facade at the requested pinned
generation. `approve_writeback` is the only root publication path and must
recheck the operation identity, session, approval, and expected generation
before invoking the typed facade operation. The caller allocates an operation
identity before admission and retries the same operation after recovery; the
dispatcher must not derive a new identity from each transport attempt.
These are projections over Harness and Filesystem state, rather than a second
GraphCoder storage or merge implementation. The exported
`GraphCoderWireParamsByMethod` and `GraphCoderWireResultByMethod` maps keep
native and JSON-lines dispatchers on this same schema.

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

Native hosts that own the durable runtime can expose the same contract with
`GraphCoderWireDispatcher` from the package root and
`runNodeGraphCoderDispatcher` from `@acyclic-labs/graphcoder/node-dispatcher`.
The dispatcher parses and validates each request, invokes the injected
transport, converts BigInts and byte bodies to the wire representation, and
returns typed errors. It does not open storage, start workers, or implement a
second merge path; the injected transport remains the LocalSwarm/Harness
owner. Requests are processed concurrently and correlated by `request_id`,
so a host can keep cancellation and recovery responsive while a turn runs.

The installed `graphcoder-native` command is an explicit launcher for a
packaged native runtime. Set `GRAPHCODER_RUNTIME` to the installed executable
and pass `--root` plus an explicit `--model-fixture`; the launcher starts it
with an empty environment and inherited standard streams.

Node hosts that explicitly own a local runtime executable can use
`@acyclic-labs/graphcoder/node`'s `JsonLineGraphCoderBridge`. It correlates
concurrent requests, rejects pending requests on process errors or EOF, bounds
one line, and reports stderr, malformed lines, unmatched responses, and exit
status through `onDiagnostic`. Pass `executable`, `args`, `cwd`, and `env`
explicitly; an omitted environment is empty and does not inherit host
credentials. Its default owner is supplied by the lazy
`@acyclic-labs/fs/native-process-node` host adapter; a native host can inject
the versioned owner from `@acyclic-labs/fs/native` instead. The fallback keeps
descendant cleanup explicitly uncertain when no native ownership proof exists.

`createNodeGraphCoderConnection(options)` composes that bridge with
`HarnessGraphCoderTransport` and returns both objects. The host must call
`connection.bridge.close()` when the runtime process should stop; process
launch, executable selection, and environment policy remain explicit host
decisions. Production callers can use `openNativeGraphCoderConnection` to
load and validate the native process owner before any runtime process is
spawned; it fails closed when the companion is unavailable.

After building, the Windows PTY qualification can be run when Python's
`winpty` binding is installed:

```sh
python typescript/packages/graphcoder/test/pty_smoke.py
```

It drives the same command sequence through the headless and interactive
artifact paths and checks the visible lifecycle, approval, lazy file/diff,
and cancellation markers in both transcripts.

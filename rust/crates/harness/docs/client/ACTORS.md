# Actor observations in TypeScript

`AgentClient` is the WASM export of the shared Rust client. Supply the runtime's
existing authenticated `WasmStream`, an agent path, exact conversation/status
cursors and its registered credential slot. Creating this client does not start
an agent runtime or another transport.

`next()` returns the generated Rust `AgentEvent` union. Narrow `kind` before
reading a conversation or status. `agentRecord(agent, index)` returns that same
union or `null` for a missing retained record. `agentStatus(agent)` returns the
canonical `StatusRecord` or `null` before delivery; a selected agent's absence is
never represented by an invented idle phase. An unselected agent is a typed
`not_requested` refusal.

`Phase`, `TurnInput`, `ConversationEntry`, `Sender` and terminal `AgentOutcome`
come from their production Rust serde definitions. A waiting approval includes
the actual request id in `phase.waiting_approval.id`. These observations confer
no authority to decide it. The owning host authenticates the human/parent and
uses its canonical decision and inbox publication path.

Conversation sequence, message inbox sequence, status cursor and alarm deadline
are `bigint`. `AgentFileRef` retains the ordinary generated file-reference shape
with a `bigint` descriptor byte length, matching this exact-integer serializer.
The older admitted-file projection's Number descriptor must not be assumed here.

Call `setView` with one focused agent followed by visible agents and matching
`BigUint64Array` cursor arrays. At most eight agents are admitted. Overlapping
agents keep their folds and cursors; deselection releases their records/follows.
Read one record or bounded window metadata for the visible frame. Output bodies
remain immutable file references and are fetched through the host's authorized
reader only when needed.

Input staging, approval request reads and decisions belong to the runtime host.
The browser host migration supplies those bindings; this observation adapter
does not implement another outbox, submission transport or approval policy.

The public TypeScript client no longer exports `FileClient`, `TaskVolume`,
`ClientQuery`, `ClientViews` or their workflow-file types. New consumers use the
actor observations above, Rust `PageWindow` for provider pages, and the owning
host for input and approval controls. Initialization requires those actor/page
exports without requiring the retired `WasmClientViews` class.

The old WASM FileClient, task-volume query and ClientViews adapters and their
mechanism-specific fixtures are removed. Recovery, ordering, publication,
credential admission, byte admission and document-restart custody still require
coverage through the actual actor host; the actor record observation smoke does
not qualify them. The browser paging and selective hydration assertions remain.

The browser host can return this same client with the Rust `from_session`
factory, sharing its authenticated session and credential slot. It does not
need another `WasmStream` or a polling reader.

The shared Rust `actor_error` adapter preserves indeterminate admission as an
`UnknownAgentError`: `name` is `agent_unknown` and `request` owns the original
canonical Streams `CommitRequest` protobuf bytes. The byte array is copied
before the borrowed WASM slice expires. The caller keeps its original intent
and this request; the client does not acknowledge or automatically resubmit it.
Browser host controls must use this adapter for this custody guarantee to apply.

A delivery is folded before `next()` resolves. Rendering a returned event has no
publication acknowledgement callback and cannot roll back the shared fold. A
failed render can read the retained record again; it must not advance a separate
UI cursor or submit the input again. Reopening from an explicit stream cursor
starts an empty bounded window at that edge. Reopening from zero replays records
into a new bounded window; it does not restore an unbounded history cache.

The adapter tests use canonical record appends only to exercise this boundary.
They do not qualify AgentHost creation, approval authority or durable submission.
The retired TaskVolume callback fold's buffered-publication/reconnect behavior
is not the actor client's contract. Durable command admission and recovery remain
host/Streams responsibilities, with their existing separate consumer tests.

# Native terminal presentation

`graphcoder` is an opt-in binary in `acyclic-harness` (`terminal` feature).
There is no second runtime, schema, journal, process bridge or workspace registry.
Without an explicitly selected fixture, it returns `unsupported`: main does not
yet supply the durable GraphCoder session/lifecycle host.

After building/installing the binary, `graphcoder --fixture wire` displays an
interactive prompt on a terminal; `--headless` reads the same commands from stdin.
`/next` displays one delivery, `/reset` resets only the display cursor, and `/quit`
exits. `/request PATH` reads a bounded Protobuf `ClientFrame`, with no shell
interpretation or credentials from the environment. The fixture rejects all
mutations. Ordinary input is explicitly **not queued**. Reopen, agent tree,
diff/approval operations, recursive execution and root writeback remain unavailable.

The fixture reuses `conformance/native-wasm-event-v2.json` without rewriting its
event bytes. Its banner always identifies offline playback. This is presentation
evidence only: no grants, persistence, recursion, model quality, authentic event
attestation or filesystem effects are qualified by playback. Historical payloads
remain uninterpreted; dedicated diff/approval layouts await owning public contracts.
Playback starts at the retained event's explicit predecessor cursor; it neither
fabricates the missing history nor claims to reopen a durable snapshot.

| Invariant | Mechanism | Assumptions | Verification |
|---|---|---|---|
| No command or cancellation is automatically retried | One shared `HarnessWireApi` dispatcher; no outbox | Supplied host upholds its wire contract | Indeterminate submit invocation count; identity mismatch rejection |
| Control is authorized before observe/cancel | Existing request validators and `authorize_operation_control` | Host owns keys and validates authority | Denied control invocation count zero |
| Admission is not completion | Unmodified typed admission/status rendering | Host outcome is authoritative outside fixture mode | Unknown state and error rendering |
| Inspection does not collect a live history | One delivery per request, at most 16 displayed events and 64 KiB encoded response | Host provides bounded, validated gapless deliveries; client cannot bound host allocation | Pending-tail replay and oversized-page rejection |
| Text cannot emit terminal control sequences | Control escaping on provider text; no ANSI output | Standard terminal interprets control sequences | ESC/OSC/BEL/CR and Unicode regression |
| Oversized input cannot become a second command | Bounded file/line read; abort on overflow | Native file/stdin behavior | Malformed/oversized request; installed stdin scenario |
| Fixture cannot admit effects | Empty negotiated capabilities and unsupported mutation/control methods | Explicit fixture selection | Playback cursor, unavailable submit and installed default-denial scenarios |

These are implementation/test obligations, not unrestricted correctness proofs.
`scripts/test-graphcoder.py ABSOLUTE_INSTALLED_BINARY` exercises the installed
headless presentation with an empty child `PATH`, finite process timeouts, explicit
fixture labels, page/reset counts, unavailable input and oversized-line failure.
It prints the actual binary/suite hashes, argv, exits and raw captured output for
retention by the qualification owner. It does not build or install an artifact.
Compiler, unit, installed headless, native PTY and platform evidence must be
retained for the exact source. Local builds and tests are authorized. Real runtime
and native/Chromium recursive qualification remain separate, mandatory obligations.

The remaining private code separates input/output, shared validated dispatch, and
the explicit fixture. The fixture's trait implementation is necessary to exercise
the same dispatch without inventing a product host. Existing validators and the
existing optional hex decoder are reused; no new dependency is introduced.
Response byte/event bounds are checked once, by the shared renderer before output
or display-cursor advancement; dispatch does not repeat presentation validation.
No main code was replaced, so production deletions are currently zero. Future
integration must remove fixture-only plumbing that becomes redundant, update
consumers, and repeat the production **and** validation simplification audit.

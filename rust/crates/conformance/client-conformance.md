For a live model task against any served tree, use `client-conformance` as
described in [GraphCoder on Actors](../../../docs/backends/hosted-swarm.md).
It selects an explicit endpoint before `ACYCLIC_ENDPOINT` and the managed default,
and reads `ACYCLIC_API_KEY` separately from its credential-free task scenario.

The `client-fixture-conformance` runner calls the public
`acyclic_conformance::client::verify<P: StreamProvider>` suite with the real
endpoint provider and its shared authenticated Streams session. The same
function accepts local providers and their existing in-process session adapter.
It creates no transport or agent execution loop.

The host first creates a disposable agent through AgentHost, settles one input,
and exports a JSON `client::Fixture`: `agent`, `conversation_from`, `status_from`,
`entry`, and `status`. Freeze that fixture while observing it. Each supplied
sequence must be the last record of its stream. The referenced files stay on
the actual host; this suite compares record references without loading outputs.

Configure these environment variables:

- `ACYCLIC_CONFORMANCE_ENDPOINT`: authenticated HTTPS Streams endpoint.
- `ACYCLIC_CONFORMANCE_TOKEN`: endpoint bearer credential.
- `ACYCLIC_CONFORMANCE_NAMESPACE`: the fixture's 32-character root agent id.
- `ACYCLIC_CONFORMANCE_AGENT_FIXTURE`: path to the exported fixture JSON.
- `ACYCLIC_CONFORMANCE_SESSION_CREDENTIAL`: hex-encoded canonical workload
  credential authorizing the fixture's conversation and global status streams.
- `ACYCLIC_CONFORMANCE_CA`: optional private CA file.

Invoke `cargo run --locked -p acyclic-conformance --features client-session
--bin client-fixture-conformance`. A successful invocation emits
six named observation cases. A provider refusal preserves its diagnostic and
failing step. There is no memory fallback, timeout wrapper or automatic retry.

These cases cover exact actor records, unchanged demand, atomic capacity
refusal, unrequested authority, and explicit reselection cursors. They do not
qualify actor execution, credit enforcement, outbox custody, PageWindow,
population bounds, WASM, or production mutation controls. No execution receipt
is implied by publishing this source under the temporary compile-only policy.

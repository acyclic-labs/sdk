# Original model response identity

`node rust/proofs/qualify.mjs native-call-identity` runs the existing real-server
HTTP control against two mutations of the actual response producer, then restores
the exact source and runs the same control. The oracle is hashed unchanged for
each phase. Removing request-origin metadata must fail the original-model
assertion; reusing one sequence must fail the distinct-response assertion.

The HTTP server reserves its existing recorder sequence before script admission
and encoding, in the same executor turn. All response envelopes for one request
use that ordinal, and the control joins each response back to its flushed request
record. Identity is scoped to this server's recorded run. The model comes from
the original request body, or the Gemini request path. Encoders without an
originating request leave identity and model null. No captured native-host
fixture is rewritten to supply these fields.

The same control exercises concurrent Responses, Chat, Anthropic and Gemini
requests, mismatches, tool results, and recorder ordering. The existing native
capability control separately retires the original server and requires listener
failure. No new wait or producer is introduced: the original recorder completion
still precedes HTTP publication, and recorder loss returns its existing error.

The required additional work is one checked increment instead of one unchecked
increment, one formatted response ID, and encoding the ID and borrowed model in
their protocol envelopes. A Responses message also derives its item ID from the
response ID. There are no additional IO operations, durability barriers, round
trips, tasks, locks, or history collections. Requested payload encoding remains
proportional to its bytes; physical allocation counts are not measured here.

This is a producer component control. It does not establish native-host per-call
publication, model attribution, tool execution, or cross-platform acceptance.
Those require an original native capture with source and compiler provenance,
then consumption through the shared normalization boundary. Terminal aggregate
counters must not be added to per-call observations.

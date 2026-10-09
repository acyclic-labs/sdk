# Incremental conversation history primitives

Canonical messages remain ordered, immutable references. Conversation mutation
goes through `ConversationState::append`; callers read the `messages()` slice.
Append validates the complete candidate before updating any derived index.

Identity, turn-outcome and model-position indexes are rebuilt from validated
messages on decode. They are omitted from serialization and confer no authority.
Decode rejects duplicate identities and non-increasing sequences. Sparse decoded
views support reads, but cannot admit new appends through the ordinary owner.

`page(after, through, maximum)` returns a borrowed bounded slice. A caller keeps
the same `through` cut across pages; later appends stay excluded. Zero bounds,
reversed cursors and cuts beyond the loaded tail fail explicitly. This is a page
over loaded history, not a storage cursor or bounded cold hydration mechanism.

Turn preparation validates one candidate against existing state and selects its
bounded model suffix without cloning all history. Indexed lookups replace scans
and per-selection maps. If a suffix retains a tool result without its exact call,
selection fails instead of silently dropping that result. The existing admission,
request schema, provider dispatch and journal remain authoritative.

Identity lookup costs O(log N), recent selection costs O(log N + K log N), and
page lookup costs O(log N) plus returned records for N loaded records and K selected
messages. These are source-derived bounds, not measured performance guarantees.
Resident messages and derived indexes remain O(N); rebuilding ordered indexes
on decode costs O(N log N). Historical native projection uses the existing
revision-aware API and borrows the conversation without cloning or truncating it.

Regressions cover index reconstruction, pinned multi-page traversal, duplicate
identity rejection, failed-append atomicity, sparse-view append rejection, turn
retry and selection provenance. Native and WASM execution require separate
qualification; a source review or running build is not a passing result.

The broader composition work remains open: storage cursors and checkpoints,
default compaction, immutable Summary-fork publication and receiving admission,
bounded lifetime reducer state and bounded default cold hydration. This focused
landing does not complete that goal.

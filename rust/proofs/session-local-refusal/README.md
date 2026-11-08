# Original local session refusals

Run `node rust/proofs/qualify.mjs session-local-refusal`.

The public in-process client opens an actor socket through the real session,
credential verification and socket admission. The test captures an actual OS
connection refusal at runtime and passes the shared status reader's original
error through prepare, open, outbound and inbound failures. Accepted data must
arrive before the original terminal error. The same oracle drives the actual
queue with zero credit, admits its original error, grants credit and requires
the earlier data followed by exactly one original terminal response.

The kept mutation discards the original error in `Frame::closed`; the unchanged
public-client assertion must reject it. Restored source also runs the existing
owner-retirement, clean socket completion, shutdown and indexed queue controls.
There is no timeout or polling: source output, input delivery and credit
transitions produce each awaited event on the same path. No alternate close
reader, error registry or drain queue is added.

The terminal path moves the original error through its existing channel and
control queue. It adds no payload copies, heap allocations, round trips,
durability barriers or waits. Calls with an input producer share the original
refusal through its existing reference-counted owner. The carrier adds constant
bookkeeping per terminal; the existing scheduling/index costs are unchanged.

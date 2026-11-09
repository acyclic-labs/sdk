# Codec context admission

Run `node rust/proofs/qualify.mjs stream-codec-path-admission`.

The existing writer-seed control submits append, fork and commit operations to
the production MemoryStream. It checks the emitted persisted and protobuf bytes
with the same oracle used by the retained `stream_persistence` fuzz target.
The oracle reports a refused context path as a failed check, without panicking
while constructing that context.

Two retained mutations supply an invalid followed or foreign context path.
The unchanged writer-seed control must reject each mutation; restored source
must pass. No decoder or generated record is replaced.

Context construction performs the same path validation and allocation as before.
Failure handling adds no allocation, copy, I/O or wait. This checks the original
writer corpus and oracle admission boundaries; it does not establish complete
instrumented fuzz coverage or distributed recovery.

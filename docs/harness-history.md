# Harness history and headless streams

History checkpoints pair a conversation cut with immutable volume generations.
Selecting history must publish the volume heads and conversation selection in
one conditional Streams commit. A conflict publishes neither. Checkpoints and
idle forks share structure; they never create Git snapshots or worktrees.

The public selection modes are context, files, and both. Files means captured
workspace volumes, including captured shell edits. Session-shared volumes,
children's independent work, uncaptured paths, processes, network requests and
other external side effects are outside file restoration. A restore must name
the observed current heads so concurrent edits yield a conflict.

Current checkpoint retention covers authored volume generations. Complete lazy
source, overlay and shadow cuts, together with pending shell-capture settlement,
are unfinished; authored-generation restoration does not yet restore a complete
lazy workspace.

Headless output projects the same committed streams as interactive clients.
Versioned JSONL envelopes carry the operation, source stream and replay cursor.
A reconnect resumes after the last emitted record; consumers deduplicate by
source stream and sequence. Pages and transport credit bound memory, not the
total history or session count. EOF, cancellation and follow delivery are events.

The v1 `execution` envelope borrows an original journal record, including its
turn `operation_id`, one-based `sequence`, retry identity and canonical event.
Its outer `operation` is the caller's exec identity. Model, tool and usage
payloads retain their immutable file references and read policy. The execution
query uses the same private journal admission and original inbox/journal/actor
waits as the usage projection; it exports no direct execution-stream grant.
Payload bodies, including live model deltas, use `execution_payload` envelopes
with the original immutable file identity, field and byte offset. Each read
selects the original observation under its private authority; a caller-supplied
file reference alone grants no read. Chunks contain at most 32 KiB before base64
encoding. The final chunk of the final referenced field alone acknowledges its
observation; interruption may repeat that observation and its flushed chunks.

An original waiting-approval status similarly selects its immutable request.
`approval_payload` envelopes identify its status resource, zero-based sequence,
complete length and byte offset. The status header and nonfinal chunks retain
the preceding outer cursor; the final chunk acknowledges the original status.
The canonical frame inside the header remains unchanged. Deduplicate by status
resource, sequence and offset. Reopening a saved cursor replays an interrupted
request, including after it has been decided. No query substitutes the current
pending approval or grants a private directory read.

JSONL approval input names `approve`, `approved` and `binding`. The binding's
`operation_id` and `action_digest` come unchanged from the original exported
request. The client submits that exact shown action through the existing
decision command, with no pending-request listing or retained decision history.
Missing or different bindings are refused. Each JSONL input names exactly one
prompt, approval or cancellation; cancellation requires `cancel: true`.
An archived recipient's `Closed` result is a conflict, never a successful
decision or cancellation. Exact committed decision retries retain their original
inbox receipt.

Each approval range reads one indexed status record, selects the agent's
original private authority, reads its current head and bounded publication
metadata, then reads the original authenticated content range. It adds one
control exchange and one bounded base64 allocation per chunk, with no write,
durability barrier, transcript scan or complete request allocation. The frontend
retains one status and one pinned range future beside stdin and cancellation.

Execution resume stores one selected agent, inbox input and journal continuation.
Retiring a completed input replaces that position rather than adding a cursor
for every old turn. Finite export includes the admitted opening inbox cut and
this invocation's accepted inputs. Only the actor's committed consumed-input
fence, with no retained earlier work, retires an input; a short page cannot do
so. The final ordinary stream drain opens after those inputs settle, so its
registration cuts include their jointly committed conversation records.
Nonzero offsets cannot cross an adoption cut or name an absent or control-only
input. These cursor errors are refused before opening a source wait.

One execution query reads at most eight original journal observations. It uses
one control exchange per page, ordinary journal authentication, and at most two
original producer follows for a quiet page. The frontend retains that same
in-flight future across stdin and other stream deliveries. It creates no task,
timer, repeated readiness request, per-input cursor collection or export store.
Each output line borrows its record into one serialized frame, waits on the
inherited output's original OS event, flushes, then atomically replaces the
optional cursor artifact. It adds no payload-tree clone or durability barrier.
The same asynchronous writer carries events, paged query replies and terminal
errors; a full pipe leaves the existing shutdown and diagnostic paths runnable.

Final-output projection validates strict JSON against the shared schema validator
before replacing an ordinary output artifact. Invalid structured output is a
terminal error, never a successful result. This result validation does not claim
provider-native constrained generation. Local and endpoint execution use the same
conversation client and `server::serve` path.

History queries return the newest entries first. The initial request captures
one tail and reads one bounded range ending there. Its opaque continuation
selects an older range under that same cut; appends do not invalidate it.
Requesting the initial page again selects a fresh cut. Work and retained memory
depend on the requested page, never on total history or descendant count.
Each row contains only `conversation`, `context_tail` and `volume_count`.
The source parses each descriptor without allocating its workspace names, roots
or paths; those remain at the original cut for restore and fork. Client response
validation checks the summary directly, without reserializing saved descriptors.

A progress-enabled primitive follow emits its fixed registration tail before
records. Session callers request this observation with `opening_cut` on an
unbounded ALL follow. Its watermark's `opening_tail` is separate from `after`:
observing a tail never acknowledges unread records. Watermark coalescing keeps
the opening cut. Replay drains start at their actual saved cursors and finish
once every selected source has delivered through its own fixed cut, including
when the saved cursor already equals the tail. Later appends cannot extend a
drain. Producer failure remains a typed stream failure.

The registration cut uses the existing subscription lock and snapshot. It adds
one scalar to the cursor and one requested control frame, with no extra tail
request, filesystem syscall, durability barrier or transport round trip. Plain
session follows request no extra control frame. JSONL output flushes before
cursor replacement; it introduces no per-record durability barrier. The history
query adds one tail request only on the initial page, one indexed read per
nonempty page, and bounded descriptor decoding and output copies. Older pages
perform no head read. No operation enumerates the preceding history.

Context controls open an output writer without opening an executor, constructing
model messages or expanding attachments. Restore reads its indexed descriptor;
rewind reads the latest descriptor and at most one indexed summary. Neither
reads the preceding conversation. Unchanged project generations reuse their
retained snapshot descriptor rather than creating another retention root.
File selection refuses when the saved and current access-policy revisions differ:
content-write authority cannot replace current access policy with an older one.

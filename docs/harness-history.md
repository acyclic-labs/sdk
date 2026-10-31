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

Headless output projects the same committed streams as interactive clients.
Versioned JSONL envelopes carry the operation, source stream and replay cursor.
A reconnect resumes after the last emitted record; consumers deduplicate by
source stream and sequence. Pages and transport credit bound memory, not the
total history or session count. EOF, cancellation and follow delivery are events.

Schema-constrained final output uses the shared provider schema contract and
ordinary output artifacts. Invalid structured output is a terminal error, never
a successful result. Local and endpoint execution use the same conversation
client and `server::serve` path.

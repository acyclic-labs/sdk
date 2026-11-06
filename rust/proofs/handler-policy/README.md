# Explicit handler deadline completion

Run `python3 rust/proofs/qualify.py handler-policy`.

The retained mutation restores the original runtime timeout refusal. The unchanged
property invokes three actual LocalStream actors through the public enqueue, drain,
wait and replay path, using the production platform timer with Tokio's virtual time.
It fails before waiting if the deadline leaves its original input without a terminal.
Restored production must return the exact typed deadline cause and original receipt,
keep the previous checkpoint, consume the input in the terminal's commit and preserve
that cut across a physical close and reopen.

The runtime reuses its existing deadline and failed-input publication. There is no
new timer, task, observer, scan, allocation or successful-path I/O. A deadline refusal
now makes the existing state and reply commit instead of returning without a reply.
Ordinary inputs still return the typed failure without consuming their record;
the existing manifest property retains that distinct contract under virtual time.
An omitted deadline remains unbounded, and cancellation and timer source failures
retain their original paths.

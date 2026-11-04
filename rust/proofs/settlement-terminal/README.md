# Settlement consumes the original terminal event

The local settlement helper observes the actual actor checkpoint and the
existing `hosting::Runtime::completion` event. Runtime failure has priority over
a retained checkpoint observation, including for a late waiter. The event is
produced by the actual serving task after active delivery cancellation; its
retained error also closes the authenticated Session. There is no new timer,
watcher, task or producer.

`lost_model_producer_closes_local_session_without_settling_input` starts the
real local host and Session, holds the model's dispatch on its owned channel,
and drops that producer. It retains the original queued input, zero completed
turns and original failure on usage, observation, commands and late clients.
The same control now checks that raw settlement waits while the producer is
live and returns its original error immediately after runtime completion.

Run `python3 rust/proofs/qualify.py settlement-terminal` from the repository
root. The kept mutation removes only the helper's completion join, preserving
the real serving runtime and every assertion. The unchanged control must fail
at `settlement bypassed the original runtime terminal event`; a timeout or
compile error does not qualify. The driver restores the exact source before
running the positive control.

This proves a failure-consumption boundary used by the native spawn-brief
control. It does not identify the cause of that control's macOS timeout or
qualify native projection there. Runtime completion uses two existing watch
receiver clones and a fixed-size selection; the actor's storage work is
unchanged. No population scan or repeated readiness check is introduced.

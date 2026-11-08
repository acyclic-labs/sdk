# Settlement observation controls

The existing real-child stock-executor control also runs the production
`agent::settled` over a forwarding provider that delays only the original inbox
head. All returned records come from that child's real stream. Its actor
checkpoint must be observed while the inbox read is pending, without a timer,
thread, retry or readiness loop. Releasing the one-shot finishes that original
read; dropping its sender returns `StreamError::Unavailable`. An actor refusal
must finish while the inbox producer is still held.

The same control then captures the actor checkpoint, publishes the child's
next actual input through the reopened host and releases the inbox observation.
The old checkpoint cannot settle the newer inbox cut. The host drains that
input and the existing exact-answer control checks both turns.

Apply each mutation separately, run this command and reverse the mutation:

```sh
cargo test --locked -p acyclic-harness-core --features filesystem,test-support --test main agent_model::wired::a_child_inherits_context_but_retains_its_own_observed_answer_across_turns -- --exact
```

| Mutation | Required diagnostic |
| --- | --- |
| `serial_settlement_observation.patch` | `actor checkpoint waited for inbox observation` |
| `settlement_waits_after_failure.patch` | `actor refusal waited for the inbox producer` |
| `settlement_accepts_older_inbox.patch` | `an older actor checkpoint covered a newer inbox cut` |

These controls complete by directly advancing the pending future once at each
explicit event; even a broken serialization fails an assertion immediately.
The settled-child observation uses two head requests and zero data requests,
independent of history length and child count. The two heads overlap rather
than forming a serial dependency. No provider request, allocation, copy or
durability barrier is added to the production path.

# Session connection observation retirement

`Client::with_transport` gives the original pool task ownership of handshake
publication. Retaining a client or a `Connections` observer does not retain that
task. Its destructor closes connection observation before releasing the rest of
the pool. A returned terminal error is retained before cleanup; a later destructor
cannot replace it. Counter exhaustion also closes parked observations.

The real client control captures the actual task submitted through the public
executor interface and cancels it both before and after its first poll. It keeps
the client and independent observers alive, cancels another registered wait, and
checks a terminal wake, typed refusal and late subscription without a timeout:

```sh
cargo test --locked -p acyclic-stream --features test-support --lib session::client::tests::connections::connection_observers_end_when_the_original_pool_driver_is_dropped -- --exact --nocapture
```

The Loom control calls the same production connection state machine. It races
terminal publication with registration, preserves the original refusal after
repeated close, checks cancellation isolation, and verifies exhaustion releases
its parked waiter. Its paired negative omits the terminal publication:

```sh
RUSTC_WORKSPACE_WRAPPER=./scripts/loom-rustc.sh cargo test --locked -p acyclic-stream --features model-checking --test main connection_retirement -- --nocapture
```

The retained patch removes the production task destructor's terminal event. Apply
it, run the real client control above, require failure at
`original pool driver dropped without waking its connection observer`, then
reverse the patch and require the same control to pass:

```sh
git apply rust/crates/stream/proofs/connection-retirement/producer-retirement.patch
# Run the real client control and require its named assertion to fail.
git apply -R rust/crates/stream/proofs/connection-retirement/producer-retirement.patch
# Run the real client control and require it to pass.
```

This proves finite producer retirement, not that a living remote peer responds.
Scheduler fairness remains required. Terminal publication adds one lock acquisition
and drains existing live waiters; it allocates no task, timer, payload copy or new
channel and performs no I/O. Work is constant per observer, O(live waiters) per
publication; registration remains O(log live waiters). Strict constant-cost
producer fanout and arbitrary blocking or panicking wake callbacks are not proved.

# Lost flush producers

`local::tests::failure::lost_flush_producer_terminates_original_waiters` submits
a real persistence worker holding the original journal lease and flush owner.
The worker unwinds before flushing. After its completion event, the original
publication waiter must return `Unavailable` with the retained `producer_lost`
cause. No timeout or readiness probe participates in the assertion.

Apply `omit-terminal.patch` to restore the former producer-loss behavior. The
same test must fail at its completion assertion; reverse the patch and require
the test to pass. Run with `cargo test --locked -p acyclic-stream --lib
--features local,test-support lost_flush_producer_terminates_original_waiters`.

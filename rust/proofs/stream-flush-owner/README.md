# Lost flush producers

`local::tests::failure::lost_flush_producer_terminates_original_waiters` starts
the production flush through `await_publication`. The shared seeded scheduler
holds that original submission at `native:submitted`, before worker execution.
After the caller detaches, another publication waiter remains pending. Retiring
the scheduler drops the queued producer and the owner created by `await_synced`;
the waiter must return `Unavailable` with the retained `producer_lost` cause.
No timeout or readiness probe participates in the assertion.

Apply `omit-terminal.patch` to restore the former producer-loss behavior. The
same test must fail at its completion assertion; reverse the patch and require
the test to pass. `omit-owner.patch` independently disables the production
owner wiring; the same assertion must fail. Run with `cargo test --locked -p acyclic-stream --lib
--features local,test-support lost_flush_producer_terminates_original_waiters`.

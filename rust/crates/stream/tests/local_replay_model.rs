//! Oracle checks inside Loom executions of the production durability handoff.
#![cfg(all(acyclic_loom, feature = "local"))]

use acyclic_test_support::differential::oracle::Oracle;

const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

#[test]
fn replay_and_flush_preserve_durability() {
    acyclic_stream::verification::check(|| {
        // Each case owns fresh journal bytes and the real provider's shim locks.
        // Put the oracle inside Loom so panics from the scheduler stay failures.
        let oracle = Oracle::declare(|_: &(), fault: Option<&()>, see| {
            let actual = if fault.is_some() {
                acyclic_stream::replay_durability_case::<false>()
            } else {
                acyclic_stream::replay_durability_case::<true>()
            }
            .expect("replay fixture infrastructure failed");
            see.see(actual)
        })
        .invariant("durable-replay", |actual| {
            if *actual == (1, 1, 1) {
                Ok(())
            } else {
                Err(format!("{actual:?} != (1, 1, 1)"))
            }
        })
        .control("durable-replay", (), ())
        .build();
        oracle.check(&());
        Ok(())
    });
}

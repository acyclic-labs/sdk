#[path = "../../src/registry/snapshot.rs"]
mod snapshot;
use crate::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn model(broken: bool, failed: bool) {
    loom::model(move || {
        let mut snapshot = snapshot::Snapshot::default();
        snapshot.live = broken;
        let snapshot = Arc::new(snapshot);
        let reads = Arc::new(AtomicUsize::new(0));
        let expected = if failed { Err("provider") } else { Ok((1, 2)) };
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let (snapshot, reads) = (snapshot.clone(), reads.clone());
            tasks.push(loom::thread::spawn(move || {
                snapshot
                    .read(
                        || {
                            reads.fetch_add(1, Ordering::SeqCst);
                            loom::thread::yield_now();
                            expected
                        },
                        |value| assert_eq!(*value, expected, "torn credential selection"),
                    )
                    .unwrap();
            }));
        }
        for task in tasks {
            task.join().unwrap();
        }
        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "credential sources observed twice"
        );
        let mut snapshot = Arc::try_unwrap(snapshot).ok().unwrap();
        snapshot.publish(Ok((3, 4)), false).unwrap();
        snapshot
            .read(|| unreachable!(), |value| assert_eq!(*value, expected))
            .unwrap();
        snapshot.publish(Ok((3, 4)), true).unwrap();
        snapshot
            .read(|| unreachable!(), |value| assert_eq!(*value, Ok((3, 4))))
            .unwrap();
    });
}

#[test]
fn concurrent_first_resolution_has_one_observation_and_atomic_refresh() {
    model(false, false);
    model(false, true);
}

#[test]
#[should_panic(expected = "credential sources observed twice")]
fn negative_control_detects_a_live_credential_cache() {
    model(true, false);
}

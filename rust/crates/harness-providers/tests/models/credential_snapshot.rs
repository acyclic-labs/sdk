#[path = "../../src/credentials/watched.rs"]
mod watched;
use crate::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use acyclic_auth::credential::{CredentialValidity, CredentialWatch};
type Observed = Option<Result<(u8, u8), &'static str>>;

fn model(broken: bool, failed: bool) {
    loom::model(move || {
        let revision = Arc::new(AtomicUsize::new(0));
        let events = revision.clone();
        let snapshot = Arc::new(watched::Watched::<Observed>::new(
            Vec::new(),
            Some(std::sync::Arc::new(move || {
                let observed = events.load(Ordering::SeqCst);
                let events = events.clone();
                CredentialWatch::from_custodian(
                    CredentialValidity {
                        now_unix_seconds: 0,
                        revision: Some([0; 32].into()),
                    },
                    futures::future::poll_fn(move |_| {
                        if broken || events.load(Ordering::SeqCst) != observed {
                            std::task::Poll::Ready(Ok(()))
                        } else {
                            std::task::Poll::Pending
                        }
                    }),
                )
            })),
        ));
        let reads = Arc::new(AtomicUsize::new(0));
        let expected = if failed { Err("provider") } else { Ok((1, 2)) };
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let (snapshot, reads) = (snapshot.clone(), reads.clone());
            tasks.push(loom::thread::spawn(move || {
                snapshot
                    .read(|value| {
                        let value = value.get_or_insert_with(|| {
                            reads.fetch_add(1, Ordering::SeqCst);
                            loom::thread::yield_now();
                            expected
                        });
                        assert_eq!(*value, expected, "torn credential selection");
                        Ok(())
                    })
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
        let (old, _) = snapshot
            .read(|value| {
                assert_eq!(*value, Some(expected));
                Ok(())
            })
            .unwrap();
        revision.store(1, Ordering::SeqCst);
        let (new, _) = snapshot
            .read(|value| {
                assert_eq!(*value, None, "stale credential generation");
                Ok(())
            })
            .unwrap();
        assert_ne!(old, new);
        snapshot.publish(old, Some(expected)).unwrap();
        snapshot
            .read(|value| {
                assert_eq!(*value, None, "stale publication won");
                Ok(())
            })
            .unwrap();
        snapshot.publish(new, Some(Ok((3, 4)))).unwrap();
        snapshot
            .read(|value| {
                assert_eq!(*value, Some(Ok((3, 4))));
                Ok(())
            })
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
fn negative_control_detects_unconditionally_invalidated_credentials() {
    model(true, false);
}

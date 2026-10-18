use super::*;
use futures::FutureExt as _;

fn property(progressed: bool, check: bool) {
    if check {
        assert!(progressed, "metadata await parked its coordinator owner");
    }
}

fn cut(broken: bool, check: bool) {
    // This is the production coordinator gate's shim. The IO-complete cut is
    // serial setup; only driving its parked owner releases the guard. The other
    // actor races that real release, query acquisition and query cancellation.
    // Lock order: coordinator gate only; callbacks run after its guard drops.
    let gate = Arc::new(crate::sync::Mutex::new(0u8));
    let held = loom::future::block_on(Box::pin(gate.lock()));
    let observer = loom::thread::spawn({
        let gate = gate.clone();
        move || {
            let value = loom::future::block_on(Box::pin(gate.lock()));
            assert!(*value <= 1, "torn metadata state");
        }
    });
    let mut jobs: FuturesUnordered<BoxProviderFuture<'_, Result<(Component, u64)>>> =
        FuturesUnordered::new();
    jobs.push(Box::pin(async move {
        drop(held);
        Ok((Component::new("gate-holder").unwrap(), 1))
    }));
    let parent = std::future::ready(Ok::<(), Error>(()));
    futures::pin_mut!(parent);
    let mut parent_done = false;
    let mut pending = VecDeque::new();
    let work = async {
        let mut value = gate.lock().await;
        *value += 1;
        *value
    };
    let output = if broken {
        work.now_or_never()
    } else {
        during(
            work,
            parent.as_mut(),
            &mut parent_done,
            &mut jobs,
            &mut pending,
        )
        .now_or_never()
    };
    if let Some(value) = output {
        assert_eq!(value, 1);
    }
    let progressed = parent_done && pending.len() == 2 && jobs.is_empty();
    let verdict = std::panic::catch_unwind(|| property(progressed, check));
    // Release the deliberately parked old owner before joining the observer,
    // so the negative control fails the property rather than a model timeout.
    drop(jobs);
    observer.join().unwrap();
    if let Err(payload) = verdict {
        std::panic::resume_unwind(payload);
    }
}

#[test]
fn metadata_await_drives_parked_coordinator_owners() {
    loom::model(|| cut(false, true));
    std::panic::catch_unwind(|| loom::model(|| cut(true, true)))
        .expect_err("missing background polling escaped the progress assertion");
    loom::model(|| cut(true, false));
}

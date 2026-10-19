use super::*;
use loom::{sync::Mutex as DataMutex, thread};
use std::task::{Context, Poll, Waker};

fn complete<F: Future>(future: F) -> F::Output {
    match std::pin::pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("model operation did not finish"),
    }
}

// Only the admission gate may contain provider locks, never the reverse.
// The modeled provider state stands in for the filesystem head and scheduler
// outcome; the actual gate, selection predicate and critical section are shared.
fn race(broken: bool, recovering_orphan: bool) {
    let gate = StateArc::new(Mutex::new(()));
    let state = StateArc::new(DataMutex::new((OperationId::from_bytes([1; 16]), true, 0)));
    let tasks: Vec<_> = [None, Some(OperationId::from_bytes([1; 16]))]
        .into_iter()
        .enumerate()
        .map(|(index, expected)| {
            let gate = gate.clone();
            let state = state.clone();
            thread::spawn(move || {
                let decision = async || {
                    let accepted = {
                        let state = state.lock().unwrap();
                        can_admit(Some(state.0), expected, state.1)
                    };
                    thread::yield_now();
                    if accepted {
                        let mut state = state.lock().unwrap();
                        state.0 = if recovering_orphan && expected.is_some() {
                            OperationId::from_bytes([1; 16])
                        } else {
                            OperationId::from_bytes([index as u8 + 2; 16])
                        };
                        state.1 = false;
                        state.2 += 1;
                    }
                };
                if broken {
                    complete(decision());
                } else {
                    complete(serialized(&gate, decision));
                }
            })
        })
        .collect();
    for task in tasks {
        task.join().unwrap();
    }
    assert_eq!(state.lock().unwrap().2, 1, "double admission");
    complete(serialized(&gate, async || {
        let state = state.lock().unwrap();
        assert!(!can_admit(
            Some(state.0),
            Some(OperationId::from_bytes([1; 16])),
            state.1
        ));
    }));
}

#[test]
fn settle_submit_and_replayed_settle_admit_once() {
    check(false);
}

#[test]
fn orphan_recovery_and_replacement_cannot_admit_two_active_turns() {
    check(true);
}

fn check(recovering_orphan: bool) {
    loom::model(move || race(false, recovering_orphan));
    let failure = std::panic::catch_unwind(|| loom::model(move || race(true, recovering_orphan)))
        .expect_err("unserialized negative control did not fail");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains("double admission"), "{message}");
}

// The actual admission/publication locks and HEAD-read boundary. After a
// publisher drops admission, its owned writer must finish before a reader
// samples the head. The data cell models the atomic filesystem publication.
fn cancelled_publication(skip_wait: bool) {
    let gate = StateArc::new(Mutex::new(()));
    let publication = StateArc::new(Mutex::new(()));
    let head = StateArc::new(DataMutex::new(false));
    let writer = complete(serialized(&gate, async || {
        let held = lock_owned(publication.clone()).await;
        let head = head.clone();
        thread::spawn(move || {
            let _held = held;
            thread::yield_now();
            *head.lock().unwrap() = true;
        })
    }));
    SKIP_PUBLICATION_WAIT.set(skip_wait);
    let observed = complete(serialized(&gate, async || {
        wait_for_publication(&publication).await;
        *head.lock().unwrap()
    }));
    writer.join().unwrap();
    assert!(observed, "HEAD read preceded cancelled write");
}

#[test]
fn cancelled_head_publication_precedes_the_next_head_read() {
    loom::model(|| cancelled_publication(false));
    let failure = std::panic::catch_unwind(|| loom::model(|| cancelled_publication(true)))
        .expect_err("missing publication wait was not detected");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("HEAD read preceded cancelled write"),
        "{message}"
    );
}

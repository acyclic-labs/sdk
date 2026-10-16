use super::{Arc, progress::Progress, signal::Signal};
use std::task::{Context, Poll};

fn cancellation(broken: bool) {
    loom::model(move || {
        let progress = Arc::new(Progress::new(0));
        let removed = Arc::new(Signal::new());
        let cancelling = (Arc::clone(&progress), Arc::clone(&removed));
        let cancelling = loom::thread::spawn(move || {
            if broken {
                cancelling.1.send(1_u8).unwrap();
                cancelling.0.cancel();
            } else {
                cancelling.0.cancel();
                cancelling.1.send(1_u8).unwrap();
            }
        });
        let (_, wake) = super::wake_count();
        let mut context = Context::from_waker(&wake);
        // The pool can handle removal before the already queued open command.
        let removed_first = removed.poll(&mut context).is_ready();
        let mut installed = !progress.cancelled();
        cancelling.join().unwrap();
        if let Poll::Ready(Ok(_)) = removed.poll(&mut context) {
            installed = false;
        }
        assert!(
            !installed,
            "cancellation must fence an open even after removal was handled"
        );
        if removed_first {
            assert!(progress.cancelled());
        }
    });
}

#[test]
fn cancellation_cannot_be_overtaken_by_a_queued_open() {
    cancellation(false);
}
#[test]
#[should_panic(expected = "cancellation must fence an open even after removal was handled")]
fn negative_control_notifies_removal_before_fencing_open() {
    cancellation(true);
}

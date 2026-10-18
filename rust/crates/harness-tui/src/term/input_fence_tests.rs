use super::*;
use loom::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Default)]
struct Native {
    state: Mutex<(bool, bool, bool)>,
    wake: Condvar,
}
impl Native {
    fn cancel(&self) {
        let mut state = self.state.lock().unwrap();
        // Windows does not queue cancellation before kernel admission.
        if state.0 {
            state.1 = true;
            self.wake.notify_all();
        }
    }
}

fn model(broken: bool) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(move || {
        let fence = Arc::new(InputFence::default());
        let restored = Arc::new(AtomicBool::new(false));
        let native = Arc::new(Native::default());
        let (admitted, observed) = loom::sync::mpsc::channel();
        let (reading, modes, kernel) = (
            Arc::clone(&fence),
            Arc::clone(&restored),
            Arc::clone(&native),
        );
        let reader = loom::thread::spawn(move || {
            reading
                .read(Arc::clone(&kernel), || {
                    admitted.send(()).unwrap();
                    loom::thread::yield_now();
                    let mut state = kernel.state.lock().unwrap();
                    state.0 = true;
                    while !state.1 && !state.2 {
                        state = kernel.wake.wait(state).unwrap();
                    }
                    assert!(!modes.load(Ordering::SeqCst), "read ran after restore");
                    Ok(())
                })
                .unwrap();
        });
        observed.recv().unwrap();
        if broken {
            // Removed behavior: signal once before kernel read admission and
            // restore without waiting for a cancelled read's acknowledgement.
            native.cancel();
            drop(fence.shut().unwrap());
        } else {
            fence.close(|native| native.cancel()).unwrap();
        }
        restored.store(true, Ordering::SeqCst);
        {
            // Release a broken reader so its invariant fails without hanging
            // the model runner; this wake is after restoration in both paths.
            let mut state = native.state.lock().unwrap();
            state.2 = true;
            native.wake.notify_all();
        }
        reader.join().unwrap();
        assert_eq!(
            fence
                .read(Arc::clone(&native), || Ok(()))
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotConnected
        );
    });
}

#[test]
fn cancellation_covers_kernel_admission_and_acknowledges_before_restore() {
    model(false);
}
#[test]
fn negative_control_detects_restore_before_read_acknowledgement() {
    assert!(std::panic::catch_unwind(|| model(true)).is_err());
}

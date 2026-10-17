use super::*;
use crate::term::lifecycle::{self, Arc, Mutex, State};
use loom::{
    sync::atomic::{AtomicBool, Ordering},
    thread,
};

struct Display(Arc<AtomicBool>);
impl Write for Display {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for command in bytes.windows(6) {
            if command == b"\x1b[?25h" {
                self.0.store(true, Ordering::SeqCst);
            }
            if command == b"\x1b[?25l" {
                self.0.store(false, Ordering::SeqCst);
            }
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct Output {
    state: Arc<Mutex<State<Display>>>,
    broken: bool,
}
impl Present for Output {
    fn present(&mut self, frame: &mut Vec<u8>, hidden: bool) -> io::Result<()> {
        lifecycle::active(&self.state, |guard| guard.present(frame, hidden))
    }
    fn outside_writes(&mut self) -> Option<u64> {
        lifecycle::active(&self.state, |guard| Ok(guard.outside_writes()))
            .ok()
            .flatten()
    }
    fn present_if_current(
        &mut self,
        frame: &mut Vec<u8>,
        hidden: bool,
        expected: Option<u64>,
    ) -> io::Result<Option<u64>> {
        if self.broken && frame.is_empty() {
            // Old commit returned before touching the lifecycle boundary.
            return Ok(expected);
        }
        lifecycle::present_if_current(&self.state, frame, hidden, expected)
    }
}

fn resume(state: &Mutex<State<Display>>) {
    lifecycle::restore(state, false, |_| Ok(())).unwrap();
    lifecycle::resume(state, |_| Ok(())).unwrap();
}

fn model(broken: bool, during_render: bool) {
    // State is the actual production lifecycle mutex through its loom shim.
    // Lock order: state only; the emulated device uses atomics, never another
    // lock. Rendering and worker joins happen outside that state lock.
    loom::model(move || {
        let visible = Arc::new(AtomicBool::new(true));
        let state = Arc::new(Mutex::new(State::new(Display(Arc::clone(&visible)))));
        let mut terminal = FrameTerminal::new(
            Output {
                state: Arc::clone(&state),
                broken,
            },
            Size::new(2, 2),
        )
        .unwrap();
        terminal.draw(Size::new(2, 2), |_| {}).unwrap();
        assert!(!visible.load(Ordering::SeqCst));
        if during_render {
            let resuming = Arc::clone(&state);
            terminal
                .draw(Size::new(2, 2), move |_| {
                    // Force the review's happens-before edge: resume completes
                    // after observation but before even an empty frame commits.
                    thread::spawn(move || resume(&resuming)).join().unwrap();
                })
                .unwrap();
        } else {
            let resuming = Arc::clone(&state);
            let worker = thread::spawn(move || resume(&resuming));
            let drawn = terminal.draw(Size::new(2, 2), |_| {});
            worker.join().unwrap();
            if let Err(error) = drawn {
                assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            }
            // A signal after a completed presentation legitimately changes
            // the device. The next frame must repair exactly that state.
            terminal.draw(Size::new(2, 2), |_| {}).unwrap();
        }
        assert!(
            !visible.load(Ordering::SeqCst),
            "resume before commit must not leave the cursor shown"
        );
        let mut guard = state.lock().unwrap();
        let count = guard.guard.outside_writes();
        assert_eq!(
            guard
                .guard
                .present_if_current(&mut Vec::new(), true, count)
                .unwrap(),
            count
        );
    });
}

#[test]
fn presentation_and_resume_use_the_real_lifecycle_mutex() {
    model(false, false);
}
#[test]
fn resume_after_the_render_snapshot_is_repaired() {
    model(false, true);
}
#[test]
fn negative_control_empty_frames_cannot_skip_the_lifecycle_boundary() {
    assert!(std::panic::catch_unwind(|| model(true, true)).is_err());
}

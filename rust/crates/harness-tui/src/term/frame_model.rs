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

struct Output<W: Write> {
    state: Arc<Mutex<State<W>>>,
    broken: bool,
    strip_attribute_reset: bool,
    strip_cell_repaint: bool,
}
impl<W: Write> Present for Output<W> {
    fn present(&mut self, frame: &mut Vec<u8>, effects: FrameEffects) -> io::Result<()> {
        lifecycle::active(&self.state, |guard| guard.present(frame, effects))
    }
    fn outside_writes(&mut self) -> Option<u64> {
        lifecycle::active(&self.state, |guard| Ok(guard.outside_writes()))
            .ok()
            .flatten()
    }
    fn present_if_current(
        &mut self,
        frame: &mut Vec<u8>,
        effects: FrameEffects,
        expected: Option<u64>,
    ) -> io::Result<Option<u64>> {
        if self.broken && frame.is_empty() {
            // Old commit returned before touching the lifecycle boundary.
            return Ok(expected);
        }
        if self.strip_attribute_reset && expected.is_none() && frame.starts_with(b"\x1b[0m") {
            // Keep the real checked lifecycle path and omit only its repair.
            frame.drain(..4);
        }
        if self.strip_cell_repaint && expected.is_none() {
            frame.retain(|&byte| byte != b'Z');
        }
        lifecycle::present_if_current(&self.state, frame, effects, expected)
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
                strip_attribute_reset: false,
                strip_cell_repaint: false,
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
                .present_if_current(
                    &mut Vec::new(),
                    FrameEffects {
                        cursor_hidden: true,
                        margins: false
                    },
                    count
                )
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

struct Colours {
    red: bool,
    painted_red: Arc<AtomicBool>,
}
impl Write for Colours {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut at = 0;
        while at < bytes.len() {
            if bytes[at..].starts_with(b"\x1b[31m") {
                self.red = true;
                at += 5;
            } else if bytes[at..].starts_with(b"\x1b[0m") {
                self.red = false;
                at += 4;
            } else if bytes[at..].starts_with(b"\x1b[39m") {
                self.red = false;
                at += 5;
            } else {
                if bytes[at] == b'b' {
                    self.painted_red.store(self.red, Ordering::SeqCst);
                }
                at += 1;
            }
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn attributes_model(broken: bool) {
    // The production state lock serializes the actual outside write and frame
    // presentation. Device state is private to that lock; only the observer is
    // atomic. No secondary lock is taken by the device or while joining workers.
    loom::model(move || {
        let painted_red = Arc::new(AtomicBool::new(false));
        let state = Arc::new(Mutex::new(State::new(Colours {
            red: false,
            painted_red: Arc::clone(&painted_red),
        })));
        let size = Size::new(2, 2);
        let mut terminal = FrameTerminal::new(
            Output {
                state: Arc::clone(&state),
                broken: false,
                strip_attribute_reset: broken,
                strip_cell_repaint: false,
            },
            size,
        )
        .unwrap();
        terminal
            .draw(size, |frame| {
                frame.render_widget(ratatui::widgets::Paragraph::new("a"), frame.area());
            })
            .unwrap();
        terminal
            .draw(size, |frame| {
                frame.render_widget(ratatui::widgets::Paragraph::new("b"), frame.area());
                let outside = Arc::clone(&state);
                thread::spawn(move || {
                    lifecycle::active(&outside, |guard| guard.writer().write_all(b"\x1b[31m"))
                        .unwrap();
                })
                .join()
                .unwrap();
            })
            .unwrap();
        assert!(
            !painted_red.load(Ordering::SeqCst),
            "checked recovery must reset attributes before cells"
        );
    });
}

#[test]
fn checked_presentation_recovers_attributes_under_the_real_lock() {
    attributes_model(false);
}
#[test]
fn negative_control_checked_recovery_cannot_omit_attributes() {
    assert!(std::panic::catch_unwind(|| attributes_model(true)).is_err());
}

struct Contents(Arc<loom::sync::atomic::AtomicUsize>);
impl Write for Contents {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for at in 0..bytes.len() {
            if bytes[at..].starts_with(b"\x1b[2J") {
                self.0.store(0, Ordering::SeqCst);
            } else if bytes[at] == b'Z' {
                self.0.store(1, Ordering::SeqCst);
            } else if bytes[at] == b'X' {
                self.0.store(2, Ordering::SeqCst);
            }
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn contents_model(broken: bool, during_render: bool) {
    // State is the real lifecycle mutex. Both writers use that same lock;
    // the cell observer is atomic and worker joins occur outside the lock.
    loom::model(move || {
        let cell = Arc::new(loom::sync::atomic::AtomicUsize::new(0));
        let state = Arc::new(Mutex::new(State::new(Contents(Arc::clone(&cell)))));
        let size = Size::new(2, 1);
        let mut terminal = FrameTerminal::new(
            Output {
                state: Arc::clone(&state),
                broken: false,
                strip_attribute_reset: false,
                strip_cell_repaint: broken,
            },
            size,
        )
        .unwrap();
        let paint = |frame: &mut Frame<'_>| {
            frame.render_widget(ratatui::widgets::Paragraph::new("Z"), frame.area());
        };
        terminal.draw(size, paint).unwrap();
        let write_outside = |outside: Arc<Mutex<State<Contents>>>| {
            lifecycle::active(&outside, |guard| guard.writer().write_all(b"\x1b[HX")).unwrap();
        };
        if during_render {
            terminal
                .draw(size, |frame| {
                    paint(frame);
                    let outside = Arc::clone(&state);
                    thread::spawn(move || write_outside(outside))
                        .join()
                        .unwrap();
                })
                .unwrap();
        } else {
            let outside = Arc::clone(&state);
            let worker = thread::spawn(move || write_outside(outside));
            terminal.draw(size, paint).unwrap();
            worker.join().unwrap();
        }
        // An outside write after presentation legitimately changes the cell.
        // Once that writer is joined, the next frame must restore its target.
        terminal.draw(size, paint).unwrap();
        assert_eq!(
            cell.load(Ordering::SeqCst),
            1,
            "retained contents must follow lifecycle invalidation"
        );
    });
}

#[test]
fn retained_cells_recover_under_the_real_lifecycle_lock() {
    for during_render in [false, true] {
        contents_model(false, during_render);
    }
}
#[test]
fn negative_control_checked_recovery_cannot_omit_retained_cells() {
    let failure = std::panic::catch_unwind(|| contents_model(true, false))
        .expect_err("omitted cell recovery must fail");
    let text = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        text.contains("retained contents must follow lifecycle invalidation"),
        "{text}"
    );
}

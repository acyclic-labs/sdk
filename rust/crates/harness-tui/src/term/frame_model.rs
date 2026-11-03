use super::*;
use crate::term::lifecycle::{self, Arc, Mutex, State};
use alacritty_terminal::{
    event::EventListener,
    grid::Dimensions,
    index::{Column, Line, Point},
    term::{Config, Term, TermMode},
    vte::ansi::Processor,
};
use loom::thread;

struct Grid;
impl Dimensions for Grid {
    fn total_lines(&self) -> usize {
        2
    }
    fn screen_lines(&self) -> usize {
        2
    }
    fn columns(&self) -> usize {
        2
    }
}

struct Events;
impl EventListener for Events {}

// Interpret the original production writes with the same independent terminal
// used by the native frame controls. Both interpreter and device state belong
// to the lifecycle lock; no second lock or parallel terminal model is needed.
struct Screen {
    term: Term<Events>,
    parser: Processor,
}
impl Screen {
    fn new() -> Self {
        Self {
            term: Term::new(Config::default(), &Grid, Events),
            parser: Processor::new(),
        }
    }

    fn first_cell(&self) -> &alacritty_terminal::term::cell::Cell {
        &self.term.grid()[Point::new(Line(0), Column(0))]
    }
}
impl Write for Screen {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        // A device may split a control sequence at any byte. The interpreter
        // retains framing across writes and the production frame is unchanged.
        for byte in bytes {
            self.parser
                .advance(&mut self.term, std::slice::from_ref(byte));
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

fn resume(state: &Mutex<State<Screen>>) {
    lifecycle::restore(state, false, |_| Ok(())).unwrap();
    lifecycle::resume(state, |_| Ok(())).unwrap();
}

fn model(broken: bool, during_render: bool) {
    // State is the actual production lifecycle mutex through its loom shim.
    // Rendering and worker joins happen outside that state lock.
    loom::model(move || {
        let state = Arc::new(Mutex::new(State::new(Screen::new())));
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
        assert!(
            !state
                .lock()
                .unwrap()
                .guard
                .writer()
                .term
                .mode()
                .contains(TermMode::SHOW_CURSOR)
        );
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
            !state
                .lock()
                .unwrap()
                .guard
                .writer()
                .term
                .mode()
                .contains(TermMode::SHOW_CURSOR),
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
#[should_panic(expected = "resume before commit must not leave the cursor shown")]
fn negative_control_empty_frames_cannot_skip_the_lifecycle_boundary() {
    model(true, true);
}

fn attributes_model(broken: bool) {
    // The production state lock serializes the actual outside write and frame
    // presentation. Device state is private to that lock. No secondary lock is
    // taken by the device or while joining workers.
    loom::model(move || {
        let state = Arc::new(Mutex::new(State::new(Screen::new())));
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
        let mut state = state.lock().unwrap();
        let cell = state.guard.writer().first_cell();
        assert_eq!(cell.c, 'b');
        assert_eq!(
            cell.fg,
            alacritty_terminal::vte::ansi::Color::Named(
                alacritty_terminal::vte::ansi::NamedColor::Foreground
            ),
            "checked recovery must reset attributes before cells"
        );
    });
}

#[test]
fn checked_presentation_recovers_attributes_under_the_real_lock() {
    attributes_model(false);
}
#[test]
#[should_panic(expected = "checked recovery must reset attributes before cells")]
fn negative_control_checked_recovery_cannot_omit_attributes() {
    attributes_model(true);
}

fn contents_model(broken: bool, during_render: bool) {
    // State is the real lifecycle mutex. Both writers use that same lock;
    // worker joins occur outside the lock.
    loom::model(move || {
        let state = Arc::new(Mutex::new(State::new(Screen::new())));
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
        let write_outside = |outside: Arc<Mutex<State<Screen>>>| {
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
            state.lock().unwrap().guard.writer().first_cell().c,
            'Z',
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
#[should_panic(expected = "retained contents must follow lifecycle invalidation")]
fn negative_control_checked_recovery_cannot_omit_retained_cells() {
    contents_model(true, false);
}

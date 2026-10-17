//! Frame presentation under faults: a write error, timeout or panic at any
//! byte of any frame leaves no synchronized update open and no hidden cursor
//! once the session is torn down, and an error closes the update at once.
#![cfg(feature = "tui")]

use std::cell::{Cell, RefCell};
use std::io::{self, Write};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;

use acyclic_harness_tui::ratatui::{Frame, layout::Size, text::Line, widgets::Paragraph};
use acyclic_harness_tui::term::{Capabilities, FrameTerminal, ModeState, Present, RestoreGuard};
use alacritty_terminal::event::EventListener;
use alacritty_terminal::grid::Dimensions;
use alacritty_terminal::term::{Config, Term, TermMode};
use alacritty_terminal::vte::ansi::Processor;

const SIZE: Size = Size::new(20, 4);

#[derive(Clone, Copy, Debug, PartialEq)]
enum Fault {
    /// One failed write (a timeout), then the device recovers.
    Transient,
    /// Every write fails until the session is torn down.
    Persistent,
    /// The writer unwinds once.
    Panic,
}

/// A device accepting bytes up to `at`, then failing as `fault` says.
struct Faulty {
    bytes: Rc<RefCell<Vec<u8>>>,
    at: usize,
    fault: Fault,
    spent: bool,
    healed: Rc<Cell<bool>>,
}

impl Write for Faulty {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut bytes = self.bytes.borrow_mut();
        if self.spent || self.healed.get() || bytes.len() < self.at {
            let room = if self.spent || self.healed.get() {
                buf.len()
            } else {
                buf.len().min(self.at - bytes.len())
            };
            bytes.extend_from_slice(&buf[..room]);
            return Ok(room);
        }
        drop(bytes);
        match self.fault {
            Fault::Transient => {
                self.spent = true;
                Err(io::ErrorKind::TimedOut.into())
            }
            Fault::Persistent => Err(io::ErrorKind::TimedOut.into()),
            Fault::Panic => {
                self.spent = true;
                panic!("injected writer panic");
            }
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The removed device path: bracket and write, with nothing journaled.
struct Unjournaled(Faulty);
impl Present for Unjournaled {
    fn present(&mut self, frame: &mut Vec<u8>, _cursor_hidden: bool) -> io::Result<()> {
        let bracketed = [b"\x1b[?2026h".as_slice(), frame, b"\x1b[?2026l"].concat();
        self.0.write_all(&bracketed)
    }
    fn outside_writes(&mut self) -> Option<u64> {
        None
    }
}

fn synchronized() -> Capabilities {
    let mut caps = Capabilities::default();
    caps.modes.insert(2026, ModeState::Reset);
    caps
}

/// Cursor hidden, hidden, then placed (shown).
fn scenes() -> [Option<(u16, u16)>; 3] {
    [None, None, Some((3, 1))]
}

fn paint(frame: &mut Frame<'_>, index: usize, cursor: Option<(u16, u16)>) {
    let lines = vec![Line::raw(format!("frame {index}")), Line::raw("transcript")];
    frame.render_widget(Paragraph::new(lines), frame.area());
    if let Some(position) = cursor {
        frame.set_cursor_position(position);
    }
}

#[derive(Clone, Copy)]
struct Grid;
impl Dimensions for Grid {
    fn total_lines(&self) -> usize {
        usize::from(SIZE.height)
    }
    fn screen_lines(&self) -> usize {
        usize::from(SIZE.height)
    }
    fn columns(&self) -> usize {
        usize::from(SIZE.width)
    }
}
#[derive(Clone, Copy)]
struct Events;
impl EventListener for Events {}

/// (synchronized update still open, cursor visible) after these bytes.
fn emulated(bytes: &[u8]) -> (bool, bool) {
    let mut term = Term::new(Config::default(), &Grid, Events);
    let mut parser: Processor = Processor::new();
    parser.advance(&mut term, bytes);
    let open = parser.sync_timeout().sync_timeout().is_some();
    (open, term.mode().contains(TermMode::SHOW_CURSOR))
}

/// Draw every scene with the fault at byte `at`, stopping at the first
/// failure, then tear the session down after the device recovers.
fn run<D: Present>(
    device: impl FnOnce(Faulty) -> D,
    at: usize,
    fault: Fault,
) -> (Vec<u8>, bool, bool) {
    let bytes = Rc::new(RefCell::new(Vec::new()));
    let healed = Rc::new(Cell::new(false));
    let faulty = Faulty {
        bytes: Rc::clone(&bytes),
        at,
        fault,
        spent: false,
        healed: Rc::clone(&healed),
    };
    let mut terminal = FrameTerminal::new(device(faulty), SIZE).unwrap();
    let mut failed = false;
    let mut closed_at_failure = true;
    for (index, cursor) in scenes().into_iter().enumerate() {
        let drawn = catch_unwind(AssertUnwindSafe(|| {
            terminal.draw(SIZE, |frame| paint(frame, index, cursor))
        }));
        if !matches!(drawn, Ok(Ok(()))) {
            failed = true;
            // A returned error must already have closed the update, unless
            // the device is still failing (then only teardown can).
            if fault == Fault::Transient {
                closed_at_failure = !emulated(&bytes.borrow()).0;
            }
            break;
        }
    }
    healed.set(true);
    drop(terminal);
    let bytes = bytes.take();
    let (open, visible) = emulated(&bytes);
    assert!(
        closed_at_failure,
        "a failed frame left its synchronized update open: {bytes:?}"
    );
    assert!(!open, "teardown left a synchronized update open: {bytes:?}");
    assert!(visible, "teardown left the cursor hidden: {bytes:?}");
    (bytes, failed, visible)
}

fn journaled(faulty: Faulty) -> RestoreGuard<Faulty> {
    RestoreGuard::new(faulty, &synchronized())
}

#[test]
fn every_fault_at_every_byte_closes_the_update_and_shows_the_cursor() {
    let (clean, failed, _) = run(journaled, usize::MAX, Fault::Transient);
    assert!(!failed);
    for fault in [Fault::Transient, Fault::Persistent, Fault::Panic] {
        for at in 0..=clean.len() {
            run(journaled, at, fault);
        }
    }
}

#[test]
fn normal_teardown_shows_a_hidden_cursor() {
    // Every scene hides the cursor; dropping the terminal must present
    // ratatui's restoration rather than discard it.
    let bytes = Rc::new(RefCell::new(Vec::new()));
    let faulty = Faulty {
        bytes: Rc::clone(&bytes),
        at: usize::MAX,
        fault: Fault::Transient,
        spent: false,
        healed: Rc::default(),
    };
    let mut terminal = FrameTerminal::new(journaled(faulty), SIZE).unwrap();
    terminal.draw(SIZE, |frame| paint(frame, 0, None)).unwrap();
    assert!(!emulated(&bytes.borrow()).1, "the frame hides the cursor");
    drop(terminal);
    assert_eq!(emulated(&bytes.borrow()), (false, true));
}

#[test]
#[should_panic(expected = "left")]
fn negative_control_unjournaled_frames_are_detected() {
    let clean = run(Unjournaled, usize::MAX, Fault::Transient).0;
    for fault in [Fault::Transient, Fault::Persistent, Fault::Panic] {
        for at in 0..=clean.len() {
            run(Unjournaled, at, fault);
        }
    }
}

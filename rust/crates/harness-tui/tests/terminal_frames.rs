//! Frame output: one device write per frame, showing exactly what the previous
//! per-command writes showed, optionally wrapped in synchronized output
//! (DECSET 2026); a frame identical to the screen writes nothing.
#![cfg(feature = "tui")]

use std::cell::RefCell;
use std::io::{self, Write};
use std::rc::Rc;

use acyclic_harness_tui::ratatui::{
    Frame, Terminal, TerminalOptions, Viewport,
    backend::CrosstermBackend,
    layout::{Rect, Size},
    text::Line,
    widgets::Paragraph,
};
use acyclic_harness_tui::term::{
    Capabilities, FrameTerminal, ModeState, Present, RestoreGuard, device_io,
};
use alacritty_terminal::event::EventListener;
use alacritty_terminal::grid::Dimensions;
use alacritty_terminal::index::{Column, Line as Row, Point};
use alacritty_terminal::term::{Config, Term, TermMode};
use alacritty_terminal::vte::ansi::Processor;
use proptest::prelude::*;

const WIDTH: u16 = 24;
const HEIGHT: u16 = 6;

/// Every device write call, kept separately.
#[derive(Clone, Default)]
struct Device(Rc<RefCell<Vec<Vec<u8>>>>);
impl Write for Device {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.borrow_mut().push(bytes.to_vec());
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Device {
    fn take(&self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.0.borrow_mut())
    }
}

type Scene = (Vec<String>, Option<(u16, u16)>);

fn paint(frame: &mut Frame<'_>, (rows, cursor): &Scene) {
    let lines: Vec<Line<'_>> = rows.iter().map(|row| Line::raw(row.as_str())).collect();
    frame.render_widget(Paragraph::new(lines), frame.area());
    if let Some(position) = cursor {
        frame.set_cursor_position(*position);
    }
}

/// The removed path: ratatui writing each queued command straight to the device.
fn per_command(scenes: &[Scene]) -> Vec<Vec<Vec<u8>>> {
    let device = Device::default();
    let mut terminal = Terminal::with_options(
        CrosstermBackend::new(device.clone()),
        TerminalOptions {
            viewport: Viewport::Fixed(Rect::new(0, 0, WIDTH, HEIGHT)),
        },
    )
    .unwrap();
    scenes
        .iter()
        .map(|scene| {
            terminal.draw(|frame| paint(frame, scene)).unwrap();
            device.take()
        })
        .collect()
}

/// Probed capabilities with synchronized output (DECSET 2026) known or not.
fn caps(synchronized: bool) -> Capabilities {
    let mut caps = Capabilities::default();
    if synchronized {
        caps.modes.insert(2026, ModeState::Reset);
    }
    caps
}

fn per_frame(scenes: &[Scene], synchronized: bool) -> Vec<Vec<Vec<u8>>> {
    let device = Device::default();
    let guard = RestoreGuard::new(device.clone(), &caps(synchronized));
    let mut terminal = FrameTerminal::new(guard, Size::new(WIDTH, HEIGHT)).unwrap();
    scenes
        .iter()
        .map(|scene| {
            terminal
                .draw(Size::new(WIDTH, HEIGHT), |frame| paint(frame, scene))
                .unwrap();
            device.take()
        })
        .collect()
}

/// Exact property every frame must satisfy: at most one device write.
fn assert_one_write_per_frame(frames: &[Vec<Vec<u8>>]) {
    for (index, writes) in frames.iter().enumerate() {
        assert!(
            writes.len() <= 1,
            "frame {index} reached the device {} times",
            writes.len()
        );
    }
}

#[derive(Clone, Copy)]
struct Grid;
impl Dimensions for Grid {
    fn total_lines(&self) -> usize {
        usize::from(HEIGHT)
    }
    fn screen_lines(&self) -> usize {
        usize::from(HEIGHT)
    }
    fn columns(&self) -> usize {
        usize::from(WIDTH)
    }
}
#[derive(Clone, Copy)]
struct Events;
impl EventListener for Events {}

fn screen(bytes: &[u8]) -> (String, (usize, usize), bool) {
    let mut term = Term::new(Config::default(), &Grid, Events);
    let mut parser: Processor = Processor::new();
    parser.advance(&mut term, bytes);
    // A synchronized update is applied when it ends; flush any pending batch.
    parser.stop_sync(&mut term);
    let mut text = String::new();
    for row in 0..usize::from(HEIGHT) {
        for column in 0..usize::from(WIDTH) {
            text.push(term.grid()[Point::new(Row(row as i32), Column(column))].c);
        }
        text.push('\n');
    }
    let cursor = term.grid().cursor.point;
    let shown = term.mode().contains(TermMode::SHOW_CURSOR);
    (text, (cursor.line.0 as usize, cursor.column.0), shown)
}

fn scenes() -> impl Strategy<Value = Vec<Scene>> {
    prop::collection::vec(
        (
            prop::collection::vec("[a-z日🙂 ]{0,30}", 0..=usize::from(HEIGHT)),
            prop::option::of((0..WIDTH, 0..HEIGHT)),
        ),
        1..8,
    )
}

/// Exact property of a repeated scene: the screen already shows it, so the
/// frame reaches no device.
fn assert_repeats_write_nothing(frames: &[Vec<Vec<u8>>]) {
    for (index, writes) in frames.iter().enumerate().skip(1).step_by(2) {
        assert!(
            writes.is_empty(),
            "repeated frame {index} restated the screen in {} writes",
            writes.len()
        );
    }
}

/// Every scene drawn twice in a row.
fn doubled(scenes: &[Scene]) -> Vec<Scene> {
    scenes
        .iter()
        .flat_map(|scene| [scene.clone(), scene.clone()])
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    // Property: each frame reaches the device in at most one write, wrapped in
    // 2026 only when supported; a repeated scene writes nothing; and after
    // every frame the emulated screen, cursor and cursor visibility equal the
    // per-command path's, which restates the cursor on every draw.
    #[test]
    fn frames_are_one_write_and_repeats_none(scenes in scenes()) {
        let scenes = doubled(&scenes);
        let old = per_command(&scenes);
        let plain = per_frame(&scenes, false);
        let synchronized = per_frame(&scenes, true);
        assert_one_write_per_frame(&plain);
        assert_one_write_per_frame(&synchronized);
        assert_repeats_write_nothing(&plain);
        assert_repeats_write_nothing(&synchronized);
        let (mut old_screen, mut new_screen) = (Vec::new(), Vec::new());
        for ((old, plain), synchronized) in old.iter().zip(&plain).zip(&synchronized) {
            let plain = plain.concat();
            let wrapped = if plain.is_empty() {
                Vec::new()
            } else {
                [b"\x1b[?2026h".as_slice(), &plain, b"\x1b[?2026l"].concat()
            };
            prop_assert_eq!(&synchronized.concat(), &wrapped);
            old_screen.extend(old.concat());
            new_screen.extend(plain);
            prop_assert_eq!(screen(&new_screen), screen(&old_screen));
        }
    }
}

#[test]
#[should_panic(expected = "restated the screen")]
fn negative_control_per_command_repeats_restate_the_cursor() {
    let scenes = doubled(&[(vec!["hello".into()], Some((2, 0)))]);
    assert_repeats_write_nothing(&per_command(&scenes));
}

/// Device writes and bytes of each frame after the first, exactly.
fn costs(scenes: &[Scene]) -> Vec<(usize, Vec<u8>)> {
    per_frame(scenes, false)
        .into_iter()
        .skip(1)
        .map(|writes| (writes.len(), writes.concat()))
        .collect()
}

#[test]
fn only_what_changed_reaches_the_device() {
    let rows = vec!["hello".to_string(), "world".to_string()];
    let base: Scene = (rows.clone(), Some((2, 1)));
    let moved: Scene = (rows.clone(), Some((3, 1)));
    let hidden: Scene = (rows.clone(), None);
    let edited: Scene = (vec!["hellO".into(), "world".into()], Some((2, 1)));
    for n in [8, 16] {
        // N identical frames: no write and no byte, at N and 2N.
        let costs = costs(&vec![base.clone(); n + 1]);
        assert_eq!(costs, vec![(0, Vec::new()); n]);
    }
    // A cursor move is the move alone; hiding is the hide alone.
    assert_eq!(
        costs(&[base.clone(), moved, hidden.clone(), hidden]),
        vec![
            (1, b"\x1b[2;4H".to_vec()),
            (1, b"\x1b[?25l".to_vec()),
            (0, Vec::new())
        ]
    );
    // One changed cell, then the cursor placed back where it was.
    let (writes, bytes) = costs(&[base, edited]).remove(0);
    assert_eq!(writes, 1);
    assert!(bytes.starts_with(b"\x1b[1;5HO"), "{bytes:?}");
    assert!(bytes.ends_with(b"\x1b[2;3H"), "{bytes:?}");
    assert!(!bytes.windows(6).any(|window| window == b"\x1b[?25h"));
}

#[test]
fn steady_frames_allocate_exactly_as_the_per_command_path() {
    let scene = |n: usize| -> Scene {
        (
            (0..HEIGHT)
                .map(|row| format!("row {row} frame {n}"))
                .collect(),
            Some((1, 1)),
        )
    };
    let mut old = Terminal::with_options(
        CrosstermBackend::new(io::sink()),
        TerminalOptions {
            viewport: Viewport::Fixed(Rect::new(0, 0, WIDTH, HEIGHT)),
        },
    )
    .unwrap();
    let mut new = FrameTerminal::new(
        RestoreGuard::new(io::sink(), &caps(true)),
        Size::new(WIDTH, HEIGHT),
    )
    .unwrap();
    // Warm the retained frame buffer with a full repaint.
    old.draw(|frame| paint(frame, &scene(0))).unwrap();
    new.draw(Size::new(WIDTH, HEIGHT), |frame| paint(frame, &scene(0)))
        .unwrap();
    for n in 1..4 {
        let old = acyclic_test_support::measure(|| {
            old.draw(|frame| paint(frame, &scene(n))).unwrap();
        });
        let new = acyclic_test_support::measure(|| {
            new.draw(Size::new(WIDTH, HEIGHT), |frame| paint(frame, &scene(n)))
                .unwrap();
        });
        assert_eq!(new.count_total, old.count_total, "frame {n}");
    }
    // Repeated frames do the same render work and skip one allocation each,
    // at N and 2N: crossterm formats the elided attribute reset into a String.
    for repeats in [4, 8] {
        let old = acyclic_test_support::measure(|| {
            for _ in 0..repeats {
                old.draw(|frame| paint(frame, &scene(3))).unwrap();
            }
        });
        let new = acyclic_test_support::measure(|| {
            for _ in 0..repeats {
                new.draw(Size::new(WIDTH, HEIGHT), |frame| paint(frame, &scene(3)))
                    .unwrap();
            }
        });
        assert_eq!(
            new.count_total + repeats,
            old.count_total,
            "{repeats} repeats"
        );
    }
}

#[test]
fn resize_repaints_in_one_write() {
    let device = Device::default();
    let guard = RestoreGuard::new(device.clone(), &caps(true));
    let mut terminal = FrameTerminal::new(guard, Size::new(WIDTH, HEIGHT)).unwrap();
    let scene: Scene = (vec!["before".into()], None);
    terminal
        .draw(Size::new(WIDTH, HEIGHT), |frame| paint(frame, &scene))
        .unwrap();
    device.take();
    terminal
        .draw(Size::new(WIDTH - 4, HEIGHT), |frame| paint(frame, &scene))
        .unwrap();
    let writes = device.take();
    assert_eq!(writes.len(), 1);
    // The clear for the new size and the repaint share the synchronized update.
    assert!(writes[0].starts_with(b"\x1b[?2026h"));
    assert!(writes[0].ends_with(b"\x1b[?2026l"));
    assert!(writes[0].windows(4).any(|bytes| bytes == b"\x1b[2J"));
}

#[test]
#[should_panic(expected = "reached the device")]
fn negative_control_per_command_writes_are_detected() {
    let scenes = vec![(vec!["hello".into(), "world".into()], Some((2, 1)))];
    assert_one_write_per_frame(&per_command(&scenes));
}

/// Submissions reaching the real terminal output half: writes for the old
/// path, presented frames for the new one. Device calls beneath them (PTY-full
/// retries, console parts) are counted at the device by `device_io`.
struct Calls<W>(W, Rc<std::cell::Cell<u64>>, Rc<std::cell::Cell<u64>>);
impl<W: Write> Write for Calls<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.1.set(self.1.get() + 1);
        let written = self.0.write(bytes)?;
        self.2.set(self.2.get() + written as u64);
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}
impl<W: Present> Present for Calls<W> {
    fn present(&mut self, frame: &mut Vec<u8>, cursor_hidden: bool) -> io::Result<()> {
        self.1.set(self.1.get() + 1);
        self.2.set(self.2.get() + frame.len() as u64);
        self.0.present(frame, cursor_hidden)
    }
    fn outside_writes(&mut self) -> Option<u64> {
        self.0.outside_writes()
    }
}

#[cfg(unix)]
fn real_terminal() -> io::Result<(acyclic_harness_tui::term::Output, Box<dyn std::any::Any>)> {
    use std::io::Read;
    let pty = rustix_openpty::openpty(None, None)?;
    let mut controller = std::fs::File::from(pty.controller);
    // The kernel PTY is drained as fast as an emulator would read it.
    let drain = std::thread::spawn(move || {
        let mut bytes = [0; 65536];
        while matches!(controller.read(&mut bytes), Ok(count) if count > 0) {}
    });
    let (tty, output) =
        acyclic_harness_tui::term::Tty::from_file(std::fs::File::from(pty.user))?.split();
    Ok((output, Box::new((tty, drain))))
}

#[cfg(windows)]
fn real_terminal() -> io::Result<(acyclic_harness_tui::term::Output, Box<dyn std::any::Any>)> {
    // The attached console (conhost or ConPTY) with VT processing enabled.
    let (tty, output) = acyclic_harness_tui::term::Tty::open()?.split();
    Ok((output, Box::new(tty)))
}

const PROSE: &str = "prose with `inline code`, wrapped words and tool output lines that differ on every transcript row of the page";

fn scrolled(offset: usize) -> impl FnOnce(&mut Frame<'_>) {
    use acyclic_harness_tui::ratatui::{
        style::{Color, Style, Stylize},
        text::Span,
    };
    move |frame| {
        let height = usize::from(frame.area().height);
        let lines: Vec<Line<'_>> = (offset..offset + height)
            .map(|row| {
                Line::from(vec![
                    Span::styled(
                        format!("{row:>6} "),
                        Style::default().fg(Color::Rgb(120, 120, 140)),
                    ),
                    Span::raw("Streaming ").bold(),
                    Span::styled("markdown", Style::default().fg(Color::Rgb(200, 120, 60))),
                    // Distinct text per row, as when paging through a transcript.
                    Span::raw(format!(" {}", &PROSE[row % 25..row % 25 + 80])),
                ])
            })
            .collect();
        frame.render_widget(Paragraph::new(lines), frame.area());
        frame.set_cursor_position((2, frame.area().height - 1));
    }
}

/// Real terminal cost per page-scroll frame (every row repainted): `cargo test --release -p acyclic-harness-tui
/// --features tui --test terminal_frames real_terminal -- --ignored --nocapture`.
#[test]
#[ignore = "real terminal measurement, not an assertion"]
fn real_terminal_frame_cost() -> io::Result<()> {
    const FRAMES: usize = 200;
    let mut report = Vec::new();
    // A process owns one terminal lifecycle; both paths share it.
    let (mut output, _guard) = real_terminal()?;
    for size in [Size::new(120, 40), Size::new(240, 70)] {
        let viewport = TerminalOptions {
            viewport: Viewport::Fixed(Rect::new(0, 0, size.width, size.height)),
        };
        for per_command in [true, false] {
            let calls = Rc::default();
            let bytes = Rc::default();
            let device = Calls(&mut output, Rc::clone(&calls), Rc::clone(&bytes));
            let before = device_io();
            let start = std::time::Instant::now();
            if per_command {
                let mut terminal =
                    Terminal::with_options(CrosstermBackend::new(device), viewport.clone())?;
                for offset in 0..FRAMES {
                    terminal.draw(scrolled(offset * usize::from(size.height)))?;
                }
            } else {
                let mut terminal = FrameTerminal::new(device, size)?;
                for offset in 0..FRAMES {
                    terminal.draw(size, scrolled(offset * usize::from(size.height)))?;
                }
            }
            let elapsed = start.elapsed();
            let after = device_io();
            let per_frame = |count: u64| count as f64 / FRAMES as f64;
            report.push(format!(
                "{} {}x{} {}: {:.0} bytes/frame, {:.1} submissions/frame, {:.1} device writes/frame, {:.2} full-queue waits/frame, {:.1} us/frame",
                std::env::consts::OS,
                size.width,
                size.height,
                if per_command { "per-command" } else { "frame" },
                per_frame(bytes.get()),
                per_frame(calls.get()),
                per_frame(after.writes - before.writes),
                per_frame(after.waits - before.waits),
                elapsed.as_secs_f64() * 1e6 / FRAMES as f64
            ));
        }
    }
    for line in report {
        eprintln!("{line}");
    }
    Ok(())
}

/// A frame far beyond any realistic screen (16 MiB) reaches the real console:
/// bounded calls each finish within their own deadline. One unbounded call
/// of 4 MiB measured 110 ms against the 100 ms deadline and timed out.
#[cfg(windows)]
#[test]
#[ignore = "needs an attached console: run with --ignored"]
fn very_large_frames_reach_the_real_console() -> io::Result<()> {
    let (mut output, _guard) = real_terminal()?;
    for mib in [1usize, 4, 16] {
        let mut frame = Vec::new();
        let mut row = 0u32;
        while frame.len() < mib * 1024 * 1024 {
            frame.extend_from_slice(format!("\x1b[{};1H", row % 40 + 1).as_bytes());
            for cell in 0..100u32 {
                let glyph = char::from(b'a' + (cell % 26) as u8);
                frame.extend_from_slice(
                    format!("\x1b[38;2;{cell};{};90m{glyph}", row % 256).as_bytes(),
                );
            }
            row += 1;
        }
        let before = device_io();
        let start = std::time::Instant::now();
        output.present(&mut frame, false)?;
        let elapsed = start.elapsed();
        eprintln!(
            "windows {mib} MiB frame: {:.1} ms, {} console writes",
            elapsed.as_secs_f64() * 1e3,
            device_io().writes - before.writes
        );
    }
    output.write_all(b"\x1b[0m\x1b[2J")
}

/// A guard shared with the test, so the session's lifecycle (suspend, resume,
/// direct writes) can reach the device between frames, as `Output` does.
#[derive(Clone)]
struct Shared(Rc<RefCell<RestoreGuard<Device>>>, bool);
impl Present for Shared {
    fn present(&mut self, frame: &mut Vec<u8>, cursor_hidden: bool) -> io::Result<()> {
        self.0.borrow_mut().present(frame, cursor_hidden)
    }
    fn outside_writes(&mut self) -> Option<u64> {
        // The negative control reports an unchanging count: outside writes go
        // unseen, as before tracking was tied to the lifecycle path.
        if self.1 {
            self.0.borrow_mut().outside_writes()
        } else {
            Some(0)
        }
    }
}

/// The screen after a frame, an outside access, and the same frame again
/// (at `resized` size when given).
fn after_outside_access(
    scene: &Scene,
    tracked: bool,
    resized: Option<Size>,
    outside: impl FnOnce(&mut RestoreGuard<Device>),
) -> (String, (usize, usize), bool) {
    let device = Device::default();
    let guard = Rc::new(RefCell::new(RestoreGuard::new(
        device.clone(),
        &caps(false),
    )));
    let size = Size::new(WIDTH, HEIGHT);
    let mut terminal = FrameTerminal::new(Shared(Rc::clone(&guard), tracked), size).unwrap();
    terminal.draw(size, |frame| paint(frame, scene)).unwrap();
    outside(&mut guard.borrow_mut());
    terminal
        .draw(resized.unwrap_or(size), |frame| paint(frame, scene))
        .unwrap();
    screen(&device.take().concat())
}

#[test]
fn suspend_and_resume_never_leave_a_hidden_cursor_shown() {
    let hidden: Scene = (vec!["prompt".into()], None);
    // Suspend restores the cursor (shown); resume re-enters the session.
    let suspend = |guard: &mut RestoreGuard<Device>| {
        guard.restore().unwrap();
        guard
            .setup(acyclic_harness_tui::term::ScreenMode::Inline)
            .unwrap();
    };
    for resized in [None, Some(Size::new(WIDTH, HEIGHT - 1))] {
        let (_, _, shown) = after_outside_access(&hidden, true, resized, suspend);
        assert!(!shown, "resized={resized:?}");
        // Negative control: with outside writes unseen, the hide is elided.
        let (_, _, shown) = after_outside_access(&hidden, false, None, suspend);
        assert!(shown);
    }
}

#[test]
fn an_outside_cursor_move_is_corrected_by_an_unchanged_frame() {
    let placed: Scene = (vec!["prompt".into()], Some((3, 2)));
    let move_home = |guard: &mut RestoreGuard<Device>| {
        guard.writer().write_all(b"\x1b[1;1H").unwrap();
    };
    let (_, cursor, shown) = after_outside_access(&placed, true, None, move_home);
    assert_eq!((cursor, shown), ((2, 3), true));
    // Negative control: with outside writes unseen, the move is elided and
    // the cursor stays where the outside write left it.
    let (_, cursor, _) = after_outside_access(&placed, false, None, move_home);
    assert_eq!(cursor, (0, 0));
}

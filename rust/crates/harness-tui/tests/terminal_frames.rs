//! Frame output: one device write per frame, byte-identical to the previous
//! per-command writes, optionally wrapped in synchronized output (DECSET 2026).
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
use acyclic_harness_tui::term::{Capabilities, FrameTerminal, ModeState, Present, RestoreGuard};
use alacritty_terminal::event::EventListener;
use alacritty_terminal::grid::Dimensions;
use alacritty_terminal::index::{Column, Line as Row, Point};
use alacritty_terminal::term::{Config, Term};
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
        assert!(writes.len() <= 1, "frame {index} reached the device {} times", writes.len());
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

fn screen(bytes: &[u8]) -> (String, (usize, usize)) {
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
    (text, (cursor.line.0 as usize, cursor.column.0))
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    // Property: each frame reaches the device in at most one write whose bytes
    // equal the old per-command writes, wrapped in 2026 only when supported,
    // and the emulated screen and cursor are identical.
    #[test]
    fn frames_are_one_write_with_identical_bytes(scenes in scenes()) {
        let old = per_command(&scenes);
        let plain = per_frame(&scenes, false);
        let synchronized = per_frame(&scenes, true);
        assert_one_write_per_frame(&plain);
        assert_one_write_per_frame(&synchronized);
        for ((old, plain), synchronized) in old.iter().zip(&plain).zip(&synchronized) {
            let old = old.concat();
            prop_assert_eq!(&plain.concat(), &old);
            let wrapped = if old.is_empty() {
                Vec::new()
            } else {
                [b"\x1b[?2026h".as_slice(), &old, b"\x1b[?2026l"].concat()
            };
            prop_assert_eq!(&synchronized.concat(), &wrapped);
        }
        let old: Vec<u8> = old.concat().concat();
        prop_assert_eq!(screen(&synchronized.concat().concat()), screen(&old));
    }
}

#[test]
fn steady_frames_allocate_exactly_as_the_per_command_path() {
    let scene = |n: usize| -> Scene {
        ((0..HEIGHT).map(|row| format!("row {row} frame {n}")).collect(), Some((1, 1)))
    };
    let mut old = Terminal::with_options(
        CrosstermBackend::new(io::sink()),
        TerminalOptions {
            viewport: Viewport::Fixed(Rect::new(0, 0, WIDTH, HEIGHT)),
        },
    )
    .unwrap();
    let mut new =
        FrameTerminal::new(RestoreGuard::new(io::sink(), &caps(true)), Size::new(WIDTH, HEIGHT))
            .unwrap();
    // Warm the retained frame buffer with a full repaint.
    old.draw(|frame| paint(frame, &scene(0))).unwrap();
    new.draw(Size::new(WIDTH, HEIGHT), |frame| paint(frame, &scene(0))).unwrap();
    for n in 1..4 {
        let old = acyclic_test_support::measure(|| {
            old.draw(|frame| paint(frame, &scene(n))).unwrap();
        });
        let new = acyclic_test_support::measure(|| {
            new.draw(Size::new(WIDTH, HEIGHT), |frame| paint(frame, &scene(n))).unwrap();
        });
        assert_eq!(new.count_total, old.count_total, "frame {n}");
    }
}

#[test]
fn resize_repaints_in_one_write() {
    let device = Device::default();
    let guard = RestoreGuard::new(device.clone(), &caps(true));
    let mut terminal = FrameTerminal::new(guard, Size::new(WIDTH, HEIGHT)).unwrap();
    let scene: Scene = (vec!["before".into()], None);
    terminal.draw(Size::new(WIDTH, HEIGHT), |frame| paint(frame, &scene)).unwrap();
    device.take();
    terminal.draw(Size::new(WIDTH - 4, HEIGHT), |frame| paint(frame, &scene)).unwrap();
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

/// Device calls reaching the real terminal output half: writes for the old
/// path, presented frames for the new one.
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
    let (tty, output) = acyclic_harness_tui::term::Tty::from_file(std::fs::File::from(pty.user))?.split();
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
                    Span::styled(format!("{row:>6} "), Style::default().fg(Color::Rgb(120, 120, 140))),
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
            report.push(format!(
                "{} {}x{} {}: {:.0} bytes/frame, {:.1} device writes/frame, {:.1} us/frame",
                std::env::consts::OS,
                size.width,
                size.height,
                if per_command { "per-command" } else { "frame" },
                bytes.get() as f64 / FRAMES as f64,
                calls.get() as f64 / FRAMES as f64,
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
            frame.extend_from_slice(format!("[{};1H", row % 40 + 1).as_bytes());
            for cell in 0..100u32 {
                let glyph = char::from(b'a' + (cell % 26) as u8);
                frame.extend_from_slice(format!("[38;2;{cell};{};90m{glyph}", row % 256).as_bytes());
            }
            row += 1;
        }
        let start = std::time::Instant::now();
        output.present(&mut frame, false)?;
        eprintln!("windows {mib} MiB frame: {:.1} ms", start.elapsed().as_secs_f64() * 1e3);
    }
    output.write_all(b"[0m[2J")
}

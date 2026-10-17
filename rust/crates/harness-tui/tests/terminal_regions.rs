//! Region scrolls: a band of rows that moved is scrolled on the device in
//! place (DECSTBM + SU/SD) inside the frame, and only rows that still differ
//! are repainted. The screen is always the one the per-command path shows.
#![cfg(feature = "tui")]

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::io::{self, Write};
use std::rc::Rc;

use acyclic_harness_tui::ratatui::{
    Frame, Terminal, TerminalOptions, Viewport,
    backend::CrosstermBackend,
    layout::{Rect, Size},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
};
use acyclic_harness_tui::term::{
    Capabilities, FrameEffects, FrameTerminal, Present, RegionScroll, RestoreGuard,
};
use alacritty_terminal::event::EventListener;
use alacritty_terminal::grid::Dimensions;
use alacritty_terminal::index::{Column, Line as Row, Point};
use alacritty_terminal::term::{Config, Term, TermMode};
use alacritty_terminal::vte::ansi::Processor;
use proptest::prelude::*;

const WIDTH: u16 = 24;
const HEIGHT: u16 = 8;

#[derive(Clone, Default)]
struct Device(Rc<RefCell<Vec<u8>>>);
impl Write for Device {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Device {
    fn take(&self) -> Vec<u8> {
        std::mem::take(&mut self.0.borrow_mut())
    }
}

/// Rows of text and the cursor.
type Scene = (Vec<Line<'static>>, Option<(u16, u16)>);

fn paint(frame: &mut Frame<'_>, (rows, cursor): &Scene) {
    frame.render_widget(Paragraph::new(rows.clone()), frame.area());
    if let Some(position) = cursor {
        frame.set_cursor_position(*position);
    }
}

/// The independent baseline: ratatui writing each command to the device.
fn per_command(scenes: &[Scene]) -> Vec<Vec<u8>> {
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

fn scrolled(steps: &[(Scene, Option<RegionScroll>)], size: Size) -> Vec<Vec<u8>> {
    let device = Device::default();
    let guard = RestoreGuard::new(device.clone(), &Capabilities::default());
    let mut terminal = FrameTerminal::new(guard, size).unwrap();
    steps
        .iter()
        .map(|(scene, moved)| {
            terminal
                .draw_scrolled(size, *moved, |frame| paint(frame, scene))
                .unwrap();
            device.take()
        })
        .collect()
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

const DEFAULT_COLOURS: &str = "Named(Foreground)Named(Background)";

/// Emulated screens after each frame: text, colours and a shown cursor.
fn screens(frames: &[Vec<u8>]) -> Vec<String> {
    let mut term = Term::new(Config::default(), &Grid, Events);
    let mut parser: Processor = Processor::new();
    frames
        .iter()
        .map(|bytes| {
            parser.advance(&mut term, bytes);
            let mut text = String::new();
            for row in 0..usize::from(HEIGHT) {
                for column in 0..usize::from(WIDTH) {
                    let cell = &term.grid()[Point::new(Row(row as i32), Column(column))];
                    text.push(cell.c);
                    if format!("{:?}{:?}", cell.fg, cell.bg) != DEFAULT_COLOURS {
                        text.push_str(&format!("{:?}{:?}", cell.fg, cell.bg));
                    }
                }
                text.push('\n');
            }
            // A hidden cursor's position is not observable.
            if term.mode().contains(TermMode::SHOW_CURSOR) {
                let cursor = term.grid().cursor.point;
                text.push_str(&format!("{cursor:?}"));
            }
            text
        })
        .collect()
}

/// Lines `top..top + height` of a long transcript.
fn window(top: usize, height: u16) -> Scene {
    let rows = (top..top + usize::from(height))
        .map(|line| Line::raw(format!("transcript line {line:04}")))
        .collect();
    (rows, Some((2, height - 1)))
}

fn hint() -> impl Strategy<Value = Option<RegionScroll>> {
    prop::option::of((0u16..2, 0..HEIGHT, 0..=HEIGHT, -10isize..10).prop_map(
        |(x, y, height, rows)| RegionScroll {
            region: Rect::new(x, y, WIDTH - x, height),
            rows,
        },
    ))
}

fn steps() -> impl Strategy<Value = Vec<(Scene, Option<RegionScroll>)>> {
    let style = (0u8..8, 0u8..8, any::<bool>()).prop_map(|(fg, bg, bold)| {
        Style::default()
            .fg(Color::Indexed(fg))
            .bg(Color::Indexed(bg))
            .add_modifier(if bold {
                Modifier::BOLD
            } else {
                Modifier::empty()
            })
    });
    let row = (
        prop::collection::vec(("[a-z日 ]{0,12}", style.clone()), 0..4),
        style,
    )
        .prop_map(|(spans, style)| {
            Line::from(
                spans
                    .into_iter()
                    .map(|(text, style)| Span::styled(text, style))
                    .collect::<Vec<_>>(),
            )
            .style(style)
        });
    let scene = prop_oneof![
        (0usize..40).prop_map(|top| window(top, HEIGHT)),
        (
            prop::collection::vec(row, 0..=usize::from(HEIGHT)),
            prop::option::of((0..WIDTH, 0..HEIGHT)),
        ),
    ];
    prop::collection::vec((scene, hint()), 1..10)
}

#[test]
fn styled_blanks_and_moved_colours_match_the_independent_screen() {
    let styled_window = |top: usize| -> Scene {
        let rows = (top..top + usize::from(HEIGHT))
            .map(|row| {
                let style = Style::default()
                    .fg(Color::Indexed((row % 8) as u8))
                    .bg(Color::Indexed((row % 7 + 1) as u8));
                Line::styled(
                    if row % 2 == 0 {
                        format!("row {row}")
                    } else {
                        String::new()
                    },
                    style,
                )
            })
            .collect();
        (rows, Some((2, HEIGHT - 1)))
    };
    let region = Rect::new(0, 0, WIDTH, HEIGHT);
    // Both directions, a partial band, and a transition to default blanks.
    let steps = vec![
        (styled_window(0), None),
        (styled_window(2), Some(RegionScroll { region, rows: 2 })),
        (styled_window(0), Some(RegionScroll { region, rows: -2 })),
        (
            styled_window(3),
            Some(RegionScroll {
                region: Rect::new(0, 1, WIDTH, HEIGHT - 2),
                rows: 1,
            }),
        ),
        (
            (vec![Line::raw(""); usize::from(HEIGHT)], None),
            Some(RegionScroll { region, rows: 2 }),
        ),
    ];
    let scenes: Vec<_> = steps.iter().map(|(scene, _)| scene.clone()).collect();
    let expected = screens(&per_command(&scenes));
    assert_eq!(
        screens(&scrolled(&steps, Size::new(WIDTH, HEIGHT))),
        expected
    );
    // Negative control: losing blank-row backgrounds is observable.
    let mut unstyled = steps.clone();
    for (scene, _) in &mut unstyled {
        for line in &mut scene.0 {
            line.style = Style::default();
        }
    }
    assert_ne!(
        screens(&scrolled(&unstyled, Size::new(WIDTH, HEIGHT))),
        expected
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Property: any hint, right, wrong or out of bounds, leaves exactly the
    // screen the per-command path shows after every frame.
    #[test]
    fn every_hint_shows_the_per_command_screen(steps in steps()) {
        let scenes: Vec<Scene> = steps.iter().map(|(scene, _)| scene.clone()).collect();
        // Fresh per-command frames are the independent screen oracle. The
        // incremental upstream diff can retain a styled wide trailing cell;
        // the explicit regression below keeps that defect as a control.
        let old: Vec<_> = scenes.iter().map(|scene| {
            screens(&per_command(std::slice::from_ref(scene))).remove(0)
        }).collect();
        let new = screens(&scrolled(&steps, Size::new(WIDTH, HEIGHT)));
        prop_assert_eq!(new, old);
    }
}

#[test]
fn uncovered_wide_trails_lose_the_old_style_with_or_without_a_scroll_hint() {
    let style = Style::default().fg(Color::Indexed(0)).bg(Color::Indexed(0));
    let scene = |text: &str| -> Scene {
        let mut lines = vec![Line::default().style(style); 4];
        lines.push(Line::styled(text.to_owned(), style));
        (lines, None)
    };
    let first = scene("日 a日");
    let second = scene("日a日");
    let expected = screens(&per_command(std::slice::from_ref(&second))).remove(0);
    for hint in [
        None,
        Some(RegionScroll {
            region: Rect::new(0, 0, WIDTH, HEIGHT - 1),
            rows: 1,
        }),
    ] {
        let steps = vec![
            (first.clone(), None),
            (second.clone(), hint),
            (second.clone(), None),
        ];
        let actual = screens(&scrolled(&steps, Size::new(WIDTH, HEIGHT)));
        assert_eq!(actual[1], expected);
        assert_eq!(actual[2], expected);
    }
    // The removed incremental reference misses the final background reset.
    assert_ne!(screens(&per_command(&[first, second]))[1], expected);
}

/// Rows a frame addresses with cursor moves (`CSI row;col H`).
fn rows_addressed(bytes: &[u8]) -> BTreeSet<u16> {
    String::from_utf8_lossy(bytes)
        .split("\x1b[")
        .filter_map(|sequence| {
            let (position, _) = sequence.split_once('H')?;
            let (row, column) = position.split_once(';')?;
            column.parse::<u16>().ok()?;
            row.parse().ok()
        })
        .collect()
}

/// The last frame of scrolling a full-screen transcript by `rows` lines.
fn scroll_frame(height: u16, rows: isize, hinted: bool) -> Vec<u8> {
    let region = Rect::new(0, 0, WIDTH, height);
    let to = usize::try_from(20 + rows).unwrap();
    let steps = [
        (window(20, height), None),
        (
            window(to, height),
            hinted.then_some(RegionScroll { region, rows }),
        ),
    ];
    scrolled(&steps, Size::new(WIDTH, height)).remove(1)
}

#[test]
fn a_scrolled_band_repaints_only_the_rows_it_exposes() {
    for height in [HEIGHT, 2 * HEIGHT] {
        let down = scroll_frame(height, 3, true);
        let scroll = format!("\x1b[1;{height}r\x1b[3S\x1b[r");
        assert!(down.starts_with(scroll.as_bytes()), "{down:?}");
        // The three exposed rows, whatever the height; the cursor row is
        // among them.
        let exposed: BTreeSet<u16> = (height - 2..=height).collect();
        assert_eq!(rows_addressed(&down), exposed);
        let up = scroll_frame(height, -2, true);
        let scroll = format!("\x1b[1;{height}r\x1b[2T\x1b[r");
        assert!(up.starts_with(scroll.as_bytes()), "{up:?}");
        assert_eq!(rows_addressed(&up), BTreeSet::from([1, 2, height]));
    }
}

#[test]
fn negative_control_without_a_hint_every_row_is_repainted() {
    for height in [HEIGHT, 2 * HEIGHT] {
        let rows = rows_addressed(&scroll_frame(height, 3, false));
        assert_eq!(rows, (1..=height).collect());
    }
}

#[test]
fn a_band_moved_by_its_height_or_not_full_width_is_repainted() {
    let size = Size::new(WIDTH, HEIGHT);
    let full = Rect::new(0, 0, WIDTH, HEIGHT);
    for moved in [
        RegionScroll {
            region: full,
            rows: isize::try_from(HEIGHT).unwrap(),
        },
        RegionScroll {
            region: Rect::new(1, 0, WIDTH - 1, HEIGHT),
            rows: 1,
        },
        RegionScroll {
            region: Rect::new(0, 4, WIDTH, HEIGHT),
            rows: 1,
        },
    ] {
        let steps = [(window(0, HEIGHT), None), (window(1, HEIGHT), Some(moved))];
        let frame = scrolled(&steps, size).remove(1);
        assert!(
            !frame.windows(3).any(|bytes| bytes == b"\x1b[r"),
            "{moved:?}"
        );
    }
    // A resize clears the device: the hint is not applied to the old frame.
    let device = Device::default();
    let guard = RestoreGuard::new(device.clone(), &Capabilities::default());
    let mut terminal = FrameTerminal::new(guard, size).unwrap();
    terminal
        .draw(size, |frame| paint(frame, &window(0, HEIGHT)))
        .unwrap();
    let larger = Size::new(WIDTH, HEIGHT + 2);
    let moved = RegionScroll {
        region: Rect::new(0, 0, WIDTH, HEIGHT + 2),
        rows: 1,
    };
    device.take();
    terminal
        .draw_scrolled(larger, Some(moved), |frame| {
            paint(frame, &window(1, HEIGHT + 2))
        })
        .unwrap();
    assert!(!device.take().windows(3).any(|bytes| bytes == b"\x1b[r"));
}

/// A device that fails one write at byte `at`, after writing the prefix.
struct FailAt {
    bytes: Vec<u8>,
    at: usize,
    failed: bool,
}
impl Write for FailAt {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if !self.failed && self.bytes.len() + bytes.len() > self.at {
            self.failed = true;
            let keep = self.at - self.bytes.len();
            self.bytes.extend_from_slice(&bytes[..keep]);
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Whether the screen scrolls as a whole after `bytes`: a line at the top
/// leaves the screen when a line feed is written on the bottom row, unless
/// stuck margins confine scrolling to a band that excludes both.
fn scrolls_whole_screen(bytes: &[u8]) -> bool {
    let probe = format!("\x1b[1;1HZ\x1b[{HEIGHT};1H\n");
    let screen = screens(&[bytes.to_vec(), probe.into_bytes()]).remove(1);
    !screen.starts_with('Z')
}

#[test]
fn a_region_scroll_cut_short_at_any_byte_restores_full_margins() {
    let size = Size::new(WIDTH, HEIGHT);
    let moved = RegionScroll {
        region: Rect::new(0, 2, WIDTH, 4),
        rows: 1,
    };
    let mut cut = 0;
    for at in 0.. {
        let mut device = FailAt {
            bytes: Vec::new(),
            at,
            failed: false,
        };
        let mut guard = RestoreGuard::new(&mut device, &Capabilities::default());
        {
            let mut terminal = FrameTerminal::new(&mut guard, size).unwrap();
            // Either frame may be the one cut short.
            let _ = terminal.draw(size, |frame| paint(frame, &window(0, HEIGHT)));
            let _ =
                terminal.draw_scrolled(size, Some(moved), |frame| paint(frame, &window(1, HEIGHT)));
        }
        let _ = guard.restore();
        drop(guard);
        assert!(scrolls_whole_screen(&device.bytes), "failure at byte {at}");
        if !device.failed {
            break;
        }
        cut += 1;
    }
    // Every byte of both frames was a failure point, the margins included.
    assert!(cut > 40, "{cut}");
    // Negative control: margins set and never reset confine the scroll.
    assert!(!scrolls_whole_screen(b"\x1b[3;6r"));
}

struct PersistentCut {
    device: Device,
    at: usize,
    healed: Rc<std::cell::Cell<bool>>,
}
impl Write for PersistentCut {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if !self.healed.get() {
            let accepted = self
                .at
                .saturating_sub(self.device.0.borrow().len())
                .min(bytes.len());
            self.device.write_all(&bytes[..accepted])?;
            if accepted < bytes.len() {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            return Ok(accepted);
        }
        self.device.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_later_cursor_frame_cannot_discharge_a_failed_margin_reset() {
    let scroll = b"\x1b[3;6r\x1b[1S\x1b[r";
    let mut controls_detected = 0;
    for at in 0..scroll.len() {
        let device = Device::default();
        let healed = Rc::new(std::cell::Cell::new(false));
        let mut guard = RestoreGuard::new(
            PersistentCut {
                device: device.clone(),
                at,
                healed: Rc::clone(&healed),
            },
            &Capabilities::default(),
        );
        let error = guard
            .present(
                &mut scroll.to_vec(),
                FrameEffects {
                    margins: true,
                    ..FrameEffects::default()
                },
            )
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        healed.set(true);
        // Teardown (or a cursor-only draw) succeeds after both the scroll
        // frame and its abort failed. It never wrote a margin reset.
        guard
            .present(&mut b"\x1b[?25h".to_vec(), FrameEffects::default())
            .unwrap();
        let before_restore = device.0.borrow().clone();
        guard.restore().unwrap();
        assert!(scrolls_whole_screen(&device.take()), "failure at {at}");
        // Negative control: the former end_frame forgot the pending record,
        // so restoration emitted nothing after this exact successful frame.
        controls_detected += usize::from(!scrolls_whole_screen(&before_restore));
    }
    assert!(
        controls_detected > 0,
        "the old journal loss must be observable"
    );
}

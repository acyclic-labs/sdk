//! Region scrolls: a band of rows that moved is scrolled on the device in
//! place (DECSTBM + SU/SD) inside the frame, and only rows that still differ
//! are repainted. The screen is always the one the per-command path shows.
#![cfg(feature = "tui")]

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::io::{self, Write};
use std::panic::{AssertUnwindSafe, catch_unwind};
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
use alacritty_terminal::term::{Config, Term, TermMode, cell::Flags};
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
    if let Some((x, y)) = cursor {
        let area = frame.area();
        frame.set_cursor_position((
            (*x).min(area.width.saturating_sub(1)),
            (*y).min(area.height.saturating_sub(1)),
        ));
    }
}

/// The independent baseline: ratatui writing each command to the device.
fn per_command(scenes: &[Scene]) -> Vec<Vec<u8>> {
    per_command_sized(scenes, Size::new(WIDTH, HEIGHT))
}

fn per_command_sized(scenes: &[Scene], size: Size) -> Vec<Vec<u8>> {
    let device = Device::default();
    let mut terminal = Terminal::with_options(
        CrosstermBackend::new(device.clone()),
        TerminalOptions {
            viewport: Viewport::Fixed(Rect::new(0, 0, size.width, size.height)),
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
    scrolled_profile(steps, size, true)
}

fn region_caps(supported: bool) -> Capabilities {
    Capabilities {
        scroll_regions: supported,
        ..Capabilities::default()
    }
}

fn scrolled_profile(
    steps: &[(Scene, Option<RegionScroll>)],
    size: Size,
    supported: bool,
) -> Vec<Vec<u8>> {
    let device = Device::default();
    let guard = RestoreGuard::new(device.clone(), &region_caps(supported));
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
struct Grid(Size);
impl Dimensions for Grid {
    fn total_lines(&self) -> usize {
        usize::from(self.0.height)
    }
    fn screen_lines(&self) -> usize {
        usize::from(self.0.height)
    }
    fn columns(&self) -> usize {
        usize::from(self.0.width)
    }
}
#[derive(Clone, Copy)]
struct Events;
impl EventListener for Events {}

const DEFAULT_COLOURS: &str = "Named(Foreground)Named(Background)";

/// Emulated screens after each frame: text, colours and a shown cursor.
fn screens(frames: &[Vec<u8>]) -> Vec<String> {
    screens_with_support(frames, true)
}

fn screens_with_support(frames: &[Vec<u8>], supported: bool) -> Vec<String> {
    let size = Size::new(WIDTH, HEIGHT);
    let sized: Vec<_> = frames.iter().map(|bytes| (size, bytes.clone())).collect();
    screens_sized(&sized, supported)
}

/// Each frame reaches an emulator first resized to the frame's size, as a
/// window resize precedes the frame drawn for it.
fn screens_sized(frames: &[(Size, Vec<u8>)], supported: bool) -> Vec<String> {
    let first = frames
        .first()
        .map_or(Size::new(WIDTH, HEIGHT), |frame| frame.0);
    let mut term = Term::new(Config::default(), &Grid(first), Events);
    let mut parser: Processor = Processor::new();
    let mut current = first;
    frames
        .iter()
        .map(|(size, bytes)| {
            if *size != current {
                term.resize(Grid(*size));
                current = *size;
            }
            if supported {
                parser.advance(&mut term, bytes);
            } else {
                parser.advance(&mut term, &without_scrolling(bytes));
            }
            let mut text = String::new();
            for row in 0..usize::from(size.height) {
                for column in 0..usize::from(size.width) {
                    let cell = &term.grid()[Point::new(Row(row as i32), Column(column))];
                    text.push(cell.c);
                    let styles = cell.flags
                        - (Flags::WRAPLINE
                            | Flags::WIDE_CHAR
                            | Flags::WIDE_CHAR_SPACER
                            | Flags::LEADING_WIDE_CHAR_SPACER);
                    if !styles.is_empty() {
                        text.push_str(&format!("{styles:?}"));
                    }
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

// Model a VT that ignores unsupported CSI scrolling and margin commands.
// All other bytes reach the independent VT processor unchanged.
fn without_scrolling(bytes: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at..].starts_with(b"\x1b[") {
            if let Some(end) = bytes[at + 2..]
                .iter()
                .position(|byte| (0x40..=0x7e).contains(byte))
            {
                let end = at + 2 + end;
                if matches!(bytes[end], b'S' | b'T' | b'r') {
                    at = end + 1;
                    continue;
                }
            }
        }
        result.push(bytes[at]);
        at += 1;
    }
    result
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
    prop::collection::vec((scene(), hint()), 1..10)
}

fn scene() -> impl Strategy<Value = Scene> {
    let style = (0u8..8, 0u8..8, 0u16..512).prop_map(|(fg, bg, modifiers)| {
        Style::default()
            .fg(Color::Indexed(fg))
            .bg(Color::Indexed(bg))
            .add_modifier(Modifier::from_bits_truncate(modifiers))
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
    prop_oneof![
        (0usize..40).prop_map(|top| window(top, HEIGHT + 3)),
        (
            prop::collection::vec(row, 0..=usize::from(HEIGHT + 3)),
            prop::option::of((0..WIDTH, 0..HEIGHT + 3)),
        ),
    ]
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
        for supported in [false, true] {
            let frames = scrolled_profile(&steps, Size::new(WIDTH, HEIGHT), supported);
            let new = screens_with_support(&frames, supported);
            prop_assert_eq!(&new, &old);
            if !supported { prop_assert_eq!(frames.iter().map(|frame| without_scrolling(frame)).collect::<Vec<_>>(), frames); }
        }
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
    let guard = RestoreGuard::new(device.clone(), &region_caps(true));
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
    let resized = device.take();
    assert!(resized.windows(3).any(|bytes| bytes == b"\x1b[r"));
    assert!(!resized.windows(4).any(|bytes| bytes == b"\x1b[1S"));
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
        let mut guard = RestoreGuard::new(&mut device, &region_caps(true));
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

#[test]
fn negative_control_shifting_an_unsupported_device_changes_the_screen() {
    let region = Rect::new(0, 0, WIDTH, HEIGHT);
    let steps = [
        (window(0, HEIGHT), None),
        (window(1, HEIGHT), Some(RegionScroll { region, rows: 1 })),
    ];
    let baseline = screens(&per_command(
        &steps
            .iter()
            .map(|(scene, _)| scene.clone())
            .collect::<Vec<_>>(),
    ));
    // Wrongly enabling hints on a VT that ignores them is the former defect.
    assert_ne!(
        screens_with_support(
            &scrolled_profile(&steps, Size::new(WIDTH, HEIGHT), true),
            false
        ),
        baseline
    );
    assert_eq!(
        screens_with_support(
            &scrolled_profile(&steps, Size::new(WIDTH, HEIGHT), false),
            false
        ),
        baseline
    );
}

fn grapheme_allocations<const CLONE: bool>() {
    const COLUMNS: u16 = 12;
    for height in [8u16, 16] {
        let area = Rect::new(0, 0, COLUMNS, height);
        let size = Size::new(COLUMNS, height);
        let scene = |top: u16| -> Scene {
            (
                (top..top + height)
                    .map(|row| {
                        let base = char::from(b'a' + u8::try_from(row % 26).unwrap());
                        let cluster = format!("{base}{}", "\u{301}".repeat(40));
                        Line::raw(cluster.repeat(usize::from(COLUMNS)))
                    })
                    .collect(),
                None,
            )
        };
        let mut old = Terminal::with_options(
            CrosstermBackend::new(io::sink()),
            TerminalOptions {
                viewport: Viewport::Fixed(area),
            },
        )
        .unwrap();
        let mut new =
            FrameTerminal::new(RestoreGuard::new(io::sink(), &region_caps(true)), size).unwrap();
        let first = scene(0);
        old.draw(|frame| paint(frame, &first)).unwrap();
        new.draw(size, |frame| paint(frame, &first)).unwrap();
        for (top, moved) in [
            (0, None),
            (
                1,
                Some(RegionScroll {
                    region: area,
                    rows: 1,
                }),
            ),
            (1, None),
            (
                0,
                Some(RegionScroll {
                    region: area,
                    rows: -1,
                }),
            ),
        ] {
            let scene = scene(top);
            let before = acyclic_test_support::measure(|| {
                old.draw(|frame| paint(frame, &scene)).unwrap();
            });
            let after = acyclic_test_support::measure(|| {
                new.draw_scrolled(size, moved, |frame| {
                    paint(frame, &scene);
                    if CLONE {
                        std::hint::black_box(frame.buffer_mut().clone());
                    }
                })
                .unwrap();
            });
            assert_eq!(
                // An unchanged frame also skips crossterm's one formatted
                // attribute-reset allocation, as the ordinary frame path does.
                after.count_total + u64::from(moved.is_none()),
                before.count_total,
                "one retained baseline must match the independent path"
            );
        }
    }
}
#[test]
fn heap_backed_graphemes_do_not_allocate_a_second_screen() {
    grapheme_allocations::<false>();
}
#[test]
#[should_panic(expected = "one retained baseline must match the independent path")]
fn negative_control_cloning_heap_backed_cells_is_detected() {
    grapheme_allocations::<true>();
}

fn assert_changed_body_cursor<const KEEP_POSITION: bool>() {
    let region = Rect::new(0, 0, WIDTH, HEIGHT);
    for moved in [None, Some(RegionScroll { region, rows: 1 })] {
        let mut first = window(0, HEIGHT);
        let mut next = window(1, HEIGHT);
        first.1 = Some((3, 2));
        next.1 = first.1;
        let old = per_command(&[first.clone(), next.clone(), next.clone()]);
        let mut actual = scrolled(
            &[(first, None), (next.clone(), moved), (next, None)],
            Size::new(WIDTH, HEIGHT),
        );
        if !KEEP_POSITION {
            // Preserve every cell and the checked presentation; restore only
            // the stale cursor-position cache after manually encoding a body.
            let command = b"\x1b[3;4H";
            let at = actual[1]
                .windows(command.len())
                .rposition(|part| part == command)
                .unwrap();
            actual[1].drain(at..at + command.len());
        }
        assert_eq!(
            screens(&actual),
            screens(&old),
            "body encoding must restore the requested cursor"
        );
        assert!(actual[2].is_empty(), "an identical frame reaches no device");
    }
}

#[test]
fn changed_and_scrolled_bodies_restore_an_unchanged_cursor_position() {
    assert_changed_body_cursor::<true>();
}
#[test]
#[should_panic(expected = "body encoding must restore the requested cursor")]
fn negative_control_caching_the_position_across_body_encoding_is_detected() {
    assert_changed_body_cursor::<false>();
}

/// Window sizes a session moves between: shorter, narrower and taller.
const SIZES: [(u16, u16); 4] = [
    (WIDTH, HEIGHT),
    (WIDTH, HEIGHT - 3),
    (WIDTH - 6, HEIGHT),
    (WIDTH - 6, HEIGHT + 2),
];

fn sized_steps() -> impl Strategy<Value = Vec<(Size, Scene, Option<RegionScroll>)>> {
    let step = (
        prop::sample::select(SIZES.to_vec()),
        scene(),
        prop::option::of((0u16..2, 0u16..64, 0u16..64, -10isize..10)),
    )
        .prop_map(|((width, height), scene, hint)| {
            // Bands anywhere in this frame's area, meeting its edges included.
            let hint = hint.map(|(x, y, rows, moved)| RegionScroll {
                region: Rect::new(x, y % height, width - x, rows % (height + 1)),
                rows: moved,
            });
            (Size::new(width, height), scene, hint)
        });
    prop::collection::vec(step, 1..12)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Property: across resizes, any hint at any band leaves exactly the
    // screen a fresh per-command repaint shows at that size, on supported
    // and unsupported profiles.
    #[test]
    fn every_hint_across_resizes_shows_the_per_command_screen(steps in sized_steps()) {
        let old: Vec<_> = steps
            .iter()
            .map(|(size, scene, _)| {
                let frame = per_command_sized(std::slice::from_ref(scene), *size).remove(0);
                screens_sized(&[(*size, frame)], true).remove(0)
            })
            .collect();
        for supported in [false, true] {
            let device = Device::default();
            let guard = RestoreGuard::new(device.clone(), &region_caps(supported));
            let mut terminal = FrameTerminal::new(guard, steps[0].0).unwrap();
            let frames: Vec<_> = steps
                .iter()
                .map(|(size, scene, moved)| {
                    terminal.draw_scrolled(*size, *moved, |frame| paint(frame, scene)).unwrap();
                    (*size, device.take())
                })
                .collect();
            prop_assert_eq!(&screens_sized(&frames, supported), &old);
        }
    }
}

/// A transcript band between a fixed header row and a fixed prompt row.
fn framed(top: usize) -> Scene {
    let mut rows = vec![Line::raw("HEADER fixed")];
    rows.extend(window(top, HEIGHT - 2).0);
    rows.push(Line::raw("> prompt fixed"));
    (rows, Some((2, HEIGHT - 1)))
}

fn mutated(frames: &[Vec<u8>], from: &str, to: &str) -> Vec<Vec<u8>> {
    let all: Vec<_> = frames
        .iter()
        .map(|frame| String::from_utf8(frame.clone()).unwrap())
        .collect();
    assert!(all.iter().any(|frame| frame.contains(from)), "{from:?}");
    all.iter()
        .map(|frame| frame.replace(from, to).into_bytes())
        .collect()
}

#[test]
fn a_band_meeting_fixed_rows_scrolls_exactly_and_off_by_one_margins_are_caught() {
    let band = Rect::new(0, 1, WIDTH, HEIGHT - 2);
    let steps = [
        (framed(10), None),
        (
            framed(13),
            Some(RegionScroll {
                region: band,
                rows: 3,
            }),
        ),
        (
            framed(11),
            Some(RegionScroll {
                region: band,
                rows: -2,
            }),
        ),
    ];
    let scenes: Vec<_> = steps.iter().map(|(scene, _)| scene.clone()).collect();
    let expected = screens(&per_command(&scenes));
    let frames = scrolled(&steps, Size::new(WIDTH, HEIGHT));
    assert_eq!(screens(&frames), expected);
    // Negative controls: each margin one row off, into or away from a fixed
    // row, and each distance one line off, is visible to the oracle.
    let (top, bottom) = (2, HEIGHT - 1);
    let margins = format!("\x1b[{top};{bottom}r");
    for (from, to) in [
        (margins.clone(), format!("\x1b[{};{bottom}r", top - 1)),
        (margins.clone(), format!("\x1b[{};{bottom}r", top + 1)),
        (margins.clone(), format!("\x1b[{top};{}r", bottom + 1)),
        (margins.clone(), format!("\x1b[{top};{}r", bottom - 1)),
        ("\x1b[3S".into(), "\x1b[4S".into()),
        ("\x1b[3S".into(), "\x1b[2S".into()),
        ("\x1b[2T".into(), "\x1b[3T".into()),
        ("\x1b[2T".into(), "\x1b[1T".into()),
    ] {
        assert_ne!(screens(&mutated(&frames, &from, &to)), expected, "{to:?}");
    }
}

#[test]
fn a_render_unwinding_after_a_region_scroll_leaves_the_next_frame_exact() {
    let size = Size::new(WIDTH, HEIGHT);
    let region = Rect::new(0, 0, WIDTH, HEIGHT);
    let device = Device::default();
    let guard = RestoreGuard::new(device.clone(), &region_caps(true));
    let mut terminal = FrameTerminal::new(guard, size).unwrap();
    terminal
        .draw(size, |frame| paint(frame, &window(0, HEIGHT)))
        .unwrap();
    let mut frames = vec![device.take()];
    let unwound = catch_unwind(AssertUnwindSafe(|| {
        terminal.draw_scrolled(size, Some(RegionScroll { region, rows: 2 }), |_| {
            panic!("render failed")
        })
    }));
    assert!(unwound.is_err());
    frames.push(device.take());
    terminal
        .draw(size, |frame| paint(frame, &window(2, HEIGHT)))
        .unwrap();
    frames.push(device.take());
    let expected = screens(&per_command(&[window(0, HEIGHT), window(2, HEIGHT)]));
    // Without invalidation the baseline keeps the scroll the device never
    // received, and only the two rows it exposed are repainted.
    assert_eq!(
        screens(&frames)[2],
        expected[1],
        "a render unwind must not leave a scrolled baseline: {:?}",
        String::from_utf8_lossy(&frames[2])
    );
}

#[path = "../terminal_regions/native_profiles.rs"]
mod native_profiles;

#[path = "../terminal_regions/recovery.rs"]
mod recovery;

#[test]
fn the_screen_oracle_observes_wide_cell_style_loss() {
    for modifier in [
        Modifier::BOLD,
        Modifier::DIM,
        Modifier::ITALIC,
        Modifier::UNDERLINED,
        Modifier::REVERSED,
        Modifier::HIDDEN,
        Modifier::CROSSED_OUT,
    ] {
        let scene = |styled| {
            (
                vec![Line::styled(
                    "日 a日",
                    Style::default().add_modifier(if styled {
                        modifier
                    } else {
                        Modifier::empty()
                    }),
                )],
                None,
            )
        };
        let baseline = screens(&per_command(&[scene(true)]));
        let candidate = scrolled(&[(scene(true), None)], Size::new(WIDTH, HEIGHT));
        assert_eq!(screens(&candidate), baseline, "{modifier:?}");
        assert_ne!(
            screens(&per_command(&[scene(false)])),
            baseline,
            "{modifier:?} stripping must fail the same screen oracle"
        );
    }
}

#[test]
fn blink_attributes_reach_the_device_in_wide_cell_frames() {
    // Alacritty intentionally ignores blink; check its wire attributes too.
    for (modifier, sequence) in [
        (Modifier::SLOW_BLINK, b"\x1b[5m".as_slice()),
        (Modifier::RAPID_BLINK, b"\x1b[6m".as_slice()),
    ] {
        let scene = |modifier| {
            (
                vec![Line::styled(
                    "日 a日",
                    Style::default().add_modifier(modifier),
                )],
                None,
            )
        };
        let has_attribute = |frames: Vec<Vec<u8>>| {
            frames
                .concat()
                .windows(sequence.len())
                .any(|bytes| bytes == sequence)
        };
        assert!(has_attribute(scrolled(
            &[(scene(modifier), None)],
            Size::new(WIDTH, HEIGHT)
        )));
        assert!(has_attribute(per_command(&[scene(modifier)])));
        assert!(
            !has_attribute(scrolled(
                &[(scene(Modifier::empty()), None)],
                Size::new(WIDTH, HEIGHT)
            )),
            "removing the production attribute must fail the same wire oracle"
        );
    }
}

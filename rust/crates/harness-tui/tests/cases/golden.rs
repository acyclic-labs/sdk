//! Full-screen goldens through the shipping encoder and an independent terminal.
#![cfg(feature = "tui")]
use acyclic_harness_tui::{
    BlockKind, Editor, Hyperlinks, PromptInput, RowLink, Screen, TranscriptRows, Viewport,
    config::ConfigView,
    ratatui::{layout::Size, style::Modifier, text::Line},
    term::{Capabilities, ColourDepth, FrameEffects, FrameTerminal, Present, RestoreGuard},
};
use alacritty_terminal::{
    event::EventListener,
    grid::Dimensions,
    index::{Column, Line as Row, Point},
    term::{Config, Term, cell::Flags},
    vte::ansi::Processor,
};
use std::{
    cell::RefCell,
    io::{self, Write},
    path::PathBuf,
    rc::Rc,
};

const VIEWS: &[&str] = &[
    "conversation",
    "code",
    "tools",
    "approval",
    "help",
    "history",
    "diff",
    "feedback",
    "completion",
    "panels",
    "terminal",
    "reasoning",
    "editing",
    "notification",
];
const CODE: &str = "```rust\n// Answer the question.\nfn answer() -> u32 {\n    let text = \"forty-two\";\n    42\n}\n```\n\n```sh\necho \"$HOME\" # where\n```";
const MARKDOWN: &str = "# Review\n\n**Bold**, *emphasis*, ~~removed~~ and `code`.\n\n1. first\n2. second\n   - nested\n\n> Read [the **docs**](https://example.com/docs).\n\n| crate | note |\n| :- | -: |\n| tui | 界面 👩‍💻 e\u{301} |";

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
struct Events;
impl EventListener for Events {}

// Retain physical rows, complete attributes, background bands and OSC 8 targets.
fn snapshot(bytes: &[u8], size: Size) -> String {
    let mut term = Term::new(Config::default(), &Grid(size), Events);
    let mut parser: Processor = Processor::new();
    parser.advance(&mut term, bytes);
    let mut out = format!("{}x{}\n", size.width, size.height);
    for y in 0..size.height {
        let mut row = String::new();
        let mut previous = String::new();
        for x in 0..size.width {
            let cell = &term.grid()[Point::new(Row(i32::from(y)), Column(usize::from(x)))];
            if cell.flags.contains(Flags::WIDE_CHAR_SPACER) {
                continue;
            }
            let target = cell
                .hyperlink()
                .map(|link| link.uri().to_owned())
                .unwrap_or_default();
            let style = format!(
                "{:?}/{:?}/{:?}/{target}",
                cell.fg,
                cell.bg,
                cell.flags - Flags::WIDE_CHAR - Flags::WIDE_CHAR_SPACER
            );
            if style != previous {
                row.push_str(&format!("«{style}»"));
                previous = style;
            }
            row.push(cell.c);
            if let Some(extra) = cell.zerowidth() {
                row.extend(extra);
            }
        }
        out.push_str(row.trim_end());
        out.push('\n');
    }
    out
}

fn frame(view: &str, size: Size, profile: &Capabilities) -> io::Result<Vec<u8>> {
    frame_config(view, size, profile, &ConfigView::default())
}

fn frame_config(
    view: &str,
    size: Size,
    profile: &Capabilities,
    config: &ConfigView,
) -> io::Result<Vec<u8>> {
    let theme = config
        .theme(profile)
        .map_err(|errors| io::Error::other(format!("{errors:?}")))?;
    let mut transcript = TranscriptRows::default();
    transcript.set_theme(theme);
    transcript.rows(size.width);
    transcript.push(BlockKind::Prompt, "Review the implementation");
    if view == "tools" {
        transcript.push(
            BlockKind::Tool { failed: false },
            "shell  cargo test -p tui  4.2s  (312 lines)",
        );
        transcript.push(BlockKind::Tool { failed: true }, "read  missing.rs");
    }
    transcript.push(
        BlockKind::Answer,
        if view == "code" { CODE } else { MARKDOWN },
    );
    let mut input = PromptInput::new(Editor::new(if view == "completion" {
        "/"
    } else {
        "Follow up"
    }));
    input.theme = theme;
    if view == "editing" {
        input.edit_message("previous-message".into(), "Revise 界面");
    }
    if view == "completion" {
        input.completion.set_file("commands/catalog.json", Some(br#"[{"name":"help","description":"Show commands"},{"name":"diff","description":"Workspace changes"},{"name":"theme","description":"auto, dark or light"}]"#)).map_err(|error| io::Error::other(format!("{error:?}")))?;
        input.refresh_completions();
    }
    input.busy = view == "reasoning";
    let overlay: Vec<Line<'static>> = match view {
        "approval" => vec![
            "shell  cargo test -p tui",
            "principal: user · device: local",
            "workspace: /project",
            "y: allow once · a: allow rule · n: deny · e: edit",
        ],
        "help" => vec![
            "/help  Commands",
            "/diff  Workspace changes",
            "/history  Settled history",
            "/theme auto|dark|light",
            "Esc: close",
        ],
        "history" => vec![
            "Cut 12 · 1 project volume · context through 42",
            "Cut 8 · 1 project volume · context through 30",
            "PgDn: older · PgUp: newest · Esc: close",
        ],
        "diff" => vec![
            "src/main.rs (modified) +1 -1",
            "@@ -1 +1 @@",
            "-let answer = 41;",
            "+let answer = 42;",
            "PgDn: next · Esc: close",
        ],
        _ => Vec::new(),
    }
    .into_iter()
    .map(Line::raw)
    .collect();
    let terminal = [
        Line::raw("$ cargo test -p tui"),
        Line::raw("test result: ok. 12 passed"),
    ];
    let diff = [
        Line::raw("src/main.rs +1 -1"),
        Line::styled(
            "+ answer = 42",
            theme.token(acyclic_harness_tui::config::Token::DiffAdd),
        ),
    ];
    let goal = [Line::raw("Ship the theme")];
    let plans = [Line::raw("[x] tokens"), Line::raw("[ ] review")];
    let reasoning = [Line::styled(
        "Checking the public contract…",
        theme
            .token(acyclic_harness_tui::config::Token::Muted)
            .add_modifier(Modifier::ITALIC),
    )];
    let usage = [Line::raw("model · 24% (2400/10000) · $0.12 · 2 children").right_aligned()];
    let notifications = [Line::styled(
        "Waiting for your input",
        theme.token(acyclic_harness_tui::config::Token::Attention),
    )];
    let (rows, links, code) = transcript.presentation();
    let screen = Screen {
        header: Line::raw("○ GraphCoder · project · main"),
        transcript: rows,
        links,
        code,
        overlay: &overlay,
        overlay_title: view,
        terminal: if view == "terminal" { &terminal } else { &[] },
        diff: if view == "panels" { &diff } else { &[] },
        goal: if view == "panels" { &goal } else { &[] },
        plans: if view == "panels" { &plans } else { &[] },
        reasoning: if view == "reasoning" { &reasoning } else { &[] },
        feedback: view == "feedback",
        notifications: if view == "notification" {
            &notifications
        } else {
            &[]
        },
        usage: &usage,
        status: Line::raw("Enter: send · Tab: queue · Ctrl-D: quit"),
        ..Screen::default()
    };
    let device = Device::default();
    let mut terminal = FrameTerminal::new(RestoreGuard::new(device.clone(), profile), size)?;
    let mut viewport = Viewport::default();
    terminal.draw_linked(size, None, |frame| {
        let areas = screen.render(frame, &input, &mut viewport, 0, 1000);
        screen.hyperlinks(&areas, 0)
    })?;
    let bytes = device.0.borrow().clone();
    let before = bytes.len();
    terminal.draw_linked(size, None, |frame| {
        let areas = screen.render(frame, &input, &mut viewport, 0, 1000);
        screen.hyperlinks(&areas, 0)
    })?;
    assert_eq!(
        device.0.borrow().len(),
        before,
        "unchanged {view} repainted"
    );
    Ok(bytes)
}

#[test]
fn forced_ansi_code_has_contrast_against_opposite_terminal_defaults() -> io::Result<()> {
    use acyclic_harness_tui::config::{ConfigFile, Scope};
    use alacritty_terminal::vte::ansi::Color;
    let size = Size::new(80, 24);
    for (mode, background, foreground, band) in
        [("light", [0; 3], 0, 15), ("dark", [255; 3], 15, 0)]
    {
        let profile = Capabilities {
            colour: ColourDepth::Ansi16,
            background: Some(background),
            ..Default::default()
        };
        let mut config = ConfigView::default();
        config.set_file(
            Scope::User,
            ConfigFile::Theme,
            Some(&format!("{{\"mode\":\"{mode}\"}}")),
        );
        let bytes = frame_config("code", size, &profile, &config)?;
        let contrast = |bytes: &[u8]| {
            let mut term = Term::new(Config::default(), &Grid(size), Events);
            let mut parser: Processor = Processor::new();
            parser.advance(&mut term, bytes);
            (0..size.height).any(|row| {
                (0..size.width).any(|col| {
                    let cell =
                        &term.grid()[Point::new(Row(i32::from(row)), Column(usize::from(col)))];
                    cell.c == '{'
                        && cell.fg == Color::Indexed(foreground)
                        && cell.bg == Color::Indexed(band)
                })
            })
        };
        assert!(
            contrast(&bytes),
            "forced {mode} code inherited an incompatible terminal foreground"
        );
        assert!(
            !contrast(&strip(&bytes, b'[', b'm')),
            "missing code colours passed the contrast oracle"
        );
    }
    Ok(())
}

#[test]
fn views_match_terminal_goldens() -> Result<(), Box<dyn std::error::Error>> {
    let directory = std::env::var_os("GOLDEN_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| acyclic_test_support::crate_path!("tests/golden"));
    let update = std::env::var_os("UPDATE_GOLDEN").is_some();
    for view in VIEWS {
        for (mode, background) in [("dark", [0; 3]), ("light", [255; 3])] {
            for size in [Size::new(80, 24), Size::new(120, 40)] {
                for (depth, colour) in [
                    ("rgb", ColourDepth::TrueColour),
                    ("indexed256", ColourDepth::Indexed256),
                    ("ansi16", ColourDepth::Ansi16),
                    ("monochrome", ColourDepth::Monochrome),
                ] {
                    let profile = Capabilities {
                        colour,
                        background: Some(background),
                        ..Default::default()
                    };
                    let actual = snapshot(&frame(view, size, &profile)?, size);
                    let name = format!("{view}-{mode}-{}x{}-{depth}.txt", size.width, size.height);
                    if update {
                        std::fs::create_dir_all(&directory)?;
                        std::fs::write(directory.join(&name), actual)?;
                    } else {
                        assert_eq!(
                            std::fs::read_to_string(directory.join(&name))?.replace("\r\n", "\n"),
                            actual,
                            "{name}"
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn terminal_golden_detects_lost_styles_and_destinations() -> Result<(), Box<dyn std::error::Error>>
{
    let size = Size::new(80, 24);
    for colour in [
        ColourDepth::TrueColour,
        ColourDepth::Indexed256,
        ColourDepth::Ansi16,
        ColourDepth::Monochrome,
    ] {
        for background in [[0; 3], [255; 3]] {
            let profile = Capabilities {
                colour,
                background: Some(background),
                ..Default::default()
            };
            let code = frame("code", size, &profile)?;
            assert_ne!(
                snapshot(&code, size),
                snapshot(&strip(&code, b'[', b'm'), size),
                "lost code styling passed"
            );
            let markdown = frame("conversation", size, &profile)?;
            assert_ne!(
                snapshot(&markdown, size),
                snapshot(&strip(&markdown, b']', b'\\'), size),
                "lost OSC 8 destinations passed"
            );
        }
    }
    Ok(())
}

fn linked_frame(target: &str) -> Hyperlinks {
    let mut links = Hyperlinks::default();
    links.row(
        acyclic_harness_tui::ratatui::layout::Rect::new(0, 0, 12, 1),
        &[RowLink {
            columns: 0..4,
            target: target.into(),
        }],
    );
    links
}

#[test]
fn destination_only_edits_removal_and_unsafe_targets_repaint_the_original_label() -> io::Result<()>
{
    let device = Device::default();
    let size = Size::new(12, 2);
    let mut terminal = FrameTerminal::new(
        RestoreGuard::new(device.clone(), &Capabilities::default()),
        size,
    )?;
    let draw = |frame: &mut acyclic_harness_tui::ratatui::Frame<'_>| {
        frame.render_widget(Line::raw("docs"), frame.area())
    };
    terminal.draw_linked(size, None, |frame| {
        draw(frame);
        linked_frame("https://example.com/first")
    })?;
    let first = snapshot(&device.0.borrow(), size);
    terminal.draw_linked(size, None, |frame| {
        draw(frame);
        linked_frame("https://example.com/second")
    })?;
    let second = snapshot(&device.0.borrow(), size);
    assert_ne!(first, second, "destination-only changes must repaint");
    assert!(second.contains("https://example.com/second"));
    assert!(!second.contains("https://example.com/first"));
    terminal.draw(size, draw)?;
    let plain = snapshot(&device.0.borrow(), size);
    assert!(!plain.contains("https://example.com/"));
    let before = device.0.borrow().len();
    terminal.draw_linked(size, None, |frame| {
        draw(frame);
        linked_frame("https://example.com/\x1b]2;injected\x07")
    })?;
    assert_eq!(
        device.0.borrow().len(),
        before,
        "unsafe URI changed printable cells or injected OSC"
    );
    Ok(())
}

struct Cut {
    device: Device,
    remaining: Option<usize>,
}
impl Write for Cut {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Some(remaining) = &mut self.remaining {
            if *remaining == 0 {
                self.remaining = None;
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            let length = bytes.len().min(*remaining);
            *remaining -= length;
            return self.device.write(&bytes[..length]);
        }
        self.device.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn links_restore_after_every_cut(journal: bool) -> io::Result<bool> {
    let device = Device::default();
    let size = Size::new(12, 2);
    let mut terminal = FrameTerminal::new(
        RestoreGuard::new(device.clone(), &Capabilities::default()),
        size,
    )?;
    terminal.draw_linked(size, None, |frame| {
        frame.render_widget(Line::raw("docs"), frame.area());
        linked_frame("https://example.com/docs")
    })?;
    let frame = device.0.borrow().clone();
    for cut in 0..frame.len() {
        let capture = Device::default();
        let output = Cut {
            device: capture.clone(),
            remaining: Some(cut),
        };
        let mut guard = RestoreGuard::new(output, &Capabilities::default());
        let mut bytes = frame.clone();
        assert!(
            guard
                .present(
                    &mut bytes,
                    FrameEffects {
                        hyperlinks: journal,
                        ..Default::default()
                    }
                )
                .is_err()
        );
        drop(guard);
        capture.0.borrow_mut().extend_from_slice(b"\x1b[HAFTER");
        let mut term = Term::new(Config::default(), &Grid(size), Events);
        let mut parser: Processor = Processor::new();
        parser.advance(&mut term, &capture.0.borrow());
        for (column, expected) in "AFTER".chars().enumerate() {
            let cell = &term.grid()[Point::new(Row(0), Column(column))];
            if cell.c != expected || cell.hyperlink().is_some() {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

#[test]
fn interrupted_links_never_escape_the_frame() -> io::Result<()> {
    assert!(
        links_restore_after_every_cut(true)?,
        "interrupted OSC 8 leaked into later output"
    );
    assert!(
        !links_restore_after_every_cut(false)?,
        "lost restoration journal passed the same oracle"
    );
    Ok(())
}

// Remove just the selected escape family from original production output.
fn strip(bytes: &[u8], family: u8, end: u8) -> Vec<u8> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at..].starts_with(&[27, family]) {
            let mut next = at + 2;
            if family == b']' {
                while next + 1 < bytes.len() && &bytes[next..next + 2] != b"\x1b\\" {
                    next += 1;
                }
                at = (next + 2).min(bytes.len());
                continue;
            }
            while next < bytes.len() && !(bytes[next].is_ascii_alphabetic() || bytes[next] == end) {
                next += 1;
            }
            if bytes.get(next) == Some(&end) {
                at = next + 1;
                continue;
            }
        }
        out.push(bytes[at]);
        at += 1;
    }
    out
}

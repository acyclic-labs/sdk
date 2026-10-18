//! Headless render-loop measurement over the executable's real composition:
//! ClientViews, ClientRenderer, followed command files and CrosstermBackend.
use acyclic_harness::{
    OperationId, client,
    client::{ClientViews, CommandSession},
    task_volume::TaskVolume,
    tui::{
        ClientRenderer, Editor, PromptBox, PromptInput, ScreenAreas, Viewport,
        key_event::{InputEvent, KeyCode, KeyEvent, KeyModifiers, MouseEvent, MouseEventKind},
        ratatui::{
            Frame, Terminal,
            backend::{Backend, ClearType, CrosstermBackend, WindowSize},
            buffer::Cell,
            layout::{Position, Size},
        },
        term::{Capabilities, FrameTerminal, RestoreGuard},
    },
    wire,
};
use acyclic_stream::{MemoryStream, StreamPath};
use alacritty_terminal::{
    event::EventListener,
    grid::Dimensions,
    term::{Config, Term, TermMode, cell::Flags},
    vte::ansi::{Color, Processor},
};
use std::{cell::RefCell, io, rc::Rc, sync::Arc};

struct NoFiles;
impl acyclic_harness::conversation::ContentResidencyVerifier for NoFiles {}

/// Bytes, write calls and flushes reaching the terminal output half.
#[derive(Clone, Default)]
struct Counter(Rc<RefCell<(Vec<u8>, u64, u64)>>);
impl io::Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut device = self.0.borrow_mut();
        device.0.extend_from_slice(bytes);
        device.1 += 1;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.borrow_mut().2 += 1;
        Ok(())
    }
}

/// The removed output path, kept as the independent baseline: CrosstermBackend
/// writing every queued command to the device (size supplied, as in FrameTerminal).
pub(super) struct PerCommand {
    inner: CrosstermBackend<Counter>,
    size: Size,
}
impl Backend for PerCommand {
    type Error = io::Error;
    fn draw<'a, I: Iterator<Item = (u16, u16, &'a Cell)>>(&mut self, content: I) -> io::Result<()> {
        self.inner.draw(content)
    }
    fn hide_cursor(&mut self) -> io::Result<()> {
        self.inner.hide_cursor()
    }
    fn show_cursor(&mut self) -> io::Result<()> {
        self.inner.show_cursor()
    }
    fn get_cursor_position(&mut self) -> io::Result<Position> {
        Ok(Position::ORIGIN)
    }
    fn set_cursor_position<P: Into<Position>>(&mut self, position: P) -> io::Result<()> {
        self.inner.set_cursor_position(position)
    }
    fn clear(&mut self) -> io::Result<()> {
        self.inner.clear()
    }
    fn clear_region(&mut self, clear_type: ClearType) -> io::Result<()> {
        self.inner.clear_region(clear_type)
    }
    fn size(&self) -> io::Result<Size> {
        Ok(self.size)
    }
    fn window_size(&mut self) -> io::Result<WindowSize> {
        Ok(WindowSize {
            columns_rows: self.size,
            pixels: Size::default(),
        })
    }
    fn flush(&mut self) -> io::Result<()> {
        Backend::flush(&mut self.inner)
    }
}

/// Mixed Markdown: long wrapped prose, fenced code and tool-like output.
pub(super) fn transcript(lines: usize) -> String {
    let prose = "Streaming **markdown** prose with `inline code`, links and enough words to wrap several times at ordinary terminal widths; ".repeat(3);
    let mut text = String::new();
    let mut count = 0;
    let mut block = 0;
    while count < lines {
        match block % 4 {
            0 => text.push_str(&format!("## Section {block}\n\n{prose}\n\n")),
            1 => text.push_str("```rust\nfn main() {\n    let value = compute(42);\n    println!(\"{value}\");\n}\n```\n\n"),
            2 => text.push_str("```text\n$ cargo test\n   Compiling graphcoder v0.2.0\n    Finished test target\ntest result: ok. 12 passed\n```\n\n"),
            _ => text.push_str(&format!("- item {block} with **bold** text\n- second item\n\n")),
        }
        count = text.bytes().filter(|byte| *byte == b'\n').count();
        block += 1;
    }
    text
}

pub(super) struct Rig {
    runtime: tokio::runtime::Runtime,
    owner: wire::Authority,
    pub views: ClientViews,
    files: CommandSession<MemoryStream>,
    renderer: ClientRenderer,
    pub input: PromptInput,
    prompt_files: client::PromptFiles,
    viewport: Viewport,
    screen: Output,
    size: Size,
    output: Counter,
    areas: ScreenAreas,
    maximum_scroll: usize,
    pub frames: u64,
    /// Device length after each frame.
    boundaries: Vec<usize>,
}

enum Output {
    Frame(FrameTerminal<RestoreGuard<Counter>>),
    PerCommand(Terminal<PerCommand>),
}

impl Output {
    fn draw(&mut self, size: Size, render: impl FnOnce(&mut Frame<'_>)) {
        match self {
            Self::Frame(terminal) => terminal.draw(size, render).unwrap(),
            Self::PerCommand(terminal) => {
                terminal.backend_mut().size = size;
                terminal.draw(render).unwrap();
            }
        }
    }
}

/// Totals for one measured interval.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct Work {
    pub frames: u64,
    pub bytes: u64,
    pub writes: u64,
    pub flushes: u64,
    pub allocations: u64,
}

impl Rig {
    /// `per_command` selects the removed output path as an independent baseline.
    pub fn with_output(lines: usize, width: u16, height: u16, per_command: bool) -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let files = runtime.block_on(async {
            let volume = Arc::new(
                TaskVolume::open(
                    Arc::new(MemoryStream::default()),
                    StreamPath::new("perf").unwrap(),
                )
                .await
                .unwrap(),
            );
            CommandSession::open(
                volume,
                OperationId::from_bytes([7; 16])
                    .to_string()
                    .parse()
                    .unwrap(),
                1 << 20,
                64,
            )
            .await
            .unwrap()
        });
        let owner = wire::Authority {
            kind: wire::AggregateKind::Task as i32,
            id: "task".into(),
        };
        let mut views = super::file_views::empty();
        super::file_views::append_text(&mut views, &owner.id, false, &transcript(lines));
        let output = Counter::default();
        let mut rig = Self {
            runtime,
            owner,
            views,
            files,
            renderer: ClientRenderer::default(),
            input: PromptInput::new(Editor::new("")),
            prompt_files: client::PromptFiles::default(),
            viewport: Viewport::default(),
            screen: if per_command {
                Output::PerCommand(
                    Terminal::new(PerCommand {
                        inner: CrosstermBackend::new(output.clone()),
                        size: Size::new(width, height),
                    })
                    .unwrap(),
                )
            } else {
                Output::Frame(
                    FrameTerminal::new(
                        RestoreGuard::new(output.clone(), &Capabilities::default()),
                        Size::new(width, height),
                    )
                    .unwrap(),
                )
            },
            size: Size::new(width, height),
            output,
            areas: ScreenAreas::default(),
            maximum_scroll: 0,
            frames: 0,
            boundaries: Vec::new(),
        };
        rig.frame();
        rig
    }

    /// One iteration of view.rs before its event wait: fold, configure, lay out, draw.
    pub fn frame(&mut self) {
        let Self {
            renderer,
            views,
            owner,
            files,
            input,
            prompt_files,
            screen,
            viewport,
            size,
            ..
        } = self;
        renderer.update(views, owner);
        prompt_files.refresh(files, input).unwrap();
        input.refresh_completions();
        let back = client::tui_scroll(files).unwrap();
        renderer.configure(views, owner, files).unwrap();
        input.busy = renderer.is_busy();
        let size = *size;
        renderer
            .layout(files, input, size.width, size.height)
            .unwrap();
        let mut maximum_scroll = 0;
        let mut areas = ScreenAreas::default();
        let view = renderer.screen_for(files).unwrap();
        screen.draw(size, |frame| {
            maximum_scroll = view.tail_scroll(frame.area(), input);
            let scroll = maximum_scroll.saturating_sub(back);
            areas = view.render(frame, input, viewport, scroll, 1000);
        });
        self.areas = areas;
        self.maximum_scroll = maximum_scroll;
        self.frames += 1;
        self.boundaries.push(self.output.0.borrow().0.len());
    }

    /// view.rs after its wait: prompt input, file effects, then prompt persistence.
    pub fn apply(&mut self, event: InputEvent) {
        let effects = self.input.event(
            event,
            PromptBox::input_area(self.areas.prompt),
            &mut self.viewport,
            1000,
        );
        let Self {
            runtime,
            files,
            input,
            prompt_files,
            areas,
            maximum_scroll,
            ..
        } = self;
        runtime.block_on(async {
            for effect in effects {
                client::file_tui_input(
                    files,
                    &NoFiles,
                    effect,
                    usize::from(areas.transcript.height),
                    *maximum_scroll,
                )
                .await
                .unwrap();
            }
            prompt_files.save(files, input).await.unwrap();
        });
    }

    /// The executable renders once per received event or file change.
    pub fn deliver(&mut self, events: impl IntoIterator<Item = InputEvent>) {
        for event in events {
            self.apply(event);
            self.frame();
        }
    }

    pub fn resize(&mut self, width: u16, height: u16) {
        self.size = Size::new(width, height);
    }

    pub fn scroll_back(&mut self, rows: u32) {
        let Self { runtime, files, .. } = self;
        runtime.block_on(async {
            files
                .write_state(
                    client::CommandFileRoot::Session,
                    "layout.json",
                    &serde_json::json!({ "transcript_back": rows }),
                )
                .await
                .unwrap();
        });
        self.frame();
    }

    pub fn stream(&mut self, delta: &str) {
        super::file_views::append_text(&mut self.views, &self.owner.id, false, delta);
    }

    pub fn measure(&mut self, run: impl FnOnce(&mut Self)) -> Work {
        let (bytes, writes, flushes) = self.counts();
        let frames = self.frames;
        let allocations = acyclic_test_support::measure(|| run(self)).count_total;
        let (b, w, f) = self.counts();
        Work {
            frames: self.frames - frames,
            bytes: b - bytes,
            writes: w - writes,
            flushes: f - flushes,
            allocations,
        }
    }

    fn counts(&self) -> (u64, u64, u64) {
        let device = self.output.0.borrow();
        (device.0.len() as u64, device.1, device.2)
    }

    /// What a terminal emulator shows after each frame since construction.
    fn screens(&self) -> Vec<Screen> {
        let bytes = &self.output.0.borrow().0;
        let mut emulator = Emulator::new();
        let mut start = 0;
        self.boundaries
            .iter()
            .map(|&end| {
                emulator.advance(&bytes[start..end]);
                start = end;
                emulator.screen()
            })
            .collect()
    }
}

/// Every cell (character, colours, flags), the cursor and its visibility.
type Screen = (Vec<(char, Color, Color, Flags)>, (i32, usize), bool);

struct Emulator {
    term: Term<Events>,
    parser: Processor,
}

#[derive(Clone, Copy)]
struct Events;
impl EventListener for Events {}

#[derive(Clone, Copy)]
struct Grid;
impl Dimensions for Grid {
    fn total_lines(&self) -> usize {
        40
    }
    fn screen_lines(&self) -> usize {
        40
    }
    fn columns(&self) -> usize {
        120
    }
}

impl Emulator {
    fn new() -> Self {
        Self {
            term: Term::new(Config::default(), &Grid, Events),
            parser: Processor::new(),
        }
    }

    fn advance(&mut self, bytes: &[u8]) {
        self.parser.advance(&mut self.term, bytes);
        // A synchronized update is applied when it ends.
        self.parser.stop_sync(&mut self.term);
    }

    fn screen(&self) -> Screen {
        let cells = self
            .term
            .grid()
            .display_iter()
            .map(|cell| (cell.c, cell.fg, cell.bg, cell.flags))
            .collect();
        let cursor = self.term.grid().cursor.point;
        let shown = self.term.mode().contains(TermMode::SHOW_CURSOR);
        (cells, (cursor.line.0, cursor.column.0), shown)
    }
}

/// Frames whose screen differs from the one before: the only frames that
/// need to reach the device.
fn changed(screens: &[Screen]) -> u64 {
    screens.windows(2).filter(|pair| pair[0] != pair[1]).count() as u64
}

pub(super) fn wheel(up: bool) -> InputEvent {
    InputEvent::Mouse(MouseEvent {
        kind: if up {
            MouseEventKind::ScrollUp
        } else {
            MouseEventKind::ScrollDown
        },
        column: 10,
        row: 5,
        modifiers: KeyModifiers::NONE,
    })
}

pub(super) fn key(code: KeyCode) -> InputEvent {
    InputEvent::Key(KeyEvent::new(code, KeyModifiers::NONE))
}

type Scenario = (&'static str, usize, fn(&mut Rig));

fn scenarios() -> Vec<Scenario> {
    vec![
        ("wheel burst x500", 500, |rig| {
            rig.deliver((0..500).map(|_| wheel(true)))
        }),
        ("page up x50", 50, |rig| {
            rig.deliver((0..50).map(|_| key(KeyCode::PageUp)))
        }),
        ("page down x50 (from top)", 50, |rig| {
            rig.deliver((0..50).map(|_| key(KeyCode::PageDown)))
        }),
        ("type 200 keys, 4 KiB prompt", 200, |rig| {
            rig.deliver((0..200).map(|i| key(KeyCode::Char((b'a' + (i % 26) as u8) as char))))
        }),
        ("resize x20 (100<->120 cols)", 20, |rig| {
            for i in 0..20 {
                rig.resize(if i % 2 == 0 { 120 } else { 100 }, 40);
                rig.deliver([InputEvent::Resize(0, 0)]);
            }
        }),
        ("stream 500 deltas, scrolled back", 500, |rig| {
            for i in 0..500 {
                rig.stream(if i % 25 == 24 { "word.\n\n" } else { "word " });
                rig.frame();
            }
        }),
        ("stream 500 deltas, at tail", 500, |rig| {
            for i in 0..500 {
                rig.stream(if i % 25 == 24 { "word.\n\n" } else { "word " });
                rig.frame();
            }
        }),
        ("idle frame x100 (no change)", 100, |rig| {
            for _ in 0..100 {
                rig.frame();
            }
        }),
    ]
}

fn prepare(rig: &mut Rig, name: &str) {
    if name.starts_with("type") {
        rig.input.editor.set_text(&"prompt text ".repeat(340), 0);
        rig.frame();
    }
    if name.contains("scrolled back") {
        rig.scroll_back(100);
    } else if name.contains("from top") {
        rig.scroll_back(u32::MAX);
    }
}

/// Release: `PERF_LINES=1000,10000,100000 cargo test --release -p graphcoder
/// --bin graphcoder perf::baseline -- --ignored --nocapture`.
#[test]
#[ignore = "measurement table, not an assertion"]
fn baseline() {
    let lines = std::env::var("PERF_LINES").unwrap_or_else(|_| "1000,10000".into());
    println!(
        "| scenario | lines | output | events | frames | bytes/event | writes/event | allocs/event | us/event |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    for lines in lines.split(',').map(|n| n.parse::<usize>().unwrap()) {
        for per_command in [true, false] {
            for (name, events, run) in scenarios() {
                let mut rig = Rig::with_output(lines, 100, 40, per_command);
                prepare(&mut rig, name);
                let start = std::time::Instant::now();
                let work = rig.measure(run);
                let elapsed = start.elapsed();
                let per = |value: u64| value as f64 / events as f64;
                println!(
                    "| {name} | {lines} | {} | {events} | {} | {:.0} | {:.1} | {:.0} | {:.1} |",
                    if per_command { "per-command" } else { "frame" },
                    work.frames,
                    per(work.bytes),
                    per(work.writes),
                    per(work.allocations),
                    elapsed.as_secs_f64() * 1e6 / events as f64,
                );
            }
        }
    }
}

/// Exact property of the production output path: one device write for each
/// frame that changes the screen, none for a frame that restates it.
fn assert_one_write_per_changed_frame(rig: &Rig, work: &Work) {
    let screens = rig.screens();
    let measured = &screens[screens.len() - 1 - work.frames as usize..];
    assert_eq!(
        work.writes,
        changed(measured),
        "device writes must equal changed frames: {work:?}"
    );
}

/// Page, wheel, typing, resize and streaming frames through one rig. A file
/// change the loop observes (its own layout write) draws again, unchanged.
fn mixed(rig: &mut Rig) {
    for _ in 0..4 {
        rig.deliver([key(KeyCode::PageUp)]);
        rig.frame();
    }
    rig.deliver((0..4).map(|_| wheel(true)));
    rig.deliver((0..4).map(|_| key(KeyCode::PageDown)));
    rig.deliver("typed".chars().map(|c| key(KeyCode::Char(c))));
    rig.resize(90, 30);
    rig.frame();
    for _ in 0..4 {
        rig.stream("streamed words ");
        rig.frame();
        rig.frame();
    }
}

#[test]
fn every_changed_frame_is_one_device_write_independent_of_transcript_length() {
    let mut work = Vec::new();
    for lines in [150, 300] {
        // The removed per-command path is the independent baseline.
        let mut old = Rig::with_output(lines, 100, 40, true);
        let mut new = Rig::with_output(lines, 100, 40, false);
        let before = old.measure(mixed);
        let after = new.measure(mixed);
        assert_eq!(
            new.screens(),
            old.screens(),
            "identical screens every frame"
        );
        assert_eq!(after.frames, before.frames);
        assert!(after.bytes < before.bytes, "restatements are not encoded");
        // Retained frame buffer: the same allocations, less the one String
        // crossterm formats for each elided attribute reset.
        let screens = new.screens();
        let unchanged =
            after.frames - changed(&screens[screens.len() - 1 - after.frames as usize..]);
        assert!(unchanged > 0, "unchanged frames are measured");
        assert_eq!(after.allocations + unchanged, before.allocations);
        assert!(
            before.writes > 10 * before.frames,
            "baseline issues per-command writes"
        );
        assert_one_write_per_changed_frame(&new, &after);
        work.push(after);
    }
    assert_eq!(work[0].writes, work[1].writes);
    assert_eq!(work[0].frames, work[1].frames);
}

/// The live pass: each page key writes the layout file and the loop draws
/// again on the change it observes. N and 2N pages cost exactly N and 2N
/// device writes over 2N and 4N frames.
#[test]
fn a_page_and_its_observed_layout_change_are_one_device_write() {
    for pages in [3, 6] {
        let mut rig = Rig::with_output(300, 100, 40, false);
        let work = rig.measure(|rig| {
            for _ in 0..pages {
                rig.deliver([key(KeyCode::PageUp)]);
                rig.frame();
            }
        });
        assert_eq!((work.frames, work.writes), (2 * pages, pages));
        assert_one_write_per_changed_frame(&rig, &work);
    }
}

#[test]
#[should_panic(expected = "device writes must equal changed frames")]
fn negative_control_per_command_output_is_detected() {
    let mut rig = Rig::with_output(150, 100, 40, true);
    let work = rig.measure(mixed);
    assert_one_write_per_changed_frame(&rig, &work);
}

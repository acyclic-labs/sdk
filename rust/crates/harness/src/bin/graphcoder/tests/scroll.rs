use acyclic_harness::{
    OperationId,
    client::{
        CommandFileRoot, CommandSession, InputBatch, file_tui_inputs, frame_due, input_burst,
        scroll_tui, transcript_motion, tui_scroll,
    },
    task_volume::{Query, TaskVolume},
    tui::{
        Action, Editor, InputEffect, PromptBox, PromptInput, Screen, Viewport, WHEEL_ROWS,
        key_event::{InputEvent, KeyCode, KeyEvent, KeyModifiers, MouseEvent, MouseEventKind},
        ratatui::{Terminal, backend::TestBackend, layout::Rect, text::Line},
    },
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;

struct NoFiles;
impl acyclic_harness::conversation::ContentResidencyVerifier for NoFiles {}

fn notch(up: bool) -> InputEvent {
    let kind = if up {
        MouseEventKind::ScrollUp
    } else {
        MouseEventKind::ScrollDown
    };
    InputEvent::Mouse(MouseEvent {
        kind,
        column: 3,
        row: 1,
        modifiers: KeyModifiers::NONE,
    })
}

fn key(code: KeyCode) -> InputEvent {
    InputEvent::Key(KeyEvent::new(code, KeyModifiers::NONE))
}

/// Transcript rows per page in the driven layout.
const PAGE: usize = 8;

async fn session(name: &str) -> (Arc<TaskVolume<MemoryStream>>, CommandSession<MemoryStream>) {
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new(name).unwrap(),
        )
        .await
        .unwrap(),
    );
    let identity = OperationId::from_bytes([12; 16]).to_string();
    let files = CommandSession::open(volume.clone(), identity.parse().unwrap(), 65_536, 64)
        .await
        .unwrap();
    (volume, files)
}

async fn position(volume: &TaskVolume<MemoryStream>) -> u64 {
    volume
        .snapshot(&Query::Root, 64)
        .await
        .unwrap()
        .generation
        .position
}

/// How the driven loop handles a decoded read.
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    /// Route one event and present a frame per event (the pre-wheel loop).
    PerEvent,
    /// Coalesce motions, but present after every burst (negative control).
    PerBurst,
    /// The `GraphCoder` loop: coalesce motions, present when `frame_due`.
    Loop,
}

/// Drive the input step of the `GraphCoder` loop over decoded reads, returning
/// frames presented, layout positions appended and the final offset.
async fn drive_with(
    reads: Vec<Vec<InputEvent>>,
    mode: Mode,
    input: &mut PromptInput,
) -> (usize, u64, usize) {
    let (volume, mut files) = session("burst").await;
    let before = position(&volume).await;
    let mut frames = 0;
    let mut effects = Vec::new();
    for read in reads {
        let mut batch: InputBatch = read.into();
        while !batch.is_empty() {
            let mut route =
                |event| input.event(event, Rect::new(0, 9, 80, 1), &mut Viewport::default(), 1);
            if mode == Mode::PerEvent {
                effects.clear();
                if let Some(event) = batch.pop_front() {
                    effects.extend(route(event));
                }
            } else {
                input_burst(&mut batch, PAGE, &mut effects, route);
            }
            file_tui_inputs(&mut files, &NoFiles, effects.drain(..), PAGE, 10_000)
                .await
                .unwrap();
            // The loop's next iteration presents only when `frame_due`.
            if mode != Mode::Loop || frame_due(&batch) {
                frames += 1;
            }
        }
    }
    let appended = position(&volume).await - before;
    (frames, appended, tui_scroll(&files).unwrap())
}

async fn drive(reads: Vec<Vec<InputEvent>>, coalesce: bool) -> (usize, u64, usize) {
    let mode = if coalesce { Mode::Loop } else { Mode::PerEvent };
    drive_with(reads, mode, &mut PromptInput::new(Editor::new("draft"))).await
}

#[test]
fn a_wheel_flick_or_held_page_key_is_one_scroll_and_one_frame_per_input_batch() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let one = drive(vec![vec![notch(true)]], true).await;
        let per_write = one.1;
        assert!(per_write > 0);
        assert_eq!(one, (1, per_write, WHEEL_ROWS.unsigned_abs()));
        for motions in [32usize, 64] {
            // One read: N motions (wheel notches, one reversed, and a
            // PageUp), then a typed key that ends the burst. Exactly one
            // frame and one layout write whatever N.
            let mut read: Vec<_> = (0..motions)
                .map(|at| match at {
                    5 => notch(false),
                    7 => key(KeyCode::PageUp),
                    _ => notch(true),
                })
                .collect();
            read.push(key(KeyCode::Char('x')));
            let rows = WHEEL_ROWS.unsigned_abs() * (motions - 2) - WHEEL_ROWS.unsigned_abs() + PAGE;
            assert_eq!(drive(vec![read.clone()], true).await, (1, per_write, rows));
            // Negative control: per-event handling draws N + 1 frames and
            // writes the layout N times for the same final offset.
            let per_event = drive(vec![read], false).await;
            assert_eq!(per_event, (motions + 1, per_write * motions as u64, rows));
        }
        // A non-motion event ends the burst: later motions are a new layout
        // change, presented in the same single frame of the read.
        let read = vec![notch(true), key(KeyCode::Char('x')), notch(true)];
        assert_eq!(drive(vec![read], true).await, (1, 2 * per_write, 6));
        // Reads are never merged: one frame each, however soon the next
        // read completes.
        let reads = vec![vec![notch(true), notch(true)], vec![notch(true)]];
        assert_eq!(drive(reads, true).await, (2, 2 * per_write, 9));
    });
}

/// Held keys and pasted text without bracketed paste decode as one key event
/// per character. Every event of the read is routed, in order, and the read
/// presents exactly one frame; a pointer event that reads the presented prompt
/// viewport is the only event preceded by its own frame.
#[test]
fn a_key_repeat_or_unbracketed_paste_read_presents_one_frame() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for n in [32usize, 64] {
            let read: Vec<_> = (0..n).map(|_| key(KeyCode::Char('z'))).collect();
            let mut input = PromptInput::new(Editor::new("draft"));
            let coalesced = drive_with(vec![read.clone()], Mode::Loop, &mut input).await;
            assert_eq!(coalesced.0, 1, "one read must present one frame");
            assert_eq!(input.editor.text().matches('z').count(), n);
            // Negative control: presenting after every burst, as each
            // non-motion event ends one, presents N frames for the same text.
            let mut control = PromptInput::new(Editor::new("draft"));
            let per_burst = drive_with(vec![read], Mode::PerBurst, &mut control).await;
            assert_eq!(per_burst.0, n);
            assert_eq!((coalesced.1, coalesced.2), (per_burst.1, per_burst.2));
            assert_eq!(input.editor.text(), control.editor.text());
        }
        // A click routes against a presented viewport: one frame before it,
        // one after the read.
        let click = InputEvent::Mouse(MouseEvent {
            kind: MouseEventKind::Down(acyclic_harness::tui::key_event::MouseButton::Left),
            column: 3,
            row: 9,
            modifiers: KeyModifiers::NONE,
        });
        let read = vec![key(KeyCode::Char('a')), click, key(KeyCode::Char('b'))];
        let mut input = PromptInput::new(Editor::new("draft"));
        assert_eq!(drive_with(vec![read], Mode::Loop, &mut input).await.0, 2);
    });
}

/// Render and route exactly as the shipping loop does. The independent oracle
/// renders per event; the mutant restores consumption of a pointer behind a
/// motion, using the viewport from before the preceding key burst.
fn pointer_after_motions<const ABSORB_POINTER: bool>(
    count: usize,
    width: u16,
    wrap: bool,
    motion: InputEvent,
    per_event: bool,
) -> (String, usize, std::ops::Range<usize>, usize) {
    let screen = Screen::default();
    let area = Rect::new(0, 0, width, 10);
    let mut input = PromptInput::default();
    let mut settings = *input.editor.settings();
    settings.wrap = wrap;
    input.set_settings(settings);
    // Locate the visible click independently of either driven viewport.
    let mut probe = input.clone();
    probe.editor.set_text(&"x".repeat(count), 0);
    let click_area = PromptBox::input_area(screen.areas(area, &probe).prompt);
    let click = InputEvent::Mouse(MouseEvent {
        kind: MouseEventKind::Down(acyclic_harness::tui::key_event::MouseButton::Left),
        column: click_area.x,
        row: click_area.y,
        modifiers: KeyModifiers::NONE,
    });
    let mut batch: InputBatch = (0..count)
        .map(|_| key(KeyCode::Char('x')))
        .chain([motion, click])
        .collect();
    let mut terminal = Terminal::new(TestBackend::new(width, 10)).unwrap();
    let mut viewport = Viewport::default();
    let mut effects = Vec::new();
    let mut frames = 0;
    let mut present = true;
    while !batch.is_empty() {
        let mut areas = screen.areas(area, &input);
        if present {
            terminal
                .draw(|frame| {
                    areas = screen.render(frame, &input, &mut viewport, 0, 0);
                })
                .unwrap();
            frames += 1;
        }
        let page = usize::from(areas.transcript.height);
        let mut route =
            |event| input.event(event, PromptBox::input_area(areas.prompt), &mut viewport, 0);
        if per_event {
            effects.clear();
            effects.extend(route(batch.pop_front().unwrap()));
        } else {
            input_burst(&mut batch, page, &mut effects, &mut route);
            if ABSORB_POINTER
                && !effects.is_empty()
                && effects
                    .iter()
                    .all(|effect| transcript_motion(effect, page).is_some())
                && !batch.is_empty()
                && frame_due(&batch)
            {
                effects.extend(route(batch.pop_front().unwrap()));
            }
        }
        present = per_event || frame_due(&batch);
    }
    terminal
        .draw(|frame| {
            screen.render(frame, &input, &mut viewport, 0, 0);
        })
        .unwrap();
    (
        input.editor.text().to_owned(),
        input.editor.cursor(),
        input.editor.selection(),
        frames + 1,
    )
}

fn assert_pointer_after_motions<const ABSORB_POINTER: bool>(count: usize, width: u16, wrap: bool) {
    for motion in [
        notch(true),
        notch(false),
        key(KeyCode::PageUp),
        key(KeyCode::PageDown),
    ] {
        let expected = pointer_after_motions::<false>(count, width, wrap, motion.clone(), true);
        let actual = pointer_after_motions::<ABSORB_POINTER>(count, width, wrap, motion, false);
        assert_eq!(
            (&actual.0, actual.1, &actual.2),
            (&expected.0, expected.1, &expected.2),
            "pointer routing must match per-event rendering"
        );
        assert_eq!(
            actual.3, 3,
            "one initial frame, one before the click, one after the read"
        );
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    // Property: layout-only key routing followed by any scroll motion and a
    // pointer must preserve the editor text, cursor and selection of painting
    // every event, with exactly one presentation boundary before the pointer.
    #[test]
    fn pointers_behind_motions_use_the_current_prompt_viewport(
        count in 32usize..96, width in 8u16..24, wrap in proptest::bool::ANY,
    ) {
        assert_pointer_after_motions::<false>(count, width, wrap);
    }
}

#[test]
#[should_panic(expected = "pointer routing must match per-event rendering")]
fn negative_control_absorbing_a_pointer_uses_the_stale_prompt_viewport() {
    assert_pointer_after_motions::<true>(64, 12, true);
}

/// Final offset and positions appended for `motions` from offset `start`,
/// published one layout change per motion or one for the whole burst.
async fn scrolled(start: usize, motions: &[isize], maximum: usize, burst: bool) -> (usize, u64) {
    let (volume, mut files) = session("bounds").await;
    scroll_tui(&mut files, [isize::try_from(start).unwrap()], maximum)
        .await
        .unwrap();
    let before = position(&volume).await;
    if burst {
        scroll_tui(&mut files, motions.iter().copied(), maximum)
            .await
            .unwrap();
    } else {
        for &rows in motions {
            scroll_tui(&mut files, [rows], maximum).await.unwrap();
        }
    }
    (
        tui_scroll(&files).unwrap(),
        position(&volume).await - before,
    )
}

#[test]
fn a_burst_against_a_bound_ends_where_ordered_scrolling_ends() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (down, up) = (-WHEEL_ROWS, WHEEL_ROWS);
        // At the tail, Down clamps and Up then moves: 3 rows back.
        assert_eq!(scrolled(0, &[down, up], 40, false).await.0, 3);
        assert_eq!(
            scrolled(0, &[down, up], 40, true).await,
            (3, scrolled(0, &[up], 40, true).await.1)
        );
        // At the top, Up clamps and Down then moves.
        assert_eq!(scrolled(40, &[up, down], 40, true).await.0, 37);
        // Negative control: the summed delta loses the clamp.
        assert_eq!(scrolled(0, &[down + up], 40, true).await.0, 0);
    });
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(48))]
    // Property: from either limit, a burst folded in memory ends exactly where
    // publishing each motion in order ends, in at most one layout write.
    #[test]
    fn coalesced_scrolling_equals_ordered_scrolling_at_both_limits(
        motions in proptest::collection::vec(proptest::sample::select(vec![-isize::try_from(PAGE).unwrap(), -WHEEL_ROWS, WHEEL_ROWS, isize::try_from(PAGE).unwrap()]), 1..24),
        maximum in 0usize..40,
        at_top in proptest::bool::ANY,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let start = if at_top { maximum } else { 0 };
            let (ordered, ordered_writes) = scrolled(start, &motions, maximum, false).await;
            let (burst, burst_writes) = scrolled(start, &motions, maximum, true).await;
            proptest::prop_assert_eq!(burst, ordered);
            if burst == start {
                proptest::prop_assert_eq!(burst_writes, 0);
            } else {
                proptest::prop_assert!(burst_writes > 0 && burst_writes <= ordered_writes);
            }
            Ok(())
        })?;
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    // Property: page keys and wheel notches decode to bounded row motions that
    // preserve the prompt, paint the selected rows, survive reopen, and append
    // the same single layout write per change and none for a clamped motion.
    #[test]
    fn transcript_pages_and_wheel_notches_are_bounded_followed_files(sequence in proptest::collection::vec((proptest::bool::ANY, proptest::bool::ANY), 1..16), height in 6u16..32) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let volume = Arc::new(TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("scroll").unwrap()).await.unwrap());
            let identity = OperationId::from_bytes([11;16]).to_string();
            let mut files = CommandSession::open(volume.clone(), identity.parse().unwrap(), 65_536, 64).await.unwrap();
            let lines: Vec<_> = (0..60).map(|row| Line::raw(format!("Transcript row {row:03}"))).collect();
            let screen = Screen { transcript: &lines, ..Screen::default() };
            let mut input = PromptInput::new(Editor::new("draft"));
            let area = Rect::new(0, 0, 80, height);
            let areas = screen.areas(area, &input);
            let page = usize::from(areas.transcript.height);
            let maximum = screen.tail_scroll(area, &input);
            let mut expected = 0usize;
            let mut per_write = None;
            for (up, wheel) in sequence {
                let (event, effect, step) = if wheel {
                    let kind = if up { MouseEventKind::ScrollUp } else { MouseEventKind::ScrollDown };
                    let mouse = MouseEvent { kind, column: 3, row: areas.transcript.y, modifiers: KeyModifiers::NONE };
                    (InputEvent::Mouse(mouse), InputEffect::Scroll(if up { WHEEL_ROWS } else { -WHEEL_ROWS }), WHEEL_ROWS.unsigned_abs())
                } else {
                    let key = KeyEvent::new(if up { KeyCode::PageUp } else { KeyCode::PageDown }, KeyModifiers::NONE);
                    (InputEvent::Key(key), InputEffect::Host(if up { Action::TranscriptUp } else { Action::TranscriptDown }), page)
                };
                let effects:Vec<_> = input.event(event, areas.prompt, &mut Viewport::default(), 1).collect();
                proptest::prop_assert_eq!(&effects, &vec![effect]);
                let rows = effects.first().and_then(|effect| transcript_motion(effect, page)).unwrap();
                let before = volume.snapshot(&Query::Root, 64).await.unwrap().generation;
                scroll_tui(&mut files, [rows], maximum).await.unwrap();
                let previous = expected;
                expected = if up { expected.saturating_add(step).min(maximum) } else { expected.saturating_sub(step) };
                proptest::prop_assert_eq!(tui_scroll(&files).unwrap(), expected);
                let appended = volume.snapshot(&Query::Root, 64).await.unwrap().generation.position - before.position;
                if previous == expected {
                    proptest::prop_assert_eq!(appended, 0);
                } else {
                    proptest::prop_assert!(appended > 0);
                    proptest::prop_assert_eq!(*per_write.get_or_insert(appended), appended);
                }
                proptest::prop_assert_eq!(input.editor.text(), "draft");
                let mut terminal = Terminal::new(TestBackend::new(80, height)).unwrap();
                terminal.draw(|frame| { screen.render(frame, &input, &mut Viewport::default(), maximum - expected, 0); }).unwrap();
                proptest::prop_assert!(super::screen_text(&terminal).contains(&format!("Transcript row {:03}", maximum - expected)), "selected transcript row is visible");
                let reopened = CommandSession::open(volume.clone(), identity.parse().unwrap(), 65_536, 64).await.unwrap();
                proptest::prop_assert_eq!(tui_scroll(&reopened).unwrap(), expected);
            }
            // Resize clamps stale offsets before the next movement.
            scroll_tui(&mut files, [isize::MAX], 2).await.unwrap();
            proptest::prop_assert_eq!(tui_scroll(&files).unwrap(), 2);
            files.write_state(CommandFileRoot::Session, "layout.json", &serde_json::json!({"transcript_back":false})).await.unwrap();
            let before = volume.snapshot(&Query::Root, 64).await.unwrap().generation;
            proptest::prop_assert!(scroll_tui(&mut files, [WHEEL_ROWS], maximum).await.is_err());
            proptest::prop_assert_eq!(volume.snapshot(&Query::Root, 64).await.unwrap().generation, before);
            Ok(())
        })?;
    }
}

fn assert_reused_burst_storage<const ALLOCATE: bool>() {
    let mut input = PromptInput::default();
    let mut viewport = Viewport::default();
    let area = Rect::new(0, 0, 80, 1);
    let mut effects = Vec::new();
    // Warm the same storage once, then both N and 2N fit its retained capacity.
    let mut warm: InputBatch = (0..64).map(|_| notch(true)).collect();
    input_burst(&mut warm, PAGE, &mut effects, |event| {
        input.event(event, area, &mut viewport, 0)
    });
    let capacity = effects.capacity();
    for n in [32, 64] {
        for _ in 0..2 {
            let mut batch: InputBatch = (0..n).map(|at| notch(at % 3 != 0)).collect();
            let work = acyclic_test_support::measure(|| {
                if ALLOCATE {
                    input_burst(&mut batch, PAGE, &mut effects, |event| {
                        input
                            .event(event, area, &mut viewport, 0)
                            .collect::<Vec<_>>()
                    });
                } else {
                    input_burst(&mut batch, PAGE, &mut effects, |event| {
                        input.event(event, area, &mut viewport, 0)
                    });
                }
            });
            assert_eq!(
                (work.count_total, work.bytes_total),
                (0, 0),
                "reused burst routing allocates nothing"
            );
            assert_eq!(effects.len(), n);
            assert_eq!(effects.capacity(), capacity);
            assert!(batch.is_empty());
        }
    }
}

#[test]
fn a_wheel_burst_reuses_storage_at_n_and_2n() {
    assert_reused_burst_storage::<false>();
}
#[test]
#[should_panic(expected = "reused burst routing allocates nothing")]
fn negative_control_per_notch_vec_fails_reused_burst_assertion() {
    assert_reused_burst_storage::<true>();
}

fn assert_file_motion_allocations<const COLLECT: bool>() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut costs = Vec::new();
    for n in [32, 64] {
        let (_, mut files) = runtime.block_on(session("motion-allocations"));
        runtime.block_on(files.write_state(CommandFileRoot::Session,"layout.json",&serde_json::json!({
            "transcript_back":1000,"selected_task":acyclic_harness::TaskId::from_bytes([41;16]).to_string()
        }))).unwrap();
        let mut input = PromptInput::default();
        let mut effects = Vec::new();
        let mut batch: InputBatch = (0..n).map(|_| notch(true)).collect();
        input_burst(&mut batch, PAGE, &mut effects, |event| {
            input.event(event, Rect::new(0, 0, 80, 1), &mut Viewport::default(), 0)
        });
        let measured = acyclic_test_support::measure(|| {
            if COLLECT {
                let motions = effects
                    .drain(..)
                    .filter_map(|effect| transcript_motion(&effect, PAGE))
                    .collect::<Vec<_>>();
                runtime
                    .block_on(file_tui_inputs(
                        &mut files,
                        &NoFiles,
                        motions.into_iter().map(InputEffect::Scroll),
                        PAGE,
                        10000,
                    ))
                    .unwrap();
            } else {
                runtime
                    .block_on(file_tui_inputs(
                        &mut files,
                        &NoFiles,
                        effects.drain(..),
                        PAGE,
                        10000,
                    ))
                    .unwrap();
            }
        });
        assert_eq!(
            tui_scroll(&files).unwrap(),
            1000 + n * WHEEL_ROWS.unsigned_abs()
        );
        assert!(effects.is_empty());
        costs.push((measured.count_total, measured.bytes_total));
    }
    assert_eq!(
        costs[0], costs[1],
        "file motion folding retains no per-burst motion buffer"
    );
}

#[test]
fn file_motion_folding_allocations_do_not_grow_with_the_burst() {
    assert_file_motion_allocations::<false>();
}
#[test]
#[should_panic(expected = "file motion folding retains no per-burst motion buffer")]
fn negative_control_collected_motions_fail_the_same_allocation_assertion() {
    assert_file_motion_allocations::<true>();
}

use acyclic_harness::{
    OperationId,
    client::{
        CommandFileRoot, CommandSession, InputBatch, file_tui_inputs, input_burst, scroll_tui,
        transcript_motion, tui_scroll,
    },
    task_volume::{Query, TaskVolume},
    tui::{
        Action, Editor, InputEffect, PromptInput, Screen, Viewport, WHEEL_ROWS,
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

/// Drive the input step of the `GraphCoder` loop over decoded reads, returning
/// frames drawn, layout positions appended and the final offset.
async fn drive(reads: Vec<Vec<InputEvent>>, coalesce: bool) -> (usize, u64, usize) {
    let (volume, mut files) = session("burst").await;
    let mut input = PromptInput::new(Editor::new("draft"));
    let before = position(&volume).await;
    let mut frames = 0;
    for read in reads {
        let mut batch: InputBatch = read.into();
        while !batch.is_empty() {
            frames += 1;
            let mut route =
                |event| input.event(event, Rect::new(0, 9, 80, 1), &mut Viewport::default(), 1);
            let effects = if coalesce {
                input_burst(&mut batch, PAGE, route)
            } else {
                batch.pop_front().map(&mut route).unwrap_or_default()
            };
            file_tui_inputs(&mut files, &NoFiles, effects, PAGE, 10_000)
                .await
                .unwrap();
        }
    }
    let appended = position(&volume).await - before;
    (frames, appended, tui_scroll(&files).unwrap())
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
        // A non-motion event ends the burst: later motions are a new frame.
        let read = vec![notch(true), key(KeyCode::Char('x')), notch(true)];
        assert_eq!(drive(vec![read], true).await, (2, 2 * per_write, 6));
        // Reads are never merged: one frame each, however soon the next
        // read completes.
        let reads = vec![vec![notch(true), notch(true)], vec![notch(true)]];
        assert_eq!(drive(reads, true).await, (2, 2 * per_write, 9));
    });
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
                let effects = input.event(event, areas.prompt, &mut Viewport::default(), 1);
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

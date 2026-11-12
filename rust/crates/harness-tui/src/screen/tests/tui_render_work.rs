//! R25: input and streamed deltas through the real editor, transcript and paint
//! path. Work counters, rather than shared-host wall time, are the oracle.
use super::*;
use crate::{
    BlockKind, TranscriptRows,
    key_event::{KeyCode, KeyEvent, KeyModifiers},
};
use acyclic_test_support::scale::{Bound, assert_scales};

#[derive(Debug, PartialEq, Eq)]
struct Work {
    rows: usize,
    wrapped_bytes: usize,
}

fn scene(length: usize, width: u16, height: u16, rewrap: bool, scan: bool) -> [Work; 2] {
    let mut transcript = TranscriptRows::default();
    transcript.rows(width);
    for _ in 0..length {
        transcript.push(BlockKind::Answer, "retained history");
    }
    transcript.tail(Some("delta"));
    let mut input = PromptInput::default();
    input.editor.set_text("draft", 0);
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    let mut viewport = Viewport::default();
    // Prime the same frame cache before either measured event.
    let paint = |terminal: &mut Terminal<TestBackend>,
                 transcript: &mut TranscriptRows,
                 input: &PromptInput,
                 viewport: &mut Viewport| {
        terminal
            .draw(|frame| {
                let screen = Screen {
                    transcript: transcript.rows(width),
                    ..Screen::default()
                };
                let scroll = screen.tail_scroll(frame.area(), input);
                screen.render(frame, input, viewport, scroll, 0);
            })
            .unwrap();
    };
    paint(&mut terminal, &mut transcript, &input, &mut viewport);
    std::array::from_fn(|event| {
        ROW_WORK.with(|work| work.set(0));
        crate::transcript::WRAP_BYTES.with(|work| work.set(0));
        if event == 0 {
            assert!(
                input
                    .key(
                        KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE),
                        1,
                        usize::from(width)
                    )
                    .is_empty()
            );
            assert_eq!(input.editor.text(), "draftx");
        } else {
            transcript.tail(Some("delta painted"));
        }
        // Mutants exercise the same measured path and oracle: historical
        // wrapping or a pre-paint archive traversal must both be detected.
        if rewrap {
            transcript.rows(width + 1);
            transcript.rows(width);
            transcript.tail(Some("delta painted"));
        }
        if scan {
            for _ in transcript.rows(width) {
                ROW_WORK.with(|work| work.set(work.get() + 1));
            }
        }
        paint(&mut terminal, &mut transcript, &input, &mut viewport);
        let work = Work {
            rows: ROW_WORK.with(|work| work.get()),
            wrapped_bytes: crate::transcript::WRAP_BYTES.with(|work| work.get()),
        };
        let text = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(text.contains("draftx"), "input did not paint");
        assert!(
            text.contains(if event == 0 { "delta" } else { "delta painted" }),
            "delta did not paint"
        );
        work
    })
}

fn independent_of_history(width: u16, height: u16, rewrap: bool, scan: bool) {
    let small = scene(100, width, height, rewrap, scan);
    let large = scene(10_000, width, height, rewrap, scan);
    for (before, after) in small.iter().zip(&large) {
        assert_scales(
            Bound::Constant,
            (100, before.rows as u64),
            (10_000, after.rows as u64),
        );
        assert_scales(
            Bound::Constant,
            (100, before.wrapped_bytes as u64),
            (10_000, after.wrapped_bytes as u64),
        );
    }
    for work in large {
        assert!(
            work.rows <= usize::from(height),
            "paint exceeded visible rows"
        );
    }
}

fn rejects_each_viewport(rewrap: bool, scan: bool) {
    for (width, height) in [(80, 50), (120, 40)] {
        let rejection = std::panic::catch_unwind(|| {
            independent_of_history(width, height, rewrap, scan);
        })
        .expect_err("historical work escaped the viewport's oracle");
        let message = rejection
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| rejection.downcast_ref::<&str>().copied())
            .unwrap_or("non-text panic");
        assert!(
            message.contains("counted work exceeded Constant"),
            "unexpected failure at {width}x{height}: {message}"
        );
    }
}

#[test]
fn input_and_delta_paint_work_is_independent_of_history() {
    for (width, height) in [(80, 50), (120, 40)] {
        independent_of_history(width, height, false, false);
    }
}

#[test]
fn negative_control_rewrapping_history() {
    rejects_each_viewport(true, false);
}

#[test]
fn negative_control_scanning_history() {
    rejects_each_viewport(false, true);
}

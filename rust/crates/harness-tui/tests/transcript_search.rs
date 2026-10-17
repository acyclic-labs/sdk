//! Complete draft isolation and allocation accounting for transcript search.
#![cfg(feature = "tui")]
use acyclic_harness_tui::ratatui::text::Line;
use acyclic_harness_tui::{Editor, PromptInput, WrappedLines};
use proptest::prelude::*;

fn round_trip<const SEPARATE: bool>(text: &str, query: &str, cursor: usize, select: bool) {
    let mut input = PromptInput::default();
    let mut settings = *input.editor.settings();
    settings.paste_collapse_bytes = 128;
    input.set_settings(settings);
    input.editor.insert("prefix", 0);
    input.editor.paste(text, 1000);
    input
        .editor
        .set_cursor(cursor.min(input.editor.text().len()), select);
    let before = format!("{:?}", input.editor);
    let mut expected = input.editor.clone();
    if SEPARATE {
        input.set_transcript_search(Some(query));
        input.editor.insert("needle", 2000);
        input.set_transcript_search(Some("updated query"));
        input.set_transcript_search(None);
    } else {
        let draft = input.editor.text().to_owned();
        input.editor.set_text(query, 0);
        input.editor.set_text("needle", 1);
        input.editor.set_text(&draft, 2);
    }
    assert_eq!(
        format!("{:?}", input.editor),
        before,
        "complete draft must round trip"
    );
    for _ in 0..3 {
        assert_eq!(input.editor.undo(), expected.undo());
        assert_eq!(format!("{:?}", input.editor), format!("{expected:?}"));
    }
    for _ in 0..3 {
        assert_eq!(input.editor.redo(), expected.redo());
        assert_eq!(format!("{:?}", input.editor), format!("{expected:?}"));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn complete_editor_round_trip(text in "[a-zß日€🦀\n]{1,80}", query in ".{0,20}", cursor in 0usize..300, select in any::<bool>()) {
        round_trip::<true>(&format!("{}\n",text).repeat(20),&query,cursor,select);
    }
}

#[test]
fn retained_paste_seed_keeps_fold_cursor_and_undo() {
    round_trip::<true>(&"p".repeat(220), "needle", 0, false);
    let mut input = PromptInput::default();
    input.editor = Editor::new("saved chat");
    input.set_transcript_search(Some("query"));
    input.set_chat_draft("peer chat");
    assert_eq!(input.editor.text(), "query");
    input.set_transcript_search(None);
    assert_eq!(input.editor.text(), "peer chat");
}

#[test]
#[should_panic(expected = "complete draft must round trip")]
fn undoable_chat_replacement_negative_control() {
    round_trip::<false>(&"folded\n".repeat(20), "needle", 0, false);
}

fn allocation_model<const COPIES: bool, const FULL: bool>() {
    use acyclic_harness_tui::ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
    let selected = acyclic_harness_tui::config::default_style(
        acyclic_harness_tui::config::ThemeRole::Selection,
    );
    for length in [8192, 16384] {
        let text = "a".repeat(length);
        let mut projection = WrappedLines::default();
        projection.update(&[Line::raw(text.clone())], 0, 80);
        let mut search = acyclic_harness_tui::TranscriptSearch::default();
        search.project(&projection);
        let area = Rect::new(0, 0, 80, 1);
        let mut buffer = Buffer::empty(area);
        let query = acyclic_test_support::measure(|| {
            search.update(&projection, "a", 0, selected);
            search.prepare(&projection, 10, 10);
        });
        assert_eq!(query.count_total, 0, "query must reuse index storage");
        let background = acyclic_test_support::measure(|| {
            while search.pending() {
                search.advance(&projection, 256);
            }
        });
        assert_eq!(
            background.count_total, 0,
            "background must reuse index storage"
        );
        let frame = acyclic_test_support::measure(|| {
            search.update(&projection, "A", 1, selected);
            search.prepare(&projection, 10, 10);
            for row in 10..20 {
                search.row(&projection, row).render(area, &mut buffer);
            }
            if COPIES {
                for row in 10..20 {
                    std::hint::black_box(projection.rows()[row].clone());
                }
            }
            if FULL {
                std::hint::black_box(acyclic_harness_tui::transcript_matches(&text, "a"));
            }
        });
        assert_eq!(
            frame.count_total, 0,
            "unchanged frame must allocate zero times"
        );
        assert_eq!(
            frame.bytes_total, 0,
            "unchanged frame must allocate zero bytes"
        );
        let refinement = acyclic_test_support::measure(|| {
            search.update(&projection, "aa", 0, selected);
            search.prepare(&projection, 10, 10);
            while search.pending() {
                search.advance(&projection, 256);
            }
        });
        assert_eq!(
            refinement.count_total, 0,
            "refinement must reuse index storage"
        );
    }
}

#[test]
fn highlights_and_query_folds_allocate_zero_after_projection() {
    allocation_model::<false, false>();
}
#[test]
#[should_panic(expected = "unchanged frame must allocate zero times")]
fn owned_display_copies_negative_control() {
    allocation_model::<true, false>();
}
#[test]
#[should_panic(expected = "unchanged frame must allocate zero times")]
fn whole_query_reallocation_negative_control() {
    allocation_model::<false, true>();
}

#[test]
fn physical_find_shortcut_preserves_the_open_undo_group() {
    use acyclic_harness_tui::key_event::{KeyCode, KeyEvent, KeyModifiers};
    let mut input = PromptInput::default();
    input.committed_text("draft", 0);
    let before = format!("{:?}", input.editor);
    let effects = input.key(
        KeyEvent::new(KeyCode::Char('f'), KeyModifiers::CONTROL),
        1,
        80,
    );
    assert_eq!(effects.len(), 1);
    assert_eq!(format!("{:?}", input.editor), before);
    input.set_transcript_search(Some("needle"));
    input.set_transcript_search(None);
    assert_eq!(format!("{:?}", input.editor), before);
}

#[test]
fn unchanged_unicode_cells_allocate_zero() {
    use acyclic_harness_tui::{
        TranscriptSearch,
        ratatui::{buffer::Buffer, layout::Rect, widgets::Widget},
    };
    let mut projection = WrappedLines::default();
    projection.update(&[Line::raw(format!("x{} 👨‍👩‍👧‍👦", "\u{301}".repeat(40)))], 0, 80);
    let mut search = TranscriptSearch::default();
    search.update(&projection, "x", 0, Default::default());
    search.prepare(&projection, 0, 1);
    let area = Rect::new(0, 0, 80, 1);
    let mut buffer = Buffer::empty(area);
    search.row(&projection, 0).render(area, &mut buffer);
    let frame =
        acyclic_test_support::measure(|| search.row(&projection, 0).render(area, &mut buffer));
    assert_eq!((frame.count_total, frame.bytes_total), (0, 0));
}

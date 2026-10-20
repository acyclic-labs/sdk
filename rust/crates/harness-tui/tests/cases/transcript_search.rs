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
        round_trip::<true>(&format!("{text}\n").repeat(20),&query,cursor,select);
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
        search.project(&projection, true);
        let area = Rect::new(0, 0, 80, 1);
        let mut buffer = Buffer::empty(area);
        // Cold query/candidate storage grows with the query and actual matches.
        // Exact cold-allocation and capacity bounds live in memory_tests.
        search.update(&projection, "a", 0, selected);
        search.prepare(&projection, 10, 10);
        while search.pending() {
            search.advance(&projection, 256);
        }
        let frame = acyclic_test_support::measure(|| {
            search.update(&projection, "A", 1, selected);
            search.prepare(&projection, 10, 10);
            for row in 10..20 {
                search.row(&projection, row).render(area, &mut buffer);
            }
            if COPIES {
                for row in 10..20 {
                    std::hint::black_box(projection.rows().get(row).cloned());
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
        search.update(&projection, "aa", 0, selected);
        search.prepare(&projection, 10, 10);
        while search.pending() {
            search.advance(&projection, 256);
        }
        let refinement_frame = acyclic_test_support::measure(|| {
            search.update(&projection, "AA", 0, selected);
            search.prepare(&projection, 10, 10);
            for row in 10..20 {
                search.row(&projection, row).render(area, &mut buffer);
            }
        });
        assert_eq!(refinement_frame.count_total, 0);
        assert_eq!(refinement_frame.bytes_total, 0);
    }
}

#[test]
fn highlights_and_equivalent_queries_allocate_zero_after_count() {
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

fn unicode_storage_model<const RESET: bool>(marks: usize, emoji: bool) {
    use acyclic_harness_tui::{
        TranscriptSearch,
        ratatui::{buffer::Buffer, layout::Rect, widgets::Widget},
    };
    let text = format!(
        "x{} {}",
        "\u{301}".repeat(marks),
        if emoji {
            "👨‍👩‍👧‍👦"
        } else {
            ""
        }
    );
    let mut projection = WrappedLines::default();
    projection.update(&[Line::raw(text)], 0, 80);
    let mut search = TranscriptSearch::default();
    search.update(&projection, "x", 0, Default::default());
    search.prepare(&projection, 0, 1);
    let area = Rect::new(0, 0, 80, 1);
    let mut buffer = Buffer::empty(area);
    search.row(&projection, 0).render(area, &mut buffer);
    for _ in 0..3 {
        let frame = acyclic_test_support::measure(|| {
            if RESET {
                buffer.reset();
            }
            search.row(&projection, 0).render(area, &mut buffer);
        });
        assert_eq!(
            frame.count_total, 0,
            "retained Unicode frame must allocate zero times"
        );
        assert_eq!(
            frame.bytes_total, 0,
            "retained Unicode frame must allocate zero bytes"
        );
    }
}

#[test]
fn unchanged_unicode_retained_buffer_allocates_zero() {
    unicode_storage_model::<false>(40, true);
}

#[test]
#[should_panic(expected = "retained Unicode frame must allocate zero times")]
fn discarded_unicode_buffer_storage_negative_control() {
    unicode_storage_model::<true>(40, true);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn unchanged_unicode_retained_buffer_property(marks in 0usize..80, emoji in any::<bool>()) {
        unicode_storage_model::<false>(marks, emoji);
    }
}

#[test]
fn unchanged_unicode_terminal_draws_measure_unpatched_residual() {
    use acyclic_harness_tui::{
        TranscriptSearch,
        ratatui::{Terminal, backend::TestBackend},
    };
    let mut projection = WrappedLines::default();
    projection.update(&[Line::raw(format!("x{} 👨‍👩‍👧‍👦", "\u{301}".repeat(40)))], 0, 80);
    let mut search = TranscriptSearch::default();
    search.update(&projection, "x", 0, Default::default());
    search.prepare(&projection, 0, 1);
    let mut terminal = Terminal::new(TestBackend::new(80, 1)).unwrap();
    for _ in 0..2 {
        terminal
            .draw(|frame| frame.render_widget(search.row(&projection, 0), frame.area()))
            .unwrap();
    }
    for _ in 0..3 {
        let measured = acyclic_test_support::measure(|| {
            terminal
                .draw(|frame| frame.render_widget(search.row(&projection, 0), frame.area()))
                .unwrap();
        });
        // Stock Terminal::draw drops symbols in its alternating buffers.
        // Keep the measured residual explicit until a public retained-frame API is available.
        assert_eq!((measured.count_total, measured.bytes_total), (2, 113));
    }
}

#[test]
fn terminal_reset_keeps_empty_semantics_and_symbol_changes() {
    use acyclic_harness_tui::ratatui::{Terminal, backend::TestBackend, text::Span};
    let mut terminal = Terminal::new(TestBackend::new(80, 1)).unwrap();
    for text in [
        format!("x{} 👨‍👩‍👧‍👦", "\u{301}".repeat(40)),
        String::new(),
        "日".to_owned(),
        String::new(),
    ] {
        terminal
            .draw(|frame| frame.render_widget(Span::raw(&text), frame.area()))
            .unwrap();
        let symbol = terminal.backend().buffer()[(0, 0)].symbol();
        assert_eq!(
            symbol,
            if text.is_empty() {
                " "
            } else {
                unicode_segmentation::UnicodeSegmentation::graphemes(text.as_str(), true)
                    .next()
                    .unwrap()
            }
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn retained_terminal_cells_equal_fresh_reset_baseline(frames in prop::collection::vec("[x日€ \u{301}👨‍👩‍👧‍👦]{0,40}", 1..16)) {
        use acyclic_harness_tui::ratatui::{Terminal, backend::TestBackend, buffer::Cell, text::Span};
        let mut candidate = Terminal::new(TestBackend::new(80, 1)).unwrap();
        let mut baseline = Terminal::new(TestBackend::new(80, 1)).unwrap();
        for text in frames {
            let actual = candidate.draw(|frame| frame.render_widget(Span::raw(&text), frame.area())).map(|_| ());
            let expected = baseline.draw(|frame| {
                frame.buffer_mut().content.fill(Cell::EMPTY);
                frame.render_widget(Span::raw(&text), frame.area());
            }).map(|_| ());
            prop_assert_eq!(actual.map_err(|error| error.to_string()), expected.map_err(|error| error.to_string()));
            prop_assert_eq!(candidate.backend().buffer(), baseline.backend().buffer());
        }
    }
}

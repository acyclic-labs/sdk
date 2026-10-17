//! Complete draft isolation and allocation accounting for transcript search.
#![cfg(feature = "tui")]
use acyclic_harness_tui::ratatui::{
    style::Modifier,
    text::{Line, Span},
};
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

fn allocation_model<const PER_GRAPHEME: bool>() {
    let text = "a".repeat(8192);
    let source = [Line::raw(text.clone())];
    let mut projection = WrappedLines::default();
    projection.update(&source, 0, 80);
    let rows = 8192usize.div_ceil(80);
    let selected = acyclic_harness_tui::config::default_style(
        acyclic_harness_tui::config::ThemeRole::Selection,
    )
    .add_modifier(Modifier::UNDERLINED);
    // Independent fixed-width ASCII baseline; no candidate layout or state.
    let mut baseline = Vec::with_capacity(rows);
    let reference = acyclic_test_support::measure(|| {
        for chunk in text.as_bytes().chunks(80) {
            baseline.push(Line::from(vec![Span::styled(
                String::from_utf8(chunk.to_vec()).unwrap(),
                selected,
            )]));
        }
    });
    let candidate = acyclic_test_support::measure(|| {
        projection.highlight(&[0..8192], None, selected);
        if PER_GRAPHEME {
            let spans: Vec<_> = text.chars().map(|c| Span::raw(c.to_string())).collect();
            std::hint::black_box(spans);
        }
    });
    assert_eq!(reference.count_total, (2 * rows) as u64);
    assert_eq!(
        reference.bytes_total,
        (8192 + rows * std::mem::size_of::<Span<'static>>()) as u64
    );
    assert_eq!(
        candidate.count_total, reference.count_total,
        "highlight allocation count must equal coalesced rows"
    );
    assert_eq!(
        candidate.bytes_total, reference.bytes_total,
        "highlight allocation bytes must equal coalesced rows"
    );
    assert_eq!(
        projection
            .rows()
            .iter()
            .map(|row| row.spans.len())
            .sum::<usize>(),
        rows
    );
    assert_eq!(projection.rows(), baseline);
    // The complete public query path also has an exact budget: matcher buffers,
    // 8192 retained match locations, and one extra span for the selected byte.
    let mut search = acyclic_harness_tui::TranscriptSearch::default();
    let query = acyclic_test_support::measure(|| search.update(&mut projection, "a", 0, selected));
    assert_eq!(search.count(), 8192);
    assert_eq!(
        query.count_total,
        (2 * rows + 18) as u64,
        "full query allocation count"
    );
    assert_eq!(
        query.bytes_total,
        (8192 + (rows + 4) * std::mem::size_of::<Span<'static>>() + 327672) as u64,
        "full query allocation bytes"
    );

    assert_eq!(
        projection
            .rows()
            .iter()
            .flat_map(|row| &row.spans)
            .map(|span| span.content.as_ref())
            .collect::<String>(),
        text
    );
}

#[test]
fn highlights_allocate_exactly_one_owned_span_per_uniform_row() {
    allocation_model::<false>();
}

#[test]
#[should_panic(expected = "highlight allocation count must equal coalesced rows")]
fn per_grapheme_spans_negative_control() {
    allocation_model::<true>();
}

//! Wrapping invariants, included in the focused screen test target.
use acyclic_harness_tui::ratatui::{
    Terminal,
    backend::TestBackend,
    style::{Color, Modifier, Style},
    text::{Line, Span},
};
use acyclic_harness_tui::{PromptInput, Screen, Viewport, WrappedLines};
use proptest::prelude::*;

fn text(rows: &[Line<'_>]) -> String {
    rows.iter()
        .flat_map(|line| line.spans.iter())
        .map(|span| span.content.as_ref())
        .collect()
}

#[test]
fn wrapped_graphemes_keep_style_and_exact_width_has_no_cursor_row() {
    let red = Style::default().fg(Color::Red).add_modifier(Modifier::BOLD);
    let source = [Line::from(vec![
        Span::styled("e", red),
        Span::raw("\u{301}界👩‍💻"),
    ])];
    let mut cache = WrappedLines::default();
    cache.update(&source, 0, 2);
    assert_eq!(text(cache.rows()), "e\u{301}界👩‍💻");
    assert_eq!(cache.rows().len(), 3);
    assert_eq!(
        cache
            .rows()
            .first()
            .and_then(|row| row.spans.first())
            .map(|span| span.style),
        Some(red)
    );
    assert_eq!(
        cache
            .rows()
            .first()
            .and_then(|row| row.spans.first())
            .map(|span| span.content.as_ref()),
        Some("e\u{301}")
    );
    assert_eq!(
        cache
            .rows()
            .get(2)
            .and_then(|row| row.spans.first())
            .map(|span| span.content.as_ref()),
        Some("👩‍💻")
    );
    let allocations = acyclic_test_support::measure(|| cache.update(&source, source.len(), 2));
    assert_eq!(
        allocations.count_total, 0,
        "unchanged frames must reuse the projection"
    );

    let mut terminal = Terminal::new(TestBackend::new(12, 10)).unwrap();
    terminal
        .draw(|frame| {
            Screen {
                transcript: cache.rows(),
                ..Screen::default()
            }
            .render(
                frame,
                &PromptInput::default(),
                &mut Viewport::default(),
                0,
                0,
            );
        })
        .unwrap();
    let first = &terminal.backend().buffer()[(0, 1)];
    assert_eq!(first.symbol(), "e\u{301}");
    assert_eq!(first.fg, Color::Red);
    assert!(first.modifier.contains(Modifier::BOLD));
}

#[test]
fn crlf_split_across_styles_is_one_line_break() {
    let source = [Line::from(vec![
        Span::raw("first\r"),
        Span::styled("\nsecond", Style::default().fg(Color::Blue)),
    ])];
    let mut cache = WrappedLines::default();
    cache.update(&source, 0, 20);
    assert_eq!(cache.rows().len(), 2);
    assert_eq!(text(cache.rows()), "firstsecond");
    assert_eq!(
        cache
            .rows()
            .get(1)
            .and_then(|row| row.spans.first())
            .map(|span| span.style.fg),
        Some(Some(Color::Blue))
    );
    cache.update(&source, source.len(), 5);
    assert_eq!(
        cache.rows().len(),
        3,
        "no cursor-only row before a hard break"
    );
    assert_eq!(text(cache.rows()), "firstsecond");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    // Property: every append/replacement/truncation and width change has the
    // same styled rows as a fresh projection, and wrapping preserves all text.
    #[test]
    fn incremental_projection_equals_reopen(
        changes in prop::collection::vec((0usize..8, prop::collection::vec(prop::sample::select(vec!["a", "日", "界", "👩‍💻", "e\u{301}", " "]), 0..20), 2u16..24), 1..16)
    ) {
        let mut cache = WrappedLines::default();
        let mut source = Vec::new();
        for (cut, pieces, width) in changes {
            let cut = cut.min(source.len());
            source.truncate(cut);
            source.push(Line::raw(pieces.concat()));
            cache.update(&source, cut, width);
            let mut replay = WrappedLines::default();
            replay.update(&source, 0, width);
            prop_assert_eq!(cache.rows(), replay.rows());
            prop_assert_eq!(text(cache.rows()), text(&source));
            prop_assert!(cache.rows().iter().all(|row| row.width() <= usize::from(width)));
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    // Partitioning identical styled text at any character boundary preserves
    // every projected row, including CR/LF and combining/ZWJ boundaries.
    #[test]
    fn span_partitions_preserve_the_same_logical_newlines(
        pieces in prop::collection::vec(prop::sample::select(vec!["a", "日", "👩‍💻", "e", "\u{301}", "\r", "\n", "\t"]), 0..24),
        cut in 0usize..64, width in 1u16..24,
    ) {
        let text = pieces.concat();
        let middle = text.char_indices().nth(cut).map_or(text.len(), |(index, _)| index);
        let source = [Line::from(vec![Span::raw(&text)])];
        let (head, tail) = text.split_at_checked(middle).unwrap_or((&text, ""));
        let split = [Line::from(vec![Span::raw(head), Span::raw(tail)])];
        let mut whole = WrappedLines::default();
        let mut divided = WrappedLines::default();
        whole.update(&source, 0, width);
        divided.update(&split, 0, width);
        prop_assert_eq!(whole.rows(), divided.rows());
    }
}

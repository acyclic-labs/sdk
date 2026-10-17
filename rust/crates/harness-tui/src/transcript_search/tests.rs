use super::*;
use proptest::prelude::*;
fn settle(view: &mut TranscriptSearch, wrapped: &crate::WrappedLines) {
    view.prepare(wrapped, 0, wrapped.rows().len());
    while view.pending() {
        view.advance(wrapped, 31);
    }
}
fn painted(
    view: &TranscriptSearch,
    wrapped: &crate::WrappedLines,
) -> Vec<ratatui::text::Line<'static>> {
    use ratatui::{
        buffer::Buffer,
        layout::Rect,
        style::{Color, Style},
        text::{Line, Span},
        widgets::Widget,
    };
    wrapped
        .rows()
        .iter()
        .enumerate()
        .map(|(row, line)| {
            let width = line.width() as u16;
            if width == 0 {
                return Line::default();
            }
            let area = Rect::new(0, 0, width, 1);
            let mut buffer = Buffer::empty(area);
            view.row(wrapped, row).render(area, &mut buffer);
            let mut spans: Vec<Span<'static>> = Vec::new();
            let mut skip = 0;
            for cell in buffer.content {
                if skip != 0 {
                    skip -= 1;
                    continue;
                }
                skip = unicode_width::UnicodeWidthStr::width(cell.symbol()).saturating_sub(1);
                let mut style = Style::default().add_modifier(cell.modifier);
                if cell.fg != Color::Reset {
                    style = style.fg(cell.fg);
                }
                if cell.bg != Color::Reset {
                    style = style.bg(cell.bg);
                }
                if let Some(last) = spans.last_mut().filter(|last| last.style == style) {
                    last.content.to_mut().push_str(cell.symbol());
                } else {
                    spans.push(Span::styled(cell.symbol().to_owned(), style));
                }
            }
            Line::from(spans)
        })
        .collect()
}
fn selection() -> ratatui::style::Style {
    crate::config::default_style(crate::config::ThemeRole::Selection)
}

// Independent whole-string fold plus a naive window scan. The production path
// retains only needle-sized origins and uses prefix links rather than rescans.
fn reference(text: &str, query: &str) -> Vec<Range<usize>> {
    let folded: Vec<_> = text.chars().default_case_fold().collect();
    let needle: Vec<_> = query.chars().default_case_fold().collect();
    if needle.is_empty() {
        return Vec::new();
    }
    let mut origins = Vec::new();
    for (start, character) in text.char_indices() {
        let count = character.to_string().chars().default_case_fold().count();
        origins.extend(std::iter::repeat_n(
            start..start + character.len_utf8(),
            count,
        ));
    }
    let mut found = Vec::new();
    for (at, window) in folded.windows(needle.len()).enumerate() {
        if window == needle {
            let range = origins[at].start..origins[at + needle.len() - 1].end;
            if found.last() != Some(&range) {
                found.push(range);
            }
        }
    }
    found
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn folded_search_preserves_source_ranges_and_has_linear_work(
        text in prop::collection::vec(any::<char>(), 0..96),
        query in prop::collection::vec(any::<char>(), 0..16),
    ) {
        let text: String = text.into_iter().collect();
        let query: String = query.into_iter().collect();
        let (found, work) = matches(&text, &query);
        prop_assert_eq!(&found, &reference(&text, &query));
        for range in found {
            prop_assert!(text.get(range).is_some());
        }
        let size = text.chars().default_case_fold().count() + query.chars().default_case_fold().count();
        prop_assert!(work <= 3 * size);
    }

    #[test]
    fn highlights_preserve_display_text_across_spans_and_widths(
        text in "[a-zA-ZßΣσςİ日€🦀 ]{0,40}",
        query in "[a-zA-ZßΣσςİ日€🦀 ]{0,8}",
        split in 0usize..48,
        width in 1u16..81,
    ) {
        use ratatui::text::{Line,Span};
        let at = text.char_indices().nth(split).map_or(text.len(), |(at,_)|at);
        let source = [Line::from(vec![Span::raw(text[..at].to_owned()),Span::raw(text[at..].to_owned())])];
        let mut view = TranscriptSearch::default();
        let mut wrapped=crate::WrappedLines::default();
        wrapped.update(&source,0,width);
        view.update(&mut wrapped,&query,0,selection());
    settle(&mut view, &wrapped);
        prop_assert_eq!(view.count(),reference(&text,&query).len());
        let actual: String=painted(&view,&wrapped).iter().flat_map(|line| &line.spans).map(|span|span.content.as_ref()).collect();
        let layout=crate::Layout::new(&crate::Editor::new(&text),usize::from(width));
        let expected: String=layout.rows.iter().flat_map(|row| &row.cells).map(|cell|cell.symbol.as_str()).collect();
        prop_assert_eq!(actual,expected);
        if view.count()!=0 {prop_assert!(view.selected_row(&wrapped, -1)<wrapped.rows().len());}
    }
}

#[test]
fn corpus_expansions_and_overlapping_matches() {
    for (text, query) in [
        ("Straße STRASSE", "strasse"),
        ("İi\u{307} 日本語", "i\u{307}"),
        ("Σσς", "σ"),
        ("ßß", "sss"),
        ("aaaaa", "aaa"),
        ("🦀 € 日", "日"),
    ] {
        assert_eq!(transcript_matches(text, query), reference(text, query));
    }
}

#[test]
fn highlighted_ranges_cross_spans_and_preserve_markdown_styles() {
    use ratatui::{
        style::{Modifier, Style},
        text::{Line, Span},
    };
    let mut view = TranscriptSearch::default();
    let source = [
        Line::from(vec![
            Span::styled("Stra", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw("ße"),
        ]),
        Line::raw("STRASSE"),
    ];
    let mut wrapped = crate::WrappedLines::default();
    wrapped.update(&source, 0, 80);
    view.update(&mut wrapped, "strasse", 0, selection());
    settle(&mut view, &wrapped);
    let painted_rows = painted(&view, &wrapped);
    assert_eq!(view.count(), 2);
    assert_eq!(view.selected_row(&wrapped, -1), 1);
    assert_eq!(view.selected_row(&wrapped, 2), 0);
    assert!(
        painted_rows[0].spans[0]
            .style
            .add_modifier
            .contains(Modifier::BOLD | Modifier::UNDERLINED)
    );
    assert!(
        painted_rows
            .iter()
            .flat_map(|line| &line.spans)
            .all(|span| span.style.add_modifier.contains(Modifier::UNDERLINED))
    );
    assert_eq!(
        painted_rows
            .iter()
            .map(|line| line
                .spans
                .iter()
                .map(|span| span.content.as_ref())
                .collect::<String>())
            .collect::<Vec<_>>(),
        ["Straße", "STRASSE"]
    );
    wrapped.update(&source, 0, 3);
    view.update(&mut wrapped, "strasse", 0, selection());
    settle(&mut view, &wrapped);
    assert_eq!(view.count(), 2, "wrapping must not split the search needle");
    assert!(view.selected_row(&wrapped, 1) > 0);
}

#[test]
#[should_panic(expected = "ASCII folding loses Unicode matches")]
fn ascii_only_search_is_detected_negative_control() {
    let broken: Vec<_> = "Straße"
        .to_ascii_lowercase()
        .match_indices("strasse")
        .map(|(at, text)| at..at + text.len())
        .collect();
    assert_eq!(
        broken,
        reference("Straße", "strasse"),
        "ASCII folding loses Unicode matches"
    );
}

#[test]
#[should_panic(expected = "search must have linear work")]
fn rescanning_prefixes_is_detected_negative_control() {
    let text = format!("{}b", "a".repeat(128));
    let query = format!("{}b", "a".repeat(64));
    let mut work = 0;
    for window in text.as_bytes().windows(query.len()) {
        for (left, right) in window.iter().zip(query.as_bytes()) {
            work += 1;
            if left != right {
                break;
            }
        }
    }
    assert!(
        work <= 3 * (text.len() + query.len()),
        "search must have linear work"
    );
}

#[test]
fn clearing_highlights_restores_the_same_wrapped_rows() {
    use ratatui::{
        style::{Modifier, Style},
        text::{Line, Span},
    };
    let source = [Line::from(vec![
        Span::styled("Straße\t", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw("STRASSE\nend"),
    ])];
    let mut wrapped = crate::WrappedLines::default();
    wrapped.update(&source, 0, 5);
    let ordinary = wrapped.rows().to_vec();
    let mut search = TranscriptSearch::default();
    search.update(&mut wrapped, "strasse", 0, selection());
    settle(&mut search, &wrapped);
    assert_eq!(search.count(), 2);
    assert_ne!(painted(&search, &wrapped), ordinary);
    assert_eq!(wrapped.rows(), ordinary);
}

#[test]
fn partial_matches_preserve_every_style_boundary() {
    use ratatui::{
        style::{Modifier, Style},
        text::{Line, Span},
    };
    let bold = Style::default().add_modifier(Modifier::BOLD);
    let italic = Style::default().add_modifier(Modifier::ITALIC);
    let source = [Line::from(vec![
        Span::styled("ab", bold),
        Span::styled("cd", italic),
    ])];
    let mut wrapped = crate::WrappedLines::default();
    wrapped.update(&source, 0, 80);
    let mut search = TranscriptSearch::default();
    search.update(&mut wrapped, "bc", 0, selection());
    settle(&mut search, &wrapped);
    let rows = painted(&search, &wrapped);
    let spans = &rows[0].spans;
    assert_eq!(
        spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<Vec<_>>(),
        ["a", "b", "c", "d"]
    );
    assert_eq!(spans[0].style, bold);
    assert_eq!(spans[3].style, italic);
    assert!(
        spans[1]
            .style
            .add_modifier
            .contains(Modifier::BOLD | Modifier::UNDERLINED)
    );
    assert!(
        spans[2]
            .style
            .add_modifier
            .contains(Modifier::ITALIC | Modifier::UNDERLINED)
    );
}

fn selected_theme_model<const INDEX: bool, const THEME: bool>() {
    use ratatui::{
        style::{Color, Modifier, Style},
        text::Line,
    };
    let mut wrapped = crate::WrappedLines::default();
    wrapped.update(&[Line::raw("one one")], 0, 80);
    let mut search = TranscriptSearch::default();
    let red = Style::default().fg(Color::Red);
    search.update(&mut wrapped, "one", 0, red);
    settle(&mut search, &wrapped);
    assert_eq!(search.position(0), 1);
    assert!(
        painted(&search, &wrapped)[0].spans[0]
            .style
            .add_modifier
            .contains(Modifier::REVERSED)
    );
    search.update(&mut wrapped, "one", if INDEX { 1 } else { 0 }, red);
    settle(&mut search, &wrapped);
    assert!(
        !painted(&search, &wrapped)[0].spans[0]
            .style
            .add_modifier
            .contains(Modifier::REVERSED),
        "selected occurrence must change within the same row"
    );
    assert!(
        painted(&search, &wrapped)[0]
            .spans
            .last()
            .unwrap()
            .style
            .add_modifier
            .contains(Modifier::REVERSED)
    );
    assert_eq!(
        search.selected_row(&wrapped, 0),
        search.selected_row(&wrapped, 1)
    );
    assert_eq!(search.position(1), 2);
    let blue = Style::default().fg(Color::Blue).bg(Color::Yellow);
    search.update(&mut wrapped, "one", 1, if THEME { blue } else { red });
    settle(&mut search, &wrapped);
    assert_eq!(
        painted(&search, &wrapped)[0].spans[0].style.fg,
        Some(Color::Blue),
        "resolved selection color must change"
    );
    assert_eq!(
        painted(&search, &wrapped)[0].spans.last().unwrap().style.bg,
        Some(Color::Yellow)
    );
}

#[test]
fn selected_occurrence_and_resolved_theme_repaint_the_shared_rows() {
    selected_theme_model::<true, true>();
}

#[test]
#[should_panic(expected = "selected occurrence must change within the same row")]
fn missing_selected_index_negative_control() {
    selected_theme_model::<false, true>();
}

#[test]
#[should_panic(expected = "resolved selection color must change")]
fn hardcoded_selection_theme_negative_control() {
    selected_theme_model::<true, false>();
}

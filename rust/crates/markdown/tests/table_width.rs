use super::*;
use crate::{Markdown, Parser, options};
use proptest::prelude::*;

fn full(source: &str, width: u16) -> Vec<Line> {
    let mut lines = Vec::new();
    let mut renderer = render::Renderer::with_width(width);
    let mut depth = 0usize;
    for event in Parser::new_ext(source, options()) {
        match &event {
            Event::Start(_) => depth += 1,
            Event::End(_) => depth -= 1,
            _ => {}
        }
        renderer.event(&event);
        if depth == 0 {
            renderer.flush();
            lines.append(&mut renderer.lines);
            lines.push(Line::default());
            renderer = render::Renderer::with_width(width);
        }
    }
    while lines.last().is_some_and(|line| line.spans.is_empty()) {
        lines.pop();
    }
    lines
}

fn bounded<'a>(
    lines: impl IntoIterator<Item = &'a Line> + Copy,
    input: usize,
    rows: usize,
    width: u16,
) -> usize {
    let mut seen = std::collections::HashSet::new();
    let retained: usize = lines
        .into_iter()
        .flat_map(|line| &line.spans)
        .map(|span| span.text.retained(&mut seen).0)
        .sum();
    let bound = 12 * (input + rows * usize::from(width.max(1)));
    assert!(
        retained <= bound,
        "{retained} retained output bytes > {bound}: input {input}, rows {rows}, width {width}"
    );
    for line in lines {
        let text: String = line.spans.iter().map(|span| span.text.as_str()).collect();
        assert!(
            UnicodeWidthStr::width(text.as_str()) <= usize::from(width.max(1)),
            "display width exceeds {width}: {text:?}"
        );
    }
    retained
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/table_width.txt")
        ))),
        ..ProptestConfig::with_cases(32)
    })]
    #[test]
    fn table_output_is_bounded(
        width in 0u16..129,
        columns in 1usize..24,
        rows in 1usize..65,
        length in 0usize..513,
        atom in prop::sample::select(vec!["x", "界", "❤️", "❤\u{fe0e}", "❤\u{fe0f}", "👩‍💻", "👨‍👩‍👧‍👦", "e\u{301}", "a\u{308}\u{301}", "❤**\u{fe0f}**", "👩**\u{200d}**💻", "**e**\u{301}", "&#10;", "[x](u)", "**x**", "\\*"]),
        short_rows in any::<bool>(),
        large_in_header in any::<bool>(),
        large_row in 0usize..65,
    ) {
        let large = atom.repeat(length);
        let header_cell = if large_in_header { large.as_str() } else { "h" };
        let header = format!("|{header_cell}{}|\n", "|**h**".repeat(columns - 1));
        let separator = format!("|{}\n", ":---:|".repeat(columns));
        let row = if short_rows { "|x|\n".to_owned() } else { format!("|{}\n", "y|".repeat(columns)) };
        let body: String = (0..rows).map(|index| {
            if !large_in_header && index == large_row % rows {
                format!("|{large}{}|\n", "|y".repeat(columns - 1))
            } else { row.clone() }
        }).collect();
        let source = format!("{header}{separator}{body}");
        let mut markdown = Markdown::with_width(width);
        markdown.push(&source);
        bounded(markdown.lines(), source.len(), rows + 1, width);
        prop_assert_eq!(markdown.lines(), full(&source, width));
        prop_assert_eq!(markdown.into_source(), source.as_str());

        let mut streamed = Markdown::with_width(width);
        let mut consumer = Vec::new();
        let mut prefix = String::new();
        for delta in source.split_inclusive('\n') {
            prefix.push_str(delta);
            let changed = streamed.push(delta);
            consumer.truncate(changed);
            consumer.extend((streamed.lines().slice(changed..).expect("valid change boundary")).iter().cloned());
            prop_assert_eq!(&consumer, &full(&prefix, width));
        }
        bounded(streamed.lines(), source.len(), rows + 1, width);
    }
}

#[test]
fn large_cell_and_many_rows_keep_bounded_padding() {
    let source = format!(
        "| {} |\n|---|\n{}",
        "x".repeat(100_000),
        "| x |\n".repeat(10_000)
    );
    let mut markdown = Markdown::with_width(80);
    markdown.push(&source);
    let retained = bounded(markdown.lines(), source.len(), 10_001, 80);
    println!(
        "{} input bytes, 10,001 rows, width 80: {retained} retained output bytes",
        source.len()
    );
}

#[test]
fn table_width_survives_clear_and_late_definitions() {
    let mut markdown = Markdown::with_width(17);
    for _ in 0..2 {
        let mut source = String::new();
        let mut consumer = Vec::new();
        for delta in [
            "| [a very long label][r] | second |\n|---|---|\n",
            "| x | y |\n",
            "\n[r]: /target\n\n",
        ] {
            source.push_str(delta);
            let changed = markdown.push(delta);
            consumer.truncate(changed);
            consumer.extend(
                (markdown
                    .lines()
                    .slice(changed..)
                    .expect("valid change boundary"))
                .iter()
                .cloned(),
            );
            assert_eq!(consumer, full(&source, 17));
            bounded(markdown.lines(), source.len(), 2, 17);
        }
        markdown.clear();
    }
}

#[test]
#[should_panic(expected = "retained output bytes")]
fn output_property_detects_unbounded_column_width() {
    let source = format!("|{}|\n|---|\n{}", "x".repeat(4096), "|x|\n".repeat(128));
    let mut table = Table::new(vec![Alignment::None], Style::default(), 8);
    // Deliberately restore the old unbounded-width behavior in the real path.
    table.cell_width = usize::MAX;
    for event in Parser::new_ext(&source, options()) {
        table.event(&event, None);
    }
    bounded(&table.render(), source.len(), 129, 8);
}

#[test]
fn table_trim_preserves_graphemes_across_spans() {
    for text in [
        "❤️xx",
        "❤\u{fe0e}xx",
        "👩‍💻xx",
        "👨‍👩‍👧‍👦xx",
        "e\u{301}xx",
        "❤**\u{fe0f}**xx",
    ] {
        for width in 5..12 {
            let source = format!("|{text}|\n|---|\n");
            let mut markdown = Markdown::with_width(width);
            markdown.push(&source);
            bounded(markdown.lines(), source.len(), 1, width);
        }
    }
}

fn table_limit_chunks(columns: usize, rows: usize, batch: usize, cells: usize, recover: bool) {
    let header = format!("|{}\n|{}\n", "x|".repeat(columns), "---|".repeat(columns));
    let mut source = header.clone();
    let mut markdown = Markdown::with_width(80);
    markdown.push(&header);
    let mut consumer = markdown.lines().to_vec();
    let row = format!("|{}\n", "x|".repeat(cells));
    let mut count = 0;
    while count < rows {
        let take = batch.min(rows - count);
        let delta = row.repeat(take);
        source.push_str(&delta);
        let changed = markdown.push(&delta);
        assert!(changed <= markdown.lines().len());
        consumer.truncate(changed);
        consumer.extend(
            (markdown.lines().slice(changed..).expect("change boundary"))
                .iter()
                .cloned(),
        );
        count += take;
        if count < rows && rows - count < batch {
            assert_eq!(consumer, full(&source, 80));
        }
    }
    assert_eq!(consumer, full(&source, 80));
    if recover {
        for delta in ["|x".to_owned(), format!("{}|\n", "|x".repeat(columns - 1))] {
            source.push_str(&delta);
            let changed = markdown.push(&delta);
            consumer.truncate(changed);
            consumer.extend(
                (markdown.lines().slice(changed..).expect("change boundary"))
                    .iter()
                    .cloned(),
            );
            assert_eq!(consumer, full(&source, 80));
        }
    }
    bounded(markdown.lines(), source.len(), rows + 2, 80);
}

#[test]
fn table_stream_preserves_empty_cell_limit() {
    table_limit_chunks(1025, 258, 1, 2, false);
    table_limit_chunks(1025, 256, 64, 2, true);
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/table_width.txt")
        ))),
        ..ProptestConfig::with_cases(8)
    })]
    #[test]
    fn table_shape_and_limits_survive_chunk_splits(
        columns in 1023usize..1028,
        around in 0usize..4,
        batch in 64usize..130,
        cells in 1usize..4,
    ) {
        let threshold = (1 << 18) / (columns - cells);
        table_limit_chunks(columns, threshold - 1 + around, batch, cells, true);
    }
}

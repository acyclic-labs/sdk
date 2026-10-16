use super::*;
use crate::{Markdown, Parser, blocks, options};
use proptest::prelude::*;

fn full(source: &str, width: u16) -> Vec<Line> {
    let mut lines = Vec::new();
    for (_, events) in blocks(Parser::new_ext(source, options()).into_offset_iter()) {
        let mut renderer = render::Renderer::with_width(width);
        for event in events {
            renderer.event(&event);
        }
        renderer.flush();
        lines.extend(renderer.lines);
        lines.push(Line::default());
    }
    while lines.last().is_some_and(|line| line.spans.is_empty()) {
        lines.pop();
    }
    lines
}

fn bounded(lines: &[Line], input: usize, rows: usize, width: u16) -> usize {
    let retained: usize = lines
        .iter()
        .flat_map(|line| &line.spans)
        .map(|span| span.text.capacity())
        .sum();
    let bound = 12 * (input + rows * usize::from(width.max(1)));
    assert!(
        retained <= bound,
        "{retained} retained output bytes > {bound}: input {input}, rows {rows}, width {width}"
    );
    for line in lines {
        assert!(super::width(line) <= usize::from(width.max(1)));
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
        atom in prop::sample::select(vec!["x", "界", "👩‍💻", "e\u{301}", "&#10;", "[x](u)", "**x**", "\\*"]),
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
            consumer.extend_from_slice(&streamed.lines()[changed..]);
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
            consumer.extend_from_slice(&markdown.lines()[changed..]);
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

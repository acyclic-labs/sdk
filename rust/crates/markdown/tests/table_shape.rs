use super::*;

#[test]
#[should_panic(expected = "clipped parse header changed table output")]
fn table_limit_property_detects_clipped_parse_shape() {
    let header = format!("|{}\n|{}\n", "x|".repeat(1025), "---|".repeat(1025));
    let body = "|x|y|\n".repeat(258);
    let mut streamed = Markdown::with_width(80);
    streamed.push(&header);
    let stream = streamed.table.as_mut().expect("open table");
    // Restore the old coupling between parsing and visible columns.
    let visible = stream.table.alignments.len();
    stream.columns = visible;
    stream.header = format!("|{}\n|{}\n", "x|".repeat(visible), "---|".repeat(visible));
    streamed.push(&body);
    let mut full = Markdown::with_width(80);
    full.push(&format!("{header}{body}"));
    assert!(
        streamed.lines() == full.lines(),
        "clipped parse header changed table output"
    );
}

use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/table_width.txt")
        ))),
        ..ProptestConfig::with_cases(64)
    })]
    #[test]
    fn table_logical_cell_count_matches_parser(
        columns in 1usize..65,
        suffix in prop_oneof![
            "[a-z|\\\\ \t`*#<>&;]{0,256}",
            (0usize..17).prop_map(|count| format!("{}|x|", "\\".repeat(count))),
        ],
    ) {
        let header = format!("|{}\n|{}\n", "x|".repeat(columns), "---|".repeat(columns));
        let row = format!("|x{suffix}|\n");
        let source = format!("{header}{row}");
        let mut row_end = 0;
        let mut expected = 0;
        for (event, range) in Parser::new_ext(&source, options()).into_offset_iter() {
            match event {
                Event::Start(Tag::TableRow) => row_end = range.end,
                Event::Start(Tag::TableCell) if range.is_empty() && range.end == row_end => expected += 1,
                _ => {}
            }
        }
        prop_assert_eq!(columns.saturating_sub(row_cells(&row)), expected);
    }
}

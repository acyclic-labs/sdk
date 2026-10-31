use super::*;
use proptest::prelude::*;

fn discover(source: &str, offset: u64, limit: Option<u64>, chunk: usize) -> Result<Selection> {
    let mut cursor = JsonSpans::default();
    let mut selected = Selection::new(offset, limit, u64::MAX);
    let bounds = JsonBounds {
        maximum_bytes: source.len() as u64,
        maximum_items: u64::MAX,
        maximum_depth: 128,
    };
    for bytes in source.as_bytes().chunks(chunk) {
        let mut consumed = 0;
        while consumed < bytes.len() {
            let count = cursor.feed(
                &bytes[consumed..],
                bounds,
                None,
                &mut |_| Ok(()),
                &mut |event| selected.event(event, None),
            )?;
            assert!(count > 0);
            consumed += count;
            if let Some((start, end, key)) = selected.pending() {
                selected.supply(&source.as_bytes()[start as usize..end as usize], key, None)?;
            }
        }
    }
    cursor.finish(None, &mut |event| selected.event(event, None))?;
    Ok(selected)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn cell_spans_survive_arbitrary_chunks_and_escaped_root_fields(
        sources in prop::collection::vec(".{0,100}", 0..20),
        offset in 0u64..25, limit in prop::option::of(0u64..25), chunk in 1usize..100,
    ) {
        let cells: Vec<_> = sources.into_iter().enumerate().map(|(index, source)| json!({
            "id":format!("cell-{index}"), "cell_type":"code", "source":[source],
            "outputs":[{"output_type":"stream", "text":"untouched"}], "metadata":{},
        })).collect();
        let original = json!({"cells":cells, "metadata":{"nested":{"cells":[]}}, "nbformat":4});
        let source = serde_json::to_string_pretty(&original).unwrap()
            .replace("\"cells\"", "\"ce\\u006cls\"").replace("\"nbformat\"", "\"nbf\\u006frmat\"");
        let selected = discover(&source, offset, limit, chunk).unwrap();
        prop_assert_eq!(selected.count, cells.len() as u64);
        prop_assert_eq!(selected.format, Some(4));
        prop_assert!(selected.seen_cells);
        let expected: Vec<_> = cells.iter().skip(offset as usize).take(limit.unwrap_or(u64::MAX) as usize).cloned().collect();
        let expected_count = expected.len() as u64;
        if let Some((start, end)) = selected.interval {
            let text = format!("[{}]", &source[start as usize..end as usize]);
            prop_assert_eq!(serde_json::from_str::<Vec<Value>>(&text).unwrap(), expected);
        } else {
            prop_assert!(expected.is_empty());
        }
        prop_assert_eq!(selected.next_cell, offset + expected_count);
    }
}

#[test]
fn duplicate_or_mistyped_notebook_fields_fail_on_the_same_scan() {
    for source in [
        r#"{"cells":[],"cells":[],"nbformat":4}"#,
        r#"{"cells":[],"nbformat":4,"nbf\u006frmat":4}"#,
        r#"{"cells":[null],"nbformat":4}"#,
        r#"{"cells":{},"nbformat":4}"#,
        r#"{"cells":[],"nbformat":"4"}"#,
    ] {
        assert!(discover(source, 0, None, 1).is_err(), "accepted {source}");
    }
}

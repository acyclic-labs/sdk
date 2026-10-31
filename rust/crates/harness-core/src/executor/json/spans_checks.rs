use super::*;
use proptest::prelude::*;

fn scan(bytes: &[u8], chunk: usize, depth: u32) -> Result<Vec<SpanEvent>> {
    let mut cursor = JsonSpans::default();
    let mut events = Vec::new();
    let bounds = JsonBounds {
        maximum_bytes: bytes.len() as u64,
        maximum_items: u64::MAX,
        maximum_depth: depth,
    };
    for bytes in bytes.chunks(chunk) {
        let mut consumed = 0;
        while consumed < bytes.len() {
            let count = cursor.feed(
                &bytes[consumed..],
                bounds,
                None,
                &mut |_| Ok(()),
                &mut |event| {
                    events.push(event);
                    // Yield at every event to exercise the same byte's multiple
                    // completions and continuation without rescanning its prefix.
                    Ok(true)
                },
            )?;
            assert!(count > 0);
            consumed += count;
        }
    }
    cursor.finish(None, &mut |event| {
        events.push(event);
        Ok(false)
    })?;
    Ok(events)
}

fn documents() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(Value::from),
        ".{0,80}".prop_map(Value::String)
    ];
    leaf.prop_recursive(5, 80, 8, |item| {
        prop_oneof![
            prop::collection::vec(item.clone(), 0..8).prop_map(Value::Array),
            prop::collection::btree_map(".{0,30}", item, 0..8)
                .prop_map(|map| Value::Object(map.into_iter().collect()))
        ]
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn streamed_spans_match_typed_json_across_chunks(value in documents(), chunk in 1usize..100) {
        let bytes = serde_json::to_vec(&value).unwrap();
        let events = scan(&bytes, chunk, 128).unwrap();
        let mut roots = 0;
        for event in events {
            match event {
                SpanEvent::End { depth, start, end, .. } => {
                    let selected: Value = serde_json::from_slice(&bytes[start as usize..end as usize]).unwrap();
                    if depth == 0 { prop_assert_eq!(selected, value.clone()); roots += 1; }
                }
                SpanEvent::Key { start, end, .. } => {
                    prop_assert!(serde_json::from_slice::<String>(&bytes[start as usize..end as usize]).is_ok());
                }
                _ => {}
            }
        }
        prop_assert_eq!(roots, 1);
        let mut truncated = bytes.clone();
        // A valid trailing token and a non-whitespace suffix are different.
        truncated.push(b'!');
        prop_assert!(scan(&truncated, chunk, 128).is_err());
    }

    #[test]
    fn arbitrary_frames_agree_with_raw_json(bytes in prop::collection::vec(any::<u8>(), 0..300), chunk in 1usize..40) {
        let expected = serde_json::from_slice::<&serde_json::value::RawValue>(&bytes).is_ok();
        prop_assert_eq!(scan(&bytes, chunk, 128).is_ok(), expected);
    }
}

#[test]
fn malformed_tokens_nesting_and_credit_refuse_without_a_continuation_wait() {
    for source in [
        r#"{"a" 1}"#,
        r#"{"a":}"#,
        r#"{"a":1,}"#,
        "[1,]",
        "[1 2]",
        "[}",
        "-",
        "01",
        "1.",
        "1e+",
        "tru",
        "false0",
        "NaN",
        r#""\q""#,
        r#""\uDC00""#,
        r#""\uD800x""#,
        r#""\uD800\u0000""#,
        "\"\n\"",
    ] {
        assert!(
            scan(source.as_bytes(), 1, 128).is_err(),
            "accepted {source:?}"
        );
    }
    assert!(scan(br#""\uD834\uDD1E""#, 1, 128).is_ok());
    assert!(scan(b"[[]]", 1, 0).is_err());
    for source in [
        &b"\"\xc0\x80\""[..],
        &b"\"\xed\xa0\x80\""[..],
        &b"\"\xf4\x90\x80\x80\""[..],
    ] {
        assert!(scan(source, 1, 128).is_err());
    }
    let mut cursor = JsonSpans::default();
    let bounds = JsonBounds {
        maximum_bytes: 1,
        maximum_items: 1,
        maximum_depth: 128,
    };
    assert!(
        cursor
            .feed(b"[]", bounds, None, &mut |_| Ok(()), &mut |_| Ok(false))
            .is_err()
    );
    let mut cursor = JsonSpans::default();
    let bounds = JsonBounds {
        maximum_bytes: 4,
        maximum_items: 1,
        maximum_depth: 128,
    };
    assert!(
        cursor
            .feed(b"[0]", bounds, None, &mut |_| Ok(()), &mut |_| Ok(false))
            .is_err()
    );
}

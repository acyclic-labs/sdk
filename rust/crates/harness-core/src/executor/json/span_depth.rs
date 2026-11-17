use super::super::*;

#[test]
fn spans_accept_the_producer_depth_boundary_and_refuse_the_next_container() {
    for empty in [Value::Array(Vec::new()), Value::Object(Default::default())] {
        let mut value = empty;
        for _ in 0..RECORD_MAXIMUM_DEPTH {
            value = Value::Array(vec![value]);
        }
        let bytes = serde_json::to_vec(&value).unwrap();
        let length =
            json_byte_length_at_depth(&value, None, RECORD_MAXIMUM_DEPTH, &mut None).unwrap();
        assert_eq!(length, bytes.len() as u64);
        for chunk in [1, 127, bytes.len()] {
            let mut cursor = JsonSpans::default();
            let mut deepest = false;
            let mut emit = |event| -> Result<bool> {
                deepest |= matches!(
                    event,
                    SpanEvent::Start {
                        depth: RECORD_MAXIMUM_DEPTH,
                        ..
                    }
                );
                Ok(false)
            };
            let bounds = JsonBounds {
                maximum_bytes: Some(bytes.len() as u64),
                maximum_items: None,
                maximum_depth: RECORD_MAXIMUM_DEPTH,
            };
            for part in bytes.chunks(chunk) {
                assert_eq!(
                    cursor
                        .feed(part, bounds, None, &mut |_| Ok(()), &mut emit)
                        .unwrap(),
                    part.len()
                );
            }
            cursor.finish(None, &mut emit).unwrap();
            assert!(deepest);
        }
        let excessive = Value::Array(vec![value]);
        assert!(
            json_byte_length_at_depth(&excessive, None, RECORD_MAXIMUM_DEPTH, &mut None,).is_err()
        );
        let bytes = serde_json::to_vec(&excessive).unwrap();
        let bounds = JsonBounds {
            maximum_bytes: Some(bytes.len() as u64),
            maximum_items: None,
            maximum_depth: RECORD_MAXIMUM_DEPTH,
        };
        assert!(
            JsonSpans::default()
                .feed(&bytes, bounds, None, &mut |_| Ok(()), &mut |_| Ok(false))
                .is_err()
        );
    }
}

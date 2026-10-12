use super::*;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn hidden_raw_values_are_bounded_before_decode() {
    let raw = |payload: &str| {
        format!(
            "{{\"$serde_json::private::RawValue\":{}}}",
            serde_json::to_string(payload).unwrap()
        )
    };
    let limits = JsonBounds {
        maximum_bytes: 16 * 1024 * 1024,
        maximum_items: 2 * 1024 * 1024,
        maximum_depth: 256,
    };
    let payload = format!("[{}0]", "0,".repeat(999_999));
    let frames = (0..6).fold("0".to_owned(), |payload, _| raw(&payload));
    let deep = raw(&format!("{}0{}", "[".repeat(1024), "]".repeat(1024)));
    for (document, bounds, maximum_inspected) in [
        (
            raw(&payload),
            JsonBounds {
                maximum_items: 64,
                ..limits
            },
            payload.len() as u64,
        ),
        (
            frames,
            JsonBounds {
                maximum_depth: 3,
                ..limits
            },
            u64::MAX,
        ),
        (
            deep,
            JsonBounds {
                maximum_depth: 3,
                ..limits
            },
            u64::MAX,
        ),
        (
            raw("[0]"),
            JsonBounds {
                maximum_depth: 1,
                ..limits
            },
            u64::MAX,
        ),
    ] {
        let (mut admitted, mut inspected) = (false, 0);
        let result: Result<Value> = json_from_slice_bounded(
            std::marker::PhantomData,
            document.as_bytes(),
            bounds,
            0,
            Some(crate::wire::ErrorCode::Storage),
            Some(JsonValueSchema::Root),
            &mut |event| {
                admitted |= matches!(event, JsonCodecCharge::Admission { .. });
                if let JsonCodecCharge::Inspect { items } = event {
                    inspected += items;
                }
                Ok(())
            },
        );
        assert!(!admitted && inspected <= maximum_inspected);
        assert!(matches!(
            result,
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                ..
            })
        ));
    }
    let document = raw("[0]");
    let expected: Value = serde_json::from_slice(document.as_bytes()).unwrap();
    assert_eq!(expected, serde_json::json!([0]));
    assert_eq!(
        json_from_slice_bounded(
            std::marker::PhantomData::<Value>,
            document.as_bytes(),
            JsonBounds {
                maximum_depth: 2,
                ..limits
            },
            0,
            Some(crate::wire::ErrorCode::Storage),
            Some(JsonValueSchema::Root),
            &mut |_| Ok(())
        )
        .unwrap(),
        expected
    );
}

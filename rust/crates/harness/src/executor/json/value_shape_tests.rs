use super::*;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn fixed_input_failures_keep_their_kind_independent_of_the_wire_code() {
    for code in [
        crate::wire::ErrorCode::Invalid,
        crate::wire::ErrorCode::Storage,
    ] {
        for (bytes, expected) in [
            (b"]".as_slice(), JsonInputError::Nesting),
            (b"{".as_slice(), JsonInputError::Incomplete),
            (b"[0]".as_slice(), JsonInputError::Depth),
        ] {
            let error = admission::source(
                bytes,
                JsonBounds {
                    maximum_bytes: 3,
                    maximum_items: 3,
                    maximum_depth: 0,
                },
                None,
                Some(code),
                &mut |_| Ok(()),
            )
            .unwrap_err();
            assert!(
                matches!(error, Error::NativeJsonAdmission { code: actual, kind }
                if actual == code && kind == expected)
            );
            assert!(error.is_native_failure());
        }
    }
}

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
    for (document, bounds, maximum_inspected, expected_kind) in [
        (
            raw(&payload),
            JsonBounds {
                maximum_items: 64,
                ..limits
            },
            payload.len() as u64,
            None,
        ),
        (
            frames,
            JsonBounds {
                maximum_depth: 3,
                ..limits
            },
            u64::MAX,
            Some(JsonInputError::Depth),
        ),
        (
            deep,
            JsonBounds {
                maximum_depth: 3,
                ..limits
            },
            u64::MAX,
            Some(JsonInputError::Depth),
        ),
        (
            raw("[0]"),
            JsonBounds {
                maximum_depth: 1,
                ..limits
            },
            u64::MAX,
            Some(JsonInputError::Depth),
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
        let error = result.unwrap_err();
        assert_eq!(error.code(), crate::wire::ErrorCode::Invalid);
        match expected_kind {
            Some(expected) => assert!(matches!(error,
                Error::NativeJsonAdmission { kind, .. } if kind == expected)),
            None => assert!(matches!(error, Error::NativeBoundary { .. })),
        }
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

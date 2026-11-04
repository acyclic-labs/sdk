use acyclic_harness::executor::json::{
    JsonBounds, JsonCodecCharge, canonical_json_bytes_bounded, json_bytes_bounded,
    json_from_slice_bounded, json_value_bounded,
};
use serde_json::Value;

pub fn check(input: &[u8]) {
    if input.len() > 16_384 {
        return;
    }
    let bounds = JsonBounds {
        maximum_bytes: 131_072,
        maximum_items: 1_048_576,
        maximum_depth: 256,
    };
    let raw = serde_json::from_slice::<&serde_json::value::RawValue>(input);
    let mut admissions = 0;
    let mut scratch = 0_u64;
    let decoded = json_from_slice_bounded(
        std::marker::PhantomData::<&serde_json::value::RawValue>,
        input,
        bounds,
        None,
        None,
        &mut |event| {
            match event {
                JsonCodecCharge::Admission { .. } => admissions += 1,
                JsonCodecCharge::Retain { bytes } => scratch = scratch.checked_add(bytes).unwrap(),
                JsonCodecCharge::Release { bytes } => scratch = scratch.checked_sub(bytes).unwrap(),
                _ => {}
            }
            Ok(())
        },
    );
    assert_eq!(scratch, 0, "typed JSON admission must release its scratch");
    let Ok(raw) = raw else {
        assert!(
            decoded.is_err(),
            "typed JSON must refuse malformed original input"
        );
        return;
    };
    if exceeds_depth(raw.get().as_bytes(), bounds.maximum_depth) {
        assert!(
            decoded.is_err(),
            "typed raw JSON must refuse documents beyond the declared depth"
        );
        return;
    }
    let decoded = decoded.expect("typed raw JSON must admit independently valid raw input");
    assert_eq!(decoded.get(), raw.get());
    assert_eq!(admissions, 1, "typed JSON decode must request admission");
    for frontier in 0..5 {
        check_refusal(input, bounds, frontier);
    }
    let Ok(mut expected) = serde_json::from_slice::<Value>(input) else {
        return;
    };
    expected.sort_all_objects();
    let mut compact = json_bytes_bounded(&raw, bounds, None, &mut |_| Ok(())).unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&compact).unwrap(), expected);
    let bytes = canonical_json_bytes_bounded(&raw, bounds, None, &mut |_| Ok(())).unwrap();
    assert_eq!(bytes, serde_json::to_vec(&expected).unwrap());
    assert_eq!(
        json_value_bounded(&raw, bounds, None, &mut |_| Ok(())).unwrap(),
        expected
    );
    // Reuse the actual writer output to reach complete-source syntax failure,
    // independently of which malformed inputs a byte campaign discovers.
    compact.extend_from_slice(b" null");
    check_refusal(&compact, bounds, 5);
}

// RawValue has already checked syntax. Count only unquoted structural depth,
// independently of the production admission walk. Depth is zero-based: an
// empty container at the limit has no child whose depth could exceed it.
fn exceeds_depth(source: &[u8], maximum: u32) -> bool {
    let (mut depth, mut quoted, mut escaped) = (0_u32, false, false);
    for &byte in source {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
        } else {
            if depth > maximum
                && !matches!(
                    byte,
                    b']' | b'}' | b',' | b':' | b' ' | b'\n' | b'\r' | b'\t'
                )
            {
                return true;
            }
            match byte {
                b'"' => quoted = true,
                b'[' | b'{' => depth += 1,
                b']' | b'}' => depth -= 1,
                _ => {}
            }
        }
    }
    false
}

fn check_refusal(source: &[u8], bounds: JsonBounds, frontier: usize) {
    let mut refused_once = false;
    let mut scratch = 0_u64;
    let refused = json_from_slice_bounded(
        std::marker::PhantomData::<&serde_json::value::RawValue>,
        source,
        bounds,
        None,
        None,
        &mut |event| {
            if frontier == 5 {
                match event {
                    JsonCodecCharge::Admission {
                        maximum_owned_bytes,
                        ..
                    } => {
                        assert_eq!(maximum_owned_bytes, 0);
                    }
                    JsonCodecCharge::Retain { bytes } => {
                        scratch = scratch.checked_add(bytes).unwrap()
                    }
                    JsonCodecCharge::Release { bytes } => {
                        scratch = scratch.checked_sub(bytes).unwrap()
                    }
                    _ => {}
                }
            }
            if !refused_once
                && matches!(
                    (frontier, event),
                    (0, JsonCodecCharge::Read { .. })
                        | (1, JsonCodecCharge::Work { .. })
                        | (2, JsonCodecCharge::Retain { .. })
                        | (3, JsonCodecCharge::Release { .. })
                        | (4 | 5, JsonCodecCharge::Admission { .. })
                )
            {
                refused_once = true;
                Err(acyclic_harness::Error::Invalid(
                    "caller refused JSON charge".into(),
                ))
            } else {
                Ok(())
            }
        },
    );
    assert!(
        matches!(refused, Err(acyclic_harness::Error::Invalid(reason))
        if reason == "caller refused JSON charge"),
        "typed JSON decode must preserve caller charge refusal at frontier {frontier}"
    );
    if frontier == 5 {
        assert_eq!(
            scratch, 0,
            "typed JSON syntax admission must release its scratch"
        );
    }
}

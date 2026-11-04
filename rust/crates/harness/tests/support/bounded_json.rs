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
    let Ok(raw) = serde_json::from_slice::<&serde_json::value::RawValue>(input) else {
        assert!(
            json_from_slice_bounded(
                std::marker::PhantomData::<&serde_json::value::RawValue>,
                input,
                bounds,
                None,
                None,
                &mut |_| Ok(()),
            )
            .is_err(),
            "typed JSON must refuse malformed original input"
        );
        return;
    };
    let Ok(mut expected) = serde_json::from_slice::<Value>(input) else {
        return;
    };
    expected.sort_all_objects();
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
    )
    .unwrap();
    assert_eq!(decoded.get(), raw.get());
    assert_eq!(admissions, 1, "typed JSON decode must request admission");
    assert_eq!(scratch, 0, "typed JSON admission must release its scratch");
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
    for (source, frontier) in (0..5)
        .map(|frontier| (input, frontier))
        .chain(std::iter::once((compact.as_slice(), 5)))
    {
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
}

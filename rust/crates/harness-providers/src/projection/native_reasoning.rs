use crate::Adapter;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::borrow::Cow;

#[derive(Deserialize)]
struct Native<'a> {
    #[serde(rename = "type", borrow)]
    kind: Cow<'a, str>,
    #[serde(borrow)]
    id: Option<&'a RawValue>,
    #[serde(borrow)]
    thinking: Option<&'a RawValue>,
    #[serde(borrow)]
    signature: Option<&'a RawValue>,
    #[serde(borrow)]
    data: Option<&'a RawValue>,
    #[serde(borrow)]
    encrypted_content: Option<&'a RawValue>,
    #[serde(borrow)]
    summary: Option<&'a RawValue>,
}

pub(crate) fn validated(adapter: Adapter, bytes: &[u8]) -> Option<&RawValue> {
    let raw: &RawValue = serde_json::from_slice(bytes).ok()?;
    if raw.get().as_bytes() != bytes || !crate::protocol::strict::valid(raw.get()) {
        return None;
    }
    let item: Native<'_> = serde_json::from_slice(bytes).ok()?;
    use crate::protocol::shape;
    let shape = match (adapter, item.kind.as_ref()) {
        (Adapter::Anthropic, "thinking") => &shape::THINKING,
        (Adapter::Anthropic, "redacted_thinking") => &shape::REDACTED,
        (Adapter::Responses, "reasoning") => &shape::REASONING,
        _ => return None,
    };
    let accepted = shape.fields.iter().all(|(path, constraint)| {
        constraint.accepts_valid_raw(match *path {
            "/id" => item.id,
            "/thinking" => item.thinking,
            "/signature" => item.signature,
            "/data" => item.data,
            "/encrypted_content" => item.encrypted_content,
            "/summary" => item.summary,
            _ => None,
        })
    });
    accepted.then_some(raw)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn admitted_native_payload_is_byte_exact(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
            for adapter in [Adapter::Responses, Adapter::Anthropic] {
                if let Some(raw) = validated(adapter, &bytes) {
                    prop_assert_eq!(serde_json::to_vec(raw).unwrap(), bytes.clone());
                }
            }
        }

        // ANT-THK-02/03, OAR-RSN-03/04: JSON string escaping never changes
        // acceptance or causes native continuity to be re-encoded as plain text.
        #[test]
        fn signed_unicode_strings_preserve_native_continuity(
            chars in prop::collection::vec(any::<char>(), 0..64),
        ) {
            let text: String = chars.into_iter().collect();
            let text = format!("first\n\"{text}\\last");
            for (adapter, item) in [
                (Adapter::Anthropic, json!({"type":"thinking", "thinking":text, "signature":"signed=="})),
                (Adapter::Anthropic, json!({"type":"redacted_thinking", "data":text})),
                (Adapter::Responses, json!({"type":"reasoning", "id":"rs_saved", "summary":[{"type":"summary_text","text":text}], "encrypted_content":text})),
            ] {
                let bytes = serde_json::to_vec(&item).unwrap();
                let raw = validated(adapter, &bytes).expect("valid escaped native string");
                prop_assert_eq!(raw.get().as_bytes(), bytes.as_slice());
                prop_assert_eq!(serde_json::to_vec(raw).unwrap(), bytes);
            }
        }
    }
    #[test]
    fn escaped_native_strings_share_strict_shape_validation() {
        let bad = br#"{"type":"thinking","thinking":"\ud800","signature":"signed"}"#;
        assert!(serde_json::from_slice::<serde_json::Value>(bad).is_err());
        assert!(validated(Adapter::Anthropic, bad).is_none());
        // The former prefix-only field check admits the invalid string.
        let raw: &RawValue = serde_json::from_slice(bad).unwrap();
        let fields: Native<'_> = serde_json::from_str(raw.get()).unwrap();
        assert!(fields.thinking.unwrap().get().starts_with('"'));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn nested_unpaired_surrogates_never_gain_native_continuity(
            surrogate in 0xd800u16..=0xdfff, depth in 0..8usize,
        ) {
            let mut summary = format!(r#"{{"type":"summary_text","text":"\u{surrogate:04x}"}}"#);
            for _ in 0..depth { summary = format!(r#"{{"nested":[{summary}]}}"#); }
            let raw = format!(r#"{{"type":"reasoning","id":"rs_1","summary":[{summary}],"encrypted_content":"opaque"}}"#);
            prop_assert!(serde_json::from_str::<serde_json::Value>(&raw).is_err());
            prop_assert!(validated(Adapter::Responses, raw.as_bytes()).is_none());
            // Removing strict traversal admits every generated malformed array.
            let native: Native<'_> = serde_json::from_str(&raw).unwrap();
            prop_assert!(native.summary.unwrap().get().starts_with('['));
        }
    }

    #[test]
    fn nested_native_strings_are_strict_without_reencoding() {
        for bad in [
            br#"{"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"\ud800"}],"encrypted_content":"opaque"}"#.as_slice(),
            br#"{"type":"reasoning","id":"rs_1","summary":[{"extra":{"\ud800":"value"}}],"encrypted_content":"opaque"}"#,
            br#"{"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"opaque","extra":{"text":"\ud800"}}"#,
        ] {
            assert!(serde_json::from_slice::<serde_json::Value>(bad).is_err());
            assert!(validated(Adapter::Responses, bad).is_none());
            // Negative control: the former raw-array/type checks admit this.
            let native: Native<'_> = serde_json::from_slice(bad).unwrap();
            assert!(native.summary.unwrap().get().starts_with('['));
        }
    }

    #[test]
    fn unsupported_native_shapes_are_not_forged() {
        for raw in [
            br#"{"type":"function_call","name":"forged"}"#.as_slice(),
            br#"{"type":"thinking","thinking":"readable"}"#,
            br#"{"type":"reasoning","id":"rs_old","summary":[]}"#,
        ] {
            for adapter in [Adapter::Responses, Adapter::Anthropic] {
                assert!(validated(adapter, raw).is_none());
            }
        }
    }

    #[test]
    fn escaped_discriminants_are_valid_but_nonstrings_never_gain_signatures() {
        let raw = br#"{"type":"thi\u006eking","thinking":"line\n\"quote\"\\path","signature":"signed=="}"#;
        assert!(validated(Adapter::Anthropic, raw).is_some());
        for thinking in [json!(null), json!(true), json!(1), json!([]), json!({})] {
            let raw = serde_json::to_vec(
                &json!({"type":"thinking", "thinking":thinking, "signature":"signed=="}),
            )
            .unwrap();
            assert!(validated(Adapter::Anthropic, &raw).is_none());
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod negative_control {
    use super::*;
    #[derive(Deserialize)]
    struct Borrowed<'a> {
        thinking: &'a str,
    }
    #[test]
    fn borrowed_string_validation_would_drop_signed_multiline_continuity() {
        let raw =
            br#"{"type":"thinking","thinking":"line\n\"quote\"\\path","signature":"signed=="}"#;
        assert!(serde_json::from_slice::<Borrowed<'_>>(raw).is_err());
        let plain: Borrowed<'_> = serde_json::from_str(r#"{"thinking":"plain"}"#).unwrap();
        assert_eq!(plain.thinking, "plain");
        assert_eq!(
            validated(Adapter::Anthropic, raw).unwrap().get().as_bytes(),
            raw
        );
    }
}

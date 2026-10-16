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

fn string(value: Option<&RawValue>) -> bool {
    value.is_some_and(|value| value.get().starts_with('"'))
}

fn nonempty_string(value: Option<&RawValue>) -> bool {
    string(value) && value.is_some_and(|value| value.get().len() > 2)
}

pub(crate) fn validated(adapter: Adapter, bytes: &[u8]) -> Option<&RawValue> {
    let raw: &RawValue = serde_json::from_slice(bytes).ok()?;
    if raw.get().as_bytes() != bytes {
        return None;
    }
    let item: Native<'_> = serde_json::from_slice(bytes).ok()?;
    let accepted = match adapter {
        // ANT-THK-02 / ANT-THK-03: complete original signed or redacted block.
        Adapter::Anthropic => match item.kind.as_ref() {
            "thinking" => string(item.thinking) && nonempty_string(item.signature),
            "redacted_thinking" => nonempty_string(item.data),
            _ => false,
        },
        // OAR-RSN-03 / OAR-RSN-04: stateless replay needs encrypted content.
        Adapter::Responses => {
            item.kind == "reasoning"
                && nonempty_string(item.id)
                && nonempty_string(item.encrypted_content)
                && item
                    .summary
                    .is_some_and(|summary| summary.get().starts_with('['))
        }
        _ => false,
    };
    accepted.then_some(raw)
}

#[cfg(test)]
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
                (Adapter::Responses, json!({"type":"reasoning", "id":"rs_saved", "summary":[], "encrypted_content":text})),
            ] {
                let bytes = serde_json::to_vec(&item).unwrap();
                let raw = validated(adapter, &bytes).expect("valid escaped native string");
                prop_assert_eq!(raw.get().as_bytes(), bytes.as_slice());
                prop_assert_eq!(serde_json::to_vec(raw).unwrap(), bytes);
            }
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

#[cfg(test)]
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

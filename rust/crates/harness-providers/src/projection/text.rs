//! N-01 / ANT-TXT-02: normalize complete ordinary text runs, never native parts.
use super::wire::Item;
use crate::Adapter;
use serde_json::Value;

pub(super) fn normalize(adapter: Adapter, parts: &mut Vec<Item<'_>>) {
    if !matches!(adapter, Adapter::Anthropic | Adapter::Gemini) {
        return;
    }
    let mut written = 0;
    let mut previous: Option<(usize, bool)> = None;
    for read in 0..parts.len() {
        let item = std::mem::replace(&mut parts[read], Item::Json(Value::Null));
        let blank = match &item {
            Item::Json(value) => value
                .get("text")
                .and_then(Value::as_str)
                .map(|text| text.trim().is_empty()),
            _ => None,
        };
        if let (Some(blank), Some((index, previous_blank))) = (blank, previous) {
            if blank || previous_blank {
                if let (Item::Json(previous), Item::Json(mut next)) = (&mut parts[index], item) {
                    let Value::String(text) = next["text"].take() else {
                        unreachable!()
                    };
                    let Some(Value::String(previous)) = previous.get_mut("text") else {
                        unreachable!()
                    };
                    previous.push_str(&text);
                }
                previous = Some((index, previous_blank && blank));
                continue;
            }
        }
        parts[written] = item;
        previous = blank.map(|blank| (written, blank));
        written += 1;
    }
    parts.truncate(written);
    if adapter == Adapter::Anthropic {
        parts.retain(|item| match item {
            Item::Json(value) => value
                .get("text")
                .and_then(Value::as_str)
                .is_none_or(|text| !text.trim().is_empty()),
            _ => true,
        });
    }
}

#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
pub(crate) fn assert_normalization(fragments: &[String]) {
    let expected = fragments.concat();
    for adapter in [Adapter::Anthropic, Adapter::Gemini] {
        let mut parts: Vec<_> = fragments
            .iter()
            .map(|text| Item::Json(serde_json::json!({"text":text})))
            .collect();
        normalize(adapter, &mut parts);
        let encoded = serde_json::to_vec(&parts).unwrap();
        let values: Vec<Value> = serde_json::from_slice(&encoded).unwrap();
        let actual: String = values
            .iter()
            .map(|value| value["text"].as_str().unwrap())
            .collect();
        assert_eq!(
            actual,
            if adapter == Adapter::Anthropic && expected.trim().is_empty() {
                ""
            } else {
                &expected
            }
        );
        normalize(adapter, &mut parts);
        assert_eq!(serde_json::to_vec(&parts).unwrap(), encoded);
        let mut messages = Vec::new();
        for fragment in fragments.iter().filter(|fragment| !fragment.is_empty()) {
            let item = Item::Json(serde_json::json!({"type":"text", "text":fragment}));
            let message = if adapter == Adapter::Anthropic {
                super::wire::Message::Anthropic {
                    role: "assistant",
                    content: vec![item],
                }
            } else {
                super::wire::Message::Gemini {
                    role: "model",
                    parts: vec![item],
                }
            };
            super::sequence::push(adapter, &mut messages, message).unwrap();
        }
        super::sequence::finish(adapter, &mut messages);
        let joined: String = messages
            .iter()
            .flat_map(|message| match message {
                super::wire::Message::Anthropic {
                    role: "assistant",
                    content,
                } => content.as_slice(),
                super::wire::Message::Gemini {
                    role: "model",
                    parts,
                } => parts.as_slice(),
                _ => &[],
            })
            .filter_map(|item| match item {
                Item::Json(value) => value["text"].as_str(),
                _ => None,
            })
            .collect();
        assert_eq!(
            joined, actual,
            "cross-message normalization differs from whole-run normalization"
        );
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use proptest::prelude::*;
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn text_normalization_preserves_visible_runs_and_is_idempotent(
            fragments in prop::collection::vec(prop::collection::vec(any::<char>(), 0..32).prop_map(|chars| chars.into_iter().collect::<String>()), 0..24),
        ) { assert_normalization(&fragments); }
    }
    #[test]
    fn dropping_whitespace_fragments_loses_answer_content() {
        let fragments = ["answer".to_owned(), "\r".to_owned(), " tail".to_owned()];
        assert_normalization(&fragments);
        let broken: String = fragments
            .iter()
            .filter(|text| !text.trim().is_empty())
            .cloned()
            .collect();
        assert_ne!(broken, fragments.concat());
    }
    #[test]
    fn signed_parts_are_never_merged_or_reencoded() {
        let raw: &serde_json::value::RawValue =
            serde_json::from_str(r#"{ "text":" ", "thoughtSignature":"sig" }"#).unwrap();
        let mut parts = vec![
            Item::Json(serde_json::json!({"text":"a"})),
            Item::Native(raw),
            Item::Json(serde_json::json!({"text":"b"})),
        ];
        let original = serde_json::to_vec(&parts).unwrap();
        normalize(Adapter::Gemini, &mut parts);
        assert_eq!(serde_json::to_vec(&parts).unwrap(), original);
    }
}

//! Oracles call the completion decoder used by the original usage writer.
use super::{ModelTokenUsage, ModelUsageRecord};
use serde_json::Value;
use std::collections::BTreeSet;

/// Compare the production fixed-field decoder with set consensus over the
/// public metadata paths. A known count requires exactly one distinct, valid
/// reported value. Null is absence; a malformed nonnull value poisons consensus.
/// This deliberately uses JSON pointers and a set rather than the decoder's
/// indexing and early-return fold, so shared chunking or round-trip mistakes
/// cannot make lost counters, contradictory aliases or default zero agree.
pub fn check_metadata(metadata: &Value) {
    let actual = ModelTokenUsage::reported(metadata);
    let fields: &[(&[&str], Option<u64>)] = &[
        (
            &[
                "/usage/input_tokens",
                "/usage/prompt_tokens",
                "/usage/promptTokenCount",
            ],
            actual.input,
        ),
        (
            &[
                "/usage/cache_read_input_tokens",
                "/usage/input_tokens_details/cached_tokens",
                "/usage/prompt_tokens_details/cached_tokens",
                "/usage/cachedContentTokenCount",
            ],
            actual.cached,
        ),
        (&["/usage/cache_creation_input_tokens"], actual.cache_write),
        (
            &[
                "/usage/output_tokens",
                "/usage/completion_tokens",
                "/usage/candidatesTokenCount",
            ],
            actual.output,
        ),
        (
            &[
                "/usage/output_tokens_details/reasoning_tokens",
                "/usage/completion_tokens_details/reasoning_tokens",
                "/usage/thoughtsTokenCount",
            ],
            actual.reasoning,
        ),
    ];
    for (paths, actual) in fields {
        let observations: BTreeSet<_> = paths
            .iter()
            .filter_map(|path| metadata.pointer(path))
            .filter(|value| !value.is_null())
            .map(Value::as_u64)
            .collect();
        let expected = if observations.len() == 1 {
            observations.first().copied().flatten()
        } else {
            None
        };
        assert_eq!(*actual, expected, "reported counter consensus at {paths:?}");
    }
}

/// Arbitrary metadata and durable row bytes reach their original JSON decoders.
/// A row round trip checks syntax/value preservation, not terminal authority.
/// Both operations are synchronous and wait on no external producer.
pub fn check(bytes: &[u8]) {
    if check_metadata_bytes(bytes).is_err() {
        let mut decoder = serde_json::Deserializer::from_slice(bytes);
        assert!(
            crate::deserialize_json_value(&mut decoder)
                .and_then(|_| decoder.end())
                .is_err(),
            "production-accepted metadata skipped usage fuzz validation"
        );
    }
    if let Ok(record) = serde_json::from_slice::<ModelUsageRecord>(bytes) {
        let encoded = serde_json::to_vec(&record).expect("accepted usage row encodes");
        let restored: ModelUsageRecord =
            serde_json::from_slice(&encoded).expect("original usage writer output decodes");
        assert_eq!(
            serde_json::to_value(record).unwrap(),
            serde_json::to_value(restored).unwrap()
        );
    }
}

pub(super) fn check_metadata_bytes(bytes: &[u8]) -> serde_json::Result<()> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let metadata = crate::deserialize_json_value(&mut decoder)?;
    decoder.end()?;
    check_metadata(&metadata);
    Ok(())
}

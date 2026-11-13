use crate::OperationId;
use serde::Deserialize;

pub(crate) const SUMMARY_PREFIX: &str = ".system/summaries/";

pub(crate) fn is_output_key(key: &str) -> bool {
    let mut parts = key.split(':');
    matches!(
        (
            parts.next(),
            parts.next().and_then(|step| step.parse::<u32>().ok()),
            parts.next(),
            parts.next()
        ),
        (Some("summary"), Some(_), Some("output"), None)
            | (Some("backstop"), Some(_), Some("marker"), None)
    )
}

// Only published conversation summaries qualify; execution state stays private.
pub(crate) fn summary_operation(path: &str) -> Option<OperationId> {
    let tail = path.strip_prefix(SUMMARY_PREFIX)?;
    let (operation, name) = tail.split_once('/')?;
    let digest = name.strip_suffix(".json")?;
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    OperationId::deserialize(
        serde::de::value::StrDeserializer::<serde::de::value::Error>::new(operation),
    )
    .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn published_summary_paths_round_trip(operation in any::<[u8;16]>(), digest in "[a-f0-9]{64}", extra in "[a-z]{1,16}") {
            let operation = OperationId::from_bytes(operation);
            let path = format!("{SUMMARY_PREFIX}{operation}/{digest}.json");
            prop_assert_eq!(summary_operation(&path), Some(operation));
            for invalid in [format!("{extra}{path}"), format!("{path}/{extra}"), format!("{path}{extra}"), path.replace(".json", ".json/../secret"), format!(".system/execution/{operation}/{digest}.json")] {
                prop_assert_eq!(summary_operation(&invalid), None);
            }
        }
    }
}

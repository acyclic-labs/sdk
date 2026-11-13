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

// Storage class does not confer summary authority. The journal authenticates
// the complete immutable output through its private provenance receipt.
pub(crate) fn summary_operation(path: &str) -> Option<OperationId> {
    let tail = path.strip_prefix(".system/")?;
    let (parent, name) = tail.rsplit_once('/')?;
    let (_, operation) = parent.rsplit_once('/')?;
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
        fn operation_addressed_summary_paths_round_trip(operation in any::<[u8;16]>(), digest in "[a-f0-9]{64}", extra in "[a-z]{1,16}") {
            let operation = OperationId::from_bytes(operation);
            for namespace in [SUMMARY_PREFIX, ".system/execution/", ".system/other/nested/"] {
                let path = format!("{namespace}{operation}/{digest}.json");
                prop_assert_eq!(summary_operation(&path), Some(operation));
                for invalid in [format!("{extra}{path}"), format!("{path}/{extra}"), format!("{path}{extra}"), path.replace(".json", ".json/../secret")] {
                    prop_assert_eq!(summary_operation(&invalid), None);
                }
            }
        }
    }
}

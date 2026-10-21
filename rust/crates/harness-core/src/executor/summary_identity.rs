use crate::OperationId;
use serde::Deserialize;

// Only the journal's complete, immutable execution payload namespace qualifies.
pub(crate) fn summary_operation(path: &str) -> Option<OperationId> {
    let tail = path.strip_prefix(".system/execution/")?;
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
        fn execution_summary_paths_round_trip(operation in any::<[u8;16]>(), digest in "[a-f0-9]{64}", extra in "[a-z]{1,16}") {
            let operation = OperationId::from_bytes(operation);
            let path = format!(".system/execution/{operation}/{digest}.json");
            prop_assert_eq!(summary_operation(&path), Some(operation));
            for invalid in [format!("{extra}{path}"), format!("{path}/{extra}"), format!("{path}{extra}"), path.replace(".json", ".json/../secret")] {
                prop_assert_eq!(summary_operation(&invalid), None);
            }
        }
    }
}

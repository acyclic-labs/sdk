use crate::OperationId;
use serde::Deserialize;

/// The journal keys of model-visible summary outputs: a deliberate summary's
/// text and the mechanical backstop marker.
pub(crate) fn is_summary_key(key: &str) -> bool {
    let step = |rest: &str, suffix: &str| {
        rest.strip_suffix(suffix)
            .is_some_and(|step| !step.is_empty() && step.bytes().all(|byte| byte.is_ascii_digit()))
    };
    key.strip_prefix("summary:")
        .is_some_and(|rest| step(rest, ":output"))
        || key
            .strip_prefix("backstop:")
            .is_some_and(|rest| step(rest, ":marker"))
}

// Only the journal's complete, immutable summary payload namespace qualifies.
pub(crate) fn summary_operation(path: &str) -> Option<OperationId> {
    let tail = path.strip_prefix(".system/summaries/")?;
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
            let path = format!(".system/summaries/{operation}/{digest}.json");
            prop_assert_eq!(summary_operation(&path), Some(operation));
            // Host-private execution records are never summaries.
            prop_assert_eq!(summary_operation(&path.replace("summaries", "execution")), None);
            for invalid in [format!("{extra}{path}"), format!("{path}/{extra}"), format!("{path}{extra}"), path.replace(".json", ".json/../secret")] {
                prop_assert_eq!(summary_operation(&invalid), None);
            }
        }
    }
}

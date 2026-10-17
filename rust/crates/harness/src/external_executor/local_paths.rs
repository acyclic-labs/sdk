//! Local bindings use one request/result stream pair per canonical task ID.
use crate::{Error, Result, TaskId};
use acyclic_stream::StreamPath;

pub(crate) const REQUESTS: &str = "external/requests";
pub(crate) const RESULTS: &str = "external/results";

pub(crate) fn path(parent: &str, task: TaskId) -> StreamPath {
    StreamPath::new(format!("{parent}/{task}")).expect("canonical task path")
}

pub(crate) fn task(parent: &str, path: &StreamPath) -> Result<TaskId> {
    let text = path
        .as_str()
        .strip_prefix(parent)
        .and_then(|suffix| suffix.strip_prefix('/'))
        .ok_or_else(|| Error::Unauthorized("path is outside the task subtree".into()))?;
    let id = TaskId::from_bytes(
        uuid::Uuid::parse_str(text)
            .map_err(|_| Error::Unauthorized("task path is not a task ID".into()))?
            .into_bytes(),
    );
    if text != id.to_string() {
        return Err(Error::Unauthorized("task path is not canonical".into()));
    }
    Ok(id)
}

#[cfg(feature = "test-support")]
pub(crate) fn fuzz(bytes: &[u8]) {
    if let Ok(text) = std::str::from_utf8(bytes) {
        if let Ok(input) = StreamPath::new(text) {
            for parent in [REQUESTS, RESULTS] {
                if let Ok(id) = task(parent, &input) {
                    assert_eq!(path(parent, id), input);
                }
            }
        }
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]
        #[test]
        fn only_the_canonical_direct_task_child_round_trips(id in any::<[u8;16]>(), extra in "[a-z]{1,12}") {
            let id = TaskId::from_bytes(id);
            for parent in [REQUESTS, RESULTS] {
                let exact = path(parent,id);
                prop_assert_eq!(task(parent,&exact).unwrap(),id);
                for text in [format!("{exact}/{extra}"),format!("{parent}/{extra}"),
                    format!("{parent}/{}",uuid::Uuid::from_bytes(id.into_bytes()).simple()),
                    format!("{parent}/{{{id}}}"),format!("other/{id}")] {
                    let wrong = StreamPath::new(text).unwrap();
                    prop_assert!(task(parent,&wrong).is_err());
                }
            }
        }
    }
}

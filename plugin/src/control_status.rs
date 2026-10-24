//! The closed outcome vocabulary of agent lifecycle commands.
//!
//! The service reports an outcome with one of these statuses and every
//! dispatcher entry point (the CLI exit status, the commandless MCP tool's
//! error flag) derives success from the same type, so an incomplete merge
//! cannot read as a clean one through any of them.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ControlStatus {
    /// A merge or a conflicted merge's continuation published the child.
    Applied,
    /// A merge found nothing to publish.
    NoChanges,
    /// A conflicted merge was abandoned and the parent restored.
    Aborted,
    /// A child and its descendants were discarded.
    Discarded,
    /// Conflicted paths were declared resolved.
    Resolved,
    /// A merge stopped on conflicts that the caller must resolve.
    Conflicted,
    /// The merge target moved before the merge committed.
    StaleTarget,
    /// A multi-root merge stopped before publishing every root.
    Paused,
}

impl ControlStatus {
    /// Whether the command did what it was asked to do.
    pub(crate) const fn completed(self) -> bool {
        match self {
            Self::Applied | Self::NoChanges | Self::Aborted | Self::Discarded | Self::Resolved => {
                true
            }
            Self::Conflicted | Self::StaleTarget | Self::Paused => false,
        }
    }
}

/// The failure report for a response whose outcome did not complete, in the
/// form Git prints it, or `None` when the response completed or carries no
/// lifecycle outcome. A status outside the vocabulary is itself a failure.
pub(crate) fn incomplete_outcome(response: &Value) -> Option<String> {
    let status = response.get("status")?;
    let Ok(status) = serde_json::from_value::<ControlStatus>(status.clone()) else {
        return Some(format!("error: unrecognized command outcome {status}"));
    };
    if status.completed() {
        return None;
    }
    let mut report = String::new();
    match status {
        ControlStatus::Conflicted => {
            for conflict in response
                .get("conflicts")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let kind = conflict
                    .get("kind")
                    .and_then(Value::as_str)
                    .unwrap_or("content")
                    .to_lowercase();
                let path = conflict.get("path").and_then(Value::as_str).unwrap_or("?");
                report.push_str(&format!("CONFLICT ({kind}): {path}\n"));
            }
            if response.get("truncated").and_then(Value::as_bool) == Some(true) {
                report.push_str("(further conflicts not listed)\n");
            }
            report.push_str("Automatic merge failed");
            if let Some(help) = response.get("help").and_then(Value::as_str) {
                report.push_str("; ");
                report.push_str(help);
            } else {
                report.push('.');
            }
        }
        ControlStatus::StaleTarget => {
            report.push_str(
                "error: the merge target changed before the merge committed; retry the merge",
            );
        }
        ControlStatus::Paused => {
            report.push_str("error: the merge paused before publishing every root");
            if let Some(root) = response.get("root").and_then(Value::as_str) {
                report.push_str(&format!(" (paused at root {root})"));
            }
        }
        ControlStatus::Applied
        | ControlStatus::NoChanges
        | ControlStatus::Aborted
        | ControlStatus::Discarded
        | ControlStatus::Resolved => return None,
    }
    Some(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ALL: [ControlStatus; 8] = [
        ControlStatus::Applied,
        ControlStatus::NoChanges,
        ControlStatus::Aborted,
        ControlStatus::Discarded,
        ControlStatus::Resolved,
        ControlStatus::Conflicted,
        ControlStatus::StaleTarget,
        ControlStatus::Paused,
    ];

    #[test]
    fn only_incomplete_outcomes_fail() {
        for status in ALL {
            let response = json!({"status": status});
            assert_eq!(
                incomplete_outcome(&response).is_some(),
                !status.completed(),
                "{response}"
            );
        }
        assert_eq!(
            ALL.map(|status| json!(status)
                .as_str()
                .map(str::to_owned)
                .unwrap_or_default()),
            [
                "applied",
                "no-changes",
                "aborted",
                "discarded",
                "resolved",
                "conflicted",
                "stale-target",
                "paused"
            ]
        );
    }

    #[test]
    fn a_conflicted_merge_lists_its_conflicts_as_git_does() {
        let report = incomplete_outcome(&json!({
            "status": "conflicted",
            "conflictCount": 2,
            "conflicts": [
                {"path": "/same.txt", "kind": "Text"},
                {"path": "/image.png", "kind": "Binary"}
            ],
            "truncated": true,
            "help": "resolve, then run `acyclic git merge --continue`"
        }))
        .expect("a conflicted merge fails");
        assert_eq!(
            report,
            "CONFLICT (text): /same.txt\nCONFLICT (binary): /image.png\n\
             (further conflicts not listed)\n\
             Automatic merge failed; resolve, then run `acyclic git merge --continue`"
        );
    }

    #[test]
    fn responses_without_an_outcome_and_unknown_outcomes_are_classified() {
        assert_eq!(incomplete_outcome(&json!({"agents": []})), None);
        assert_eq!(incomplete_outcome(&json!("text")), None);
        assert!(
            incomplete_outcome(&json!({"status": "stale-target"}))
                .is_some_and(|report| report.contains("retry"))
        );
        assert!(
            incomplete_outcome(&json!({"status": "paused", "root": "ab"}))
                .is_some_and(|report| report.contains("root ab"))
        );
        assert!(
            incomplete_outcome(&json!({"status": "merged-ish"}))
                .is_some_and(|report| report.contains("unrecognized"))
        );
    }
}

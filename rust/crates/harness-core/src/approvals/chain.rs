//! Shared policy-chain traversal for execution, clients and host adapters.
use super::{ApprovalPolicy, ApprovalReviewFile, Approver, RuleVerdict};
use crate::{
    Error, Result, TaskId,
    interaction::ApprovalDecisionFile,
    task_volume::{Component, FileKey, FileValue, TaskFile},
};
use std::collections::BTreeMap;

/// Active stage selected from the immutable request and retained review files.
pub enum NextStage<'a> {
    /// An approver must review or authenticate a decision for this stage.
    Review {
        /// Configured approver.
        approver: &'a Approver,
        /// Exact binding required for the retained review.
        expected: ApprovalReviewFile,
        /// Previously published review, after validating its complete binding.
        retained: Option<ApprovalReviewFile>,
    },
    /// First decisive automatic review, or denial when escalation is exhausted.
    Decided(ApprovalReviewFile),
}

/// One traversal decides the active policy stage for execution and every client.
pub fn next_stage<'a>(
    policy: &'a ApprovalPolicy,
    digest: [u8; 32],
    parent: Option<TaskId>,
    request: &FileValue,
    files: &BTreeMap<FileKey, FileValue>,
    start: usize,
) -> Result<NextStage<'a>> {
    static HUMAN_POLICY: std::sync::LazyLock<ApprovalPolicy> =
        std::sync::LazyLock::new(ApprovalPolicy::default);
    let interaction: crate::interaction::Interaction =
        crate::contract::json_from_slice(&request.bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    let policy = match interaction {
        crate::interaction::Interaction::Approval {
            subject:
                crate::interaction::ApprovalSubject::Tool {
                    route: crate::interaction::ApprovalRoute::Human,
                    ..
                },
            ..
        }
        | crate::interaction::Interaction::Approval {
            subject: crate::interaction::ApprovalSubject::Legacy {}, ..
        } => &*HUMAN_POLICY,
        crate::interaction::Interaction::Approval { .. } => policy,
        _ => return Err(Error::Invalid("not an approval request".into())),
    };
    let hash = fingerprint(request)?;
    for (stage, approver) in policy.approvers.iter().enumerate().skip(start) {
        let principal = match approver {
            Approver::Rules { principal, .. }
            | Approver::Llm { principal, .. }
            | Approver::Human { principal } => principal.clone(),
            Approver::Parent => match parent {
                Some(parent) => format!("parent:{parent}"),
                None => continue,
            },
        };
        let expected = ApprovalReviewFile {
            principal,
            request_sha256: hash.clone(),
            policy_digest: digest,
            stage,
            verdict: RuleVerdict::Ask,
            reason: None,
        };
        let retained = retained_review(
            files.get(&review_key(request, &expected.principal)?),
            &expected,
        )?;
        if !matches!(approver, Approver::Human { .. } | Approver::Parent)
            && let Some(review) = &retained
        {
            if review.verdict == RuleVerdict::Ask {
                continue;
            }
            return Ok(NextStage::Decided(review.clone()));
        }
        return Ok(NextStage::Review {
            approver,
            expected,
            retained,
        });
    }
    Ok(NextStage::Decided(ApprovalReviewFile {
        principal: "policy:exhausted".into(),
        request_sha256: hash,
        policy_digest: digest,
        stage: policy.approvers.len(),
        verdict: RuleVerdict::Deny,
        reason: Some("approval chain exhausted without permission".into()),
    }))
}

/// Request component of an approval request file.
pub fn request_id(file: &FileValue) -> Result<&Component> {
    match &file.key {
        FileKey::Task {
            file: TaskFile::ApprovalRequest(request),
            ..
        } => Ok(request),
        _ => Err(Error::Invalid("not an approval request".into())),
    }
}
/// Canonical review-file key for a request and authenticated principal.
pub fn review_key(request: &FileValue, principal: &str) -> Result<FileKey> {
    let FileKey::Task { task, .. } = &request.key else {
        return Err(Error::Invalid("not a task file".into()));
    };
    Ok(FileKey::Task {
        task: task.clone(),
        file: TaskFile::ApprovalReview {
            request: request_id(request)?.clone(),
            principal: Component::new(ApprovalDecisionFile::principal_file(principal))?,
        },
    })
}
/// Digest binding to the exact immutable request bytes.
pub fn fingerprint(request: &FileValue) -> Result<String> {
    let descriptor = crate::conversation::FileDescriptor::from_bytes(
        &request.bytes,
        "application/json",
        &mut None,
    )?;
    Ok(ApprovalDecisionFile::new("binding", descriptor.sha256(), 1, false, None)?.request_sha256)
}
fn retained_review(
    file: Option<&FileValue>,
    expected: &ApprovalReviewFile,
) -> Result<Option<ApprovalReviewFile>> {
    file.map(|file| {
        let review: ApprovalReviewFile = serde_json::from_slice(&file.bytes)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if review.principal != expected.principal
            || review.request_sha256 != expected.request_sha256
            || review.policy_digest != expected.policy_digest
            || review.stage != expected.stage
        {
            return Err(Error::Conflict("approval review binding changed".into()));
        }
        Ok(review)
    })
    .transpose()
}

#[cfg(test)]
mod tests;

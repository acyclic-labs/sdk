//! Shared policy-chain traversal for execution, clients and host adapters.
use super::{
    ApprovalFiles, ApprovalPolicy, ApprovalRequest, ApprovalReviewFile, Approver, RuleVerdict,
};
use crate::{Error, Result, agent::AgentId, interaction::ApprovalDecisionFile};

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
pub async fn next_stage<'a>(
    policy: &'a ApprovalPolicy,
    digest: [u8; 32],
    parent: Option<AgentId>,
    request: &ApprovalRequest<'_>,
    files: &dyn ApprovalFiles,
    start: usize,
) -> Result<NextStage<'a>> {
    let parent = parent.map(|parent| format!("parent:{parent}"));
    next_stage_with_parent(policy, digest, parent.as_deref(), request, files, start).await
}

// The temporary task adapter retains its original parent principal spelling.
// Only this function traverses the chain; agent callers derive the principal
// from the admitted native agent identity above.
pub(super) async fn next_stage_with_parent<'a>(
    policy: &'a ApprovalPolicy,
    digest: [u8; 32],
    parent: Option<&str>,
    request: &ApprovalRequest<'_>,
    files: &dyn ApprovalFiles,
    start: usize,
) -> Result<NextStage<'a>> {
    use crate::runtime::ToolPolicy as _;
    if policy.bind()?.identity().digest != digest {
        return Err(Error::Conflict("approval policy digest changed".into()));
    }
    static HUMAN_POLICY: std::sync::LazyLock<ApprovalPolicy> =
        std::sync::LazyLock::new(ApprovalPolicy::default);
    let interaction: crate::interaction::Interaction =
        crate::contract::json_from_slice(request.bytes())
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
            subject: crate::interaction::ApprovalSubject::Legacy {},
            ..
        } => &*HUMAN_POLICY,
        crate::interaction::Interaction::Approval { .. } => policy,
        _ => return Err(Error::Invalid("not an approval request".into())),
    };
    let hash = fingerprint(request.bytes())?;
    for (stage, approver) in policy.approvers.iter().enumerate().skip(start) {
        let principal = match approver {
            Approver::Rules { principal, .. }
            | Approver::Llm { principal, .. }
            | Approver::Human { principal } => principal.clone(),
            Approver::Parent => match parent {
                Some(parent) => parent.to_owned(),
                None => {
                    return Ok(NextStage::Decided(ApprovalReviewFile {
                        principal: "policy:exhausted".into(),
                        request_sha256: hash,
                        policy_digest: digest,
                        stage,
                        verdict: RuleVerdict::Deny,
                        reason: Some("approval parent is unavailable".into()),
                    }));
                }
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
        let retained_bytes = files
            .read(
                &request.review_path(&expected.principal),
                request.maximum_bytes(),
            )
            .await?;
        let retained = retained_review(
            retained_bytes.as_deref(),
            &expected,
            request.maximum_bytes(),
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

/// Authenticated role supplied by the host, never decoded from decision bytes.
/// Keeping the role prevents a model principal named "human" from satisfying
/// an explicit human guard. Parent identity comes from the authenticated actor.
#[derive(Clone, Copy)]
pub enum ApprovalResponder<'a> {
    /// Authenticated human connection.
    Human(&'a str),
    /// Parent actor identity.
    Parent(&'a AgentId),
    /// Configured policy evaluator identity.
    Rules(&'a str),
    /// Independent model owner identity.
    Llm(&'a str),
    /// Internal exhaustion or parent-unavailable refusal.
    Policy,
}

/// Checks a proposed decision against the current stage before the agent owner
/// commits it. `responder` comes from the connection or actor, never the
/// decision body. The owner still verifies the action binding,
/// fences this stage against races and publishes the decision plus inbox wake.
pub async fn validate_agent_decision(
    policy: &ApprovalPolicy,
    digest: [u8; 32],
    parent: Option<AgentId>,
    request: &ApprovalRequest<'_>,
    files: &dyn ApprovalFiles,
    responder: ApprovalResponder<'_>,
    expected_version: u64,
    decision: &ApprovalDecisionFile,
) -> Result<()> {
    decision.validate()?;
    let principal = match responder {
        ApprovalResponder::Human(principal)
        | ApprovalResponder::Rules(principal)
        | ApprovalResponder::Llm(principal) => principal.to_owned(),
        ApprovalResponder::Parent(parent) => format!("parent:{parent}"),
        ApprovalResponder::Policy => "policy:exhausted".into(),
    };
    if principal != decision.principal
        || decision.expected_version != expected_version
        || decision.request_sha256 != fingerprint(request.bytes())?
    {
        return Err(Error::Unauthorized(
            "approval decision binding differs".into(),
        ));
    }
    let allowed = match next_stage(policy, digest, parent, request, files, 0).await? {
        NextStage::Review {
            approver: Approver::Human { .. },
            expected,
            ..
        } => matches!(responder, ApprovalResponder::Human(_)) && expected.principal == principal,
        NextStage::Review {
            approver: Approver::Parent,
            expected,
            ..
        } => matches!(responder, ApprovalResponder::Parent(_)) && expected.principal == principal,
        NextStage::Decided(review) => {
            let role_matches = match (policy.approvers.get(review.stage), responder) {
                (Some(Approver::Rules { .. }), ApprovalResponder::Rules(_))
                | (Some(Approver::Llm { .. }), ApprovalResponder::Llm(_)) => true,
                (_, ApprovalResponder::Policy) => {
                    review.principal == "policy:exhausted" && review.verdict == RuleVerdict::Deny
                }
                _ => false,
            };
            role_matches
                && review.principal == principal
                && review.verdict != RuleVerdict::Ask
                && (review.verdict == RuleVerdict::Allow) == decision.approved()
                && review.reason == decision.reason
        }
        _ => false,
    };
    if !allowed {
        return Err(Error::Unauthorized(
            "principal is not the active approval stage".into(),
        ));
    }
    Ok(())
}

/// Digest binding to the exact immutable request bytes.
pub fn fingerprint(bytes: &[u8]) -> Result<String> {
    let descriptor =
        crate::conversation::FileDescriptor::from_bytes(bytes, "application/json", &mut None)?;
    Ok(ApprovalDecisionFile::new("binding", descriptor.sha256(), 1, false, None)?.request_sha256)
}
fn retained_review(
    bytes: Option<&[u8]>,
    expected: &ApprovalReviewFile,
    maximum_bytes: u64,
) -> Result<Option<ApprovalReviewFile>> {
    bytes
        .map(|bytes| {
            super::files::check_bound(bytes.len(), maximum_bytes)?;
            let review: ApprovalReviewFile = crate::contract::json_from_slice(bytes)
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

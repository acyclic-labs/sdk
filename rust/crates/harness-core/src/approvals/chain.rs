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
    next_stage_with_io(policy, digest, parent.as_ref(), request, files, start, None).await
}

pub(super) async fn next_stage_with_io<'a>(
    policy: &'a ApprovalPolicy,
    digest: [u8; 32],
    parent: Option<&AgentId>,
    request: &ApprovalRequest<'_>,
    files: &dyn ApprovalFiles,
    start: usize,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<NextStage<'a>> {
    super::io::step(io, 1)?;
    let parent = parent
        .map(|parent| super::io::text(format_args!("parent:{parent}"), io))
        .transpose()?;
    // This owner pinned and validated the policy at construction. Public callers
    // still authenticate their supplied digest at the same boundary.
    if io.is_none() {
        use crate::runtime::ToolPolicy as _;
        if policy.bind()?.identity().digest != digest {
            return Err(Error::Conflict("approval policy digest changed".into()));
        }
    }
    static HUMAN_POLICY: std::sync::LazyLock<ApprovalPolicy> =
        std::sync::LazyLock::new(ApprovalPolicy::default);
    let interaction: crate::interaction::Interaction = super::io::decode(request.bytes(), io)?;
    let policy = match interaction {
        crate::interaction::Interaction::Approval {
            subject:
                crate::interaction::ApprovalSubject::Tool {
                    route: crate::interaction::ApprovalRoute::Human,
                    ..
                },
            ..
        } => &*HUMAN_POLICY,
        crate::interaction::Interaction::Approval { .. } => policy,
        _ => {
            return Err(super::io::error(
                io,
                crate::wire::ErrorCode::Invalid,
                "not an approval request",
            ));
        }
    };
    let hash = fingerprint_with_io(request.bytes(), io)?;
    for (stage, approver) in policy.approvers.iter().enumerate().skip(start) {
        super::io::step(io, 1)?;
        let principal = match approver {
            Approver::Rules { principal, .. }
            | Approver::Llm { principal, .. }
            | Approver::Human { principal } => super::io::copy(principal, io)?,
            Approver::Parent => match &parent {
                Some(parent) => super::io::copy(parent, io)?,
                None => {
                    return Ok(NextStage::Decided(ApprovalReviewFile {
                        principal: super::io::copy("policy:exhausted", io)?,
                        request_sha256: hash,
                        policy_digest: digest,
                        stage,
                        verdict: RuleVerdict::Deny,
                        reason: Some(super::io::copy("approval parent is unavailable", io)?),
                    }));
                }
            },
        };
        let expected = ApprovalReviewFile {
            principal,
            request_sha256: super::io::copy(&hash, io)?,
            policy_digest: digest,
            stage,
            verdict: RuleVerdict::Ask,
            reason: None,
        };
        let path = request.review_path_with_io(&expected.principal, io)?;
        let retained_bytes = files
            .read_with_io(&path, request.maximum_bytes(), io)?
            .await?;
        let retained = retained_review(
            retained_bytes.as_deref(),
            &expected,
            request.maximum_bytes(),
            io,
        )?;
        let retained = if !matches!(approver, Approver::Human { .. } | Approver::Parent) {
            match retained {
                Some(review) if review.verdict == RuleVerdict::Ask => continue,
                Some(review) => return Ok(NextStage::Decided(review)),
                None => None,
            }
        } else {
            retained
        };
        return Ok(NextStage::Review {
            approver,
            expected,
            retained,
        });
    }
    Ok(NextStage::Decided(ApprovalReviewFile {
        principal: super::io::copy("policy:exhausted", io)?,
        request_sha256: hash,
        policy_digest: digest,
        stage: policy.approvers.len(),
        verdict: RuleVerdict::Deny,
        reason: Some(super::io::copy(
            "approval chain exhausted without permission",
            io,
        )?),
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
    fingerprint_with_io(bytes, None)
}

pub(super) fn fingerprint_with_io(
    bytes: &[u8],
    io: Option<&crate::executor::PreparationIo>,
) -> Result<String> {
    let descriptor = crate::executor::with_history_io(io, |work| {
        crate::conversation::FileDescriptor::from_bytes(bytes, "application/json", work)
    })?;
    let mut encoded = [0; 64];
    for (index, byte) in descriptor.sha256().iter().enumerate() {
        encoded[2 * index] = b"0123456789abcdef"[(byte >> 4) as usize];
        encoded[2 * index + 1] = b"0123456789abcdef"[(byte & 15) as usize];
    }
    super::io::copy(std::str::from_utf8(&encoded).expect("hex"), io)
}

fn retained_review(
    bytes: Option<&[u8]>,
    expected: &ApprovalReviewFile,
    maximum_bytes: u64,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Option<ApprovalReviewFile>> {
    bytes
        .map(|bytes| {
            super::files::check_bound_with_io(bytes.len(), maximum_bytes, io)?;
            let review: ApprovalReviewFile = super::io::decode(bytes, io)?;
            if review.principal != expected.principal
                || review.request_sha256 != expected.request_sha256
                || review.policy_digest != expected.policy_digest
                || review.stage != expected.stage
            {
                return Err(super::io::error(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "approval review binding changed",
                ));
            }
            Ok(review)
        })
        .transpose()
}

#[cfg(test)]
mod tests;

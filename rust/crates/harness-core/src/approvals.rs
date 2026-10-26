//! Approval configuration is data; every verdict passes through the file owner.
use crate::{
    Error, Result,
    registry::ComponentIdentity,
    runtime::{RuntimeScope, ToolPolicy, ToolPolicyDecision},
    tool::ToolInvocation,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod chain;
pub use chain::{
    ApprovalResponder, NextStage, fingerprint as request_fingerprint, next_stage as next_agent_stage,
    validate_agent_decision,
};
mod files;
pub use files::{ApprovalCreate, ApprovalFiles, ApprovalRequest};
mod legacy;
pub use legacy::{automatic_review, fingerprint, next_stage, request_id, review_key};
mod automatic;
pub use automatic::automatic_review as automatic_agent_review;

mod selectors;
pub use selectors::ApprovalSelector;

/// Ordered escalation chain. An `ask` advances to the next approver.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalPolicy {
    /// First decisive verdict settles the request. Exhaustion refuses the action.
    pub approvers: Vec<Approver>,
}

/// One independently identified approver in the chain.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Approver {
    /// Tool/argument selectors, with an explicit fallback.
    Rules {
        /// Authenticated rule identity.
        principal: String,
        /// Matching refusals take precedence over matching permissions.
        rules: Vec<ApprovalRule>,
        /// Verdict when no selector matches.
        fallback: RuleVerdict,
    },
    /// A separate model binding and its own system prompt; it has no tools.
    Llm {
        /// Authenticated model identity.
        principal: String,
        /// Independent model binding.
        model: String,
        /// This approver's private instruction.
        prompt: String,
    },
    /// A human authenticated by the client connection, never by file contents.
    Human {
        /// Connection-authenticated human identity.
        principal: String,
    },
    /// The retained parent task, authenticated through its existing inbox.
    Parent,
}

/// Conjunctive rule. Omitted selectors match every admitted tool call.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalRule {
    /// Optional exact tool name.
    pub tool: Option<String>,
    /// Optional exact arguments, including all object fields.
    pub arguments: Option<Value>,
    /// Additional argument selectors, all of which must match.
    #[serde(default)]
    pub selectors: Vec<ApprovalSelector>,
    /// Result when every selector matches.
    pub verdict: RuleVerdict,
}

/// Policy and model results use the same escalation semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleVerdict {
    /// Submit an approving decision.
    Allow,
    /// Submit a denying decision.
    Deny,
    /// Escalate to the next approver.
    Ask,
}

impl Default for ApprovalPolicy {
    fn default() -> Self {
        Self {
            approvers: vec![Approver::Human {
                principal: "human".into(),
            }],
        }
    }
}

impl ApprovalPolicy {
    /// Validates and pins the complete configuration, including escalation order.
    pub fn bind(&self) -> Result<ApprovalGate> {
        if self.approvers.is_empty() || self.approvers.len() > 32 {
            return Err(Error::Invalid(
                "approval chain must contain 1..32 approvers".into(),
            ));
        }
        let mut principals = std::collections::BTreeSet::new();
        for approver in &self.approvers {
            let principal = match approver {
                Approver::Rules {
                    principal, rules, ..
                } => {
                    if rules.len() > 1024 {
                        return Err(Error::Invalid("too many approval rules".into()));
                    }
                    for rule in rules {
                        for selector in &rule.selectors {
                            selector.validate()?;
                        }
                    }
                    principal.as_str()
                }
                Approver::Llm {
                    principal,
                    model,
                    prompt,
                } => {
                    if model.is_empty() || prompt.trim().is_empty() {
                        return Err(Error::Invalid(
                            "approver model and prompt are required".into(),
                        ));
                    }
                    principal.as_str()
                }
                Approver::Human { principal } => principal.as_str(),
                Approver::Parent => "parent",
            };
            if principal.is_empty()
                || principal.len() > 255
                || principal.starts_with("parent:")
                || principal.starts_with("model-start:")
                || principal == "policy:exhausted"
                || !principals.insert(principal)
            {
                return Err(Error::Invalid(
                    "approval principals must be distinct and nonempty".into(),
                ));
            }
        }
        let bytes = serde_json::to_vec(self).map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(ApprovalGate {
            identity: ComponentIdentity {
                name: "acyclic.approval-policy".into(),
                version: "1".into(),
                digest: *blake3::hash(&bytes).as_bytes(),
            },
        })
    }
}

impl ApprovalRule {
    /// A matching refusal wins over every matching permission in one rule set.
    pub fn evaluate(
        rules: &[Self],
        fallback: RuleVerdict,
        tool: &str,
        arguments: &Value,
    ) -> RuleVerdict {
        Self::decide(
            rules.iter().filter(|rule| rule.matches(tool, arguments)),
            fallback,
        )
    }

    /// Applies selectors without inventing tool metadata for sealed actions.
    /// Only rules with no selectors match an explicitly opaque subject.
    pub fn evaluate_subject(
        rules: &[Self],
        fallback: RuleVerdict,
        subject: &crate::interaction::ApprovalSubject,
    ) -> RuleVerdict {
        match subject {
            crate::interaction::ApprovalSubject::Tool {
                tool, arguments, ..
            } => Self::evaluate(rules, fallback, tool, arguments),
            crate::interaction::ApprovalSubject::Legacy {} => RuleVerdict::Ask,
            crate::interaction::ApprovalSubject::Opaque {} => Self::decide(
                rules.iter().filter(|rule| {
                    rule.tool.is_none() && rule.arguments.is_none() && rule.selectors.is_empty()
                }),
                fallback,
            ),
        }
    }

    fn decide<'a>(rules: impl Iterator<Item = &'a Self>, fallback: RuleVerdict) -> RuleVerdict {
        rules
            .map(|rule| rule.verdict)
            .reduce(|left, right| match (left, right) {
                (RuleVerdict::Deny, _) | (_, RuleVerdict::Deny) => RuleVerdict::Deny,
                (RuleVerdict::Ask, _) | (_, RuleVerdict::Ask) => RuleVerdict::Ask,
                _ => RuleVerdict::Allow,
            })
            .unwrap_or(fallback)
    }

    /// Matches the exact subject that the interaction owner published.
    pub fn matches(&self, tool: &str, arguments: &Value) -> bool {
        self.tool.as_deref().is_none_or(|expected| expected == tool)
            && self
                .arguments
                .as_ref()
                .is_none_or(|expected| expected == arguments)
            && self
                .selectors
                .iter()
                .all(|selector| selector.matches(arguments))
    }
}

/// Admission gate pinned to policy data. Automatic approvers never bypass it.
pub struct ApprovalGate {
    identity: ComponentIdentity,
}
const PROMPT: &str = "Approve the exact tool action under the admitted approval policy.";
impl ToolPolicy for ApprovalGate {
    fn identity(&self) -> ComponentIdentity {
        self.identity.clone()
    }
    fn evaluate<'a>(
        &'a self,
        _: &'a ToolInvocation,
        _: &'a RuntimeScope,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::store::HistoryContentFuture<'a, (ToolPolicyDecision, crate::NativeCustody)>>
    {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::filesystem::PreparationIo::original_native_work) {
            return crate::NativeFutureBundle::new(work, || async move {
                use crate::store::history::HistoryIo;
                let mut io = crate::store::HistoryWork::new(
                    crate::filesystem::PreparationIo::from_native_work(work)?,
                );
                let prompt = io.clone_text(PROMPT)?;
                Ok((
                    ToolPolicyDecision::RequireApproval { prompt },
                    Some(io.into_custody()?),
                ))
            })
            .map(crate::store::HistoryContentFuture::Native);
        }
        let _ = io;
        Ok(crate::store::HistoryContentFuture::Ordinary {
            future: Box::pin(async {
                Ok((
                    ToolPolicyDecision::RequireApproval {
                        prompt: PROMPT.into(),
                    },
                    None,
                ))
            }),
            frame_bytes: 0,
        })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn identity_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ComponentIdentity, acyclic_fs::NativeAllocation)> {
        use crate::store::history::HistoryIo;
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        io.retain_owned(std::mem::size_of::<ComponentIdentity>() as u64)?;
        let name = io.clone_text(&self.identity.name)?;
        let version = io.clone_text(&self.identity.version)?;
        Ok((
            ComponentIdentity {
                name,
                version,
                digest: self.identity.digest,
            },
            io.into_custody()?,
        ))
    }
}

/// Immutable audit of one policy stage, retained before escalation or settlement.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalReviewFile {
    /// Stage's configured identity.
    pub principal: String,
    /// Digest of the immutable request bytes.
    pub request_sha256: String,
    /// Digest of the entire admitted approval chain.
    pub policy_digest: [u8; 32],
    /// Ordered stage index.
    pub stage: usize,
    /// Approve, refuse or escalate.
    pub verdict: RuleVerdict,
    /// Rule or model explanation.
    pub reason: Option<String>,
}

#[cfg(test)]
mod tests;

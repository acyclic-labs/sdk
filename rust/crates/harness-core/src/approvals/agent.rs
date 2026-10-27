//! Approvals of one native agent: request, reviews and decisions are files on the
//! agent's private volume, and a decision wakes the agent with one inbox record.
//!
//! The agent is the approval owner. Its journal opens the request file once, and
//! each turn step walks the shared policy chain over the retained files: automatic
//! stages review in-line, human and parent stages wait for their decision file.
//! A responder writes that file through [`decide`], then appends
//! `InboxRecord::Approval { id }` so the waiting turn resumes; nothing polls.
use super::{
    ApprovalFiles, ApprovalPolicy, ApprovalRequest, ApprovalResponder, Approver, NextStage,
    RuleVerdict,
};
use crate::{
    Error, InteractionId, Result,
    agent::AgentId,
    conversation::Limits,
    interaction::{ApprovalDecisionFile, Interaction, InteractionOutcome},
};
use std::sync::{Arc, Mutex};

/// Requests are immutable, so every decision binds their first and only version.
const VERSION: u64 = 1;

/// Request identity of approval interaction `id`: its request directory and
/// the intent of the `Approval` record that resumes the turn waiting on it.
/// Every approval of one tool call has its own interaction, so its own files.
#[must_use]
pub fn request_id(id: InteractionId) -> String {
    let mut text = String::with_capacity(41);
    text.push_str("approval-");
    for byte in id.into_bytes() {
        for nibble in [byte >> 4, byte & 0x0f] {
            text.extend(char::from_digit(u32::from(nibble), 16));
        }
    }
    text
}

fn request_path(id: &str) -> String {
    format!("approvals/{id}/request.json")
}

fn decision_path(id: &str, principal: &str) -> String {
    format!(
        "approvals/{id}/decisions/{}.json",
        ApprovalDecisionFile::principal_file(principal)
    )
}

/// The bound policy chain of one agent: its policy (the human default when the
/// agent has none), the parent that answers parent stages, and the independent
/// models of automatic stages.
pub struct AgentApprovalPolicy {
    policy: ApprovalPolicy,
    digest: [u8; 32],
    parent: Option<AgentId>,
    models: super::ApproverModels,
    limits: Limits,
}

impl AgentApprovalPolicy {
    /// Pins `policy` for an agent spawned by `parent`.
    pub fn new(
        policy: Option<ApprovalPolicy>,
        parent: Option<AgentId>,
        models: super::ApproverModels,
        limits: Limits,
    ) -> Result<Self> {
        use crate::runtime::ToolPolicy as _;
        let policy = policy.unwrap_or_default();
        let digest = policy.bind()?.identity().digest;
        Ok(Self {
            policy,
            digest,
            parent,
            models,
            limits,
        })
    }

    /// The immutable request file of approval `id`.
    pub(crate) async fn request(&self, files: &dyn ApprovalFiles, id: &str) -> Result<Vec<u8>> {
        files
            .read(&request_path(id), self.limits.file_bytes)
            .await?
            .ok_or_else(|| Error::NotFound("approval request is not open".into()))
    }
}

/// The approvals of one running turn: its journal's interaction owner.
pub struct AgentApprovals {
    files: Arc<dyn ApprovalFiles>,
    policy: Arc<AgentApprovalPolicy>,
    /// The request the turn waits on, and whether its stage is the parent's.
    pending: Mutex<Option<(String, bool)>>,
}

impl AgentApprovals {
    /// Owner of the requests on `files` under `policy`.
    #[must_use]
    pub fn new(files: Arc<dyn ApprovalFiles>, policy: Arc<AgentApprovalPolicy>) -> Self {
        Self {
            files,
            policy,
            pending: Mutex::new(None),
        }
    }

    /// The request the turn now waits on, and whether the parent decides it.
    #[must_use]
    pub fn pending(&self) -> Option<(String, bool)> {
        self.pending.lock().ok().and_then(|pending| pending.clone())
    }

    fn wait(&self, pending: Option<(String, bool)>) -> Result<()> {
        *self
            .pending
            .lock()
            .map_err(|_| Error::Storage("approvals lock".into()))? = pending;
        Ok(())
    }

    /// Opens the request file of approval `id` exactly once.
    pub async fn open(&self, id: InteractionId, interaction: &Interaction) -> Result<()> {
        if !matches!(interaction, Interaction::Approval { .. }) {
            return Err(Error::Unsupported(
                "agents open only approval interactions".into(),
            ));
        }
        let request = request_id(id);
        let bytes = crate::contract::canonical_json_bytes(interaction)?;
        super::files::create_exact(
            &*self.files,
            &request_path(&request),
            bytes,
            self.policy.limits.file_bytes,
        )
        .await?;
        Ok(())
    }

    /// The decided outcome of `id`, running due automatic stages; `None` while a
    /// human or parent decision is outstanding.
    pub async fn outcome(&self, id: InteractionId) -> Result<Option<InteractionOutcome>> {
        let request = request_id(id);
        let policy = &*self.policy;
        let bytes = policy.request(&*self.files, &request).await?;
        let view = ApprovalRequest::new(&request, &bytes, policy.limits.file_bytes)?;
        // Each automatic review is durable before the traversal moves past it,
        // so the chain advances at most once per approver.
        for _ in 0..=policy.policy.approvers.len() {
            let stage = super::next_agent_stage(
                &policy.policy,
                policy.digest,
                policy.parent.clone(),
                &view,
                &*self.files,
                0,
            )
            .await?;
            let (approver, expected) = match stage {
                NextStage::Decided(review) => {
                    self.wait(None)?;
                    return Ok(Some(if review.verdict == RuleVerdict::Allow {
                        InteractionOutcome::Approved {}
                    } else {
                        InteractionOutcome::Denied {}
                    }));
                }
                NextStage::Review {
                    approver, expected, ..
                } => (approver, expected),
            };
            if matches!(approver, Approver::Rules { .. } | Approver::Llm { .. }) {
                super::automatic_agent_review(
                    &policy.models,
                    &*self.files,
                    &view,
                    approver,
                    expected,
                    policy.limits,
                )
                .await?;
                continue;
            }
            let path = decision_path(&request, &expected.principal);
            let Some(decided) = self.files.read(&path, policy.limits.file_bytes).await? else {
                self.wait(Some((request, matches!(approver, Approver::Parent))))?;
                return Ok(None);
            };
            let decision = ApprovalDecisionFile::decode(&decided, policy.limits.file_bytes)?;
            if decision.principal != expected.principal
                || decision.request_sha256 != expected.request_sha256
                || decision.expected_version != VERSION
            {
                return Err(Error::Conflict("approval decision binding differs".into()));
            }
            self.wait(None)?;
            return Ok(Some(if decision.approved() {
                InteractionOutcome::Approved {}
            } else {
                InteractionOutcome::Declined {}
            }));
        }
        Err(Error::Conflict("approval chain did not settle".into()))
    }
}

/// `responder` decides request `id` of the agent whose approvals are `files`
/// under `policy`: the decision must bind the active stage, and its file is
/// created once. The caller then appends `InboxRecord::Approval { id }` to
/// the agent's inbox, which resumes the waiting turn.
pub async fn decide(
    files: &dyn ApprovalFiles,
    policy: &AgentApprovalPolicy,
    id: &str,
    responder: ApprovalResponder<'_>,
    approved: bool,
    reason: Option<String>,
) -> Result<()> {
    let bytes = policy.request(files, id).await?;
    let view = ApprovalRequest::new(id, &bytes, policy.limits.file_bytes)?;
    let principal = match responder {
        ApprovalResponder::Human(principal)
        | ApprovalResponder::Rules(principal)
        | ApprovalResponder::Llm(principal) => principal.to_owned(),
        ApprovalResponder::Parent(parent) => format!("parent:{parent}"),
        ApprovalResponder::Policy => "policy:exhausted".into(),
    };
    let descriptor =
        crate::conversation::FileDescriptor::from_bytes(&bytes, "application/json", &mut None)?;
    let decision =
        ApprovalDecisionFile::new(&principal, descriptor.sha256(), VERSION, approved, reason)?;
    super::validate_agent_decision(
        &policy.policy,
        policy.digest,
        policy.parent.clone(),
        &view,
        files,
        responder,
        VERSION,
        &decision,
    )
    .await?;
    super::files::create_exact(
        files,
        &decision_path(id, &principal),
        serde_json::to_vec(&decision).map_err(|error| Error::Storage(error.to_string()))?,
        policy.limits.file_bytes,
    )
    .await?;
    Ok(())
}

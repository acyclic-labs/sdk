//! Standing human decisions are immutable protected files, scoped to the
//! original agent, policy, principal and exact shared ApprovalRule descriptor.
use super::*;
use crate::{
    agent::AgentId,
    interaction::{ApprovalRoute, ApprovalSubject},
};

pub(super) fn exact(subject: ApprovalSubject) -> Option<ApprovalRule> {
    match subject {
        ApprovalSubject::Tool {
            route: ApprovalRoute::Policy,
            tool,
            arguments,
            target,
            ..
        } => Some(ApprovalRule {
            tool: Some(tool),
            arguments: Some(arguments),
            selectors: Vec::new(),
            target: target.map(|target| TargetSelector {
                device: Some(target.device),
                owner: Some(target.owner),
            }),
            verdict: RuleVerdict::Allow,
        }),
        // Opaque preimages and an explicit human guard never become a rule.
        _ => None,
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct Record<'a> {
    version: u32,
    agent: &'a AgentId,
    policy_digest: [u8; 32],
    principal: &'a str,
    rule: &'a ApprovalRule,
}

pub(super) struct Grant {
    path: String,
    bytes: Vec<u8>,
    digest: [u8; 32],
}

impl Grant {
    pub(super) fn new(
        agent: &AgentId,
        policy_digest: [u8; 32],
        principal: &str,
        rule: &ApprovalRule,
    ) -> Result<Self> {
        let bytes = crate::contract::canonical_json_bytes(&Record {
            version: 1,
            agent,
            policy_digest,
            principal,
            rule,
        })?;
        let digest = blake3::hash(&bytes);
        let path = format!("approvals/rules/{}.json", digest.to_hex());
        Ok(Self {
            path,
            bytes,
            digest: *digest.as_bytes(),
        })
    }

    pub(super) fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub(super) async fn present(&self, files: &dyn ApprovalFiles, maximum: u64) -> Result<bool> {
        let Some(bytes) = files.read(&self.path, maximum).await? else {
            return Ok(false);
        };
        if bytes != self.bytes {
            return Err(Error::Conflict(
                "standing approval rule binding differs".into(),
            ));
        }
        Ok(true)
    }

    pub(super) async fn publish(self, files: &dyn ApprovalFiles, maximum: u64) -> Result<()> {
        super::files::create_exact(files, &self.path, self.bytes, maximum)
            .await
            .map(|_| ())
    }
}

//! Exact-intent approvals in the existing private task volume.
use super::*;
use crate::{
    InteractionId, TaskId,
    interaction::{ApprovalBinding, Interaction},
};

/// Immutable request published by the executing task owner.
#[derive(Clone, Debug, PartialEq)]
pub struct FileApproval {
    /// Original task whose tool is suspended.
    pub task: TaskId,
    /// Stable interaction identity, derived from the exact intent.
    pub id: InteractionId,
    /// Prompt and exact operation/action binding.
    pub request: Interaction,
    request_sha256: [u8; 32],
}
impl FileApproval {
    /// Canonical request path beneath the original task.
    pub fn key(&self) -> crate::Result<FileKey> {
        Ok(FileKey::Task {
            task: Component::new(self.task.to_string())?,
            file: TaskFile::ApprovalRequest(Component::new(self.id.to_string())?),
        })
    }
    /// Select this request and its decisions without earlier task history.
    pub fn query(&self) -> crate::Result<Query> {
        Ok(Query::Approval {
            task: Component::new(self.task.to_string())?,
            request: Component::new(self.id.to_string())?,
        })
    }
    /// Extract the exact intent, rejecting non-approval interactions.
    pub fn binding(&self) -> crate::Result<ApprovalBinding> {
        let Interaction::Approval {
            operation_id,
            action_digest,
            ..
        } = &self.request
        else {
            return Err(crate::Error::Invalid(
                "approval file contains another interaction".into(),
            ));
        };
        let binding = ApprovalBinding {
            operation_id: *operation_id,
            action_digest: *action_digest,
        };
        binding.validate()?;
        Ok(binding)
    }
    /// Decode the canonical request bytes, deriving identities only from its path.
    pub fn decode(file: &FileValue) -> crate::Result<Self> {
        let FileKey::Task {
            task,
            file: TaskFile::ApprovalRequest(id),
        } = &file.key
        else {
            return Err(crate::Error::Invalid("not an approval request file".into()));
        };
        let descriptor = crate::conversation::FileDescriptor::from_bytes(
            &file.bytes,
            "application/json",
            &mut None,
        )?;
        let value = Self {
            request_sha256: *descriptor.sha256(),
            task: TaskId::parse(task.as_str())?,
            id: InteractionId::from_bytes(
                uuid::Uuid::parse_str(id.as_str())
                    .map_err(|e| crate::Error::Invalid(e.to_string()))?
                    .into_bytes(),
            ),
            request: crate::contract::json_from_slice(&file.bytes)
                .map_err(|e| crate::Error::Invalid(e.to_string()))?,
        };
        value.binding()?;
        if value.key()? != file.key {
            return Err(crate::Error::Invalid(
                "noncanonical approval request path".into(),
            ));
        }
        Ok(value)
    }
    /// Check a display projection against the shared exact-request decision codec.
    /// Execution still requires its owner's authenticated durable resolution.
    pub fn decision(&self, key: &FileKey, bytes: &[u8]) -> crate::Result<Option<bool>> {
        let FileKey::Task {
            task,
            file: TaskFile::ApprovalDecision { request, principal },
        } = key
        else {
            return Ok(None);
        };
        if task.as_str() != self.task.to_string() || request.as_str() != self.id.to_string() {
            return Ok(None);
        }
        if self.request.is_legacy_approval()
            && principal.as_str()
                == crate::interaction::ApprovalDecisionFile::principal_file(
                    crate::interaction::LEGACY_DECISION_PRINCIPAL,
                )
        {
            // Display only: execution checks the original detail descriptor,
            // committed outcome and canonical durable receipt in its owner.
            let response: crate::interaction::InteractionResponse =
                crate::contract::json_from_slice(bytes)
                    .map_err(|error| crate::Error::Invalid(error.to_string()))?;
            return match response {
                crate::interaction::InteractionResponse::Approval { approved, .. } => {
                    Ok(Some(approved))
                }
                _ => Err(crate::Error::Invalid(
                    "not a legacy approval decision".into(),
                )),
            };
        }
        let decision = crate::interaction::ApprovalDecisionFile::decode(bytes, bytes.len() as u64)?;
        if !decision.binds(&self.request_sha256, 1)
            || principal.as_str()
                != crate::interaction::ApprovalDecisionFile::principal_file(&decision.principal)
        {
            return Err(crate::Error::Unauthorized(
                "approval decision names another request or principal".into(),
            ));
        }
        Ok(Some(decision.approved()))
    }
}

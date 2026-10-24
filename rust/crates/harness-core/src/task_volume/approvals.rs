//! Exact-intent approvals in the existing private task volume.
use super::*;
use crate::{
    InteractionId, TaskId,
    interaction::{ApprovalBinding, Interaction},
};

/// Immutable request published by the executing task owner.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileApproval {
    /// Original task whose tool is suspended.
    pub task: TaskId,
    /// Stable interaction identity, derived from the exact intent.
    pub id: InteractionId,
    /// Prompt and exact operation/action binding.
    pub request: Interaction,
}
/// An approver's immutable decision; display text cannot substitute an intent.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileApprovalDecision {
    /// Exact operation and action accepted or refused.
    pub binding: ApprovalBinding,
    /// Whether the action may execute.
    pub approved: bool,
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
    /// Decode a decision only if both its path and its intent belong to this request.
    pub fn decision(&self, key: &FileKey, bytes: &[u8]) -> crate::Result<Option<bool>> {
        let FileKey::Task {
            task,
            file: TaskFile::ApprovalDecision { request, .. },
        } = key
        else {
            return Ok(None);
        };
        if task.as_str() != self.task.to_string() || request.as_str() != self.id.to_string() {
            return Ok(None);
        }
        let decision: FileApprovalDecision = serde_json::from_slice(bytes)
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        if decision.binding != self.binding()? {
            return Err(crate::Error::Unauthorized(
                "approval decision names another action".into(),
            ));
        }
        Ok(Some(decision.approved))
    }
}
impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    /// Publish through an already-authorized approver's private-volume handle.
    /// Ordinary agent writers (`TaskFiles::write`) cannot write these records.
    pub async fn decide_approval(
        &self,
        request: &FileApproval,
        principal: Component,
        approved: bool,
    ) -> crate::Result<()> {
        let key = FileKey::Task {
            task: Component::new(request.task.to_string())?,
            file: TaskFile::ApprovalDecision {
                request: Component::new(request.id.to_string())?,
                principal,
            },
        };
        let bytes = serde_json::to_vec(&FileApprovalDecision {
            binding: request.binding()?,
            approved,
        })
        .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        let request_key = request.key()?;
        let request_bytes = serde_json::to_vec(request)
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        loop {
            let snapshot = self.snapshot(&request.query()?, 2).await?;
            let original = snapshot.files.iter().find(|file| file.key == request_key);
            if original.map(|file| file.bytes.as_ref()) != Some(request_bytes.as_slice()) {
                return Err(crate::Error::Conflict(
                    "approval request changed or is missing".into(),
                ));
            }
            for prior in &snapshot.files {
                if let Some(decided) = request.decision(&prior.key, &prior.bytes)? {
                    return if decided == approved {
                        Ok(())
                    } else {
                        Err(crate::Error::Conflict(
                            "approval decision is immutable".into(),
                        ))
                    };
                }
            }
            match self
                .append(
                    snapshot.generation,
                    &FileValue::new(key.clone(), bytes.clone()),
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(Error::Conflict(_)) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}

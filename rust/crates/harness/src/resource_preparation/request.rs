use super::*;
use crate::{
    conversation::FileRef, runtime::TaskAdmissionRecord,
    source_authorization::ProjectSourceAuthorization, workflow::WorkflowCommand,
};

/// Immutable creation input retained by an ordinary preparation task.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcePreparationRequest {
    /// Pinned creation provider.
    pub provider: ProviderRef,
    /// Complete intended child admission.
    pub child: TaskAdmissionRecord,
    /// Parent-narrowed creation capability.
    pub creation_grant: String,
    /// Resource count bound.
    pub maximum_resources: u32,
    /// Provider work bound.
    pub maximum_work: u64,
    /// Canonical input and provider IO byte bound.
    pub maximum_bytes: u64,
    /// Immutable provider-specific input.
    pub payload: Value,
    /// Ordinary resumable outbox command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<WorkflowCommand>,
    /// Original caller checkpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_checkpoint: Option<FileRef>,
    /// Explicit immutable child inputs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_files: Vec<FileRef>,
    /// Selected Project authorization, verified by the owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_authorization: Option<ProjectSourceAuthorization>,
}
impl ResourcePreparationRequest {
    /// Created identities selected by this provider's child proofs.
    pub fn resources(&self) -> impl Iterator<Item = &ResourceRef> {
        self.child
            .resource_proofs
            .iter()
            .filter(|proof| proof.provider == self.provider)
            .map(|proof| &proof.resource)
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        sum([
            self.provider.logical_owned_bytes()?,
            self.child.logical_owned_bytes()?,
            self.creation_grant.capacity() as u64,
            crate::executor::json::json_value_logical_owned_bytes(&self.payload)?,
            vector_bytes(&self.input_files, FileRef::logical_owned_bytes)?,
            self.source_checkpoint
                .as_ref()
                .map(FileRef::logical_owned_bytes)
                .transpose()?
                .unwrap_or(0),
            self.command
                .as_ref()
                .map(|c| add(c.kind.capacity() as u64, c.payload.logical_owned_bytes()?))
                .transpose()?
                .unwrap_or(0),
            self.read_authorization
                .as_ref()
                .map(ProjectSourceAuthorization::logical_owned_bytes)
                .transpose()?
                .unwrap_or(0),
        ])
    }
    /// Validates exact inputs, authority and finite preparation bounds.
    pub fn validate(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        charge(work, 0)?;
        let invalid = |work: &mut Option<&mut dyn HistoryIo>| {
            diagnostic(work, Invalid, "invalid resource preparation")
        };
        if self.child.parent.is_none()
            || self.resources().next().is_none()
            || self.resources().count() as u64 > u64::from(self.maximum_resources)
            || matches!(self.maximum_work, 0 | u64::MAX)
            || matches!(self.maximum_bytes, 0 | u64::MAX)
            || self.creation_grant.is_empty()
            || self.creation_grant.chars().any(char::is_control)
            || self.input_files.len() as u64 > self.maximum_work
        {
            return Err(invalid(work));
        }
        crate::contract::validate_trusted_json_byte_bound(self, self.maximum_bytes, work)?;
        if let Some(checkpoint) = &self.source_checkpoint {
            checkpoint.validate(work)?;
            if checkpoint.descriptor().byte_length() > self.maximum_bytes {
                return Err(invalid(work));
            }
        }
        let mut total = 0;
        for (index, file) in self.input_files.iter().enumerate() {
            charge(work, index as u64 + 1)?;
            file.validate(work)?;
            total = add(total, file.descriptor().byte_length())?;
            if total > self.maximum_bytes || self.input_files[..index].contains(file) {
                return Err(invalid(work));
            }
        }
        if let Some(authorization) = &self.read_authorization {
            authorization.validate(work)?;
        }
        if let Some(command) = &self.command {
            command.validate(work)?;
            if command.operation_id == self.child.operation_id {
                return Err(invalid(work));
            }
        }
        self.child.validate(work)?;
        Ok(())
    }
}

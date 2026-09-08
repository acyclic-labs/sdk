//! Atomic publication manifest for prepared environment forks.

use crate::{
    Error, OperationId, Result,
    core::Authority,
    resources::{CheckpointRef, GenerationRef, StreamRef},
};
use serde::{Deserialize, Serialize};

/// Immutable references prepared before a child becomes visible.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ForkManifest {
    /// Stable publication operation used for reconciliation.
    pub operation_id: OperationId,
    /// Parent authority publishing the child.
    pub parent: Authority,
    /// Exact parent revision before publication.
    pub parent_revision: u64,
    /// Child aggregate made visible by the manifest append.
    pub child: Authority,
    /// Immutable forked Stream prefix.
    pub stream: StreamRef,
    /// Immutable Filesystem generation inherited by the child.
    pub workspace: GenerationRef,
    /// Optional Machines checkpoint inherited by the child.
    pub machine: Option<CheckpointRef>,
}

impl ForkManifest {
    /// Validates all identities and provider references after deserialization.
    pub fn validate(&self) -> Result<()> {
        if self.parent == self.child {
            return Err(Error::Invalid("fork child cannot equal its parent".into()));
        }
        self.parent.stream_path()?;
        self.child.stream_path()?;
        self.stream.validate()?;
        self.workspace.validate()?;
        if self.stream.as_resource().provider().family() != "stream"
            || self.workspace.as_resource().provider().family() != "filesystem"
        {
            return Err(Error::Invalid(
                "fork resource provider family mismatch".into(),
            ));
        }
        if let Some(machine) = &self.machine {
            machine.validate()?;
            if machine.as_resource().provider().family() != "machines" {
                return Err(Error::Invalid(
                    "fork machine provider family mismatch".into(),
                ));
            }
        }
        Ok(())
    }
}

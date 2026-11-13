//! Run input identity, bound to the original conditional file publication.
use super::Policy;
use crate::{Error, OperationId, Result, conversation::FileRef};
use serde::{Deserialize, Serialize};

/// Version-one `runs/<operation>.json`. The immutable publication receipt,
/// rather than the current writable file, selects this run's original inputs.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunAdmission {
    /// Persisted format, currently one.
    pub version: u32,
    /// Original client intent, also owning the atomic file batch.
    pub operation: OperationId,
    /// BLAKE3 of the original goal bytes.
    pub goal_hash: [u8; 32],
    /// BLAKE3 of the original serialized policy bytes.
    pub policy_hash: [u8; 32],
}

impl RunAdmission {
    /// Bind the exact submitted bytes, without inserting policy in model input.
    pub fn new(operation: OperationId, goal: &[u8], policy: &[u8]) -> Self {
        Self {
            version: 1,
            operation,
            goal_hash: *blake3::hash(goal).as_bytes(),
            policy_hash: *blake3::hash(policy).as_bytes(),
        }
    }

    /// Refuse a foreign intent, changed input or unsupported policy before work.
    pub fn verify(&self, operation: OperationId, goal: &[u8], policy: &[u8]) -> Result<Policy> {
        let expected = Self::new(operation, goal, policy);
        if self.version != 1
            || self.operation != expected.operation
            || self.goal_hash != expected.goal_hash
            || self.policy_hash != expected.policy_hash
        {
            return Err(Error::Conflict(
                "run admission differs from original input".into(),
            ));
        }
        if crate::goal::GoalState::from_bytes(goal, u64::MAX)?.status
            != crate::goal::GoalStatus::Active
        {
            return Err(Error::Invalid("a new run requires an active goal".into()));
        }
        let policy: Policy = serde_json::from_slice(policy)
            .map_err(|_| Error::Invalid("invalid run supervision policy".into()))?;
        policy
            .admit()
            .map_err(|error| Error::Invalid(error.to_string()))
    }
}

/// Original file sources authenticated by one run's retained publication.
/// Later model edits cannot replace these versions or increase its capacity.
pub struct RunAdmissionSource {
    /// Original admission declaration.
    pub admission: FileRef,
    /// Original active goal; current goal progress remains independently writable.
    pub goal: FileRef,
    /// Original owner policy, retained by exact version.
    pub policy: FileRef,
    /// Policy decoded and validated from that exact original version.
    pub value: Policy,
}

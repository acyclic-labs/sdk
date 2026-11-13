//! Approval policy born in the same journal generation and commit as its root.
use super::*;
use crate::conversation::FileDescriptor;

pub(in crate::filesystem) struct RootPolicySeed {
    files: [(&'static str, acyclic_stream::OwnedBytes); 1],
    descriptor: FileDescriptor,
}

impl RootPolicySeed {
    pub(in crate::filesystem) fn new(
        policy: crate::approvals::ApprovalPolicy,
    ) -> crate::Result<Self> {
        policy.bind()?;
        let bytes = crate::contract::canonical_json_bytes(&policy)
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        let descriptor = FileDescriptor::from_bytes(&bytes, "application/json", &mut None)?;
        Ok(Self {
            files: [("/approval-policy-v1.json", bytes.into())],
            descriptor,
        })
    }

    pub(in crate::filesystem) fn files(&self) -> &[(&'static str, acyclic_stream::OwnedBytes)] {
        &self.files
    }

    pub(in crate::filesystem) fn bind(
        &self,
        request: &mut acyclic_stream::CommitRequest,
        child: &AgentPath,
        journal: &crate::conversation::VolumeRef,
        generation: acyclic_fs::GenerationId,
    ) -> Result<(), Error> {
        let policy = FileRef::new(
            journal.clone(),
            self.files[0].0.trim_start_matches('/').to_owned(),
            hex::encode(generation.to_bytes()),
            self.descriptor.clone(),
            "policy".to_owned(),
            &mut None,
        )
        .map_err(turn)?;
        crate::agent::bind_root_policy(request, child, policy)
    }
}

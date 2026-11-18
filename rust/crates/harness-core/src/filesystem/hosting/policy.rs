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

    /// The file's content metadata, written beside it in the same creation.
    pub(in crate::filesystem) fn attributes(
        &self,
        volume: &crate::conversation::VolumeRef,
        operation: acyclic_fs_format::identity::OperationId,
    ) -> Result<Vec<acyclic_fs::CreationAttribute>, Error> {
        let path = self.files[0].0.trim_start_matches('/');
        let version = hex::encode(operation.into_bytes());
        let file = FileRef::new(
            volume.clone(),
            path.to_owned(),
            version,
            self.descriptor.clone(),
            "policy".to_owned(),
            &mut None,
        )
        .map_err(turn)?;
        let key = crate::filesystem::host::content_metadata_key_io(volume, path, None)?;
        let io = crate::filesystem::PreparationIo::for_unmeasured(
            &acyclic_fs::CancellationToken::new(),
        )?;
        let value = io.encode(
            &(
                1_u8,
                (file, Option::<&crate::resources::GenerationRef>::None),
            ),
            io.remaining().map_err(crate::Error::from)?.bytes_encoded,
            true,
        )?;
        Ok(vec![acyclic_fs::CreationAttribute {
            path: self.files[0].0.to_owned(),
            name: key,
            value,
        }])
    }

    pub(in crate::filesystem) fn bind(
        &self,
        request: &mut acyclic_stream::CommitRequest,
        child: &AgentPath,
        journal: &crate::conversation::VolumeRef,
        generation: acyclic_fs_format::object_reference::GenerationId,
    ) -> Result<(), Error> {
        let policy = FileRef::new(
            journal.clone(),
            self.files[0].0.trim_start_matches('/').to_owned(),
            hex::encode(generation.to_bytes().as_slice()),
            self.descriptor.clone(),
            "policy".to_owned(),
            &mut None,
        )
        .map_err(turn)?;
        crate::agent::bind_root_policy(request, child, policy)
    }
}

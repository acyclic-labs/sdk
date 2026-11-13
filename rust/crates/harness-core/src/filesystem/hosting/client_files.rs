//! Client files use the same grants, conditional publisher and change producer
//! as every other volume. No client state is kept in the serving process.
use super::*;
use crate::{
    Capabilities,
    client_files::{ClientFile, ClientFileChange, ClientFiles},
    conversation::{ContentChange, ContentGrant, VolumeOperation, VolumeRef},
    resources::GenerationRef,
};
use std::collections::BTreeMap;

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> Bound<P, O> {
    async fn client_volume(
        &self,
        agent: &AgentPath,
        session: Option<OperationId>,
    ) -> Result<(VolumeRef, Scope, crate::core::AuthorityVerifier), Error> {
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        let volume = match session {
            Some(session) => {
                crate::client_files::volume(self.volumes.provider.clone(), agent, session)
                    .map_err(turn)?
            }
            None => state
                .volumes
                .iter()
                .find(|binding| binding.role == crate::agent::WorkspaceRole::Private)
                .ok_or(Error::Record("agent has no private volume"))?
                .volume()?
                .clone(),
        };
        let admitted = super::super::agent_turns::AdmittedAgent::verify(
            &self.issuer,
            &self.admission,
            agent,
            &state.volumes,
        )
        .map_err(turn)?;
        admitted.authorize_volume(&volume).map_err(turn)?;
        let issuer = self.admission.verify(&self.issuer, agent).map_err(turn)?;
        let grants = Capabilities::new([
            volume
                .capability(VolumeOperation::Read, &mut None)
                .map_err(turn)?,
            volume
                .capability(VolumeOperation::Write, &mut None)
                .map_err(turn)?,
            volume
                .directory_read_capability("", &mut None)
                .map_err(turn)?,
        ]);
        let parent = match &self.admission {
            TurnAdmission::LocalRoot => None,
            TurnAdmission::Admitted { scope, .. } => Some(scope),
        };
        let scope = issuer
            .issue(
                parent,
                "client-files",
                &grants,
                Some(crate::AgentId::from_bytes(agent.id_bytes())),
            )
            .await
            .map_err(turn)?;
        let verifier = issuer.verifier(&mut None).map_err(turn)?;
        Ok((volume, scope, verifier))
    }

    /// Reads exactly the requested files at one generation. With `after`, waits
    /// on this volume's original publication stream when its head is unchanged.
    /// Closing that producer returns `Unavailable`, never an empty snapshot.
    pub async fn client_files(
        &self,
        agent: &AgentPath,
        session: OperationId,
        paths: &[String],
        after: Option<&GenerationRef>,
    ) -> Result<ClientFiles, Error> {
        self.files(agent, Some(session), paths, after).await
    }

    /// Files owned by the agent, including its authored UI, through the same
    /// conditional publisher and publication follow as client layout files.
    pub async fn agent_files(
        &self,
        agent: &AgentPath,
        paths: &[String],
        after: Option<&GenerationRef>,
    ) -> Result<ClientFiles, Error> {
        self.files(agent, None, paths, after).await
    }

    async fn files(
        &self,
        agent: &AgentPath,
        session: Option<OperationId>,
        paths: &[String],
        after: Option<&GenerationRef>,
    ) -> Result<ClientFiles, Error> {
        let (volume, scope, verifier, generation) =
            self.file_selection(agent, session, after).await?;
        let grant = ContentGrant::verify_directory_read(&verifier, &scope, &volume, "", &mut None)
            .map_err(turn)?;
        let mut files = BTreeMap::new();
        let access = ContentAccess {
            ceiling: Some(scope.capabilities()),
            io: None,
        };
        for path in paths {
            let file = match self
                .volumes
                .read_private_path(
                    &volume,
                    &grant,
                    path,
                    Some(&generation),
                    self.maximum_bytes,
                    access,
                )
                .await
            {
                Ok((file, bytes)) => Some(ClientFile {
                    file,
                    text: std::str::from_utf8(&bytes)
                        .map_err(|_| Error::Record("client file is not UTF-8"))?
                        .to_owned(),
                }),
                Err(error) if error.code() == crate::wire::ErrorCode::NotFound => None,
                Err(error) => return Err(turn(error)),
            };
            files.insert(path.clone(), file);
        }
        Ok(ClientFiles { generation, files })
    }

    async fn file_selection(
        &self,
        agent: &AgentPath,
        session: Option<OperationId>,
        after: Option<&GenerationRef>,
    ) -> Result<
        (
            VolumeRef,
            Scope,
            crate::core::AuthorityVerifier,
            GenerationRef,
        ),
        Error,
    > {
        let (volume, scope, verifier) = self.client_volume(agent, session).await?;
        let initial = if after.is_none() && session.is_some() {
            Some(
                self.volumes
                    .create_volume_admitted(
                        &volume,
                        match &self.admission {
                            TurnAdmission::Admitted { scope, .. } => Some(scope),
                            _ => None,
                        },
                    )
                    .await
                    .map_err(turn)?
                    .generation,
            )
        } else {
            None
        };
        let generation = self.volume_generation(&volume, initial, after).await?;
        Ok((volume, scope, verifier, generation))
    }

    /// Read only the caller's display credit, at the same live snapshot used
    /// by complete file queries. A split final UTF-8 scalar is omitted.
    pub async fn agent_file_previews(
        &self,
        agent: &AgentPath,
        paths: &[String],
        after: Option<&GenerationRef>,
        bytes: u64,
    ) -> Result<crate::client_files::FilePreviews, Error> {
        let (volume, scope, verifier, generation) = self.file_selection(agent, None, after).await?;
        let grant = ContentGrant::verify_directory_read(&verifier, &scope, &volume, "", &mut None)
            .map_err(turn)?;
        let access = ContentAccess {
            ceiling: Some(scope.capabilities()),
            io: None,
        };
        let mut files = BTreeMap::new();
        for path in paths {
            let text = match self
                .volumes
                .read_private_prefix(&volume, &grant, path, &generation, bytes, access)
                .await
            {
                Ok(data) => {
                    let prefix = match std::str::from_utf8(&data) {
                        Ok(text) => text,
                        Err(error) if error.error_len().is_none() => {
                            std::str::from_utf8(&data[..error.valid_up_to()])
                                .map_err(|_| Error::Record("file preview is not UTF-8"))?
                        }
                        Err(_) => return Err(Error::Record("file preview is not UTF-8")),
                    };
                    Some(prefix.to_owned())
                }
                Err(error) if error.code() == crate::wire::ErrorCode::NotFound => None,
                Err(error) => return Err(turn(error)),
            };
            files.insert(path.clone(), text);
        }
        Ok(crate::client_files::FilePreviews { generation, files })
    }

    /// Publishes one atomic conditional batch. Stable operation identity and
    /// original source descriptors retain the filesystem's recovery semantics.
    pub async fn publish_client_files(
        &self,
        agent: &AgentPath,
        session: OperationId,
        operation: OperationId,
        changes: &[ClientFileChange],
    ) -> Result<GenerationRef, Error> {
        self.publish_files(agent, Some(session), operation, changes)
            .await
    }

    /// Conditionally publish the agent's own files. Source descriptors and
    /// operation identity have exactly the client-file publication semantics.
    pub async fn publish_agent_files(
        &self,
        agent: &AgentPath,
        operation: OperationId,
        changes: &[ClientFileChange],
    ) -> Result<GenerationRef, Error> {
        self.publish_files(agent, None, operation, changes).await
    }

    async fn publish_files(
        &self,
        agent: &AgentPath,
        session: Option<OperationId>,
        operation: OperationId,
        changes: &[ClientFileChange],
    ) -> Result<GenerationRef, Error> {
        let (volume, scope, verifier) = self.client_volume(agent, session).await?;
        if session.is_some() {
            self.volumes
                .create_volume_admitted(
                    &volume,
                    match &self.admission {
                        TurnAdmission::Admitted { scope, .. } => Some(scope),
                        _ => None,
                    },
                )
                .await
                .map_err(turn)?;
        }
        let publisher = FilesystemContentPublisher::new(
            self.volumes.clone(),
            &volume,
            &verifier,
            &scope,
            self.maximum_bytes,
            None,
        )
        .map_err(turn)?;
        let changes: Vec<_> = changes
            .iter()
            .map(|change| match &change.expected {
                Some(source) => ContentChange::UpdateExpected {
                    source,
                    bytes: change.text.as_bytes(),
                },
                None => ContentChange::CreateAbsent {
                    path: &change.path,
                    bytes: change.text.as_bytes(),
                    media_type: "application/json",
                    display_name: change.path.rsplit('/').next().unwrap_or_default(),
                },
            })
            .collect();
        let (published, _hold) = publisher
            .stage_changes(
                &volume,
                operation,
                &changes,
                ContentAccess {
                    ceiling: Some(scope.capabilities()),
                    io: None,
                },
            )
            .map_err(turn)?
            .await
            .map_err(turn)?;
        Ok(published.generation)
    }
}

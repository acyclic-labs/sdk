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
        session: OperationId,
    ) -> Result<(VolumeRef, Scope, crate::core::AuthorityVerifier), Error> {
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        let volume = crate::client_files::volume(self.volumes.provider.clone(), agent, session)
            .map_err(turn)?;
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
        maximum_bytes: u64,
    ) -> Result<ClientFiles, Error> {
        let (volume, scope, verifier) = self.client_volume(agent, session).await?;
        let grant = ContentGrant::verify_directory_read(&verifier, &scope, &volume, "", &mut None)
            .map_err(turn)?;
        let reference =
            super::super::bounded_io::content_workspace(&self.volumes.provider, &volume, None)
                .map_err(turn)?;
        let initial = if after.is_none() {
            Some(self.volumes.create_volume(&volume).await.map_err(turn)?)
        } else {
            None
        };
        let waiting = if after.is_some() {
            let workspace = self.volumes.open(&reference, None).await.map_err(turn)?;
            let path = acyclic_fs::session::changes_path(
                &self.volumes.filesystem,
                workspace.id().volume_id(),
            )
            .map_err(Error::Stream)?;
            let request = acyclic_stream::HeadsRequest {
                paths: [path.clone()].into_iter().collect(),
                retained: Default::default(),
                durable_through: None,
                projection: acyclic_stream::HeadProjection::Tail,
                children: None,
                limit: 1,
                at_least: Default::default(),
            };
            let heads = self
                .agents
                .provider()
                .heads(request.clone())
                .await
                .map_err(Error::Stream)?;
            heads.validate_for(&request).map_err(Error::Stream)?;
            Some(acyclic_stream::FollowRequest {
                path,
                from: heads
                    .heads
                    .first()
                    .and_then(Option::as_ref)
                    .map_or(0, |head| head.tail),
                preceding: None,
                bootstrap_head: false,
                until: None,
                when_absent: acyclic_stream::WhenAbsent::Wait,
                filter: Default::default(),
                progress: false,
                at_least: Default::default(),
            })
        } else {
            None
        };
        let mut selected = match initial {
            Some(selected) => selected,
            None => self.volumes.resolve(&reference, None).await.map_err(turn)?,
        };
        if after == Some(&selected.generation) {
            use futures::TryStreamExt as _;
            let updates = self
                .agents
                .provider()
                .follow(waiting.ok_or(Error::Record("client files wait has no producer"))?)
                .await
                .map_err(Error::Stream)?;
            acyclic_stream::records_only(updates)
                .try_next()
                .await
                .map_err(Error::Stream)?
                .ok_or(Error::Stream(acyclic_stream::StreamError::Unavailable))?;
            selected = self.volumes.resolve(&reference, None).await.map_err(turn)?;
        }
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
                    Some(&selected.generation),
                    maximum_bytes,
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
                Err(crate::Error::ToolRejected {
                    code: crate::wire::ErrorCode::NotFound,
                    ..
                }) => None,
                Err(error) => return Err(turn(error)),
            };
            files.insert(path.clone(), file);
        }
        Ok(ClientFiles {
            generation: selected.generation,
            files,
        })
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
        let (volume, scope, verifier) = self.client_volume(agent, session).await?;
        self.volumes.create_volume(&volume).await.map_err(turn)?;
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
                    display_name: &change.path,
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

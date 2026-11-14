//! Client files use the same grants, conditional publisher and change producer
//! as every other volume. No client state is kept in the serving process.
use super::*;
use crate::{
    Capabilities,
    client_files::{ClientFile, ClientFileChange, ClientFiles, FileRoot},
    conversation::{ContentChange, ContentGrant, VolumeOperation, VolumeRef},
    resources::GenerationRef,
};
use std::collections::BTreeMap;

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> Bound<P, O> {
    /// Original host trace interest for one admitted agent. Selecting it
    /// creates no recorder and grants no authority to the returned stream.
    pub fn trace_recorder(
        &self,
        agent: &AgentPath,
    ) -> Result<Option<Arc<acyclic_stream::trace::Recorder<P>>>, Error> {
        self.admit_path(agent)?;
        self.traces
            .recorder(agent.as_str())
            .map_err(|error| turn(crate::Error::from(error)))
    }
    pub(super) async fn file_volume(
        &self,
        agent: &AgentPath,
        root: FileRoot,
        include_delete: bool,
    ) -> Result<(VolumeRef, Scope, crate::core::AuthorityVerifier), Error> {
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        let volume = match root {
            FileRoot::ClientSession { session } => {
                crate::client_files::volume(self.volumes.provider.clone(), agent, session)
                    .map_err(turn)?
            }
            FileRoot::Private {} | FileRoot::Home {} => {
                let role = match root {
                    FileRoot::Home {} => crate::agent::WorkspaceRole::Home,
                    _ => crate::agent::WorkspaceRole::Private,
                };
                state
                    .volumes
                    .iter()
                    .find(|binding| binding.role == role)
                    .ok_or(Error::Record("selected agent file root is not bound"))?
                    .volume()?
                    .clone()
            }
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
        let mut grants = vec![
            volume
                .capability(VolumeOperation::Read, &mut None)
                .map_err(turn)?,
            volume
                .capability(VolumeOperation::Write, &mut None)
                .map_err(turn)?,
            volume
                .directory_read_capability("", &mut None)
                .map_err(turn)?,
        ];
        if include_delete {
            grants.push(
                volume
                    .capability(VolumeOperation::Delete, &mut None)
                    .map_err(turn)?,
            );
        }
        let grants = Capabilities::new(grants);
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

    /// Reads explicit authored/default files at original input cuts. With
    /// `after`, waits on the same source streams every normal client uses.
    /// A closed producer is unavailable, never an empty or all-clear snapshot.
    pub async fn files(
        &self,
        agent: &AgentPath,
        root: FileRoot,
        paths: &[String],
        after: Option<&crate::client_files::FileCut>,
    ) -> Result<ClientFiles, Error> {
        use futures::TryStreamExt as _;
        if after.is_some_and(|cut| cut.version != 1) {
            return Err(Error::Record("client file cut version"));
        }
        let mut minimum = after.map(|cut| cut.revision.clone()).unwrap_or_default();
        let mut interests = None;
        loop {
            let (snapshot, sources) = self
                .files_snapshot(agent, root, paths, minimum, after.is_some())
                .await?;
            if after.is_none_or(|cut| !cut.same_inputs(&snapshot.cut)) {
                return Ok(snapshot);
            }
            if interests.is_none() {
                let mut admitted = Vec::new();
                for source in snapshot.cut.queries.keys() {
                    for view in &self.file_views {
                        if let Some(interest) = view.interest(agent, source).map_err(turn)? {
                            admitted.push(interest);
                        }
                    }
                }
                interests = Some(admitted);
            }
            let provider = self.agents.provider();
            let wait_minimum = snapshot.cut.revision.clone();
            let mut waiting = sources
                .into_iter()
                .map(|(path, from)| {
                    let minimum = wait_minimum.clone();
                    async move {
                        let updates = provider
                            .follow(acyclic_stream::FollowRequest {
                                path,
                                from,
                                preceding: None,
                                bootstrap_head: false,
                                until: None,
                                when_absent: acyclic_stream::WhenAbsent::Wait,
                                filter: Default::default(),
                                progress: false,
                                at_least: minimum,
                            })
                            .await
                            .map_err(Error::Stream)?;
                        acyclic_stream::records_only(updates)
                            .try_next()
                            .await
                            .map_err(Error::Stream)?
                            .ok_or(Error::Stream(acyclic_stream::StreamError::Unavailable))?;
                        Ok::<_, Error>(())
                    }
                })
                .collect::<futures::stream::FuturesUnordered<_>>();
            waiting
                .try_next()
                .await?
                .ok_or(Error::Record("client file wait has no producer"))?;
            minimum = snapshot.cut.revision;
        }
    }

    /// Read explicit members of one protected original conditional publication.
    /// Missing receipts yield absent files, never writable lookalikes. No history
    /// or directory is enumerated and this immutable selection cannot be watched.
    pub async fn files_at_publication(
        &self,
        agent: &AgentPath,
        root: FileRoot,
        paths: &[String],
        operation: OperationId,
    ) -> Result<ClientFiles, Error> {
        use crate::client_files::FileSource;
        let (volume, scope, verifier) = self.file_volume(agent, root, false).await?;
        if matches!(root, FileRoot::ClientSession { .. }) {
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
        let access = ContentAccess {
            ceiling: Some(scope.capabilities()),
            io: None,
        };
        let (receipt, _hold) = publisher
            .observe_changes(&volume, operation, self.maximum_bytes, access)
            .map_err(turn)?
            .await
            .map_err(turn)?;
        let generation = match &receipt {
            Some(receipt) => receipt.generation.clone(),
            None => {
                let reference = super::super::bounded_io::content_workspace(
                    &self.volumes.provider,
                    &volume,
                    None,
                )
                .map_err(turn)?;
                self.volumes
                    .resolve(&reference, None)
                    .await
                    .map_err(turn)?
                    .generation
            }
        };
        let mut snapshot = ClientFiles {
            root,
            volume,
            cut: crate::client_files::FileCut {
                version: 1,
                generation,
                revision: Default::default(),
                queries: Default::default(),
            },
            files: paths.iter().cloned().map(|path| (path, None)).collect(),
        };
        if let Some(receipt) = receipt {
            let grant = ContentGrant::verify_directory_read(
                &verifier,
                &scope,
                &snapshot.volume,
                "",
                &mut None,
            )
            .map_err(turn)?;
            for file in receipt.files() {
                if !snapshot.files.contains_key(file.path()) {
                    continue;
                }
                let bytes = self
                    .volumes
                    .read_content(file, &grant, self.maximum_bytes, None)
                    .await
                    .map_err(turn)?;
                snapshot.files.insert(
                    file.path().to_owned(),
                    Some(ClientFile {
                        source: FileSource::Authored { file: file.clone() },
                        text: String::from_utf8(bytes.to_vec())
                            .map_err(|_| Error::Record("client file is not UTF-8"))?,
                    }),
                );
            }
        }
        Ok(snapshot)
    }

    async fn files_snapshot(
        &self,
        agent: &AgentPath,
        root: FileRoot,
        paths: &[String],
        minimum: acyclic_stream::RevisionCut,
        watching: bool,
    ) -> Result<(ClientFiles, BTreeMap<acyclic_stream::StreamPath, u64>), Error> {
        use crate::client_files::{FileCut, FileSource};
        let (volume, scope, verifier) = self.file_volume(agent, root, false).await?;
        let grant = ContentGrant::verify_directory_read(&verifier, &scope, &volume, "", &mut None)
            .map_err(turn)?;
        let reference =
            super::super::bounded_io::content_workspace(&self.volumes.provider, &volume, None)
                .map_err(turn)?;
        let mut selected_views = Vec::with_capacity(paths.len());
        for path in paths {
            let mut selected = None;
            for (index, view) in self.file_views.iter().enumerate() {
                if let Some(source) = view.source(agent, root, path).map_err(turn)? {
                    if selected.replace((index, source)).is_some() {
                        return Err(Error::Record("ambiguous client file view"));
                    }
                }
            }
            selected_views.push(selected);
        }
        // Capture wait cuts before resolving the authored generation, so a
        // publication racing that resolution cannot be missed by the follow.
        let filesystem_source = if watching {
            let workspace = self.volumes.open(&reference, None).await.map_err(turn)?;
            Some(
                acyclic_fs::session::changes_path(
                    &self.volumes.filesystem,
                    workspace.id().volume_id(),
                )
                .map_err(Error::Stream)?,
            )
        } else {
            None
        };
        let captured = if watching {
            let sources = selected_views
                .iter()
                .flatten()
                .map(|(_, source)| source.clone())
                .chain(filesystem_source.iter().cloned())
                .collect();
            Some(file_heads(self.agents.provider().as_ref(), sources, minimum.clone()).await?)
        } else {
            None
        };
        let selected = if !watching && matches!(root, FileRoot::ClientSession { .. }) {
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
        } else {
            self.volumes.resolve(&reference, None).await.map_err(turn)?
        };
        let access = ContentAccess {
            ceiling: Some(scope.capabilities()),
            io: None,
        };
        let mut files = BTreeMap::new();
        let mut queries = BTreeMap::<(usize, acyclic_stream::StreamPath), Vec<&str>>::new();
        for (path, view) in paths.iter().zip(selected_views) {
            let file = match self
                .volumes
                .read_private_path(
                    &volume,
                    &grant,
                    path,
                    Some(&selected.generation),
                    self.maximum_bytes,
                    access,
                )
                .await
            {
                Ok((file, bytes)) => Some(ClientFile {
                    source: FileSource::Authored { file },
                    text: std::str::from_utf8(&bytes)
                        .map_err(|_| Error::Record("client file is not UTF-8"))?
                        .to_owned(),
                }),
                Err(error) if error.code() == crate::wire::ErrorCode::NotFound => None,
                Err(error) => return Err(turn(error)),
            };
            if file.is_none()
                && let Some(view) = view
            {
                queries.entry(view).or_default().push(path);
            }
            files.insert(path.clone(), file);
        }
        let (mut sources, revision) = match captured {
            Some(captured) => captured,
            None => {
                file_heads(
                    self.agents.provider().as_ref(),
                    queries.keys().map(|(_, source)| source.clone()).collect(),
                    minimum,
                )
                .await?
            }
        };
        sources.retain(|source, _| {
            filesystem_source.as_ref() == Some(source)
                || queries.keys().any(|(_, query)| query == source)
        });
        let query_cuts = sources
            .iter()
            .filter(|(source, _)| queries.keys().any(|(_, query)| query == *source))
            .map(|(source, through)| (source.clone(), *through))
            .collect();
        for ((index, source), requested) in queries {
            let through = *sources
                .get(&source)
                .ok_or(Error::Record("file view cut missing"))?;
            let view = &self.file_views[index];
            let values = view
                .read(
                    self.agents.provider().as_ref(),
                    agent,
                    &requested,
                    &source,
                    through,
                    revision.clone(),
                )
                .await
                .map_err(turn)?;
            for value in values {
                if !requested.contains(&value.path.as_str())
                    || value.input.stream != source
                    || value.input.sequence >= through
                    || self
                        .maximum_bytes
                        .is_some_and(|maximum| value.text.len() as u64 > maximum)
                    || files.get(&value.path).is_none_or(Option::is_some)
                {
                    return Err(Error::Record("file view returned an invalid selection"));
                }
                let descriptor = crate::conversation::FileDescriptor::from_bytes(
                    value.text.as_bytes(),
                    &value.media_type,
                    &mut None,
                )
                .map_err(turn)?;
                files.insert(
                    value.path,
                    Some(ClientFile {
                        source: FileSource::Query {
                            view: view.identity().into(),
                            input: value.input,
                            descriptor,
                        },
                        text: value.text,
                    }),
                );
            }
        }
        Ok((
            ClientFiles {
                root,
                volume,
                cut: FileCut {
                    version: 1,
                    generation: selected.generation,
                    revision,
                    queries: query_cuts,
                },
                files,
            },
            sources,
        ))
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
        let after = after.map(|generation| crate::client_files::FileCut {
            version: 1,
            generation: generation.clone(),
            revision: Default::default(),
            queries: Default::default(),
        });
        let root = FileRoot::Private {};
        let selection = self.files(agent, root, &[], after.as_ref()).await?;
        let generation = selection.cut.generation;
        let (volume, scope, verifier) = self.file_volume(agent, root, false).await?;
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

    /// Client-session files use the shared root query and original source cuts.
    pub async fn client_files(
        &self,
        agent: &AgentPath,
        session: OperationId,
        paths: &[String],
        after: Option<&crate::client_files::FileCut>,
    ) -> Result<ClientFiles, Error> {
        self.files(agent, FileRoot::ClientSession { session }, paths, after)
            .await
    }

    /// Agent-owned files use the same root query as controls and status views.
    pub async fn agent_files(
        &self,
        agent: &AgentPath,
        paths: &[String],
        after: Option<&crate::client_files::FileCut>,
    ) -> Result<ClientFiles, Error> {
        self.files(agent, FileRoot::Private {}, paths, after).await
    }

    /// Publish an explicitly selected client session's files.
    pub async fn publish_client_files(
        &self,
        agent: &AgentPath,
        session: OperationId,
        operation: OperationId,
        changes: &[ClientFileChange],
    ) -> Result<GenerationRef, Error> {
        self.publish_files(
            agent,
            FileRoot::ClientSession { session },
            operation,
            changes,
        )
        .await
    }

    /// Publish agent-owned files through the common conditional publisher.
    pub async fn publish_agent_files(
        &self,
        agent: &AgentPath,
        operation: OperationId,
        changes: &[ClientFileChange],
    ) -> Result<GenerationRef, Error> {
        self.publish_files(agent, FileRoot::Private {}, operation, changes)
            .await
    }

    /// Publishes one atomic conditional batch. Stable operation identity and
    /// original source descriptors retain the filesystem's recovery semantics.
    pub async fn publish_files(
        &self,
        agent: &AgentPath,
        root: FileRoot,
        operation: OperationId,
        changes: &[ClientFileChange],
    ) -> Result<GenerationRef, Error> {
        let (volume, scope, verifier) = self
            .file_volume(
                agent,
                root,
                changes.iter().any(|change| change.text.is_none()),
            )
            .await?;
        if matches!(root, FileRoot::ClientSession { .. }) {
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
            .map(|change| match (&change.expected, &change.text) {
                (Some(source), Some(text)) => Ok(ContentChange::UpdateExpected {
                    source,
                    bytes: text.as_bytes(),
                }),
                (Some(source), None) => Ok(ContentChange::DeleteExpected { source }),
                (None, Some(text)) => Ok(ContentChange::CreateAbsent {
                    path: &change.path,
                    bytes: text.as_bytes(),
                    media_type: media_type(&change.path),
                    display_name: change.path.rsplit('/').next().unwrap_or(&change.path),
                }),
                (None, None) => Err(Error::Record("file deletion requires an original source")),
            })
            .collect::<Result<_, Error>>()?;
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

async fn file_heads<P: acyclic_stream::StreamProvider + ?Sized>(
    provider: &P,
    paths: std::collections::BTreeSet<acyclic_stream::StreamPath>,
    minimum: acyclic_stream::RevisionCut,
) -> Result<
    (
        BTreeMap<acyclic_stream::StreamPath, u64>,
        acyclic_stream::RevisionCut,
    ),
    Error,
> {
    if paths.is_empty() {
        return Ok((BTreeMap::new(), minimum));
    }
    let request = acyclic_stream::HeadsRequest {
        limit: u32::try_from(paths.len())
            .map_err(|_| Error::Record("file source selection overflow"))?,
        paths: paths.into_iter().collect(),
        retained: Default::default(),
        durable_through: None,
        projection: acyclic_stream::HeadProjection::Tail,
        children: None,
        at_least: minimum,
    };
    let heads = provider
        .heads(request.clone())
        .await
        .map_err(Error::Stream)?;
    heads.validate_for(&request).map_err(Error::Stream)?;
    Ok((
        request
            .paths
            .into_iter()
            .zip(heads.heads)
            .map(|(path, head)| (path, head.map_or(0, |head| head.tail)))
            .collect(),
        acyclic_stream::RevisionCut::of(heads.snapshot),
    ))
}

fn media_type(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, extension)| extension) {
        Some("json") => "application/json",
        Some("md") => "text/markdown",
        _ => "text/plain",
    }
}

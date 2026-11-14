//! Session integration over the admitted volumes, independent of OS projection.
use super::*;
use crate::agent::{WorkspaceRole, WorkspaceTable};
use acyclic_fs::{GitCompatRepository, StreamGitCompatStore};

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> Bound<P, O> {
    /// Join a fork's project into its parent's selected project with the shared
    /// Git facade, including durable request receipts and conflict resolution.
    /// Both agents retain their original admitted volume authority.
    pub async fn merge_session(
        &self,
        agent: &AgentPath,
        parent: &AgentPath,
        operation: OperationId,
    ) -> crate::Result<Option<String>> {
        let store = StreamGitCompatStore::new(StreamAuthorityStore::new(
            self.volumes.filesystem.stream_provider(),
        ));
        self.merge_session_with_store(agent, parent, operation, store)
            .await
    }

    /// The same join using the host's existing repository and lineage store.
    /// Native adapters retain their checkout's durable Git state.
    pub async fn merge_session_with_store<S: crate::tools::git::CommandStore>(
        &self,
        agent: &AgentPath,
        parent: &AgentPath,
        operation: OperationId,
        store: S,
    ) -> crate::Result<Option<String>> {
        if agent == parent || agent.parent().is_some_and(|path| &path != parent) {
            return Err(crate::Error::Invalid(
                "merge selects a session fork or direct child".into(),
            ));
        }
        self.admit_path(agent).map_err(turn_error)?;
        self.admit_path(parent).map_err(turn_error)?;
        let child = self.agents.state(agent).await.map_err(turn_error)?;
        if child.closed {
            return Err(crate::Error::Refused(
                "archived fork cannot be merged".into(),
            ));
        }
        let parent_state = self.agents.state(parent).await.map_err(turn_error)?;
        let project = |state: &crate::agent::AgentState| {
            WorkspaceTable::primary_in(&state.volumes)
                .filter(|binding| binding.role == WorkspaceRole::User)
                .ok_or_else(|| crate::Error::Invalid("session has no project binding".into()))
                .and_then(|binding| binding.whole_volume().cloned())
        };
        let source_volume = project(&child)?;
        let destination = project(&parent_state)?;
        let (child_scope, child_verifier) = self.volume_scope(agent).await.map_err(turn_error)?;
        let (scope, verifier) = self.volume_scope(parent).await.map_err(turn_error)?;
        let open = |volume: &VolumeRef| {
            super::super::bounded_io::content_workspace(&self.volumes.provider, volume, None)
        };
        let source = self.volumes.open(&open(&source_volume)?, None).await?;
        let root = self.volumes.open(&open(&destination)?, None).await?;
        super::super::WorkspaceAccess::new(
            &child_verifier,
            &child_scope,
            child_scope.capabilities(),
            Some(&source_volume),
        )?
        .require(&source)
        .await?;
        let access = super::super::WorkspaceAccess::new(
            &verifier,
            &scope,
            scope.capabilities(),
            Some(&destination),
        )?;
        access.require(&root).await?;
        let repository = GitCompatRepository::new(root.id(), store.clone());
        let (selected, pending) = repository
            .checkout_selection()
            .await
            .map_err(crate::tools::git::compat_error::<S>)?;
        if pending.is_some() {
            return Err(crate::Error::Conflict(
                "parent checkout reconciliation is pending".into(),
            ));
        }
        let graph = acyclic_fs::WorkspaceGraph::new(store.clone());
        // Hosted forks already exist atomically in the filesystem. Authenticate
        // their original creation before exposing them as Git branch sources.
        if acyclic_fs::WorkspaceLineageStore::load(&store, source.id())
            .await
            .map_err(S::error)?
            .is_none()
        {
            let binding = WorkspaceTable::primary_in(&child.volumes)
                .ok_or_else(|| crate::Error::Invalid("session has no project binding".into()))?;
            let cancellation = CancellationToken::new();
            let initial = self
                .volumes
                .filesystem
                .workspace_for_creation_operation(
                    source.name().as_str(),
                    source.volume_config(),
                    super::super::spawn_volumes::spawned_operation(agent, binding.id),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map_err(|failure| crate::Error::from(failure.error))?
                .value
                .ok_or_else(|| {
                    crate::Error::Invalid("session has no original volume fork".into())
                })?;
            let (parents, _allocation) = initial
                .parents(WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(|failure| crate::Error::from(failure.error))?
                .value;
            let [fork_generation] = parents.as_slice() else {
                return Err(crate::Error::Invalid(
                    "session volume is not a direct fork".into(),
                ));
            };
            graph
                .register_existing_child_at_generation(
                    &root,
                    &source,
                    *fork_generation,
                    initial.id(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map_err(|failure| failure.error)
                .map_err(S::lineage_error)?;
        }
        let selected = if selected == root.id() {
            root.clone()
        } else {
            graph
                .authorize_descendant(
                    selected,
                    root.id(),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| failure.error)
                .map_err(S::lineage_error)?;
            acyclic_fs::WorkspaceResolver::resolve(
                &(self.volumes.filesystem.clone(), graph),
                selected,
            )
            .await
            .map_err(S::lineage_error)?
        };
        let branch = format!("agents/{}", agent.id());
        repository
            .register_branch_workspace(branch.clone(), source.id(), None, false)
            .await
            .map_err(crate::tools::git::compat_error::<S>)?;
        let root_cut = if selected.id() != root.id() {
            Some(
                root.capture_policy_cut(WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(|failure| crate::Error::from(failure.error))?
                    .value,
            )
        } else {
            None
        };
        let output = crate::tools::git::run_volume_with_guard(
            crate::tools::git::Workspace {
                access: &access,
                filesystem: &self.volumes.filesystem,
                store: &store,
                view: crate::tools::git::View::Volume(&selected),
                root: std::path::Path::new("/"),
                repository: root.id(),
                lease: None,
                #[cfg(not(target_arch = "wasm32"))]
                checkout: None,
                author: "agent",
                authored_at_seconds: i64::try_from(self.clock.now_unix_ms() / 1000)
                    .unwrap_or(i64::MAX),
                request: Some(acyclic_fs::GitRequest {
                    merge_source: None,
                    operation: acyclic_fs::OperationId::from_bytes(operation.into_bytes()),
                    unanswered: Arc::new(|_| true),
                }),
            },
            root_cut,
            &["merge".to_owned(), branch],
            || access.require(&root),
        )
        .await?;
        match output.command {
            Some(acyclic_fs::GitCommandOutput::Committed(commit)) => Ok(Some(commit.id.to_hex())),
            Some(
                acyclic_fs::GitCommandOutput::NoOp | acyclic_fs::GitCommandOutput::Filesystem(_),
            ) => Ok(None),
            _ => Err(crate::Error::Conflict(
                "merge has no committed result".into(),
            )),
        }
    }
}

fn turn_error(error: Error) -> crate::Error {
    crate::Error::Storage(error.to_string())
}

use super::*;
use crate::filesystem::FilesystemContentVerifier;

impl LocalHarness {
    /// An authorized fork uses the tools' shared Git facade and request receipt.
    /// Host checkout merges retain native capture and publication; retained
    /// unbound parent volumes need no OS projection or additional authority.
    #[cfg(any(test, feature = "native-execution"))]
    pub(super) async fn merge_session(
        &self,
        agent: &crate::agent::AgentPath,
        operation: OperationId,
    ) -> Result<Option<String>> {
        let workspace = self
            .workspace
            .as_ref()
            .ok_or_else(|| Error::Unsupported("session integration requires a checkout".into()))?;
        let hosted = self
            .agents
            .get()
            .ok_or_else(|| Error::Unsupported("local agents are not hosted".into()))?;
        if agent == hosted.root() {
            return Err(Error::Invalid(
                "merge selects a session fork or child".into(),
            ));
        }
        let state = hosted.agents().state(agent).await.map_err(storage)?;
        if state.closed {
            return Err(Error::Refused("archived fork cannot be merged".into()));
        }
        let project = state
            .volumes
            .iter()
            .find(|binding| binding.name == "project")
            .ok_or_else(|| Error::Invalid("session has no project binding".into()))?;
        let source = workspace.spawned_project_view(&project.volume).await?;
        let branch = format!("agents/{}", agent.id());
        if let Some(parent) = agent.parent().filter(|parent| parent != hosted.root()) {
            use super::workspace::owner::ViewOwner as _;
            let parent_state = hosted.agents().state(&parent).await.map_err(storage)?;
            let project = parent_state
                .volumes
                .iter()
                .find(|binding| binding.name == "project")
                .ok_or_else(|| Error::Invalid("parent has no project binding".into()))?;
            let (signed, verifier) = hosted
                .server()
                .bound()
                .volume_scope(&parent)
                .await
                .map_err(storage)?;
            let authority = crate::filesystem::NativeMountAuthority::admitted(
                verifier.clone(),
                signed.copy(&mut None)?,
            );
            let view = workspace.spawned_project_view(&project.volume).await?;
            if workspace.has_physical_task_source(&view)? {
                let bound = self
                    .project_workspace(
                        workspace,
                        &parent,
                        &project.volume,
                        &parent_state.volumes,
                        &signed,
                        &verifier,
                    )
                    .await?;
                return self
                    .merge_checkout(
                        &bound,
                        &signed,
                        parent,
                        operation,
                        (branch, source.workspace().id()),
                    )
                    .await;
            }
            // A volume join needs no OS projection. Keep the same selected
            // checkout and original authority without minting a native grant.
            let logical = workspace.context_task_view(view, &project.volume, &authority)?;
            let (selected, pending) = logical.selected_view().await?;
            if pending.is_some() {
                return Err(Error::Conflict(
                    "parent checkout reconciliation is pending".into(),
                ));
            }
            let destination = selected.as_ref().unwrap_or_else(|| logical.view());
            let repository = logical.view().workspace().id();
            let access = crate::filesystem::WorkspaceAccess::new(
                &verifier,
                &signed,
                signed.capabilities(),
                Some(&project.volume),
            )?;
            access.require(destination.workspace()).await?;
            acyclic_fs::GitCompatRepository::new(repository, workspace.store.clone())
                .register_branch_workspace(branch.clone(), source.workspace().id(), None, false)
                .await
                .map_err(storage)?;
            let arguments = ["merge".to_owned(), branch];
            let owner = super::workspace::owner::AgentOwner(parent);
            let command: acyclic_stream::BoxProviderFuture<'_, _> =
                Box::pin(crate::tools::git::run_with_guard(
                    crate::tools::git::Workspace {
                        access: &access,
                        filesystem: &workspace.filesystem,
                        store: &workspace.store,
                        view: destination.into(),
                        root: &workspace.path,
                        repository,
                        lease: None,
                        checkout: None,
                        reserved: &[],
                        author: "LocalHarness",
                        authored_at_seconds: i64::try_from(self.clock.now_unix_millis() / 1000)
                            .unwrap_or(i64::MAX),
                        request: Some(acyclic_fs::GitRequest {
                            operation: acyclic_fs::OperationId::from_bytes(operation.into_bytes()),
                            unanswered: Arc::new(|_| true),
                        }),
                    },
                    &arguments,
                    || owner.verify(),
                ));
            let output = command
                .await?
                .command
                .ok_or_else(|| Error::Conflict("merge has no committed result".into()))?;
            return merge_commit(output);
        }
        let signed = self.signed.copy(&mut None)?;
        self.merge_checkout(
            workspace,
            &signed,
            agent.parent().unwrap_or_else(|| agent.clone()),
            operation,
            (branch, source.workspace().id()),
        )
        .await
    }

    #[cfg(any(test, feature = "native-execution"))]
    async fn merge_checkout(
        &self,
        workspace: &Arc<super::workspace::LocalWorkspace>,
        signed: &crate::core::Scope,
        owner: crate::agent::AgentPath,
        operation: OperationId,
        (branch, source): (String, acyclic_fs::WorkspaceId),
    ) -> Result<Option<String>> {
        let scope =
            crate::runtime::RuntimeScope::new(signed.capabilities().clone(), Default::default())?;
        let owner = super::workspace::owner::AgentOwner(owner);
        let command = format!("merge {branch}");
        let configuration = super::checkout_tools::capture::configuration(workspace)?;
        let commands = super::workspace::git::Commands {
            workspace,
            signed,
            clock: self.clock.as_ref(),
            capture_options: &configuration.0,
            capture_policy: &configuration.1,
        };
        let output = commands
            .invoke(
                &command,
                super::workspace::git::CommandContext {
                    owner: &owner,
                    operation,
                    scope: &scope,
                    request: Some(acyclic_fs::GitRequest {
                        operation: acyclic_fs::OperationId::from_bytes(operation.into_bytes()),
                        unanswered: Arc::new(|_| true),
                    }),
                },
                Box::pin(async {
                    acyclic_fs::GitCompatRepository::new(
                        workspace.view().workspace().id(),
                        workspace.store.clone(),
                    )
                    .register_branch_workspace(branch, source, None, false)
                    .await
                    .map_err(storage)
                }),
            )
            .await?;
        merge_commit(output)
    }

    /// Authority selecting this local agent's followed document and task files.
    pub fn client_owner(&self) -> crate::wire::Authority {
        crate::wire_codec::encode_authority(self.verifier.audience())
    }
    /// Authenticated reader shared by file-backed client input and documents.
    pub fn client_reader(&self) -> Result<Arc<dyn crate::conversation::ContentResidencyVerifier>> {
        Ok(self.signed_reader()?)
    }

    fn signed_reader(
        &self,
    ) -> Result<Arc<FilesystemContentVerifier<LocalAuthorityBackend, LocalObjectBackend>>> {
        FilesystemContentVerifier::new(
            Arc::clone(&self.files),
            &self.verifier,
            &self.signed,
            self.limits.file_bytes,
            None,
        )
    }

    /// One page of the selected checkout's changes since its first complete
    /// import, the fork base every agent edit is measured from. Without that
    /// complete baseline the query refuses to certify a diff. Projection, disclosure
    /// authority and paging are the shared [`crate::hosting::DiffPages`],
    /// which retains this checkout's projection in `diffs`.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    pub(super) async fn project_diff(
        &self,
        diffs: &crate::hosting::DiffPages,
        page: &acyclic_harness_client::AgentPageRequest,
    ) -> Result<Vec<crate::context::DiffFilePage>> {
        let workspace = self
            .workspace
            .as_ref()
            .ok_or_else(|| Error::Unsupported("project diff requires a checkout".into()))?;
        let (selected, pending) = workspace.selected_view().await?;
        if let Some((operation, _)) = pending {
            return Err(Error::Indeterminate(OperationId::from_bytes(
                operation.into_bytes(),
            )));
        }
        let selected = selected
            .as_ref()
            .unwrap_or_else(|| workspace.view())
            .workspace();
        let cancellation = acyclic_fs::CancellationToken::new();
        // Branches retain the checkout's imported source, not a new import
        // receipt. Selection changes only the current endpoint.
        let original = workspace.view().workspace();
        let base = original
            .operation_generation(
                acyclic_fs::IdempotencyKey::from_bytes(
                    super::checkout_tools::capture::baseline_operation(original.id()).into_bytes(),
                ),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value
            .ok_or_else(|| {
                Error::Unsupported("workspace diff requires a complete source baseline".into())
            })?
            .id();
        let current = selected
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value
            .id();
        diffs
            .page(
                &*self.signed_reader()?,
                &self.project,
                (
                    &self.files.generation_ref_id(base, None)?,
                    &self.files.generation_ref_id(current, None)?,
                ),
                page,
            )
            .await
    }

    /// Publisher of this harness's prompt inputs on its private journal volume.
    pub(super) fn input_publisher_with_limit(
        &self,
        maximum_bytes: u64,
    ) -> Result<Arc<FilesystemContentPublisher<LocalAuthorityBackend, LocalObjectBackend>>> {
        FilesystemContentPublisher::new(
            self.files.clone(),
            &self.journal,
            &self.verifier,
            &self.signed,
            maximum_bytes,
            None,
        )
    }
}

#[cfg(any(test, feature = "native-execution"))]
fn merge_commit(output: acyclic_fs::GitCommandOutput) -> Result<Option<String>> {
    match output {
        acyclic_fs::GitCommandOutput::Committed(commit) => Ok(Some(commit.id.to_hex())),
        acyclic_fs::GitCommandOutput::NoOp | acyclic_fs::GitCommandOutput::Filesystem(_) => {
            Ok(None)
        }
        _ => Err(Error::Invalid(
            "merge returned another git-facade result".into(),
        )),
    }
}

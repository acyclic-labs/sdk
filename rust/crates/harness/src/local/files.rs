use super::*;
use crate::filesystem::FilesystemContentVerifier;

impl LocalHarness {
    /// A directory-authorized integration uses the same native capture,
    /// operation receipt, recovery and conflict files as ordinary `fs merge`.
    /// This binds the source's lazy cut; it creates no physical task view.
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
        if agent.parent().is_some() || agent == hosted.root() {
            return Err(Error::Invalid(
                "merge selects an independent session fork".into(),
            ));
        }
        let state = hosted.agents().state(agent).await.map_err(storage)?;
        let project = state
            .volumes
            .iter()
            .find(|binding| binding.name == "project")
            .ok_or_else(|| Error::Invalid("session has no project binding".into()))?;
        let source = workspace.spawned_project_view(&project.volume).await?;
        let branch = format!("agents/session-{}", agent.id());
        let command = format!("merge {branch}");
        let configuration = super::checkout_tools::capture::configuration(workspace)?;
        let commands = super::workspace::git::Commands {
            workspace,
            signed: &self.signed,
            clock: self.clock.as_ref(),
            capture_options: &configuration.0,
            capture_policy: &configuration.1,
        };
        let owner = super::workspace::owner::AgentOwner(agent.clone());
        let scope = crate::runtime::RuntimeScope::new(
            self.signed.capabilities().clone(),
            Default::default(),
        )?;
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
                async {
                    acyclic_fs::GitCompatRepository::new(
                        workspace.view().workspace().id(),
                        workspace.store.clone(),
                    )
                    .register_branch_workspace(branch, source.workspace().id(), None, false)
                    .await
                    .map_err(storage)
                },
            )
            .await?;
        match output {
            acyclic_fs::GitCommandOutput::Committed(commit) => Ok(Some(commit.id.to_string())),
            acyclic_fs::GitCommandOutput::NoOp | acyclic_fs::GitCommandOutput::Filesystem(_) => {
                Ok(None)
            }
            _ => Err(Error::Invalid(
                "merge returned another git-facade result".into(),
            )),
        }
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
    #[cfg(any(test, feature = "native-execution"))]
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

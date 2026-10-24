//! Read-only search selects durable views before any physical checkout setup.
use super::*;
use acyclic_fs::access::Action;

pub(super) fn command(invocation: &ToolInvocation) -> Option<crate::tools::FsCommand> {
    if invocation.name != "fs" {
        return None;
    }
    let command = invocation.arguments.get("command")?.as_str()?;
    crate::tools::parse_fs_command(command)
        .ok()
        .filter(|parsed| parsed.arguments[0] == "grep")
}

impl CheckoutTools {
    pub(super) async fn search(
        &self,
        parsed: crate::tools::FsCommand,
        context: &ToolContext,
        request: Vec<u8>,
    ) -> Result<ToolResult> {
        let owner = context
            .task()
            .owned_mailbox
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("search has no task owner".into()))?;
        owner.verify_owner(None)?.await?;
        // Created allocations are authenticated by their task seal, then
        // composed without attaching a native projection.
        if context.task().scope().created_access.is_some() {
            let access = Self::child_access(context)?;
            let mut projects = access
                .content
                .iter()
                .filter(|binding| binding.volume.class() == VolumeClass::Project);
            let project = &projects
                .next()
                .ok_or_else(|| {
                    Error::Unauthorized("child search project binding is absent".into())
                })?
                .volume;
            if projects.next().is_some() {
                return Err(Error::Unauthorized(
                    "child search project binding is ambiguous".into(),
                ));
            }
            let mut private = None;
            for binding in &access.content {
                if binding.volume.class() == VolumeClass::AgentPrivate
                    && context.effective_scope().grants().contains(
                        &binding
                            .volume
                            .capability(VolumeOperation::Read, &mut None)?,
                    )
                    && private.replace(binding.volume.clone()).is_some()
                {
                    return Err(Error::Unauthorized(
                        "child search private binding is ambiguous".into(),
                    ));
                }
            }
            let mounts = workspace::mounts::owner(
                &self.workspace.path,
                project,
                (),
                private.map(|volume| ("scratch".into(), volume, ())),
            )?;
            let (prefix, mount) = mounts.named(parsed.volume.as_deref())?;
            self.authorize_search(context, mount)?;
            let cancellation = acyclic_fs::CancellationToken::new();
            let volume = self
                .workspace
                .filesystem
                .open_workspace(
                    mount.view.volume.storage_name(&mut None)?,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map_err(storage)?
                .value;
            let lazy = if prefix.is_empty() {
                Some(self.workspace.existing_volume_view(volume.id()).await?)
            } else {
                None
            };
            let view = lazy.as_ref().map_or(
                crate::tools::git::View::Volume(&volume),
                crate::tools::git::View::Checkout,
            );
            let files = Arc::new(
                self.task_files
                    .for_created_task(context.task(), context.effective_scope())
                    .await?
                    .ok_or_else(|| Error::Unauthorized("child search files are absent".into()))?,
            );
            return self
                .search_view(&parsed, context, request, view, mount, files)
                .await;
        }
        let (prefix, mount) = self.workspace.mounts.named(parsed.volume.as_deref())?;
        let selected;
        let view = if prefix.is_empty() {
            selected = self.search_task_view(context).await?;
            crate::tools::git::View::Checkout(
                selected.as_ref().unwrap_or_else(|| self.workspace.view()),
            )
        } else {
            mount
                .view
                .source
                .as_ref()
                .ok_or_else(|| Error::NotFound("search volume is not bound".into()))?
                .workspace
                .view()
        };
        self.search_view(
            &parsed,
            context,
            request,
            view,
            mount,
            Arc::clone(&self.task_files),
        )
        .await
    }

    async fn search_task_view(&self, context: &ToolContext) -> Result<Option<workspace::View>> {
        let (task, _) = context.task().owned_task_binding(None)?;
        let ancestors = context.task().ancestors(task)?;
        futures::pin_mut!(ancestors);
        while let Some(task) = futures::TryStreamExt::try_next(&mut ancestors).await? {
            if let Some(view) = self.workspace.existing_task_view(task).await? {
                return Ok(Some(view));
            }
        }
        self.workspace.selected_view().await.map(|(view, _)| view)
    }

    async fn search_view<T: Sync>(
        &self,
        parsed: &crate::tools::FsCommand,
        context: &ToolContext,
        request: Vec<u8>,
        view: crate::tools::git::View<'_>,
        mount: &crate::workspace::Mount<T>,
        files: Arc<crate::task_volume::TaskFiles<LocalStream>>,
    ) -> Result<ToolResult> {
        self.authorize_search(context, mount)?;
        let owner = context
            .task()
            .owned_mailbox
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("search has no task owner".into()))?;
        let read_allowed = |path: &str| {
            mount
                .check(Action::Read, path, self.clock.now_unix_millis() / 1000)
                .is_ok()
        };
        let progress =
            output::SearchOutput::new(output::Output::open(context, files, request).await?);
        let reserved = if parsed.volume.is_none() {
            self.workspace.volume_owned_paths()?
        } else {
            Vec::new()
        };
        let output = crate::tools::git::run_with_guard(
            crate::tools::git::Workspace {
                filesystem: &self.workspace.filesystem,
                store: &self.workspace.store,
                view,
                root: &self.workspace.path,
                repository: view.workspace().id(),
                lease: None,
                checkout: None,
                reserved: &reserved,
                author: "LocalHarness",
                authored_at_seconds: i64::try_from(self.clock.now_unix_millis() / 1000)
                    .unwrap_or(i64::MAX),
                read_allowed: &read_allowed,
                search_progress: Some(&progress),
            },
            &parsed.arguments,
            || async { owner.verify_owner(None)?.await },
        )
        .await;
        let result = match output {
            Ok(output) => Ok(ToolResult {
                value: serde_json::json!({"result": output.command}),
            }),
            Err(
                error @ (Error::Conflict(_)
                | Error::Invalid(_)
                | Error::NotFound(_)
                | Error::Unsupported(_)),
            ) => Ok(crate::tools::rejected_input(error)),
            Err(error) => Err(error),
        }?;
        progress.finish(&result).await?;
        Ok(result)
    }

    fn authorize_search<T>(
        &self,
        context: &ToolContext,
        mount: &crate::workspace::Mount<T>,
    ) -> Result<()> {
        mount.check(Action::Read, "", self.clock.now_unix_millis() / 1000)?;
        if !context.effective_scope().grants().contains(
            &mount
                .view
                .volume
                .capability(VolumeOperation::Read, &mut None)?,
        ) {
            return Err(Error::Unauthorized(
                "search target volume is not granted".into(),
            ));
        }
        Ok(())
    }
}

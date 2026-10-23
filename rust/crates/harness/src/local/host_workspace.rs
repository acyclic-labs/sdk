//! Workspace coordination for hosts that own their agents and tool execution.
//!
//! This facade opens no model provider, executor, service or process. Task
//! contexts come from the caller's existing shared harness task runtime.
use super::*;
use crate::runtime::{OwnedTaskMailbox, TaskContext};
use acyclic_fs::facade::FsWork as _;
use acyclic_fs::{HeldRoot, LazyWorkspaceStore as _};

mod volume_guard;

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

/// Reserved mount namespace within an already writable checkout.
pub const HOST_VIEW_ROOT: &str = ".volumes/agents";

/// An admitted task's workspace, with no execution authority of its own.
pub struct HostWorkspace {
    workspace: Arc<LocalWorkspace>,
    task: TaskId,
    mount_root: Arc<HeldRoot>,
    project: VolumeRef,
    _native_volumes: Vec<workspace::projection::Entered>,
    _allocation: Option<Arc<workspace::Mounts>>,
}

impl HostWorkspace {
    /// Attaches to the existing shared workspace and its serialization guard.
    /// The host admits the checkout before entering its sandbox; this API
    /// cannot add a state directory or another writable sandbox root.
    pub async fn attach(
        workspace: Arc<LocalWorkspace>,
        context: &TaskContext,
        admitted_mount_root: HeldRoot,
    ) -> Result<Self> {
        let owner = original_owner(context)?;
        owner.verify_owner(None)?.await?;
        if owner.admission().0.parent.is_some() {
            return Err(Error::Unauthorized("host root task has a parent".into()));
        }
        let project = workspace.mounts.named(None)?.1.view.volume.clone();
        if project.class() != VolumeClass::Project {
            return Err(Error::Invalid("host project volume class differs".into()));
        }
        let canonical_root = std::fs::canonicalize(&workspace.path).map_err(storage)?;
        let mounts = std::fs::canonicalize(admitted_mount_root.path()).map_err(storage)?;
        if mounts != canonical_root.join(HOST_VIEW_ROOT)
            || workspace.mount_directory != canonical_root.join(HOST_VIEW_ROOT)
        {
            return Err(Error::Invalid(
                "host mounts require the checkout's .volumes/agents root".into(),
            ));
        }
        verify_root(&admitted_mount_root)?;
        workspace::volumes::require_available(acyclic_fs::probe_native_mount())?;
        acyclic_fs::initialize_native_mount_services().map_err(storage)?;
        let mut result = Self {
            workspace,
            task: owner.task_binding().0,
            mount_root: Arc::new(admitted_mount_root),
            project,
            _native_volumes: Vec::new(),
            _allocation: None,
        };
        result.verify(context).await?;
        result.activate_volumes(context).await?;
        Ok(result)
    }
    /// Cwd to give the native host before it starts executing this task.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.workspace.path
    }

    /// Shared task identity; no separate plugin task index is created.
    #[must_use]
    pub const fn task(&self) -> TaskId {
        self.task
    }

    /// Identity of the selected authored cut, for binding native spawn events
    /// that do not carry their parent tool-call identity.
    pub async fn fork_point(&self, context: &TaskContext) -> Result<String> {
        let owner = self.verify(context).await?;
        let _state =
            crate::sync::owned::lock_owned(workspace::StateArc::clone(&self.workspace.state)).await;
        let (selected, pending) = self.workspace.selected_view().await?;
        if pending.is_some() {
            return Err(Error::Conflict(
                "workspace reconciliation is pending".into(),
            ));
        }
        let view = selected.as_ref().unwrap_or_else(|| self.workspace.view());
        let cancellation = acyclic_fs::CancellationToken::new();
        let cut = view
            .snapshot(&mut acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation))
            .await
            .map_err(storage)?;
        owner.verify_owner(None)?.await?;
        serde_json::to_string(&cut.authored_generation.digest()).map_err(storage)
    }

    /// Registers the native host's child identity in the same git facade as
    /// `GraphCoder`. Only an authenticated direct child may receive this alias.
    pub async fn name_child(
        &self,
        parent: &TaskContext,
        child: &TaskContext,
        child_view: &Self,
        reference: &str,
    ) -> Result<()> {
        let owner = self.verify(parent).await?;
        let child_owner = child_view.verify(child).await?;
        if child_owner.admission().0.parent != Some(self.task) {
            return Err(Error::Unauthorized(
                "native alias names another parent's child".into(),
            ));
        }
        crate::task_volume::Component::new(reference).map_err(storage)?;
        let child_selected = child_view.workspace.selected_view().await?.0;
        let child_selected = child_selected
            .as_ref()
            .unwrap_or_else(|| child_view.workspace.view());
        let _state =
            crate::sync::owned::lock_owned(workspace::StateArc::clone(&self.workspace.state)).await;
        let selected = self.workspace.selected_view().await?.0;
        let selected = selected.as_ref().unwrap_or_else(|| self.workspace.view());
        acyclic_fs::GitCompatRepository::new(
            selected.workspace().id(),
            self.workspace.store.clone(),
        )
        .register_branch_workspace(
            format!("agents/{reference}"),
            child_selected.workspace().id(),
            None,
            false,
        )
        .await
        .map_err(storage)?;
        owner.verify_owner(None)?.await?;
        child_owner.verify_owner(None)?.await
    }

    /// Records completed native-host writes in the selected shared view.
    /// The caller must first quiesce native writes for this task. This method
    /// neither executes a tool nor grants access to another task's files.
    pub async fn capture(&self, context: &TaskContext, operation: OperationId) -> Result<()> {
        use super::checkout_tools::{capture, reconciliation};
        let owner = self.verify(context).await?;
        verify_root(&self.mount_root)?;
        let mut state =
            crate::sync::owned::lock_owned(workspace::StateArc::clone(&self.workspace.state)).await;
        let (selected, pending) = self.workspace.selected_view().await?;
        let view = selected.as_ref().unwrap_or_else(|| self.workspace.view());
        let verify = || async { owner.verify_owner(None)?.await };
        reconciliation::recover_pending(&self.workspace, view, pending, &verify).await?;
        state.active = selected;
        let physical = self
            .workspace
            .selected_workspace(&mut state, owner.as_ref())
            .await?;
        self.capture_volumes(context).await?;
        if let Some(mount) = &physical.mounted {
            let owner = Arc::clone(owner);
            let root = Arc::clone(&self.mount_root);
            mount
                .sync_with_permit(
                    acyclic_fs::PublicationPermit::Unrestricted,
                    &mount.native_work()?,
                    move |_| {
                        let owner = Arc::clone(&owner);
                        let root = Arc::clone(&root);
                        async move {
                            owner.verify_owner(None)?.await?;
                            verify_root(&root)
                        }
                    },
                )
                .await
                .map_err(|failure| failure.cause)?;
        } else {
            let configuration = capture::configuration(&physical)?;
            let super::checkout_tools::State {
                active, watcher, ..
            } = &mut *state;
            let selected = active.as_ref().unwrap_or_else(|| self.workspace.view());
            capture::finish_owed_write(
                (&configuration.0, &configuration.1),
                (&physical, selected),
                watcher,
                acyclic_fs::PublicationPermit::Unrestricted,
                &verify,
            )
            .await?;
            capture::record_changes(
                (&configuration.0, &configuration.1),
                (&physical, selected),
                watcher,
                (operation, 1),
                acyclic_fs::PublicationPermit::Unrestricted,
                &verify,
            )
            .await?;
        }
        self.verify(context).await?;
        verify_root(&self.mount_root)
    }

    /// Runs the shared filesystem git facade with this task's authority.
    /// Native hosts retain their own shell and tool execution.
    pub async fn fs(
        &self,
        context: &TaskContext,
        operation: OperationId,
        command: &str,
    ) -> Result<acyclic_fs::GitCommandOutput> {
        let owner = self.verify(context).await?;
        verify_root(&self.mount_root)?;
        let configuration = super::checkout_tools::capture::configuration(&self.workspace)?;
        let commands = workspace::git::Commands {
            workspace: &self.workspace,
            clock: &acyclic_stream::SystemUnixMillisClock,
            capture_options: &configuration.0,
            capture_policy: &configuration.1,
        };
        let parsed = commands.fs_arguments(command, context.scope())?;
        let mut state =
            crate::sync::owned::lock_owned(workspace::StateArc::clone(&self.workspace.state)).await;
        let (selected, pending) = self.workspace.selected_view().await?;
        super::checkout_tools::reconciliation::recover_pending(
            &self.workspace,
            selected.as_ref().unwrap_or_else(|| self.workspace.view()),
            pending,
            &|| async { owner.verify_owner(None)?.await },
        )
        .await?;
        state.active = selected;
        let physical = self
            .workspace
            .selected_workspace(&mut state, owner.as_ref())
            .await?;
        self.capture_volumes(context).await?;
        let result = commands
            .fs(
                parsed,
                &workspace::git::CommandContext {
                    task: context,
                    operation,
                    scope: context.scope(),
                },
                &mut state,
                &physical,
            )
            .await?;
        self.verify(context).await?;
        verify_root(&self.mount_root)?;
        match result {
            workspace::git::Outcome::Completed(output) => Ok(*output),
            workspace::git::Outcome::Rejected(error) => Err(*error),
        }
    }

    /// Idempotently forks an authenticated direct child from the selected view.
    /// Capture the parent's host-authored work before calling this boundary.
    pub async fn fork(&self, parent: &TaskContext, child: &TaskContext) -> Result<Self> {
        self.open_child(parent, child, false).await
    }

    /// Reopens an existing fork; a missing fork is never silently created.
    pub async fn reopen(&self, parent: &TaskContext, child: &TaskContext) -> Result<Self> {
        self.open_child(parent, child, true).await
    }

    async fn open_child(
        &self,
        parent: &TaskContext,
        child: &TaskContext,
        existing: bool,
    ) -> Result<Self> {
        let parent_owner = self.verify(parent).await?;
        let child_owner = original_owner(child)?;
        child_owner.verify_owner(None)?.await?;
        let task = child_owner.task_binding().0;
        if child_owner.admission().0.parent != Some(self.task) {
            return Err(Error::Unauthorized(
                "host child belongs to another parent".into(),
            ));
        }
        verify_root(&self.mount_root)?;
        let mut state =
            crate::sync::owned::lock_owned(workspace::StateArc::clone(&self.workspace.state)).await;
        let project = workspace::created_project(child, child.scope())?;
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            if !child
                .scope()
                .grants()
                .contains(&project.capability(operation, &mut None)?)
            {
                return Err(Error::Unauthorized(
                    "host child project is not granted".into(),
                ));
            }
        }
        if existing {
            self.require_retained_fork(project).await?;
        }
        let (selected, pending) = self.workspace.selected_view().await?;
        let selected = selected.as_ref().unwrap_or_else(|| self.workspace.view());
        super::checkout_tools::reconciliation::recover_pending(
            &self.workspace,
            selected,
            pending,
            &|| async { parent_owner.verify_owner(None)?.await },
        )
        .await?;
        let view = self.workspace.project_view(project).await?;
        let key = project.storage_name(&mut None)?;
        let mounts = if let Some(mounts) = state
            .allocated_mounts
            .get(&key)
            .and_then(std::sync::Weak::upgrade)
        {
            mounts
        } else {
            state
                .allocated_mounts
                .retain(|_, mounts| mounts.strong_count() != 0);
            let mounts = self
                .workspace
                .created_mounts(project, child, child.scope())
                .await?;
            state.allocated_mounts.insert(key, Arc::downgrade(&mounts));
            mounts
        };
        parent_owner.verify_owner(None)?.await?;
        acyclic_fs::GitCompatRepository::new(
            selected.workspace().id(),
            self.workspace.store.clone(),
        )
        .register_branch_workspace(format!("agents/{task}"), view.workspace().id(), None, false)
        .await
        .map_err(storage)?;
        let workspace = self
            .workspace
            .mount_task_view(
                view,
                child_owner,
                Some(Arc::clone(&mounts)),
                self.mount_root.path(),
            )
            .await?;
        parent_owner.verify_owner(None)?.await?;
        child_owner.verify_owner(None)?.await?;
        verify_root(&self.mount_root)?;
        let mut result = Self {
            workspace,
            task,
            mount_root: Arc::clone(&self.mount_root),
            project: project.clone(),
            _native_volumes: Vec::new(),
            _allocation: Some(mounts),
        };
        result.verify(child).await?;
        result.activate_volumes(child).await?;
        Ok(result)
    }

    async fn require_retained_fork(&self, project: &VolumeRef) -> Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let name = project.storage_name(&mut None)?;
        let id = self
            .workspace
            .filesystem
            .workspace_id(&name)
            .map_err(storage)?;
        if self
            .workspace
            .store
            .load_lazy_workspace(
                id,
                &mut acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation),
            )
            .await
            .map_err(storage)?
            .is_none()
        {
            return Err(Error::NotFound(
                "host child workspace has no retained fork".into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn verify<'a>(
        &self,
        context: &'a TaskContext,
    ) -> Result<&'a Arc<dyn OwnedTaskMailbox>> {
        let owner = original_owner(context)?;
        if owner.task_binding().0 != self.task {
            return Err(Error::Unauthorized(
                "host workspace belongs to another task".into(),
            ));
        }
        owner.verify_owner(None)?.await?;
        for (volume, operation) in std::iter::once(&self.project)
            .chain(
                self.workspace
                    .mounts
                    .mounts()
                    .filter(|(relative, _)| !relative.is_empty())
                    .map(|(_, mount)| &mount.view.volume),
            )
            .flat_map(|volume| {
                [VolumeOperation::Read, VolumeOperation::Write].map(|operation| (volume, operation))
            })
        {
            if !context
                .scope()
                .grants()
                .contains(&volume.capability(operation, &mut None)?)
            {
                return Err(Error::Unauthorized(
                    "host workspace project is not granted".into(),
                ));
            }
        }
        if acyclic_fs::capture_root_identity(&self.workspace.path).map_err(storage)?
            != self.workspace.identity
        {
            return Err(Error::Unauthorized(
                "host workspace root was replaced".into(),
            ));
        }
        Ok(owner)
    }

    async fn activate_volumes(&mut self, context: &TaskContext) -> Result<()> {
        for (relative, mount) in self.workspace.mounts.mounts() {
            let Some(source) = &mount.view.source else {
                continue;
            };
            self.verify(context).await?;
            source.prepare_command().await?;
            let root = workspace::volumes::projection_root(&source.destination, relative)?;
            let options = volume_guard::options(
                Arc::clone(self.verify(context).await?),
                Arc::clone(&self.mount_root),
                mount.mode.read_only(),
            )?;
            self._native_volumes
                .push(source.enter_with_options(root, relative, options).await?);
        }
        self.verify(context).await?;
        Ok(())
    }

    async fn capture_volumes(&self, context: &TaskContext) -> Result<()> {
        self.verify(context).await?;
        for (_, mount) in self.workspace.mounts.mounts() {
            let Some(source) = &mount.view.source else {
                continue;
            };
            let native = source.native().await;
            let native = native
                .as_ref()
                .ok_or_else(|| Error::Conflict("native volume projection is not active".into()))?;
            // The mount retains the exact owner's callback, checked again at
            // publication submission by the shared filesystem implementation.
            native.sync().await?;
        }
        Ok(())
    }
}

fn original_owner(context: &TaskContext) -> Result<&Arc<dyn OwnedTaskMailbox>> {
    context.owned_task_binding(None)?;
    context
        .owned_mailbox
        .as_ref()
        .ok_or_else(|| Error::Unauthorized("host task owner is absent".into()))
}

fn verify_root(root: &HeldRoot) -> Result<()> {
    if acyclic_fs::capture_root_identity(root.path()).map_err(storage)? != root.identity() {
        return Err(Error::Unauthorized(
            "host-admitted mount root was replaced".into(),
        ));
    }
    Ok(())
}

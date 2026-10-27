//! Workspace coordination for hosts that own their agents and tool execution.
//!
//! This facade opens no model provider, executor, service or process. Its
//! agents come from the native host's agent runtime.
use super::*;
use crate::agent::{AgentPath, VolumeBinding};
use acyclic_fs::HeldRoot;
use workspace::owner::{AgentOwner, ViewOwner};

mod volume_guard;

#[cfg(all(test, not(loom)))]
pub(super) mod tests;

pub use super::workspace::HOST_VIEW_ROOT;

/// Checks that `reference` names an `agents/<reference>` mount; a spawn
/// checks it before committing the child.
pub(super) fn child_reference(reference: &str) -> Result<()> {
    let portable = !reference.is_empty()
        && reference.len() <= 255
        && reference != "."
        && reference != ".."
        && reference
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte));
    if portable {
        Ok(())
    } else {
        Err(Error::Invalid(format!(
            "child reference {reference:?} is not one portable path component"
        )))
    }
}

/// A native-host agent's workspace, with no execution authority of its own.
pub struct HostWorkspace {
    workspace: Arc<LocalWorkspace>,
    agent: AgentPath,
    owner: Arc<dyn ViewOwner>,
    mount_root: Arc<HeldRoot>,
    project: VolumeRef,
    signed: Scope,
    scope: crate::runtime::RuntimeScope,
    _native_volumes: Vec<workspace::projection::Entered>,
    _allocation: Option<Arc<workspace::Mounts>>,
}

impl HostWorkspace {
    /// Attaches a root agent to the existing shared checkout and its
    /// serialization guard. The host admits the checkout before entering its
    /// sandbox; this API cannot add a state directory or another writable root.
    pub(super) async fn attach(
        workspace: Arc<LocalWorkspace>,
        agent: AgentPath,
        signed: &Scope,
        admitted_mount_root: HeldRoot,
    ) -> Result<Self> {
        if agent.parent().is_some() {
            return Err(Error::Unauthorized("host root agent has a parent".into()));
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
        let root = Arc::new(admitted_mount_root.try_clone().map_err(storage)?);
        let admitted = workspace.task_view_admission.get_or_init(|| root);
        if admitted.identity() != admitted_mount_root.identity() {
            return Err(Error::Conflict("host task-view admission changed".into()));
        }
        workspace::volumes::require_available(acyclic_fs::probe_native_mount())?;
        acyclic_fs::initialize_native_mount_services().map_err(storage)?;
        let mut result = Self {
            workspace,
            owner: Arc::new(AgentOwner(agent.clone())),
            agent,
            scope: crate::runtime::RuntimeScope::new(
                signed.capabilities().clone(),
                Default::default(),
            )?,
            signed: signed.copy(&mut None)?,
            mount_root: Arc::new(admitted_mount_root),
            project,
            _native_volumes: Vec::new(),
            _allocation: None,
        };
        result.verify().await?;
        result.activate_volumes().await?;
        Ok(result)
    }

    /// Cwd to give the native host before it starts executing this agent.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.workspace.path
    }

    /// The agent this view belongs to.
    #[must_use]
    pub const fn agent(&self) -> &AgentPath {
        &self.agent
    }

    /// Records completed native-host writes in the selected shared view.
    /// The caller must first quiesce native writes for this agent. This method
    /// neither executes a tool nor grants access to another agent's files.
    pub async fn capture(&self, operation: OperationId) -> Result<()> {
        use super::checkout_tools::{capture, reconciliation};
        self.verify().await?;
        verify_root(&self.mount_root)?;
        let owner = &*self.owner;
        let mut state =
            crate::sync::owned::lock_owned(workspace::StateArc::clone(&self.workspace.state)).await;
        let (selected, pending) = self.workspace.selected_view().await?;
        let view = selected.as_ref().unwrap_or_else(|| self.workspace.view());
        let verify = || owner.verify();
        reconciliation::recover_pending(&self.workspace, view, pending, &verify).await?;
        state.active = selected;
        let physical = self.workspace.selected_workspace(&mut state, owner).await?;
        self.capture_volumes().await?;
        if let Some(mount) = &physical.mounted {
            let owner = Arc::clone(&self.owner);
            let root = Arc::clone(&self.mount_root);
            mount
                .sync_with_permit(
                    acyclic_fs::PublicationPermit::Unrestricted,
                    &mount.native_work()?,
                    move |_| {
                        let owner = Arc::clone(&owner);
                        let root = Arc::clone(&root);
                        async move {
                            owner.verify().await?;
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
                None,
            )
            .await?;
        }
        self.verify().await?;
        verify_root(&self.mount_root)
    }

    /// Runs the shared filesystem git facade with this agent's authority.
    /// Native hosts retain their own shell and tool execution.
    pub async fn fs(
        &self,
        operation: OperationId,
        command: &str,
        request: Option<acyclic_fs::GitRequest>,
    ) -> Result<acyclic_fs::GitCommandOutput> {
        self.verify().await?;
        verify_root(&self.mount_root)?;
        let owner = &*self.owner;
        let configuration = super::checkout_tools::capture::configuration(&self.workspace)?;
        let commands = workspace::git::Commands {
            workspace: &self.workspace,
            signed: &self.signed,
            clock: &acyclic_stream::SystemUnixMillisClock,
            capture_options: &configuration.0,
            capture_policy: &configuration.1,
        };
        let parsed = commands.fs_arguments(command, &self.scope)?;
        let mut state =
            crate::sync::owned::lock_owned(workspace::StateArc::clone(&self.workspace.state)).await;
        let (selected, pending) = self.workspace.selected_view().await?;
        super::checkout_tools::reconciliation::recover_pending(
            &self.workspace,
            selected.as_ref().unwrap_or_else(|| self.workspace.view()),
            pending,
            &|| owner.verify(),
        )
        .await?;
        state.active = selected;
        let physical = self.workspace.selected_workspace(&mut state, owner).await?;
        self.capture_volumes().await?;
        let result = commands
            .fs(
                parsed,
                &workspace::git::CommandContext {
                    owner,
                    operation,
                    scope: &self.scope,
                    request,
                },
                &mut state,
                &physical,
            )
            .await?;
        self.verify().await?;
        verify_root(&self.mount_root)?;
        match result {
            workspace::git::Outcome::Completed(output) => Ok(*output),
            workspace::git::Outcome::Rejected(error) => Err(*error),
        }
    }

    /// Mounts spawned child `child`'s project view, idempotently, and names it
    /// `agents/<reference>` in this view's git facade. The child's volumes
    /// already exist, forked in the spawn's own commit, so this binds the
    /// lazy fork its spawn retained and mounts them; a reopened
    /// owner mounts the same fork.
    pub(super) async fn open_child(
        &self,
        child: &AgentPath,
        volumes: &[VolumeBinding],
        signed: Scope,
        reference: &str,
    ) -> Result<Self> {
        self.verify().await?;
        if child.parent().as_ref() != Some(&self.agent) {
            return Err(Error::Unauthorized(
                "host child belongs to another parent".into(),
            ));
        }
        let bound = |name: &str| {
            volumes
                .iter()
                .find(|binding| binding.name == name)
                .map(|binding| binding.volume.clone())
                .ok_or_else(|| Error::Invalid(format!("host child has no {name} volume")))
        };
        let project = bound("project")?;
        let scratch = bound("scratch")?;
        if project.class() != VolumeClass::Project {
            return Err(Error::Invalid(
                "host child project volume class differs".into(),
            ));
        }
        verify_root(&self.mount_root)?;
        let owner = &*self.owner;
        let mut state =
            crate::sync::owned::lock_owned(workspace::StateArc::clone(&self.workspace.state)).await;
        let (selected, pending) = self.workspace.selected_view().await?;
        let selected = selected.as_ref().unwrap_or_else(|| self.workspace.view());
        super::checkout_tools::reconciliation::recover_pending(
            &self.workspace,
            selected,
            pending,
            &|| owner.verify(),
        )
        .await?;
        let view = self.workspace.spawned_project_view(&project).await?;
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
                .bound_mounts(&project, scratch, &signed)
                .await?;
            state.allocated_mounts.insert(key, Arc::downgrade(&mounts));
            mounts
        };
        child_reference(reference)?;
        acyclic_fs::GitCompatRepository::new(
            selected.workspace().id(),
            self.workspace.store.clone(),
        )
        .register_branch_workspace(
            format!("agents/{reference}"),
            view.workspace().id(),
            None,
            false,
        )
        .await
        .map_err(storage)?;
        let child_owner: Arc<dyn ViewOwner> = Arc::new(AgentOwner(child.clone()));
        let workspace = self
            .workspace
            .mount_task_view(view, &*child_owner, Some(Arc::clone(&mounts)), &signed)
            .await?;
        verify_root(&self.mount_root)?;
        let mut result = Self {
            workspace,
            agent: child.clone(),
            owner: child_owner,
            mount_root: Arc::clone(&self.mount_root),
            project,
            scope: crate::runtime::RuntimeScope::new(
                signed.capabilities().clone(),
                Default::default(),
            )?,
            signed,
            _native_volumes: Vec::new(),
            _allocation: Some(mounts),
        };
        result.verify().await?;
        result.activate_volumes().await?;
        Ok(result)
    }

    /// Fails once the owner lost the view, its volume grants, or its root.
    pub(super) async fn verify(&self) -> Result<()> {
        self.owner.verify().await?;
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
            if !self
                .signed
                .capabilities()
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
        Ok(())
    }

    async fn activate_volumes(&mut self) -> Result<()> {
        for (relative, mount) in self.workspace.mounts.mounts() {
            let Some(source) = &mount.view.source else {
                continue;
            };
            self.verify().await?;
            source.prepare_command().await?;
            let root = workspace::volumes::projection_root(&source.destination, relative)?;
            let options = volume_guard::options(
                source.options.clone(),
                Arc::clone(&self.owner),
                Arc::clone(&self.mount_root),
                mount.mode.read_only(),
            )?;
            self._native_volumes
                .push(source.enter_with_options(root, relative, options).await?);
        }
        self.verify().await
    }

    async fn capture_volumes(&self) -> Result<()> {
        self.verify().await?;
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

fn verify_root(root: &HeldRoot) -> Result<()> {
    if acyclic_fs::capture_root_identity(root.path()).map_err(storage)? != root.identity() {
        return Err(Error::Unauthorized(
            "host-admitted mount root was replaced".into(),
        ));
    }
    Ok(())
}

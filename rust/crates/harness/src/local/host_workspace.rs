//! Workspace coordination for hosts that own their agents and tool execution.
//!
//! This facade opens no model provider, executor, service or process. Its
//! agents come from the native host's agent runtime.
use super::*;
use crate::agent::{AgentPath, WorkspaceEntry};
use acyclic_fs::HeldRoot;
use workspace::owner::{AgentOwner, ViewOwner};

mod volume_guard;

#[cfg(all(test, not(loom)))]
pub(super) mod tests;

pub use super::workspace::HOST_VIEW_ROOT;

/// Checks that `reference` names an `agents/<reference>` mount; a spawn
/// checks it before committing the child.
pub(crate) fn child_reference(reference: &str) -> Result<()> {
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
        Err(Error::Invalid(
            "child reference is not one portable path component".into(),
        ))
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
    verifier: crate::core::AuthorityVerifier,
    scope: crate::runtime::RuntimeScope,
    _native_volumes: Vec<workspace::projection::Entered>,
}

impl HostWorkspace {
    /// Attaches a root agent to the existing shared checkout and its
    /// serialization guard. The host admits the checkout before entering its
    /// sandbox; this API cannot add a state directory or another writable root.
    pub(super) async fn attach(
        workspace: Arc<LocalWorkspace>,
        agent: AgentPath,
        signed: &Scope,
        verifier: &crate::core::AuthorityVerifier,
        admitted_mount_root: HeldRoot,
    ) -> Result<Self> {
        if agent.depth() != 0 {
            return Err(Error::Unauthorized("host root agent has a parent".into()));
        }
        let project = workspace.mounts.named(None)?.1.view.volume.clone();
        if project.class() != VolumeClass::Project {
            return Err(Error::Invalid("host project volume class differs".into()));
        }
        let canonical_root = std::fs::canonicalize(&workspace.path).map_err(storage)?;
        let mounts = std::fs::canonicalize(admitted_mount_root.path()).map_err(storage)?;
        if mounts != std::fs::canonicalize(&workspace.mount_directory).map_err(storage)?
            || mounts.starts_with(&canonical_root)
        {
            return Err(Error::Invalid(
                "host mounts require the owner's external task-view root".into(),
            ));
        }
        verify_root(&admitted_mount_root)?;
        let root = Arc::new(admitted_mount_root.try_clone().map_err(storage)?);
        let admitted = workspace.task_view_admission.get_or_init(|| root);
        if admitted.identity() != admitted_mount_root.identity() {
            return Err(Error::Conflict(format!(
                "host task-view admission changed at {}",
                admitted_mount_root.path().display()
            )));
        }
        let mut result = Self {
            workspace,
            owner: Arc::new(AgentOwner),
            agent,
            scope: crate::runtime::RuntimeScope::new(
                signed.capabilities().clone(),
                Default::default(),
            )?,
            signed: signed.copy(&mut None)?,
            verifier: verifier.clone(),
            mount_root: Arc::new(admitted_mount_root),
            project,
            _native_volumes: Vec::new(),
        };
        result.verify().await?;
        result.activate_volumes().await?;
        Ok(result)
    }

    /// The view's canonical path: the identity cwd discovery matches against.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.workspace.path
    }

    /// Cwd to give the native host before it starts executing this agent: the
    /// canonical path as a user would type it, without a Windows verbatim
    /// prefix. Hosts resolve it back to [`Self::path`] when they report a cwd.
    #[must_use]
    pub fn host_cwd(&self) -> std::borrow::Cow<'_, Path> {
        acyclic_host_platform::paths::strip_verbatim(&self.workspace.path)
    }

    /// Native discovery for this admitted view, shared with the shell.
    pub(super) fn environment(&self) -> Result<std::collections::BTreeMap<String, String>> {
        self.workspace.mounts.environment()
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
            let configuration = capture::configuration(&physical);
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
        command: crate::tools::FsCommand,
        request: Option<acyclic_fs::GitRequest>,
    ) -> Result<acyclic_fs::GitCommandOutput> {
        self.verify().await?;
        verify_root(&self.mount_root)?;
        let owner = &*self.owner;
        let configuration = super::checkout_tools::capture::configuration(&self.workspace);
        let commands = workspace::git::Commands {
            workspace: &self.workspace,
            signed: &self.signed,
            verifier: &self.verifier,
            clock: &acyclic_stream::SystemUnixMillisClock,
            capture_options: &configuration.0,
            capture_policy: &configuration.1,
        };
        let result = commands
            .invoke(
                command,
                workspace::git::CommandContext {
                    owner,
                    operation,
                    scope: &self.scope,
                    request,
                },
                Box::pin(self.capture_volumes()),
            )
            .await?;
        self.verify().await?;
        verify_root(&self.mount_root)?;
        Ok(result)
    }

    /// Mounts spawned child `child`'s project view, idempotently, and names it
    /// `agents/<reference>` in this view's git facade. The child's volumes
    /// already exist, forked in the spawn's own commit, so this binds the
    /// lazy fork its spawn retained and mounts them; a reopened
    /// owner mounts the same fork.
    pub(super) async fn open_child(
        &self,
        child: &AgentPath,
        volumes: &[WorkspaceEntry],
        signed: Scope,
        reference: &str,
    ) -> Result<Self> {
        self.verify().await?;
        if !child.is_child_of(&self.agent) {
            return Err(Error::Unauthorized(
                "host child belongs to another parent".into(),
            ));
        }
        let bound = |name: &str| {
            volumes
                .iter()
                .find(|binding| binding.name == name)
                .and_then(|binding| binding.as_volume())
                .cloned()
                .ok_or_else(|| Error::Invalid(format!("host child has no {name} volume")))
        };
        let project = bound("project")?;
        bound(crate::filesystem::HOME_VOLUME)?;
        if project.class() != VolumeClass::Project {
            return Err(Error::Invalid(
                "host child project volume class differs".into(),
            ));
        }
        // Opening a native fork is the first operation that needs a projection.
        // Roots and physical task views use the admitted checkout directories.
        if self.workspace.mounting == WorkspaceMounting::Native {
            workspace::volumes::require_available(self.workspace.native_mount_probe.probe())?;
            acyclic_fs::initialize_native_mount_services_async()
                .await
                .map_err(storage)?;
        }
        verify_root(&self.mount_root)?;
        let owner = &*self.owner;
        let state =
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
        child_reference(reference)?;
        acyclic_fs::GitCompatRepository::new(
            selected.workspace().id(),
            self.workspace.store.clone(),
        )
        .register_branch_workspace(child.as_str(), view.workspace().id(), None, false)
        .await
        .map_err(storage)?;
        drop(state);
        let child_owner: Arc<dyn ViewOwner> = Arc::new(AgentOwner);
        let authority = self
            .workspace
            .mount_authority
            .as_ref()
            .ok_or(Error::FilesystemAccessConfiguration("native mount owner"))?
            .for_scope(&signed)?;
        let workspace = self
            .workspace
            .bound_task_view(view, &project, volumes, &*child_owner, &authority)
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
            verifier: self.verifier.clone(),
            _native_volumes: Vec::new(),
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
        if self.workspace.root_identity()? != self.workspace.identity {
            return Err(Error::Unauthorized(
                "host workspace root was replaced".into(),
            ));
        }
        Ok(())
    }

    async fn activate_volumes(&mut self) -> Result<()> {
        for (_, mount) in self.workspace.mounts.mounts() {
            let Some(source) = &mount.view.source else {
                continue;
            };
            let Some(options) = source.options.clone() else {
                continue;
            };
            self.verify().await?;
            drop(source.prepared_native().await?);
            let options = volume_guard::options(
                options,
                Arc::clone(&self.owner),
                Arc::clone(&self.mount_root),
                mount.mode.read_only(),
            )?;
            self._native_volumes
                .push(source.enter_with_options(options).await?);
        }
        self.verify().await
    }

    async fn capture_volumes(&self) -> Result<()> {
        self.verify().await?;
        for (_, mount) in self.workspace.mounts.mounts() {
            let Some(source) = &mount.view.source else {
                continue;
            };
            if source.options.is_none() {
                continue;
            }
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
        return Err(Error::Unauthorized(format!(
            "host-admitted mount root was replaced at {}",
            root.path().display()
        )));
    }
    Ok(())
}

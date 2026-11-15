use super::*;
use acyclic_fs::{CancellationToken, MountPublication, WorkBudget};
use acyclic_native_runtime::spawn;

#[cfg(test)]
pub(in crate::local) const HOME_PATH: &str = "home";
#[cfg(test)]
#[cfg(all(any(test, not(target_arch = "wasm32")), not(loom)))]
mod publication_tests;
#[cfg(test)]
#[cfg(all(target_os = "linux", not(loom)))]
mod recovery_tests;

#[cfg(target_os = "linux")]
pub(super) fn recover_after_crash(root: &acyclic_fs::HeldRoot, relative: &Path) -> Result<()> {
    let (Some(parent), Some(leaf)) = (relative.parent(), relative.file_name()) else {
        return Err(Error::Invalid("volume mount has no parent".into()));
    };
    let parent = held_directories(root, parent)?;
    if recover_binding_alias(root, &root.path().join(relative))? {
        return Ok(());
    }
    acyclic_fs::recover_disconnected_native_mount_destination_in(&parent, leaf)?;
    Ok(())
}

/// The directory at `relative` in a held root, created in held parents and
/// never through a link.
pub(in crate::local) fn held_directories(
    root: &acyclic_fs::HeldRoot,
    relative: &Path,
) -> Result<acyclic_fs::HeldRoot> {
    root.create_directories(relative, acyclic_fs::NativeDurability::Durable)
        .map_err(storage)
}

pub(in crate::local) fn require_available(
    capabilities: acyclic_fs::NativeMountCapabilities,
) -> Result<()> {
    let Some(reason) = capabilities.unavailable_reason else {
        return Ok(());
    };
    use acyclic_fs::NativeMountKind;
    Err(Error::MountUnavailable {
        reason,
        device: match capabilities.kind {
            Some(NativeMountKind::LinuxFuse) => "/dev/fuse",
            Some(NativeMountKind::WindowsProjFs) => "ProjectedFSLib.dll",
            Some(NativeMountKind::MacOsNfs) => "/sbin/mount_nfs",
            None => "native mount provider",
        },
    })
}

pub(super) async fn attach(
    filesystem: &acyclic_fs::LocalFs,
    store: &acyclic_fs::LocalCoreStateStore,
    root: &acyclic_fs::HeldRoot,
    mounts: &mut Mounts,
    authority: Option<&crate::filesystem::NativeMountAuthority>,
    admission: &Arc<std::sync::OnceLock<Arc<acyclic_fs::HeldRoot>>>,
    native: bool,
    binding: Option<&acyclic_fs::kernel::allocation::RetainedArc<super::bindings::Binding>>,
) -> Result<()> {
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    for (relative, mount) in mounts.mounts_mut() {
        if relative.is_empty() {
            continue;
        }
        if let Some(source) = &mount.view.source {
            mount.native_root = source
                .options
                .as_ref()
                .map_or(crate::workspace::NativeRoot::Logical, |_| {
                    crate::workspace::NativeRoot::Path(source.destination.clone())
                });
            continue;
        }
        let source = filesystem
            .open_workspace(
                &mount.view.volume.storage_name(&mut None)?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| Error::from(failure.error))?
            .value;
        let mut directory = blake3::hash(mount.view.volume.storage_name(&mut None)?.as_bytes())
            .to_hex()
            .to_string();
        // A read-only alias must not share the native destination and callback
        // owner of a writable binding of the same volume.
        if mount.mode.read_only() {
            directory.push_str("-ro");
        }
        if !mount.view.root.is_empty() {
            directory.push('-');
            directory.push_str(blake3::hash(mount.view.root.as_bytes()).to_hex().as_str());
        }
        let destination = root.path().join(&directory);
        let options = if native {
            let options = authority
                .ok_or(Error::FilesystemAccessConfiguration("native mount owner"))?
                .mount_options(
                    &source,
                    Some(&mount.view.volume),
                    &mount.view.root,
                    !mount.mode.read_only(),
                    &work,
                )
                .await?;
            Some(options.publication(MountPublication::Manual))
        } else {
            None
        };
        #[cfg(windows)]
        if native {
            recover_volume_after_crash(
                root.path(),
                Path::new(&directory),
                source.id().volume_id(),
            )?;
        }
        acyclic_fs::WorkspaceGraph::new(store.clone())
            .register_root(&source, &work, &cancellation)
            .await?;
        // A native binding declares the root that execution projects; a
        // logical-only binding must never acquire an OS path implicitly.
        mount.native_root = options
            .as_ref()
            .map_or(crate::workspace::NativeRoot::Logical, |_| {
                crate::workspace::NativeRoot::Path(destination.clone())
            });
        mount.view.source = Some(Arc::new(MountedVolume {
            workspace: crate::tools::git::OwnedView::Volume(source),
            binding_root: Arc::new(root.try_clone().map_err(storage)?),
            destination,
            options,
            task_view_admission: Arc::clone(admission),
            native: StateArc::new(AsyncMutex::new(None)),
            users: StateArc::new(projection::Users::default()),
            shared_views: tokio::sync::OnceCell::new(),
            _binding: binding.cloned().map(|binding| binding.erase()),
        }));
    }
    Ok(())
}

pub(in crate::local) async fn sync(mounts: &Mounts) -> Result<()> {
    for (_, mount) in ordered_mounts(mounts.mounts()) {
        if let Some(source) = &mount.view.source {
            // Host-session writes land before a command forks the head.
            let native = source.prepared_native().await?;
            if let Some(native) = native.as_ref() {
                native.sync().await?;
                native.advance_to_head().await?;
            }
        }
    }
    Ok(())
}

type BoundMount = crate::workspace::Mount<Option<Arc<MountedVolume>>>;

pub(in crate::local) fn ordered_mounts<'a>(
    mounts: impl IntoIterator<Item = (&'a str, &'a BoundMount)>,
) -> Vec<(&'a str, &'a BoundMount)> {
    let mut ordered = std::collections::BTreeMap::new();
    for (relative, mount) in mounts {
        if let Some(source) = &mount.view.source {
            // The binding is validated even when another name already selects
            // this volume. One command takes one view of each volume, never
            // one per alias in a particular task's mount table.
            ordered
                .entry(source.command_identity())
                .or_insert((relative, mount));
        }
    }
    ordered.into_values().collect()
}

type VolumeWorkspace = acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>;

/// One command's private projection. Entry and reads share the authorized
/// volume head; the shared filesystem adapter creates its ephemeral data fork
/// at the first mutation. Only verified completion hands that fork back.
struct CommandView {
    address: String,
    source: Arc<MountedVolume>,
    mount: acyclic_fs::Mount<LocalAuthorityBackend, LocalObjectBackend>,
}

impl MountedVolume {
    fn command_identity(&self) -> (acyclic_fs::WorkspaceId, &Path) {
        (self.workspace.workspace().id(), &self.destination)
    }

    async fn command_view(self: &Arc<Self>, address: &str, read_only: bool) -> Result<CommandView> {
        let options = self
            .options
            .clone()
            .ok_or_else(|| Error::Invalid("logical volume has no native projection".into()))?;
        self.shared_views
            .get_or_try_init(|| {
                acyclic_fs::hold_shared_view_instance(self.task_view_admission.get().cloned())
            })
            .await
            .map_err(|error| Error::from(acyclic_fs::MountLifecycleError::from(error)))?;
        let options = if read_only {
            options.restrict_to_read_only()
        } else {
            options
        };
        let mount = self
            .workspace
            .workspace()
            .mount_view(super::task_views::view_options(
                &self.task_view_admission,
                options.private_command(),
            ))
            .await
            .map_err(Error::from)?;
        Ok(CommandView {
            address: address.to_owned(),
            source: Arc::clone(self),
            mount,
        })
    }
}

impl CommandView {
    async fn discard(self) {
        if let Err(error) = self.mount.abandon_command().await {
            tracing::error!(?error, "command volume view retirement failed");
        }
    }

    async fn hand_back(self) -> Result<Vec<String>> {
        let Some((fork, custody)) = self.mount.complete_private_command().await? else {
            return Ok(Vec::new());
        };
        let target = self.source.workspace.workspace();
        let merged = projection::compare_and_swap(|| hand_back(&self.mount, &fork, target)).await;
        settle(custody).await;
        merged
    }
}

/// Ends a command's ephemeral fork.
async fn settle(custody: acyclic_fs::EphemeralFork) {
    if let Err(error) = custody.settle().await {
        tracing::error!(?error, "command volume fork release failed");
    }
}

/// One attempt of the compare-and-swap join of `fork` into `target`: `None`
/// when the target moved after planning.
async fn hand_back(
    mount: &acyclic_fs::Mount<LocalAuthorityBackend, LocalObjectBackend>,
    fork: &VolumeWorkspace,
    target: &VolumeWorkspace,
) -> Result<Option<Vec<String>>> {
    use acyclic_fs::JoinOutcome;
    // Git semantics: content decides a conflict, never incidental metadata
    // such as two commands' write times.
    let plan = fork
        .join_into(target)
        .git_metadata()
        .plan(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(|failure| Error::from(failure.error))?
        .value;
    let options = acyclic_fs::ApplyOptions {
        if_target: plan.target_head(),
        idempotency_key: acyclic_fs::IdempotencyKey::new(),
    };
    let (outcome, _first_allocation) = mount
        .apply_private_command_join(&plan, options, None)
        .await?;
    let (outcome, conflicts, _resolved_allocation) = match outcome {
        JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } => {
            let described = plan.describe_conflicts(conflicts, truncated).await?;
            let mut paths = Vec::new();
            let mut resolutions = std::collections::BTreeMap::new();
            for view in &described.conflicts {
                let (resolution, marked) = conflict_resolution(view);
                if marked {
                    paths.push(
                        view.path
                            .clone()
                            .unwrap_or_else(|| format!("{:?}", view.key)),
                    );
                }
                resolutions.insert(view.key.clone(), resolution);
            }
            let candidate = acyclic_fs::UnpublishedMergeCandidate {
                plan: described,
                resolutions,
            };
            let options = acyclic_fs::ApplyOptions {
                idempotency_key: acyclic_fs::IdempotencyKey::new(),
                ..options
            };
            let (outcome, allocation) = mount
                .apply_private_command_join(&plan, options, Some(&candidate))
                .await?;
            (outcome, paths, Some(allocation))
        }
        outcome => (outcome, Vec::new(), None),
    };
    match outcome {
        JoinOutcome::Applied(_) | JoinOutcome::AlreadyApplied(_) | JoinOutcome::NoChanges(_) => {
            Ok(Some(conflicts))
        }
        JoinOutcome::StaleTarget(_) => Ok(None),
        JoinOutcome::Conflicted { .. } | JoinOutcome::Fenced | JoinOutcome::IdempotencyConflict => {
            Err(Error::Conflict(
                "command volume changes could not be handed back".into(),
            ))
        }
    }
}

/// Text conflicts that do not merge cleanly carry git markers into the
/// volume. Any other conflicting value keeps the volume's side. Both are
/// reported (`true`); a clean text merge is not.
fn conflict_resolution(view: &acyclic_fs::ConflictView) -> (acyclic_fs::MergeResolution, bool) {
    if view.kind == acyclic_fs::ConflictKind::Text
        && let Ok(resolved) = acyclic_fs::DefaultTextMergeDriver.resolve_marked(view)
    {
        return resolved;
    }
    (
        acyclic_fs::MergeResolution::Select(acyclic_fs::ConflictSide::Ours),
        true,
    )
}

/// The native execution scope shared by the shell and hooks. Entry gives the
/// command its own view of every bound native volume, and its mount table
/// names those views in the command's `ACYCLIC_WORKSPACES` discovery map.
/// The body returns the claims only after physical cleanup and verification;
/// that exit hands each view back. Claims dropped unreturned, including by a
/// cancelled caller, discard their views whole and never delay another
/// command.
pub(in crate::local) struct Claims {
    views: Vec<CommandView>,
    mounts: Arc<Mounts>,
    tasks: acyclic_native_runtime::TaskSpawner,
}

impl Claims {
    /// This command's mount table, rooted at its own volume views.
    pub(in crate::local) fn mounts(&self) -> &Arc<Mounts> {
        &self.mounts
    }
}

impl Drop for Claims {
    fn drop(&mut self) {
        let views = std::mem::take(&mut self.views);
        if views.is_empty() {
            return;
        }
        // The worker drops custody only after process-tree cleanup. Dropping
        // the awaiting caller cannot cancel this owned discard.
        self.tasks.spawn(async move {
            for view in views {
                view.discard().await;
            }
        });
    }
}

pub(in crate::local) async fn native_scope<T>(
    mounts: &Mounts,
    body: impl AsyncFnOnce(Claims) -> Result<(T, Claims)>,
) -> Result<T> {
    // Volumes are independent: every view is entered at once, so a command
    // waits for its slowest volume, not for the sum of them.
    let mut views = Vec::new();
    let mut entered = Ok(());
    let entries = ordered_mounts(mounts.mounts())
        .into_iter()
        .filter_map(|(address, mount)| {
            let source = mount
                .view
                .source
                .as_ref()
                .filter(|source| source.options.is_some())?;
            let read_only = mount.mode.read_only();
            Some(Box::pin(source.command_view(address, read_only)))
        });
    for view in futures::future::join_all(entries).await {
        match view {
            Ok(view) => views.push(view),
            Err(error) => {
                if entered.is_ok() {
                    entered = Err(error);
                }
            }
        }
    }
    let table = match entered {
        Ok(()) => command_mounts(mounts, &views),
        Err(error) => {
            // No command ran: the views entered so far are discarded.
            for view in views {
                view.discard().await;
            }
            return Err(error);
        }
    };
    let claims = Claims {
        views,
        mounts: Arc::new(table),
        tasks: acyclic_native_runtime::TaskSpawner::current(),
    };
    // Boxed, as is each view's entry: these deep futures stay out of every
    // caller's layout.
    let (value, mut claims) = Box::pin(body(claims)).await?;
    let views = std::mem::take(&mut claims.views);
    // Publication is an accepted operation. Losing its caller after
    // verification must not abandon a verified view's hand-back.
    let conflicts = spawn(async move {
        let mut handed = Ok(Vec::new());
        let results = futures::future::join_all(views.into_iter().map(|view| async move {
            let address = view.address.clone();
            (address, Box::pin(view.hand_back()).await)
        }))
        .await;
        for (address, result) in results {
            if let Ok(conflicts) = &mut handed {
                match result {
                    Ok(paths) => conflicts.extend(paths.into_iter().map(|path| {
                        let path = path.trim_start_matches('/');
                        match address.as_str() {
                            "" => path.to_owned(),
                            address => format!("{}/{path}", address.trim_end_matches('/')),
                        }
                    })),
                    Err(error) => handed = Err(error),
                }
            }
        }
        handed
    })
    .await
    .map_err(|error| Error::Storage(format!("native scope hand-back interrupted: {error}")))??;
    if !conflicts.is_empty() {
        return Err(Error::Conflict(format!(
            "volume merge conflict; repair the git conflict markers in: {}",
            conflicts.join(", ")
        )));
    }
    Ok(value)
}

/// The command's mount table: each volume with a view is rooted at that view.
/// The clone shares every compiled policy; only native roots differ.
fn command_mounts(mounts: &Mounts, views: &[CommandView]) -> Mounts {
    let mut table = mounts.clone();
    for (_, mount) in table.mounts_mut() {
        if let Some(view) =
            views.iter().find(|view| {
                mount.view.source.as_ref().is_some_and(|source| {
                    source.command_identity() == view.source.command_identity()
                })
            })
        {
            mount.native_root = crate::workspace::NativeRoot::Path(view.mount.path().to_path_buf());
        }
    }
    table
}

// Every multi-volume exit attempts each volume and reports the first cause; one
// failure never strands the volumes after it.
fn keep_first(first: &mut Result<()>, next: Result<()>) {
    if first.is_ok() {
        *first = next;
    }
}

/// Retires every projection when the host disconnects. Active users wake
/// retirement after physical cleanup; the projection stays mounted until then.
/// Each volume progresses independently, so an active worker cannot delay an
/// idle volume's retirement. Report the first error in binding order after all
/// retirements complete.
pub(in crate::local) async fn retire(mounts: &Mounts) -> Result<()> {
    let mut retired = Ok(());
    let retirements = mounts
        .mounts()
        .filter_map(|(_, mount)| mount.view.source.as_ref())
        .map(|source| Box::pin(source.retire()));
    for result in futures::future::join_all(retirements).await {
        keep_first(&mut retired, result);
    }
    retired
}

/// Route auxiliary tools and process discovery directly to host state. No
/// aliases or mount parents are installed in the project tree.
pub(super) fn bind_roots(mounts: &mut Mounts) -> Result<()> {
    for (relative, mount) in mounts.mounts_mut() {
        if relative.is_empty() {
            continue;
        }
        let source = mount
            .view
            .source
            .as_ref()
            .ok_or_else(|| Error::NotFound("volume mount is not bound".into()))?;
        mount.native_root = source
            .options
            .as_ref()
            .map_or(crate::workspace::NativeRoot::Logical, |_| {
                crate::workspace::NativeRoot::Path(source.destination.clone())
            });
    }
    Ok(())
}

/// Rebinds only an empty SDK directory or the exact preceding alias.
pub(super) fn bind_alias(path: &Path, target: &Path, previous: Option<&Path>) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            let linked = alias_target(path)?
                .ok_or_else(|| Error::Conflict("volume alias changed during binding".into()))?;
            // A retired shared root may already have been reaped, including
            // its parent directory. An exact authenticated alias needs no
            // filesystem lookup to establish that identity.
            let replace = if linked == target {
                false
            } else if previous.is_some_and(|expected| linked == expected) {
                true
            } else if canonical_destination(&linked)? == canonical_destination(target)? {
                false
            } else {
                let expected = previous
                    .ok_or_else(|| Error::Conflict("volume alias names another binding".into()))?;
                if canonical_destination(&linked)? != canonical_destination(expected)? {
                    return Err(Error::Conflict("volume alias names another binding".into()));
                }
                true
            };
            if replace {
                // A fork inherits the parent's SDK alias in its authored
                // cut. Before exposing this new view, replace only that
                // exact authenticated binding with the child's volume.
                unlink_directory_alias(path).map_err(mount_io)?;
                link_directory(target, path).map_err(mount_io)?;
            }
        }
        Ok(metadata) if metadata.is_dir() => {
            // remove_dir atomically refuses a nonempty directory; user
            // content is never replaced to install a volume binding.
            std::fs::remove_dir(path).map_err(mount_io)?;
            link_directory(target, path).map_err(mount_io)?;
        }
        Ok(_) => {
            return Err(Error::Conflict(
                "volume alias replaces a project file".into(),
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            link_directory(target, path).map_err(mount_io)?;
        }
        Err(error) => return Err(mount_io(error)),
    }
    Ok(())
}

pub(in crate::local) fn alias_target(path: &Path) -> Result<Option<PathBuf>> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            let parent = path
                .parent()
                .ok_or_else(|| Error::Invalid("volume alias has no parent".into()))?;
            Ok(Some(
                parent.join(std::fs::read_link(path).map_err(mount_io)?),
            ))
        }
        Ok(_) => Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(mount_io(error)),
    }
}

/// Releases the projection's exact alias, independently of physical retirement.
/// A replacement binding belongs to its new owner and remains intact.
pub(super) fn unlink_binding(destination: &Path, view: &Path) -> Result<()> {
    if let Some(target) = alias_target(destination)?
        && (target == view || canonical_destination(&target)? == canonical_destination(view)?)
    {
        unlink_directory_alias(destination).map_err(mount_io)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn recover_binding_alias(root: &acyclic_fs::HeldRoot, destination: &Path) -> Result<bool> {
    let Some(target) = alias_target(destination)? else {
        return Ok(false);
    };
    let belongs = if acyclic_fs::shared_view_namespace_contains(&target, None)? {
        true
    } else if target.starts_with(root.path().join(".volumes/agents")) {
        let admission = held_directories(root, Path::new(".volumes/agents"))?;
        acyclic_fs::shared_view_namespace_contains(&target, Some(&admission))?
    } else {
        false
    };
    if !belongs {
        return Ok(false);
    }
    if target.try_exists().map_err(mount_io)? {
        return Ok(true);
    }
    unlink_directory_alias(destination).map_err(mount_io)?;
    Ok(false)
}

#[cfg(unix)]
fn unlink_directory_alias(path: &Path) -> std::io::Result<()> {
    std::fs::remove_file(path)
}

#[cfg(windows)]
fn unlink_directory_alias(path: &Path) -> std::io::Result<()> {
    std::fs::remove_dir(path)
}

// A command retires its physical projection. Resolve its held parent rather
// than requiring that transient destination to exist when validating an alias.
fn canonical_destination(path: &Path) -> Result<PathBuf> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::Invalid("volume destination has no parent".into()))?;
    let name = path
        .file_name()
        .ok_or_else(|| Error::Invalid("volume destination has no name".into()))?;
    Ok(std::fs::canonicalize(parent).map_err(mount_io)?.join(name))
}

#[cfg(unix)]
fn link_directory(target: &Path, path: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, path)
}

#[cfg(windows)]
fn link_directory(target: &Path, path: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_dir(target, path)
}

/// Each binding supplies its durable logical volume identity. The destination
/// fence and physical receipt must agree before any authored file is moved.
#[cfg(windows)]
pub(super) fn recover_volume_after_crash(
    root: &Path,
    relative: &Path,
    volume: acyclic_fs::VolumeId,
) -> Result<()> {
    let destination = root.join(relative);
    let [fence, ..] = acyclic_fs::native_mount_control_paths(&destination)?;
    match std::fs::symlink_metadata(&fence) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(mount_io(error)),
        Ok(_) => {}
    }
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let parent = destination
        .parent()
        .ok_or_else(|| Error::Invalid("volume projection has no parent".into()))?;
    let (parent, _) = work.open_root_directory(parent)?;
    let mut owner =
        acyclic_fs::NativeMountRecoveryOwner::acquire_in_directory(&destination, &parent, &work)?;
    let recovery = Path::new(".volumes/recovery")
        .join(uuid::Uuid::from_bytes(volume.into_bytes()).to_string());
    create_mount_directory(root, &recovery)?;
    let (recovery, _) = work.open_root_directory(&root.join(recovery))?;
    let name = uuid::Uuid::from_bytes(acyclic_fs::OperationId::new().into_bytes()).to_string();
    owner.preserve_crashed_in_directory(&recovery, Path::new(&name), volume, &work)?;
    Ok(())
}

#[cfg(any(windows, test))]
pub(super) fn create_mount_directory(root: &Path, relative: &Path) -> Result<()> {
    let root =
        acyclic_fs::hold_root(root).map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
    root.create_directories(relative, acyclic_fs::NativeDurability::Durable)
        .map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
    Ok(())
}

fn mount_io(cause: std::io::Error) -> Error {
    acyclic_fs::NativeTreeMaterializationError::Io(cause).into()
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;

    #[test]
    fn absent_fuse_device_has_a_typed_tool_admission_error() {
        let error = require_available(acyclic_fs::NativeMountCapabilities {
            kind: Some(acyclic_fs::NativeMountKind::LinuxFuse),
            unavailable_reason: Some("missing device"),
        })
        .unwrap_err();
        assert!(matches!(
            error,
            Error::MountUnavailable {
                device: "/dev/fuse",
                reason: "missing device",
            }
        ));
        assert_eq!(error.code(), crate::wire::ErrorCode::Unsupported);
    }

    #[test]
    fn mount_directory_preserves_the_native_io_cause() -> Result<()> {
        let directory = tempfile::tempdir().map_err(storage)?;
        let error = create_mount_directory(&directory.path().join("missing"), Path::new(HOME_PATH))
            .expect_err("the mount parent is absent");
        assert!(matches!(error, Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Io(error), ..
        } if error.kind() == std::io::ErrorKind::NotFound));
        Ok(())
    }

    #[test]
    fn mount_parent_cannot_redirect_creation_outside_the_view() -> Result<()> {
        let root = tempfile::tempdir().map_err(storage)?;
        let outside = tempfile::tempdir().map_err(storage)?;
        std::os::unix::fs::symlink(outside.path(), root.path().join(".volumes"))
            .map_err(storage)?;
        assert!(create_mount_directory(root.path(), Path::new(".volumes/home")).is_err());
        assert!(!outside.path().join("scratch").exists());
        Ok(())
    }

    #[test]
    fn mount_root_alias_is_refused_before_any_creation() -> Result<()> {
        let fixture = tempfile::tempdir().map_err(storage)?;
        let outside = tempfile::tempdir().map_err(storage)?;
        let root = fixture.path().join("replaced-root");
        std::os::unix::fs::symlink(outside.path(), &root).map_err(storage)?;
        assert!(create_mount_directory(&root, Path::new(HOME_PATH)).is_err());
        assert!(!outside.path().join(".volumes").exists());
        Ok(())
    }
}

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
        let directory = blake3::hash(mount.view.volume.storage_name(&mut None)?.as_bytes())
            .to_hex()
            .to_string();
        let destination = root.path().join(&directory);
        let options = if native {
            Some(
                authority
                    .ok_or(Error::FilesystemAccessConfiguration("native mount owner"))?
                    .mount_options(
                        &source,
                        Some(&mount.view.volume),
                        !mount.mode.read_only(),
                        &work,
                    )
                    .await?
                    .publication(MountPublication::Manual),
            )
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
            execution: StateArc::new(AsyncMutex::new(())),
            _binding: binding.cloned(),
        }));
    }
    Ok(())
}

pub(in crate::local) async fn sync(mounts: &Mounts) -> Result<()> {
    for (_, mount) in ordered_mounts(mounts)? {
        if let Some(source) = &mount.view.source {
            // This call precedes the command scope. Share its execution guard
            // so preflight cannot publish another worker's unverified effects.
            let _execution =
                crate::sync::owned::lock_owned(StateArc::clone(&source.execution)).await;
            source.prepare_command().await?;
            let native = source.native().await;
            if let Some(native) = native.as_ref() {
                native.sync().await?;
                native.advance_to_head().await?;
            }
        }
    }
    Ok(())
}

type BoundMount = crate::workspace::Mount<Option<Arc<MountedVolume>>>;

fn ordered_mounts(mounts: &Mounts) -> Result<Vec<(&str, &BoundMount)>> {
    let mut ordered = std::collections::BTreeMap::new();
    for (relative, mount) in mounts.mounts() {
        if let Some(source) = &mount.view.source {
            // The binding is validated even when another name already selects
            // this projection. Lock order belongs to the volume and physical
            // projection, never to aliases in a particular task's mount table.
            let key = (
                source.workspace.workspace().id(),
                source.destination.clone(),
            );
            ordered.entry(key).or_insert((relative, mount));
        }
    }
    Ok(ordered.into_values().collect())
}

/// The single native execution scope shared by the shell and hooks. Entry projects
/// every bound volume, so a command sees persisted volume data. A successful
/// exit publishes and detaches each projection and removes its SDK-owned
/// residue, so the checkout holds no mount between executions. The body owns
/// every check that must precede publication.
struct Claim {
    source: Arc<MountedVolume>,
    entered: projection::Entered,
}

pub(in crate::local) struct Claims {
    entries: Vec<Claim>,
    tasks: acyclic_native_runtime::TaskSpawner,
    unverified: Unverified,
}

/// Marks every claimed volume unverified once, at the first of: the caller
/// losing the scope, or the claims dropping unreturned. Entrants wait on that
/// mark until abandonment retires the projection, so no later command joins
/// or publishes a cancelled worker's effects.
/// The first mark counts every volume while holding the lock, so a racing
/// mark returns, and its claims' abandonment settles, only after the counts.
#[derive(Clone, Default)]
struct Unverified(Arc<std::sync::Mutex<bool>>);

impl Unverified {
    fn mark<'a>(&self, volumes: impl IntoIterator<Item = &'a Arc<MountedVolume>>) {
        let mut marked = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !std::mem::replace(&mut *marked, true) {
            for volume in volumes {
                volume.users.unverified();
            }
        }
    }
}

struct CallerScope {
    volumes: Vec<Arc<MountedVolume>>,
    unverified: Unverified,
}

impl Drop for CallerScope {
    fn drop(&mut self) {
        self.unverified.mark(&self.volumes);
    }
}

impl Drop for Claims {
    fn drop(&mut self) {
        let entries = std::mem::take(&mut self.entries);
        if entries.is_empty() {
            return;
        }
        self.unverified
            .mark(entries.iter().map(|claim| &claim.source));
        // The worker drops custody only after process-tree cleanup. Its owned
        // task now retains the execution guards through unpublished retirement;
        // dropping the awaiting caller cannot cancel this cleanup task.
        self.tasks.spawn(async move {
            for claim in entries.into_iter().rev() {
                let source = Arc::clone(&claim.source);
                if let Err(error) = source.abandon(claim.entered).await {
                    tracing::error!(?error, "cancelled native scope retirement failed");
                }
                source.users.settle();
            }
        });
    }
}

pub(in crate::local) async fn native_scope<T>(
    mounts: &Mounts,
    body: impl AsyncFnOnce(Claims) -> Result<(T, Claims)>,
) -> Result<T> {
    let mut claims = Claims {
        entries: Vec::new(),
        tasks: acyclic_native_runtime::TaskSpawner::current(),
        unverified: Unverified::default(),
    };
    let mut projected = Ok(());
    for (_, mount) in ordered_mounts(mounts)? {
        if let Some(source) = &mount.view.source
            && source.options.is_some()
        {
            // The projection's counted claims retain the mount across every
            // overlapping command. Its native owner serializes publication;
            // a command lifetime must not exclude other admitted commands.
            // A Linux task view aliases the original binding; mount at that
            // binding's root, never below another owner-only FUSE mount.
            let entry = async {
                source.prepare_command().await?;
                Box::pin(source.enter(mount.mode.read_only())).await
            }
            .await;
            match entry {
                Ok(entry) => claims.entries.push(Claim {
                    source: Arc::clone(source),
                    entered: entry,
                }),
                Err(error) => {
                    projected = Err(error);
                    break;
                }
            }
        }
    }
    if let Err(error) = projected {
        // No command ran, so the volumes already entered leave through the
        // same exit. A failed leave keeps its projection, and the next exit
        // or retirement retries it and reports that cause again.
        let _ = leave_all(std::mem::take(&mut claims.entries)).await;
        return Err(error);
    }
    // Boxed, as are projection entry and exit: these deep futures stay out of
    // every caller's layout. The body returns claims only after physical
    // cleanup and verification. Cancellation leaves them in the worker,
    // rather than this caller future.
    let mut caller = CallerScope {
        volumes: claims
            .entries
            .iter()
            .map(|claim| Arc::clone(&claim.source))
            .collect(),
        unverified: claims.unverified.clone(),
    };
    let (value, mut claims) = Box::pin(body(claims)).await?;
    // Returned claims are verified; their exit publishes them.
    caller.volumes.clear();
    let entered = std::mem::take(&mut claims.entries);
    leave_all(entered).await?;
    Ok(value)
}

async fn leave_all(entered: Vec<Claim>) -> Result<()> {
    // Publication is an accepted operation. Losing its caller after verification
    // must not release the execution guard before physical retirement finishes.
    spawn(async move {
        let mut retired = Ok(());
        for claim in entered.into_iter().rev() {
            keep_first(
                &mut retired,
                Box::pin(claim.source.leave(claim.entered)).await,
            );
        }
        retired
    })
    .await
    .map_err(|error| Error::Storage(format!("native scope retirement interrupted: {error}")))?
}

// Every multi-volume exit attempts each volume and reports the first cause; one
// failure never strands the volumes after it.
fn keep_first(first: &mut Result<()>, next: Result<()>) {
    if first.is_ok() {
        *first = next;
    }
}

/// Retires every projection when the host disconnects. Active claims wake
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

//! The process's root for views of its shared native mount instance.
//!
//! Each process gets a fresh root in the default native-mount directory or
//! an admitted host workspace. One instance serves all views in that admission,
//! however many owners (harnesses, sessions) open them. A process holds its root's owner lock
//! for its lifetime; a process that dies releases it with its descriptors,
//! leaving its root mounted with a fence nobody holds. The first process to
//! ask for its own root afterwards detaches and removes every root whose
//! owner lock it can take, so a crash never leaves a mount behind past the
//! next start. One directory lock orders creation against reaping, so a
//! root is never reaped between its creation and its owner's lock.
//!
//! The owner lock belongs to its open file description, which a forked
//! child shares until it executes (descriptors are close-on-exec). A root
//! therefore reads live while its owner, or a child it is starting, can
//! still reach the lock: liveness errs only toward keeping a root, which
//! the next start reaps.

use super::super::{NativeMountError, registry};
use acyclic_native_runtime::{FileLock, FileLockMode};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

const ROOTS: &str = "views";
const OWNER: &str = "owner";
const REAP_LOCK: &str = ".reap.lock";

struct Root {
    path: PathBuf,
    // Held for the process's lifetime: the root's liveness.
    _owner: FileLock,
}

type Admission = (PathBuf, [u8; 16]);
type Roots = std::collections::BTreeMap<Admission, Root>;
static ROOTS_BY_ADMISSION: OnceLock<Mutex<Roots>> = OnceLock::new();

fn view_serial(name: &std::ffi::OsStr) -> Option<u64> {
    name.to_str()
        .and_then(|name| name.strip_prefix('v'))
        .filter(|digits| digits.len() == 16)
        .and_then(|digits| u64::from_str_radix(digits, 16).ok())
}

/// Recognizes the allocator's namespace after reaping dead owners. This is
/// used only to recover an SDK alias; it does not claim or authorize a view.
pub fn shared_view_namespace_contains(
    destination: &Path,
    admission: Option<&crate::HeldRoot>,
) -> Result<bool, NativeMountError> {
    #[cfg(target_os = "macos")]
    {
        let namespace = native_view_root(admission)?;
        let Ok(relative) = destination.strip_prefix(namespace) else {
            return Ok(false);
        };
        let mut components = relative.components();
        Ok(matches!(
            (components.next(), components.next()),
            (Some(std::path::Component::Normal(name)), None)
                if name.to_str().is_some_and(|name|
                    name.len() == 32 && name.bytes().all(|byte| byte.is_ascii_hexdigit()))
        ))
    }
    #[cfg(not(target_os = "macos"))]
    {
        let root = match admission {
            Some(admitted) => shared_view_root_in(admitted)?,
            None => shared_view_root()?,
        };
        let Some(namespace) = root.parent() else {
            return Ok(false);
        };
        let Ok(relative) = destination.strip_prefix(namespace) else {
            return Ok(false);
        };
        let mut components = relative.components();
        let (
            Some(std::path::Component::Normal(root)),
            Some(std::path::Component::Normal(view)),
            None,
        ) = (components.next(), components.next(), components.next())
        else {
            return Ok(false);
        };
        Ok(root.to_str().is_some_and(|root| {
            root.len() == 32 && root.bytes().all(|byte| byte.is_ascii_hexdigit())
        }) && view_serial(view).is_some())
    }
}

/// This process's root for shared views: a view at `root.join(name)`,
/// mounted with [`Workspace::mount_view`](crate::Workspace::mount_view), is a
/// namespace of the one instance mounted at the root.
///
/// The first call reaps the roots of dead processes.
pub fn shared_view_root() -> Result<PathBuf, NativeMountError> {
    root_in(&default_view_roots()?)
}

fn default_view_roots() -> Result<crate::HeldRoot, NativeMountError> {
    let roots = registry::path()?
        .parent()
        .ok_or_else(|| std::io::Error::other("native-mount directory has no parent"))?
        .join(ROOTS);
    std::fs::create_dir_all(&roots)?;
    crate::hold_root(&roots)
        .map_err(crate::NativeTreeMaterializationError::from)
        .map_err(Into::into)
}

// Darwin currently owns one native session per view. Keep its destinations
// under the admitted namespace, while its existing destination fence and
// startup recovery retain mount custody. The host never selects this backend.
#[cfg(target_os = "macos")]
fn native_view_root(admission: Option<&crate::HeldRoot>) -> Result<PathBuf, NativeMountError> {
    match admission {
        Some(admitted) => {
            verify_placement(admitted)?;
            Ok(admitted.path().to_path_buf())
        }
        None => Ok(default_view_roots()?.path().to_path_buf()),
    }
}

/// Uses an already admitted writable namespace for the process's shared
/// instance. Native hosts use this to keep every view inside their existing
/// sandbox roots. Root ownership, crash reaping and instance allocation are
/// identical to [`shared_view_root`]; placement grants no additional access.
pub fn shared_view_root_in(admitted: &crate::HeldRoot) -> Result<PathBuf, NativeMountError> {
    root_in(admitted)
}

#[derive(Default)]
struct IssuedNames {
    next: u64,
    pending: std::collections::BTreeMap<u64, PathBuf>,
}

impl IssuedNames {
    fn issue(&mut self, root: &Path, claim: bool) -> Result<(u64, PathBuf), NativeMountError> {
        let serial = self
            .next
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("shared view name space exhausted"))?;
        let path = root.join(format!("v{serial:016x}"));
        if claim {
            self.pending.insert(serial, path.clone());
        }
        self.next = serial;
        Ok((serial, path))
    }

    #[cfg(any(windows, test))]
    fn claim(&mut self, destination: &Path) -> bool {
        let Some(serial) = destination.file_name().and_then(view_serial) else {
            return false;
        };
        if self
            .pending
            .get(&serial)
            .is_some_and(|issued| issued == destination)
        {
            self.pending.remove(&serial);
            return true;
        }
        false
    }
}

static ISSUED: Mutex<IssuedNames> = Mutex::new(IssuedNames {
    next: 0,
    pending: std::collections::BTreeMap::new(),
});

/// Custody of a never-reused name until the platform claims it or startup ends.
pub(crate) struct IssuedDestination {
    path: PathBuf,
    claim: Option<u64>,
}

impl IssuedDestination {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for IssuedDestination {
    fn drop(&mut self) {
        if let Some(serial) = self.claim {
            ISSUED
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pending
                .remove(&serial);
        }
    }
}

fn issue_destination(root: &Path, claim: bool) -> Result<IssuedDestination, NativeMountError> {
    let (serial, path) = ISSUED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .issue(root, claim)?;
    Ok(IssuedDestination {
        path,
        claim: claim.then_some(serial),
    })
}

/// One allocator for default and sandbox-admitted task views. Only `ProjFS`
/// needs an in-flight path claim: its callbacks identify views by pathname.
pub(crate) fn shared_view_destination(
    admission: Option<&crate::HeldRoot>,
) -> Result<IssuedDestination, NativeMountError> {
    #[cfg(target_os = "macos")]
    {
        Ok(IssuedDestination {
            path: native_view_root(admission)?.join(format!(
                "{:032x}",
                u128::from_be_bytes(crate::MountId::new().into_bytes())
            )),
            claim: None,
        })
    }
    #[cfg(not(target_os = "macos"))]
    {
        if !cfg!(any(target_os = "linux", windows)) {
            return Err(NativeMountError::UnsupportedTarget);
        }
        let root = match admission {
            Some(admitted) => shared_view_root_in(admitted)?,
            None => shared_view_root()?,
        };
        issue_destination(&root, cfg!(windows))
    }
}

#[cfg(any(windows, test))]
pub(in crate::native_mount) fn claim_issued_destination(destination: &Path) -> bool {
    ISSUED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .claim(destination)
}

fn verify_placement(admitted: &crate::HeldRoot) -> Result<(), NativeMountError> {
    if crate::capture_root_identity(admitted.path())
        .map_err(|error| std::io::Error::other(error.to_string()))?
        != admitted.identity()
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "shared-view admission no longer names the held root",
        )
        .into());
    }
    Ok(())
}

fn root_in(roots: &crate::HeldRoot) -> Result<PathBuf, NativeMountError> {
    verify_placement(roots)?;
    let key = (roots.path().to_path_buf(), roots.identity().to_bytes());
    let mut index = ROOTS_BY_ADMISSION
        .get_or_init(|| Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .map_err(|_| std::io::Error::other("shared mount root index poisoned"))?;
    if let Some(root) = index.get(&key) {
        return Ok(root.path.clone());
    }
    let root = create_root(
        roots,
        &format!(
            "{:032x}",
            u128::from_be_bytes(crate::MountId::new().into_bytes())
        ),
    )?;
    verify_placement(roots)?;
    let path = root.path.clone();
    index.insert(key, root);
    Ok(path)
}

fn create_root(roots: &crate::HeldRoot, name: &str) -> Result<Root, NativeMountError> {
    crate::native_host::require_leaf(Path::new(name))?;
    let reaping = FileLock::acquire(
        roots
            .open_file(Path::new(REAP_LOCK), true)
            .map_err(crate::NativeTreeMaterializationError::from)?,
        FileLockMode::Exclusive,
    )?;
    reap_dead_roots(roots)?;
    let owner = FileLock::try_acquire(
        roots
            .open_file(Path::new(&format!("{name}.{OWNER}")), true)
            .map_err(crate::NativeTreeMaterializationError::from)?,
        FileLockMode::Exclusive,
    )?;
    roots.root.directory.create_dir(name)?;
    let path = roots.path().join(name);
    drop(reaping);
    Ok(Root {
        path,
        _owner: owner,
    })
}

/// Detaches and removes every root under `roots` whose owner is gone; the
/// caller holds the directory's reap lock. Returns how many were removed.
fn reap_dead_roots(roots: &crate::HeldRoot) -> Result<usize, NativeMountError> {
    let entries = roots.root.directory.entries()?;
    let suffix = format!(".{OWNER}");
    let mut reaped = 0;
    for entry in entries {
        let file = entry?.file_name();
        let Some(name) = file
            .to_str()
            .and_then(|file| file.strip_suffix(suffix.as_str()))
        else {
            continue;
        };
        let owner = roots
            .open_file(Path::new(&file), false)
            .map_err(crate::NativeTreeMaterializationError::from)?;
        let _owner = match FileLock::try_acquire(owner, FileLockMode::Exclusive) {
            Ok(owner) => owner,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error.into()),
        };
        reap_root(roots, Path::new(name))?;
        roots
            .remove_file(Path::new(&file))
            .map_err(crate::NativeTreeMaterializationError::from)?;
        reaped += 1;
    }
    Ok(reaped)
}

fn reap_root(roots: &crate::HeldRoot, name: &Path) -> Result<(), NativeMountError> {
    crate::native_host::require_leaf(name)?;
    #[cfg(windows)]
    return super::super::projfs::detach_dead_shared_root(roots, name);
    #[cfg(not(windows))]
    {
        // Pinned recovery cannot detach an alias or a replacement mount.
        #[cfg(target_os = "linux")]
        super::super::recover_disconnected_native_mount_destination_in(roots, name.as_os_str())?;
        match roots.root.directory.remove_dir(name) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod issuance_tests {
    use super::*;

    #[test]
    fn wrong_root_does_not_consume_the_one_use_destination() -> Result<(), NativeMountError> {
        let root = Path::new("admitted");
        let issued = issue_destination(root, true)?;
        let foreign = Path::new("foreign").join(issued.path().file_name().unwrap_or_default());
        assert!(!claim_issued_destination(&foreign));
        assert!(claim_issued_destination(issued.path()));
        assert!(!claim_issued_destination(issued.path()));
        // The previous name-only claim would accept the foreign root.
        assert_eq!(foreign.file_name(), issued.path().file_name());
        Ok(())
    }

    #[test]
    fn cancelled_destination_custody_releases_every_unclaimed_name() -> Result<(), NativeMountError>
    {
        for _ in 0..1024 {
            let issued = issue_destination(Path::new("cancelled"), true)?;
            let serial = issued.claim.expect("path admission");
            let destination = issued.path().to_path_buf();
            drop(issued);
            assert!(
                !ISSUED
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .pending
                    .contains_key(&serial)
            );
            assert!(!claim_issued_destination(&destination));
        }
        Ok(())
    }

    #[test]
    fn inode_backends_retain_no_pending_name() -> Result<(), NativeMountError> {
        let mut names = IssuedNames::default();
        let first = names.issue(Path::new("inode-backend"), false)?.1;
        let second = names.issue(Path::new("inode-backend"), false)?.1;
        assert_ne!(first, second);
        assert!(names.pending.is_empty());
        Ok(())
    }

    #[test]
    fn issued_serials_cannot_wrap() {
        let mut names = IssuedNames {
            next: u64::MAX,
            ..IssuedNames::default()
        };
        assert!(names.issue(Path::new("inode-backend"), false).is_err());
        assert_eq!(names.next, u64::MAX);
        assert!(names.pending.is_empty());
    }
}

#[cfg(all(test, target_os = "linux"))]
pub(super) mod tests {
    use super::*;

    #[test]
    fn admitted_shared_root_is_reused_and_replaced_admission_is_refused() -> std::io::Result<()> {
        let fixture = tempfile::tempdir()?;
        let path = fixture.path().join("admitted");
        std::fs::create_dir(&path)?;
        let admitted = crate::hold_root(&path).map_err(std::io::Error::other)?;
        let first = shared_view_root_in(&admitted).map_err(std::io::Error::other)?;
        assert!(first.starts_with(&path));
        assert_eq!(
            first,
            shared_view_root_in(&admitted).map_err(std::io::Error::other)?
        );
        let replacement = fixture.path().join("replacement");
        std::fs::create_dir(&replacement)?;
        std::fs::rename(&path, fixture.path().join("held"))?;
        std::fs::rename(&replacement, &path)?;
        // The same path, now naming another directory, grants no mount root.
        assert!(shared_view_root_in(&admitted).is_err());
        assert!(std::fs::read_dir(&path)?.next().is_none());
        let replacement = crate::hold_root(&path).map_err(std::io::Error::other)?;
        let next = shared_view_root_in(&replacement).map_err(std::io::Error::other)?;
        assert_ne!(
            first, next,
            "a new admitted identity cannot adopt the old cached root"
        );
        assert!(next.is_dir());
        Ok(())
    }

    /// A root whose owner lock is held is never reaped; once released it is.
    ///
    /// The probe runs in a process of its own: in the shared test process
    /// another test's fork can share a dropped owner's lock until its child
    /// executes, which keeps that root live, as the module describes.
    #[test]
    fn only_roots_without_a_live_owner_are_reaped() -> Result<(), Box<dyn std::error::Error>> {
        let roots = tempfile::tempdir()?;
        let admission = crate::hold_root(roots.path())?;
        let live = create_root(&admission, "live")?;
        let dead = create_root(&admission, "dead")?;
        std::fs::write(roots.path().join("stray"), b"")?;
        let held = FileLock::acquire(
            admission.open_file(Path::new(REAP_LOCK), true)?,
            FileLockMode::Exclusive,
        )?;
        drop(dead);
        assert_eq!(reap_dead_roots(&admission)?, 1);
        drop(held);
        assert!(live.path.is_dir(), "a live owner keeps its root");
        assert!(!roots.path().join("dead").exists());
        assert!(!roots.path().join("dead.owner").exists());
        assert!(
            roots.path().join("stray").exists(),
            "only owned roots are touched"
        );
        // Negative control: the same root, its owner lock dropped, is reaped.
        drop(live);
        let next = create_root(&admission, "next")?;
        assert!(!roots.path().join("live").exists());
        assert!(next.path.is_dir());
        Ok(())
    }

    #[test]
    fn root_setup_and_reaping_never_reopen_the_admitted_name()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = tempfile::tempdir()?;
        let path = fixture.path().join("admitted");
        let moved = fixture.path().join("held");
        let outside = fixture.path().join("outside");
        std::fs::create_dir(&path)?;
        std::fs::create_dir(&outside)?;
        let admission = crate::hold_root(&path)?;
        drop(create_root(&admission, "dead")?);
        std::fs::rename(&path, &moved)?;
        std::os::unix::fs::symlink(&outside, &path)?;
        // Replacement after admission cannot redirect creation, owner locks,
        // enumeration or removal. The public boundary also refuses its cwd.
        let next = create_root(&admission, "next")?;
        assert!(moved.join("next").is_dir());
        assert!(!moved.join("dead").exists());
        assert!(!moved.join("dead.owner").exists());
        assert_eq!(std::fs::read_dir(&outside)?.count(), 0);
        assert!(shared_view_root_in(&admission).is_err());
        // Negative control: the former path-based creation writes outside.
        std::fs::create_dir(path.join("path-based-control"))?;
        assert!(outside.join("path-based-control").is_dir());
        drop(next);
        Ok(())
    }

    #[test]
    fn failed_reap_refuses_a_new_root_without_removing_its_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = tempfile::tempdir()?;
        let admission = crate::hold_root(fixture.path())?;
        let dead = create_root(&admission, "dead")?;
        std::fs::write(fixture.path().join("dead/authored"), b"retained")?;
        drop(dead);
        assert!(create_root(&admission, "next").is_err());
        assert!(!fixture.path().join("next").exists());
        assert!(fixture.path().join("dead.owner").exists());
        assert_eq!(
            std::fs::read(fixture.path().join("dead/authored"))?,
            b"retained"
        );
        Ok(())
    }
}

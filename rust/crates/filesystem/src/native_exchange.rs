//! Crash-recoverable whole-tree publication for native consumers.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Durable whole-tree exchange phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum NativeExchangePhase {
    /// Excluded paths are moving from the live tree into the prepared tree.
    Carrying,
    /// Whole-tree exchange is in flight.
    Exchanging,
}

/// Complete recovery state for one whole-tree publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativeExchangeJournal {
    /// Current live root.
    pub live: PathBuf,
    /// Prepared tree before publication and displaced tree after publication.
    pub prepared: PathBuf,
    /// Exact relative paths carried from the live tree.
    pub carried: Vec<PathBuf>,
    /// Stable identities of the live and prepared roots before exchange.
    pub roots: [[u8; 16]; 2],
    /// Current durable phase.
    pub phase: NativeExchangePhase,
}

/// Result of a successful or recovered exchange.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeExchangeOutcome {
    /// Whether the prepared tree was published.
    pub published: bool,
    /// Complete displaced tree when publication completed.
    pub displaced: Option<PathBuf>,
}

/// Whole-tree exchange failure.
#[derive(Debug, Error)]
pub enum NativeExchangeError {
    /// Paths are not two distinct sibling directories.
    #[error("native exchange paths must be distinct sibling directories")]
    InvalidLayout,
    /// Durable journal is malformed or does not match its requested roots.
    #[error("native exchange journal is incompatible")]
    IncompatibleJournal,
    /// Both copies of one carried path exist, so recovery cannot discard either.
    #[error("both copies of carried path exist: {0}")]
    CarriedPathConflict(PathBuf),
    /// Native filesystem operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Journal serialization failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Exchanges a prepared sibling tree with a live root.
///
/// The journal is durable before each mutation. Excluded paths are moved into
/// the prepared tree first, preserving arbitrary nested names and native path
/// encoding. On success `prepared` names the complete displaced tree.
pub fn publish_native_exchange(
    journal_path: &Path,
    live: &Path,
    prepared: &Path,
    carried: Vec<PathBuf>,
) -> Result<NativeExchangeOutcome, NativeExchangeError> {
    validate_layout(live, prepared)?;
    if journal_path.exists() {
        let recovered = recover_native_exchange(journal_path)?;
        if recovered.published {
            return Ok(recovered);
        }
    }
    let mut journal = NativeExchangeJournal {
        live: live.to_path_buf(),
        prepared: prepared.to_path_buf(),
        carried,
        roots: [root_identity(live)?, root_identity(prepared)?],
        phase: NativeExchangePhase::Carrying,
    };
    write_journal(journal_path, &journal)?;
    if let Err(error) = carry(live, prepared, &journal.carried) {
        move_back(prepared, live, &journal.carried)?;
        remove_journal(journal_path)?;
        return Err(error);
    }
    journal.phase = NativeExchangePhase::Exchanging;
    write_journal(journal_path, &journal)?;
    if let Err(error) = exchange(live, prepared) {
        return match recover_native_exchange(journal_path) {
            Ok(outcome) if outcome.published => Ok(outcome),
            Ok(_) => Err(error),
            Err(recovery) => Err(recovery),
        };
    }
    remove_journal(journal_path)?;
    Ok(NativeExchangeOutcome {
        published: true,
        displaced: Some(prepared.to_path_buf()),
    })
}

/// Exchanges two existing sibling filesystem entries without flattening
/// either entry's metadata, links, sparse allocation, or native name.
///
/// Linux and macOS perform one atomic kernel exchange. Windows uses the same
/// recoverable scratch convention as whole-tree publication and refuses to
/// overwrite a scratch entry left by an interrupted caller.
pub fn exchange_native_entries(left: &Path, right: &Path) -> Result<(), NativeExchangeError> {
    if left == right || left.parent().is_none() || right.parent().is_none() {
        return Err(NativeExchangeError::InvalidLayout);
    }
    if !entry_exists(left)? || !entry_exists(right)? {
        return Err(NativeExchangeError::InvalidLayout);
    }
    exchange(left, right)
}

/// Recovers one interrupted whole-tree publication.
pub fn recover_native_exchange(
    journal_path: &Path,
) -> Result<NativeExchangeOutcome, NativeExchangeError> {
    let journal: NativeExchangeJournal = serde_json::from_slice(&std::fs::read(journal_path)?)?;
    validate_layout(&journal.live, &journal.prepared)?;
    match journal.phase {
        NativeExchangePhase::Carrying => {
            move_back(&journal.prepared, &journal.live, &journal.carried)?;
            remove_journal(journal_path)?;
            Ok(NativeExchangeOutcome {
                published: false,
                displaced: None,
            })
        }
        NativeExchangePhase::Exchanging => recover_exchange(journal_path, &journal),
    }
}

fn validate_layout(live: &Path, prepared: &Path) -> Result<(), NativeExchangeError> {
    if live == prepared || live.parent().is_none() || live.parent() != prepared.parent() {
        return Err(NativeExchangeError::InvalidLayout);
    }
    Ok(())
}

fn carry(from: &Path, into: &Path, relative: &[PathBuf]) -> Result<(), NativeExchangeError> {
    for path in relative {
        if path.is_absolute()
            || path
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir))
        {
            return Err(NativeExchangeError::IncompatibleJournal);
        }
        let source = from.join(path);
        if !entry_exists(&source)? {
            continue;
        }
        let destination = into.join(path);
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        remove_entry(&destination)?;
        std::fs::rename(source, destination)?;
    }
    Ok(())
}

fn move_back(from: &Path, into: &Path, relative: &[PathBuf]) -> Result<(), NativeExchangeError> {
    for path in relative {
        let source = from.join(path);
        if !entry_exists(&source)? {
            continue;
        }
        let destination = into.join(path);
        if entry_exists(&destination)? {
            return Err(NativeExchangeError::CarriedPathConflict(path.clone()));
        }
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::rename(source, destination)?;
    }
    Ok(())
}

fn write_journal(path: &Path, journal: &NativeExchangeJournal) -> Result<(), NativeExchangeError> {
    use std::io::Write;
    let temporary = path.with_extension("next");
    let mut file = std::fs::File::create(&temporary)?;
    file.write_all(&serde_json::to_vec(journal)?)?;
    file.sync_all()?;
    drop(file);
    durable_rename(&temporary, path, true)?;
    Ok(())
}

fn remove_journal(path: &Path) -> Result<(), NativeExchangeError> {
    match std::fs::remove_file(path) {
        Ok(()) => sync_parent(path).map_err(Into::into),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn entry_exists(path: &Path) -> Result<bool, std::io::Error> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn root_identity(path: &Path) -> Result<[u8; 16], NativeExchangeError> {
    let root = cap_std::fs::Dir::open_ambient_dir(path, cap_std::ambient_authority())?;
    Ok(crate::NativeRootIdentity::from_metadata(&root.dir_metadata()?)?.to_bytes())
}

fn remove_entry(path: &Path) -> Result<(), std::io::Error> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    }
}

#[cfg(unix)]
fn durable_rename(from: &Path, to: &Path, _replace: bool) -> std::io::Result<()> {
    std::fs::rename(from, to)?;
    sync_parent(to)?;
    if from.parent() != to.parent() {
        sync_parent(from)?;
    }
    Ok(())
}

#[cfg(windows)]
#[allow(unsafe_code, reason = "MoveFileExW receives terminated UTF-16 paths")]
fn durable_rename(from: &Path, to: &Path, replace: bool) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows::Win32::Storage::FileSystem::{MOVE_FILE_FLAGS, MoveFileExW};
    use windows::core::PCWSTR;
    let from: Vec<u16> = from.as_os_str().encode_wide().chain([0]).collect();
    let to: Vec<u16> = to.as_os_str().encode_wide().chain([0]).collect();
    let mut flags = MOVE_FILE_FLAGS(0x8);
    if replace {
        flags |= MOVE_FILE_FLAGS(0x1);
    }
    unsafe { MoveFileExW(PCWSTR(from.as_ptr()), PCWSTR(to.as_ptr()), flags) }
        .map_err(|error| std::io::Error::from_raw_os_error(error.code().0))
}

fn sync_parent(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(
            path.parent()
                .ok_or_else(|| std::io::Error::other("path has no parent"))?,
        )?
        .sync_all()
    }
    #[cfg(windows)]
    {
        let _ = path;
        Ok(())
    }
}

#[cfg(windows)]
fn scratch(live: &Path) -> Result<PathBuf, NativeExchangeError> {
    let parent = live.parent().ok_or(NativeExchangeError::InvalidLayout)?;
    let name = live.file_name().ok_or(NativeExchangeError::InvalidLayout)?;
    Ok(parent.join(format!(".{}.acyclic-exchange", name.to_string_lossy())))
}

#[cfg(windows)]
fn exchange(live: &Path, prepared: &Path) -> Result<(), NativeExchangeError> {
    let scratch = scratch(live)?;
    if entry_exists(&scratch)? {
        return Err(NativeExchangeError::IncompatibleJournal);
    }
    durable_rename(live, &scratch, false)?;
    if let Err(error) = durable_rename(prepared, live, false) {
        let _ = durable_rename(&scratch, live, false);
        return Err(error.into());
    }
    durable_rename(&scratch, prepared, false)?;
    Ok(())
}

#[cfg(windows)]
fn recover_exchange(
    path: &Path,
    journal: &NativeExchangeJournal,
) -> Result<NativeExchangeOutcome, NativeExchangeError> {
    let scratch = scratch(&journal.live)?;
    let live = entry_exists(&journal.live)?;
    let prepared = entry_exists(&journal.prepared)?;
    let scratch_exists = entry_exists(&scratch)?;
    let published = live && scratch_exists && !prepared;
    if !live && scratch_exists {
        move_back(&journal.prepared, &scratch, &journal.carried)?;
        durable_rename(&scratch, &journal.live, false)?;
    } else if live && scratch_exists && !prepared {
        durable_rename(&scratch, &journal.prepared, false)?;
    } else if !live && prepared && !scratch_exists {
        durable_rename(&journal.prepared, &journal.live, false)?;
    } else if live && prepared && !scratch_exists {
        move_back(&journal.prepared, &journal.live, &journal.carried)?;
    } else if !(live && !prepared && !scratch_exists) {
        return Err(NativeExchangeError::IncompatibleJournal);
    }
    remove_journal(path)?;
    Ok(NativeExchangeOutcome {
        published,
        displaced: published.then_some(journal.prepared.clone()),
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn recover_exchange(
    path: &Path,
    journal: &NativeExchangeJournal,
) -> Result<NativeExchangeOutcome, NativeExchangeError> {
    let live = entry_exists(&journal.live)?;
    let prepared = entry_exists(&journal.prepared)?;
    if live && prepared {
        let observed = [
            root_identity(&journal.live)?,
            root_identity(&journal.prepared)?,
        ];
        let published = observed == [journal.roots[1], journal.roots[0]];
        if !published && observed != journal.roots {
            return Err(NativeExchangeError::IncompatibleJournal);
        }
        if !published {
            move_back(&journal.prepared, &journal.live, &journal.carried)?;
        }
        remove_journal(path)?;
        return Ok(NativeExchangeOutcome {
            published,
            displaced: published.then_some(journal.prepared.clone()),
        });
    }
    Err(NativeExchangeError::IncompatibleJournal)
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code, reason = "renamex_np receives two live C paths")]
fn exchange(live: &Path, prepared: &Path) -> Result<(), NativeExchangeError> {
    use std::os::unix::ffi::OsStrExt;
    let live = std::ffi::CString::new(live.as_os_str().as_bytes())
        .map_err(|_| NativeExchangeError::InvalidLayout)?;
    let prepared = std::ffi::CString::new(prepared.as_os_str().as_bytes())
        .map_err(|_| NativeExchangeError::InvalidLayout)?;
    if unsafe { libc::renamex_np(live.as_ptr(), prepared.as_ptr(), libc::RENAME_SWAP) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code, reason = "renameat2 receives two live C paths")]
fn exchange(live: &Path, prepared: &Path) -> Result<(), NativeExchangeError> {
    use std::os::unix::ffi::OsStrExt;
    let live = std::ffi::CString::new(live.as_os_str().as_bytes())
        .map_err(|_| NativeExchangeError::InvalidLayout)?;
    let prepared = std::ffi::CString::new(prepared.as_os_str().as_bytes())
        .map_err(|_| NativeExchangeError::InvalidLayout)?;
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            live.as_ptr(),
            libc::AT_FDCWD,
            prepared.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn recovery_recognizes_an_exchange_completed_before_journal_removal() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir(&live).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        std::fs::write(live.join("old"), b"old").expect("old file");
        std::fs::write(prepared.join("new"), b"new").expect("new file");
        let journal_path = temporary.path().join("exchange.json");
        let journal = NativeExchangeJournal {
            live: live.clone(),
            prepared: prepared.clone(),
            carried: Vec::new(),
            roots: [
                root_identity(&live).expect("live identity"),
                root_identity(&prepared).expect("prepared identity"),
            ],
            phase: NativeExchangePhase::Exchanging,
        };
        write_journal(&journal_path, &journal).expect("journal");
        exchange(&live, &prepared).expect("exchange");

        let outcome = recover_native_exchange(&journal_path).expect("recover publication");
        assert!(outcome.published);
        assert_eq!(outcome.displaced.as_deref(), Some(prepared.as_path()));
        assert_eq!(std::fs::read(live.join("new")).expect("new"), b"new");
        assert_eq!(std::fs::read(prepared.join("old")).expect("old"), b"old");
        assert!(!journal_path.exists());
    }

    #[test]
    fn exchange_publishes_whole_tree_and_carries_nested_exclusion() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir_all(live.join("private/nested")).expect("live tree");
        std::fs::create_dir_all(&prepared).expect("prepared tree");
        std::fs::write(live.join("old"), b"old").expect("old file");
        std::fs::write(live.join("private/nested/key"), b"secret").expect("excluded file");
        std::fs::write(prepared.join("new"), b"new").expect("new file");
        let journal = temporary.path().join("exchange.json");
        let outcome = publish_native_exchange(
            &journal,
            &live,
            &prepared,
            vec![PathBuf::from("private/nested")],
        )
        .expect("publish");
        assert!(outcome.published);
        assert_eq!(std::fs::read(live.join("new")).expect("new"), b"new");
        assert_eq!(
            std::fs::read(live.join("private/nested/key")).expect("excluded"),
            b"secret"
        );
        assert_eq!(std::fs::read(prepared.join("old")).expect("old"), b"old");
        assert!(!journal.exists());
    }

    #[test]
    fn entry_exchange_preserves_both_complete_entries() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let left = temporary.path().join("left");
        let right = temporary.path().join("right");
        std::fs::create_dir(&left).expect("left tree");
        std::fs::create_dir(&right).expect("right tree");
        std::fs::write(left.join("value"), b"left").expect("left value");
        std::fs::write(right.join("value"), b"right").expect("right value");
        exchange_native_entries(&left, &right).expect("exchange entries");
        assert_eq!(std::fs::read(left.join("value")).expect("left"), b"right");
        assert_eq!(std::fs::read(right.join("value")).expect("right"), b"left");
    }
}

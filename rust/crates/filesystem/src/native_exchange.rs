//! Crash-recoverable whole-tree publication for native consumers.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

use acyclic_native_runtime::durable_rename;

const LEGACY_NATIVE_EXCHANGE_JOURNAL_VERSION: u32 = 1;
const IDENTITY_NATIVE_EXCHANGE_JOURNAL_VERSION: u32 = 2;
const NATIVE_EXCHANGE_JOURNAL_VERSION: u32 = 3;

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
    /// Version of the durable journal encoding.
    #[serde(default = "legacy_journal_version")]
    pub version: u32,
    /// Stable identity of the caller-visible publication request.
    #[serde(default = "legacy_operation")]
    pub operation: crate::IdempotencyKey,
    /// Opaque caller recovery bytes retained with the durable request.
    #[serde(default)]
    pub recovery: Vec<u8>,
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
    /// Opaque consumer state stored with the recovered publication request.
    pub recovery: Vec<u8>,
}

/// Persists an exchange request before a caller changes any related durable
/// state. Retrying with the same request is idempotent; a different request at
/// the same journal path fails closed.
///
/// This is the coordination boundary for consumers that must first update a
/// logical workspace head and then publish its already materialized native
/// tree. [`publish_native_exchange`] consumes the prepared journal.
pub fn prepare_native_exchange(
    journal_path: &Path,
    live: &Path,
    prepared: &Path,
    operation: crate::IdempotencyKey,
    carried: Vec<PathBuf>,
) -> Result<(), NativeExchangeError> {
    prepare_native_exchange_with_recovery(
        journal_path,
        live,
        prepared,
        operation,
        carried,
        Vec::new(),
    )
}

/// Persists an exchange request with opaque bytes needed by consumer recovery.
pub fn prepare_native_exchange_with_recovery(
    journal_path: &Path,
    live: &Path,
    prepared: &Path,
    operation: crate::IdempotencyKey,
    carried: Vec<PathBuf>,
    recovery: Vec<u8>,
) -> Result<(), NativeExchangeError> {
    validate_layout(live, prepared)?;
    let journal = NativeExchangeJournal {
        version: NATIVE_EXCHANGE_JOURNAL_VERSION,
        operation,
        recovery,
        live: live.to_path_buf(),
        prepared: prepared.to_path_buf(),
        carried,
        roots: [root_identity(live)?, root_identity(prepared)?],
        phase: NativeExchangePhase::Carrying,
    };
    match read_journal(journal_path) {
        Ok(existing) if same_prepared_request(&existing, &journal) => Ok(()),
        Ok(_) => Err(NativeExchangeError::IncompatibleJournal),
        Err(NativeExchangeError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            write_journal(journal_path, &journal)
        }
        Err(error) => Err(error),
    }
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
    /// Durable journal was written by an unsupported format version.
    #[error("native exchange journal version {0} is unsupported")]
    UnsupportedJournalVersion(u32),
    /// A legacy in-flight exchange has no request identity and needs an explicit recovery choice.
    #[error("legacy native exchange outcome is ambiguous; recover it explicitly before retrying")]
    AmbiguousLegacyJournal,
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
    operation: crate::IdempotencyKey,
    carried: Vec<PathBuf>,
) -> Result<NativeExchangeOutcome, NativeExchangeError> {
    validate_layout(live, prepared)?;
    if journal_path.exists() {
        let journal = read_journal(journal_path)?;
        if journal.live != live || journal.prepared != prepared {
            return Err(NativeExchangeError::IncompatibleJournal);
        }
        if journal.version >= IDENTITY_NATIVE_EXCHANGE_JOURNAL_VERSION
            && (journal.operation != operation || journal.carried != carried)
        {
            return Err(NativeExchangeError::IncompatibleJournal);
        }
        if journal.version == LEGACY_NATIVE_EXCHANGE_JOURNAL_VERSION
            && journal.phase == NativeExchangePhase::Exchanging
        {
            return Err(NativeExchangeError::AmbiguousLegacyJournal);
        }
        let same_operation = journal.version >= IDENTITY_NATIVE_EXCHANGE_JOURNAL_VERSION
            && journal.operation == operation;
        let recovered = recover_native_exchange(journal_path)?;
        if recovered.published && same_operation {
            return Ok(recovered);
        }
    }
    prepare_native_exchange(journal_path, live, prepared, operation, carried)?;
    let mut journal = read_journal(journal_path)?;
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
        recovery: journal.recovery,
    })
}

fn same_prepared_request(
    existing: &NativeExchangeJournal,
    requested: &NativeExchangeJournal,
) -> bool {
    existing.version >= IDENTITY_NATIVE_EXCHANGE_JOURNAL_VERSION
        && existing.operation == requested.operation
        && existing.recovery == requested.recovery
        && existing.live == requested.live
        && existing.prepared == requested.prepared
        && existing.carried == requested.carried
        && existing.roots == requested.roots
        && existing.phase == requested.phase
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
    let journal = read_journal(journal_path)?;
    validate_layout(&journal.live, &journal.prepared)?;
    match journal.phase {
        NativeExchangePhase::Carrying => {
            move_back(&journal.prepared, &journal.live, &journal.carried)?;
            remove_journal(journal_path)?;
            Ok(NativeExchangeOutcome {
                published: false,
                displaced: None,
                recovery: journal.recovery,
            })
        }
        NativeExchangePhase::Exchanging => recover_exchange(journal_path, &journal),
    }
}

fn read_journal(path: &Path) -> Result<NativeExchangeJournal, NativeExchangeError> {
    let journal: NativeExchangeJournal = serde_json::from_slice(&std::fs::read(path)?)?;
    match journal.version {
        LEGACY_NATIVE_EXCHANGE_JOURNAL_VERSION
        | IDENTITY_NATIVE_EXCHANGE_JOURNAL_VERSION
        | NATIVE_EXCHANGE_JOURNAL_VERSION => Ok(journal),
        version => Err(NativeExchangeError::UnsupportedJournalVersion(version)),
    }
}

const fn legacy_journal_version() -> u32 {
    LEGACY_NATIVE_EXCHANGE_JOURNAL_VERSION
}

const fn legacy_operation() -> crate::IdempotencyKey {
    crate::IdempotencyKey::from_bytes([0; 16])
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
    durable_rename(
        &temporary,
        path,
        acyclic_native_runtime::RenameMode::Replace,
    )?;
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

fn sync_parent(path: &Path) -> std::io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("path has no parent"))?;
    acyclic_native_runtime::sync_parent(parent, acyclic_native_runtime::Durability::Full)
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
    durable_rename(
        live,
        &scratch,
        acyclic_native_runtime::RenameMode::NoReplace,
    )?;
    if let Err(error) = durable_rename(
        prepared,
        live,
        acyclic_native_runtime::RenameMode::NoReplace,
    ) {
        let _ = durable_rename(
            &scratch,
            live,
            acyclic_native_runtime::RenameMode::NoReplace,
        );
        return Err(error.into());
    }
    durable_rename(
        &scratch,
        prepared,
        acyclic_native_runtime::RenameMode::NoReplace,
    )?;
    Ok(())
}

#[cfg(windows)]
fn recover_exchange(
    path: &Path,
    journal: &NativeExchangeJournal,
) -> Result<NativeExchangeOutcome, NativeExchangeError> {
    let scratch = scratch(&journal.live)?;
    let live = entry_exists(&journal.live)?
        .then(|| root_identity(&journal.live))
        .transpose()?;
    let prepared = entry_exists(&journal.prepared)?
        .then(|| root_identity(&journal.prepared))
        .transpose()?;
    let scratch_identity = entry_exists(&scratch)?
        .then(|| root_identity(&scratch))
        .transpose()?;
    let original = journal.roots[0];
    let replacement = journal.roots[1];
    let displaced = if live == Some(original)
        && prepared == Some(replacement)
        && scratch_identity.is_none()
    {
        move_back(&journal.prepared, &journal.live, &journal.carried)?;
        None
    } else if live.is_none() && prepared == Some(replacement) && scratch_identity == Some(original)
    {
        move_back(&journal.prepared, &scratch, &journal.carried)?;
        durable_rename(
            &scratch,
            &journal.live,
            acyclic_native_runtime::RenameMode::NoReplace,
        )?;
        None
    } else if live == Some(replacement) && prepared.is_none() && scratch_identity == Some(original)
    {
        durable_rename(
            &scratch,
            &journal.prepared,
            acyclic_native_runtime::RenameMode::NoReplace,
        )?;
        Some(journal.prepared.clone())
    } else if live == Some(replacement) && prepared == Some(original) && scratch_identity.is_none()
    {
        Some(journal.prepared.clone())
    } else if live == Some(replacement) && scratch_identity.is_none() {
        None
    } else {
        return Err(NativeExchangeError::IncompatibleJournal);
    };
    remove_journal(path)?;
    Ok(NativeExchangeOutcome {
        published: live == Some(replacement),
        displaced,
        recovery: journal.recovery.clone(),
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn recover_exchange(
    path: &Path,
    journal: &NativeExchangeJournal,
) -> Result<NativeExchangeOutcome, NativeExchangeError> {
    let live = entry_exists(&journal.live)?
        .then(|| root_identity(&journal.live))
        .transpose()?;
    let prepared = entry_exists(&journal.prepared)?
        .then(|| root_identity(&journal.prepared))
        .transpose()?;
    let (published, displaced) = match (live, prepared) {
        (Some(live), Some(prepared)) if [live, prepared] == journal.roots => {
            move_back(&journal.prepared, &journal.live, &journal.carried)?;
            (false, None)
        }
        (Some(live), Some(prepared))
            if [live, prepared] == [journal.roots[1], journal.roots[0]] =>
        {
            (true, Some(journal.prepared.clone()))
        }
        (Some(live), _) if live == journal.roots[1] => (true, None),
        _ => return Err(NativeExchangeError::IncompatibleJournal),
    };
    remove_journal(path)?;
    Ok(NativeExchangeOutcome {
        published,
        displaced,
        recovery: journal.recovery.clone(),
    })
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
            version: NATIVE_EXCHANGE_JOURNAL_VERSION,
            operation: crate::IdempotencyKey::from_bytes([1; 16]),
            recovery: vec![7; 16],
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

        let outcome = publish_native_exchange(
            &journal_path,
            &live,
            &prepared,
            journal.operation,
            Vec::new(),
        )
        .expect("recover publication retry");
        assert!(outcome.published);
        assert_eq!(outcome.displaced.as_deref(), Some(prepared.as_path()));
        assert_eq!(outcome.recovery, vec![7; 16]);
        assert_eq!(std::fs::read(live.join("new")).expect("new"), b"new");
        assert_eq!(std::fs::read(prepared.join("old")).expect("old"), b"old");
        assert!(!journal_path.exists());
    }

    #[test]
    fn prepared_exchange_is_idempotent_and_rejects_other_requests() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir(&live).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        let journal_path = temporary.path().join("exchange.json");
        let operation = crate::IdempotencyKey::from_bytes([12; 16]);
        let carried = vec![PathBuf::from("private")];

        prepare_native_exchange_with_recovery(
            &journal_path,
            &live,
            &prepared,
            operation,
            carried.clone(),
            vec![21; 32],
        )
        .expect("prepare exchange");
        prepare_native_exchange_with_recovery(
            &journal_path,
            &live,
            &prepared,
            operation,
            carried.clone(),
            vec![21; 32],
        )
        .expect("retry preparation");

        let journal = read_journal(&journal_path).expect("read journal");
        assert_eq!(journal.operation, operation);
        assert_eq!(journal.carried, carried);
        assert_eq!(journal.recovery, vec![21; 32]);
        assert_eq!(journal.phase, NativeExchangePhase::Carrying);
        assert!(matches!(
            prepare_native_exchange(
                &journal_path,
                &live,
                &prepared,
                crate::IdempotencyKey::from_bytes([13; 16]),
                Vec::new(),
            ),
            Err(NativeExchangeError::IncompatibleJournal)
        ));
        assert!(matches!(
            publish_native_exchange(
                &journal_path,
                &live,
                &prepared,
                crate::IdempotencyKey::from_bytes([13; 16]),
                Vec::new(),
            ),
            Err(NativeExchangeError::IncompatibleJournal)
        ));
        assert_eq!(
            read_journal(&journal_path)
                .expect("preserved journal")
                .operation,
            operation
        );
    }

    #[test]
    fn prior_identity_journal_retries_without_reencoding() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir(&live).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        let journal_path = temporary.path().join("exchange.json");
        let operation = crate::IdempotencyKey::from_bytes([14; 16]);
        let journal = NativeExchangeJournal {
            version: IDENTITY_NATIVE_EXCHANGE_JOURNAL_VERSION,
            operation,
            recovery: Vec::new(),
            live: live.clone(),
            prepared: prepared.clone(),
            carried: Vec::new(),
            roots: [
                root_identity(&live).expect("live identity"),
                root_identity(&prepared).expect("prepared identity"),
            ],
            phase: NativeExchangePhase::Carrying,
        };
        write_journal(&journal_path, &journal).expect("journal");

        prepare_native_exchange(&journal_path, &live, &prepared, operation, Vec::new())
            .expect("compatible retry");
        assert_eq!(
            read_journal(&journal_path).expect("journal").version,
            IDENTITY_NATIVE_EXCHANGE_JOURNAL_VERSION
        );
    }

    #[test]
    fn publication_rejects_a_journal_for_different_roots() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let first_live = temporary.path().join("first-live");
        let first_prepared = temporary.path().join("first-prepared");
        let second_live = temporary.path().join("second-live");
        let second_prepared = temporary.path().join("second-prepared");
        for path in [&first_live, &first_prepared, &second_live, &second_prepared] {
            std::fs::create_dir(path).expect("exchange tree");
        }
        std::fs::write(second_live.join("old"), b"old").expect("old file");
        std::fs::write(second_prepared.join("new"), b"new").expect("new file");
        let journal_path = temporary.path().join("exchange.json");
        write_journal(
            &journal_path,
            &NativeExchangeJournal {
                version: NATIVE_EXCHANGE_JOURNAL_VERSION,
                operation: crate::IdempotencyKey::from_bytes([5; 16]),
                recovery: Vec::new(),
                live: first_live.clone(),
                prepared: first_prepared.clone(),
                carried: Vec::new(),
                roots: [
                    root_identity(&first_live).expect("live identity"),
                    root_identity(&first_prepared).expect("prepared identity"),
                ],
                phase: NativeExchangePhase::Carrying,
            },
        )
        .expect("journal");

        assert!(matches!(
            publish_native_exchange(
                &journal_path,
                &second_live,
                &second_prepared,
                crate::IdempotencyKey::from_bytes([6; 16]),
                Vec::new(),
            ),
            Err(NativeExchangeError::IncompatibleJournal)
        ));
        assert_eq!(std::fs::read(second_live.join("old")).expect("old"), b"old");
        assert_eq!(
            std::fs::read(second_prepared.join("new")).expect("new"),
            b"new"
        );
        assert!(journal_path.exists());
    }

    #[test]
    fn legacy_journal_without_version_or_operation_recovers() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir(&live).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        let journal_path = temporary.path().join("exchange.json");
        let mut encoded = serde_json::to_value(NativeExchangeJournal {
            version: NATIVE_EXCHANGE_JOURNAL_VERSION,
            operation: crate::IdempotencyKey::from_bytes([9; 16]),
            recovery: Vec::new(),
            live: live.clone(),
            prepared: prepared.clone(),
            carried: Vec::new(),
            roots: [
                root_identity(&live).expect("live identity"),
                root_identity(&prepared).expect("prepared identity"),
            ],
            phase: NativeExchangePhase::Carrying,
        })
        .expect("encode journal");
        let object = encoded.as_object_mut().expect("journal object");
        object.remove("version");
        object.remove("operation");
        std::fs::write(
            &journal_path,
            serde_json::to_vec(&encoded).expect("encode legacy journal"),
        )
        .expect("write legacy journal");

        let outcome = recover_native_exchange(&journal_path).expect("recover legacy journal");
        assert!(!outcome.published);
        assert!(outcome.displaced.is_none());
        assert!(live.exists());
        assert!(prepared.exists());
        assert!(!journal_path.exists());
    }

    #[test]
    fn future_journal_version_fails_closed_without_mutation() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir(&live).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        std::fs::write(live.join("old"), b"old").expect("old file");
        std::fs::write(prepared.join("new"), b"new").expect("new file");
        let journal_path = temporary.path().join("exchange.json");
        let journal = NativeExchangeJournal {
            version: NATIVE_EXCHANGE_JOURNAL_VERSION + 1,
            operation: crate::IdempotencyKey::from_bytes([10; 16]),
            recovery: Vec::new(),
            live: live.clone(),
            prepared: prepared.clone(),
            carried: Vec::new(),
            roots: [
                root_identity(&live).expect("live identity"),
                root_identity(&prepared).expect("prepared identity"),
            ],
            phase: NativeExchangePhase::Carrying,
        };
        std::fs::write(
            &journal_path,
            serde_json::to_vec(&journal).expect("encode future journal"),
        )
        .expect("write future journal");

        assert!(matches!(
            publish_native_exchange(
                &journal_path,
                &live,
                &prepared,
                crate::IdempotencyKey::from_bytes([11; 16]),
                Vec::new(),
            ),
            Err(NativeExchangeError::UnsupportedJournalVersion(version))
                if version == NATIVE_EXCHANGE_JOURNAL_VERSION + 1
        ));
        assert_eq!(std::fs::read(live.join("old")).expect("old"), b"old");
        assert_eq!(std::fs::read(prepared.join("new")).expect("new"), b"new");
        assert!(journal_path.exists());
    }

    #[cfg(windows)]
    #[test]
    fn windows_recovery_rolls_back_after_live_moves_to_scratch() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir_all(live.join("private")).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        std::fs::write(live.join("old"), b"old").expect("old file");
        std::fs::write(live.join("private/key"), b"secret").expect("carried file");
        std::fs::write(prepared.join("new"), b"new").expect("new file");
        let journal_path = temporary.path().join("exchange.json");
        let mut journal = NativeExchangeJournal {
            version: NATIVE_EXCHANGE_JOURNAL_VERSION,
            operation: crate::IdempotencyKey::from_bytes([7; 16]),
            recovery: Vec::new(),
            live: live.clone(),
            prepared: prepared.clone(),
            carried: vec![PathBuf::from("private")],
            roots: [
                root_identity(&live).expect("live identity"),
                root_identity(&prepared).expect("prepared identity"),
            ],
            phase: NativeExchangePhase::Carrying,
        };
        write_journal(&journal_path, &journal).expect("carrying journal");
        carry(&live, &prepared, &journal.carried).expect("carry exclusion");
        journal.phase = NativeExchangePhase::Exchanging;
        write_journal(&journal_path, &journal).expect("exchange journal");
        let scratch = scratch(&live).expect("scratch path");
        durable_rename(
            &live,
            &scratch,
            acyclic_native_runtime::RenameMode::NoReplace,
        )
        .expect("move live to scratch");

        let outcome = recover_native_exchange(&journal_path).expect("roll back exchange");
        assert!(!outcome.published);
        assert!(outcome.displaced.is_none());
        assert_eq!(std::fs::read(live.join("old")).expect("old"), b"old");
        assert_eq!(
            std::fs::read(live.join("private/key")).expect("carried"),
            b"secret"
        );
        assert_eq!(std::fs::read(prepared.join("new")).expect("new"), b"new");
        assert!(!scratch.exists());
        assert!(!journal_path.exists());
    }

    #[cfg(windows)]
    #[test]
    fn windows_recovery_finishes_after_replacement_moves_to_live() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir_all(live.join("private")).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        std::fs::write(live.join("old"), b"old").expect("old file");
        std::fs::write(live.join("private/key"), b"secret").expect("carried file");
        std::fs::write(prepared.join("new"), b"new").expect("new file");
        let journal_path = temporary.path().join("exchange.json");
        let mut journal = NativeExchangeJournal {
            version: NATIVE_EXCHANGE_JOURNAL_VERSION,
            operation: crate::IdempotencyKey::from_bytes([8; 16]),
            recovery: Vec::new(),
            live: live.clone(),
            prepared: prepared.clone(),
            carried: vec![PathBuf::from("private")],
            roots: [
                root_identity(&live).expect("live identity"),
                root_identity(&prepared).expect("prepared identity"),
            ],
            phase: NativeExchangePhase::Carrying,
        };
        write_journal(&journal_path, &journal).expect("carrying journal");
        carry(&live, &prepared, &journal.carried).expect("carry exclusion");
        journal.phase = NativeExchangePhase::Exchanging;
        write_journal(&journal_path, &journal).expect("exchange journal");
        let scratch = scratch(&live).expect("scratch path");
        durable_rename(
            &live,
            &scratch,
            acyclic_native_runtime::RenameMode::NoReplace,
        )
        .expect("move live to scratch");
        durable_rename(
            &prepared,
            &live,
            acyclic_native_runtime::RenameMode::NoReplace,
        )
        .expect("publish replacement");

        let outcome = recover_native_exchange(&journal_path).expect("finish exchange");
        assert!(outcome.published);
        assert_eq!(outcome.displaced.as_deref(), Some(prepared.as_path()));
        assert_eq!(std::fs::read(live.join("new")).expect("new"), b"new");
        assert_eq!(
            std::fs::read(live.join("private/key")).expect("carried"),
            b"secret"
        );
        assert_eq!(std::fs::read(prepared.join("old")).expect("old"), b"old");
        assert!(!scratch.exists());
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
            crate::IdempotencyKey::from_bytes([2; 16]),
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
    fn stale_completed_journal_does_not_suppress_the_next_publication() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir(&live).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        std::fs::write(live.join("first"), b"first").expect("first live file");
        std::fs::write(prepared.join("second"), b"second").expect("first prepared file");
        let journal_path = temporary.path().join("exchange.json");
        let stale = NativeExchangeJournal {
            version: NATIVE_EXCHANGE_JOURNAL_VERSION,
            operation: crate::IdempotencyKey::from_bytes([3; 16]),
            recovery: Vec::new(),
            live: live.clone(),
            prepared: prepared.clone(),
            carried: Vec::new(),
            roots: [
                root_identity(&live).expect("live identity"),
                root_identity(&prepared).expect("prepared identity"),
            ],
            phase: NativeExchangePhase::Exchanging,
        };
        write_journal(&journal_path, &stale).expect("stale journal");
        exchange(&live, &prepared).expect("completed first exchange");

        let recovered = recover_native_exchange(&journal_path).expect("recover completed exchange");
        assert!(recovered.published);
        remove_entry(&prepared).expect("remove displaced tree");
        std::fs::create_dir(&prepared).expect("next prepared tree");
        std::fs::write(prepared.join("third"), b"third").expect("next prepared file");

        let outcome = publish_native_exchange(
            &journal_path,
            &live,
            &prepared,
            crate::IdempotencyKey::from_bytes([4; 16]),
            Vec::new(),
        )
        .expect("publish after stale completed journal");
        assert!(outcome.published);
        assert_eq!(std::fs::read(live.join("third")).expect("third"), b"third");
        assert_eq!(
            std::fs::read(prepared.join("second")).expect("second"),
            b"second"
        );
        assert!(!journal_path.exists());
    }

    #[test]
    fn completed_legacy_journal_requires_explicit_recovery_before_new_publication() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir(&live).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        std::fs::write(live.join("first"), b"first").expect("first live file");
        std::fs::write(prepared.join("second"), b"second").expect("first prepared file");
        let journal_path = temporary.path().join("exchange.json");
        let mut encoded = serde_json::to_value(NativeExchangeJournal {
            version: NATIVE_EXCHANGE_JOURNAL_VERSION,
            operation: crate::IdempotencyKey::from_bytes([12; 16]),
            recovery: Vec::new(),
            live: live.clone(),
            prepared: prepared.clone(),
            carried: Vec::new(),
            roots: [
                root_identity(&live).expect("live identity"),
                root_identity(&prepared).expect("prepared identity"),
            ],
            phase: NativeExchangePhase::Exchanging,
        })
        .expect("encode journal");
        let object = encoded.as_object_mut().expect("journal object");
        object.remove("version");
        object.remove("operation");
        std::fs::write(
            &journal_path,
            serde_json::to_vec(&encoded).expect("encode legacy journal"),
        )
        .expect("write legacy journal");
        exchange(&live, &prepared).expect("completed first exchange");
        std::fs::remove_file(prepared.join("first")).expect("replace displaced contents");
        std::fs::write(prepared.join("third"), b"third").expect("next prepared file");

        let operation = crate::IdempotencyKey::from_bytes([13; 16]);
        assert!(matches!(
            publish_native_exchange(&journal_path, &live, &prepared, operation, Vec::new(),),
            Err(NativeExchangeError::AmbiguousLegacyJournal)
        ));
        assert_eq!(
            std::fs::read(live.join("second")).expect("second"),
            b"second"
        );
        assert_eq!(
            std::fs::read(prepared.join("third")).expect("third"),
            b"third"
        );
        assert!(journal_path.exists());

        let recovered = recover_native_exchange(&journal_path).expect("recover legacy exchange");
        assert!(recovered.published);
        assert_eq!(recovered.displaced.as_deref(), Some(prepared.as_path()));
        remove_entry(&prepared).expect("remove recovered displaced tree");
        std::fs::create_dir(&prepared).expect("rebuild prepared tree");
        std::fs::write(prepared.join("third"), b"third").expect("rebuilt prepared file");
        let outcome =
            publish_native_exchange(&journal_path, &live, &prepared, operation, Vec::new())
                .expect("publish after explicit legacy recovery");
        assert!(outcome.published);
        assert_eq!(std::fs::read(live.join("third")).expect("third"), b"third");
        assert_eq!(
            std::fs::read(prepared.join("second")).expect("second"),
            b"second"
        );
        assert!(!journal_path.exists());
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

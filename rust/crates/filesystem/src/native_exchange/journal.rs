use super::*;

pub(super) const NATIVE_EXCHANGE_JOURNAL_VERSION: u32 = 1;

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
    pub version: u32,
    /// Stable identity of the caller-visible publication request.
    pub operation: crate::IdempotencyKey,
    /// Opaque caller recovery bytes retained with the durable request.
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
    /// Both copies of one carried path exist, so recovery cannot discard either.
    #[error("both copies of carried path exist: {0}")]
    CarriedPathConflict(PathBuf),
    /// Native filesystem operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Cancellation stopped the next native boundary.
    #[error("native exchange cancelled")]
    Cancelled,
    /// The next operation exceeded its finite allowance.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
}

/// Persists an exchange request with opaque bytes needed by consumer recovery.
pub fn prepare_native_exchange(
    journal_path: &Path,
    live: &Path,
    prepared: &Path,
    operation: crate::IdempotencyKey,
    carried: Vec<PathBuf>,
    recovery: Vec<u8>,
) -> Result<(), NativeExchangeError> {
    validate_layout(live, prepared)?;
    validate_carried_paths(&carried)?;
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
        Ok(existing) if existing == journal => Ok(()),
        Ok(_) => Err(NativeExchangeError::IncompatibleJournal),
        Err(NativeExchangeError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            write_journal(journal_path, &journal)
        }
        Err(error) => Err(error),
    }
}

pub(super) fn read_journal(path: &Path) -> Result<NativeExchangeJournal, NativeExchangeError> {
    let journal: NativeExchangeJournal =
        serde_json::from_slice(&std::fs::read(path)?).map_err(std::io::Error::other)?;
    match journal.version {
        NATIVE_EXCHANGE_JOURNAL_VERSION => {
            validate_carried_paths(&journal.carried)?;
            Ok(journal)
        }
        version => Err(NativeExchangeError::UnsupportedJournalVersion(version)),
    }
}
pub(super) fn validate_layout(live: &Path, prepared: &Path) -> Result<(), NativeExchangeError> {
    if live == prepared || live.parent().is_none() || live.parent() != prepared.parent() {
        return Err(NativeExchangeError::InvalidLayout);
    }
    Ok(())
}
pub(super) fn validate_carried_paths(relative: &[PathBuf]) -> Result<(), NativeExchangeError> {
    for path in relative {
        if path.as_os_str().is_empty()
            || path
                .components()
                .any(|part| !matches!(part, std::path::Component::Normal(_)))
        {
            return Err(NativeExchangeError::IncompatibleJournal);
        }
    }
    Ok(())
}
pub(super) fn write_journal(
    path: &Path,
    journal: &NativeExchangeJournal,
) -> Result<(), NativeExchangeError> {
    use std::io::Write;
    let temporary = path.with_extension("next");
    let mut file = std::fs::File::create(&temporary)?;
    file.write_all(&serde_json::to_vec(journal).map_err(std::io::Error::other)?)?;
    file.sync_all()?;
    drop(file);
    durable_rename(
        &temporary,
        path,
        acyclic_native_runtime::RenameMode::Replace,
        None,
    )?;
    Ok(())
}
pub(super) fn remove_journal(path: &Path) -> Result<(), NativeExchangeError> {
    match std::fs::remove_file(path) {
        Ok(()) => sync_parent(path).map_err(Into::into),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

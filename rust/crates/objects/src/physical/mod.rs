use crate::body::{LocalBodyLocation, LocalBodyReference};
use crate::v1::{
    Error, NativeObjectsCause, NativeObjectsError, Publication, wire::ErrorCode::Unavailable,
};
use crate::{LocalDurability, LocalObjectsGarbageCollection};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};
const JOURNAL_FILE: &str = "mutations.log";
const MAXIMUM_INLINE_BODY_BYTES: usize = 64 * 1_024;
const SEGMENT_MAGIC: &[u8; 24] = b"ACYCLIC-OBJECT-SEGMENT\0\x02";
const SEGMENT_HEADER_BYTES: usize = SEGMENT_MAGIC.len() + 4;
const SEGMENT_RECORD_BYTES: usize = 32 + 8;
pub(crate) const MAXIMUM_SEGMENT_BODIES: usize = 1_024;
pub(crate) const MAXIMUM_SEGMENT_BYTES: usize = 4 * 1024 * 1024;
static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// One store's directory and a shared read handle on its journal, so a read of a journal-resident body does not reopen the journal (a costly open on Windows).
pub(crate) struct LocalRoot {
    pub(crate) path: PathBuf,
    journal: RwLock<Option<File>>,

    #[cfg(test)]
    read_gate: Option<JournalReadGate>,
}

/// One test's worker admission gate; ordinary roots never install one.
#[cfg(test)]
struct JournalReadGate {
    admitted: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    completed: std::sync::mpsc::Sender<Result<acyclic_stream::OwnedBytes, Error>>,
}

/// Roots are equal when they name the same directory; the handle is a cache.
impl PartialEq for LocalRoot {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl LocalRoot {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            path,
            journal: RwLock::new(None),

            #[cfg(test)]
            read_gate: None,
        }
    }

    /// Runs `replace` with the journal read handle closed and no reader able to reopen it, so Windows may replace the file and no reader keeps a handle on the replaced one.
    pub(crate) fn replace_journal(
        &self,
        replace: impl FnOnce() -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        let mut journal = self
            .journal
            .write()
            .map_err(|_| std::io::Error::other("journal reader poisoned"))?;
        *journal = None;
        replace()
    }

    pub(crate) fn read_journal_body(
        &self,
        offset: u64,
        expected_digest: &[u8; 32],
        expected_length: usize,
    ) -> Result<acyclic_stream::OwnedBytes, Error> {
        #[cfg(test)]
        if let Some(gate) = &self.read_gate {
            gate.admitted
                .send(())
                .map_err(|_| Error::from(Unavailable))?;
            gate.release
                .lock()
                .map_err(|_| Error::from(Unavailable))?
                .recv()
                .map_err(|_| Error::from(Unavailable))?;
            let result = self.read_journal_body_inner(offset, expected_digest, expected_length);
            let _ = gate.completed.send(result.clone());
            return result;
        }
        self.read_journal_body_inner(offset, expected_digest, expected_length)
    }

    fn read_journal_body_inner(
        &self,
        offset: u64,
        expected_digest: &[u8; 32],
        expected_length: usize,
    ) -> Result<acyclic_stream::OwnedBytes, Error> {
        if let Some(journal) = self
            .journal
            .read()
            .map_err(|_| Error::from(Unavailable))?
            .as_ref()
        {
            return read_journal_body(journal, offset, expected_digest, expected_length);
        }
        let mut journal = self.journal.write().map_err(|_| Error::from(Unavailable))?;
        if journal.is_none() {
            *journal = Some(
                File::open(self.path.join(JOURNAL_FILE)).map_err(|_| Error::from(Unavailable))?,
            );
        }
        read_journal_body(
            journal.as_ref().ok_or(Error::from(Unavailable))?,
            offset,
            expected_digest,
            expected_length,
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PhysicalError {
    #[error("invalid private Objects storage: {0}")]
    Invalid(&'static str),
    #[error("corrupt private Objects storage")]
    Corrupt,
    #[error("Objects storage I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

fn segment_path(root: &Path, id: &[u8; 32]) -> PathBuf {
    root.join("segments").join(format!("{}.segment", hex(id)))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[(byte >> 4) as usize]));
        output.push(char::from(DIGITS[(byte & 0x0f) as usize]));
    }
    output
}

mod garbage;
mod read;
mod validation;
mod write;
pub(crate) use garbage::collect_physical_garbage;
pub(crate) use read::read_body_at;
use read::read_journal_body;
pub(crate) use validation::validate_referenced_segments;
use validation::*;
pub(crate) use write::{
    local_path, native_durability, open_publication, persist_segment, sync_publication,
    sync_segment_directory, write_publication, write_segment,
};
use write::{native_write_error, publication_probe, sync_parent};
#[cfg(test)]
pub(crate) mod tests;

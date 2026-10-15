//! Single-writer, checksummed private client journal.

use super::{ClientStateStore, SessionRecord};
use crate::{Error, Result};
use prost::Message;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const HEADER_BYTES: usize = 68;

/// File-backed cursor and outbox recovery. Keep its directory private.
pub struct FileClientState {
    file: File,
    _lock: File,
    poisoned: bool,
    path: PathBuf,
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

impl FileClientState {
    /// Locks a stable sibling `.lock` file before opening the journal data.
    /// Keep the lock file in place across checkpoint replacement and client exit.
    pub fn open(path: &Path) -> Result<Self> {
        let absolute = std::path::absolute(path).map_err(storage)?;
        let path = match absolute.canonicalize() {
            Ok(path) => path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // Resolve directory aliases even before the journal exists. A dangling
                // file symlink is not a new journal: replacing it would change its target.
                if absolute.symlink_metadata().is_ok() {
                    return Err(storage("journal alias has no target"));
                }
                let parent = absolute
                    .parent()
                    .ok_or_else(|| storage("journal parent missing"))?;
                let name = absolute
                    .file_name()
                    .ok_or_else(|| storage("journal name missing"))?;
                parent.canonicalize().map_err(storage)?.join(name)
            }
            Err(error) => return Err(storage(error)),
        };
        #[cfg(unix)]
        if let Ok(metadata) = path.metadata() {
            use std::os::unix::fs::MetadataExt;
            if metadata.nlink() != 1 {
                return Err(storage("journal hard links are not supported"));
            }
        }
        let mut lock_path = path.as_os_str().to_owned();
        lock_path.push(".lock");
        let lock = open_file(Path::new(&lock_path))?;
        lock.try_lock()
            .map_err(|error| Error::Conflict(format!("client journal is locked: {error}")))?;
        // Open the data only after acquiring the stable lock: a paused opener must
        // see the current inode after a checkpoint replaces the data file.
        let file = open_file(&path)?;
        let mut state = Self {
            file,
            _lock: lock,
            poisoned: false,
            path,
        };
        state.sync_parent()?;
        Ok(state)
    }

    fn check(&self) -> Result<()> {
        if self.poisoned {
            return Err(Error::Storage(
                "client journal needs reopening after an I/O failure".into(),
            ));
        }
        Ok(())
    }

    fn sync_parent(&mut self) -> Result<()> {
        #[cfg(unix)]
        if let Some(parent) = self.path.parent()
            && let Err(error) = File::open(parent).and_then(|directory| directory.sync_all())
        {
            self.poisoned = true;
            return Err(storage(error));
        }
        Ok(())
    }

    fn truncate(&mut self, length: u64) -> Result<()> {
        self.file.set_len(length).map_err(storage)?;
        self.file.seek(SeekFrom::Start(length)).map_err(storage)?;
        self.file.sync_data().map_err(storage)
    }
}

impl ClientStateStore for FileClientState {
    fn load(&mut self) -> Result<Vec<SessionRecord>> {
        self.check()?;
        self.file.seek(SeekFrom::Start(0)).map_err(storage)?;
        let length = self.file.metadata().map_err(storage)?.len();
        let mut offset = 0;
        let mut records = Vec::new();
        while offset < length {
            if length - offset < HEADER_BYTES as u64 {
                self.truncate(offset)?;
                break;
            }
            let mut header = [0; HEADER_BYTES];
            self.file.read_exact(&mut header).map_err(storage)?;
            if blake3::hash(&header[..36]).as_bytes() != &header[36..] {
                return Err(storage("client journal header checksum mismatch"));
            }
            let size = u32::from_le_bytes(header[..4].try_into().map_err(storage)?);
            if u64::from(size) > length - offset - HEADER_BYTES as u64 {
                self.truncate(offset)?;
                break;
            }
            let mut bytes = vec![0; size as usize];
            self.file.read_exact(&mut bytes).map_err(storage)?;
            if blake3::hash(&bytes).as_bytes() != &header[4..36] {
                return Err(Error::Storage("client journal checksum mismatch".into()));
            }
            records.push(SessionRecord::decode(bytes.as_slice()).map_err(storage)?);
            offset += HEADER_BYTES as u64 + u64::from(size);
        }
        Ok(records)
    }

    fn append(&mut self, record: &SessionRecord) -> Result<()> {
        self.check()?;
        let bytes = encode_record(record)?;
        let start = self.file.seek(SeekFrom::End(0)).map_err(storage)?;
        if let Err(error) = self
            .file
            .write_all(&bytes)
            .and_then(|()| self.file.sync_data())
        {
            if self.truncate(start).is_err() {
                self.poisoned = true;
            }
            return Err(storage(error));
        }
        Ok(())
    }

    fn replace(&mut self, checkpoint: &SessionRecord) -> Result<()> {
        self.check()?;
        let mut temporary = self.path.as_os_str().to_owned();
        temporary.push(".checkpoint");
        let temporary = PathBuf::from(temporary);
        let mut file = open_file(&temporary)?;
        file.set_len(0).map_err(storage)?;
        file.write_all(&encode_record(checkpoint)?)
            .map_err(storage)?;
        file.sync_all().map_err(storage)?;
        std::fs::rename(&temporary, &self.path).map_err(storage)?;
        self.file = file;
        self.sync_parent()
    }
}

fn open_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).map_err(storage)
}

fn encode_record(record: &SessionRecord) -> Result<Vec<u8>> {
    let length = u32::try_from(record.encoded_len()).map_err(storage)?;
    let mut bytes = Vec::with_capacity(HEADER_BYTES + length as usize);
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.resize(HEADER_BYTES, 0);
    record.encode(&mut bytes).map_err(storage)?;
    let digest = blake3::hash(
        bytes
            .get(HEADER_BYTES..)
            .ok_or_else(|| storage("journal encoding failed"))?,
    );
    bytes
        .get_mut(4..36)
        .ok_or_else(|| storage("journal header missing"))?
        .copy_from_slice(digest.as_bytes());
    let header_digest = blake3::hash(
        bytes
            .get(..36)
            .ok_or_else(|| storage("journal header missing"))?,
    );
    bytes
        .get_mut(36..HEADER_BYTES)
        .ok_or_else(|| storage("journal header checksum missing"))?
        .copy_from_slice(header_digest.as_bytes());
    Ok(bytes)
}

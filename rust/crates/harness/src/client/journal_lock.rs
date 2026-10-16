//! One nonblocking writer lock; checkpoints never replace its inode.
use crate::{Error, Result};
use std::fs::File;

#[cfg(not(acyclic_harness_loom))]
pub(super) type WriterLock = File;

#[cfg(not(acyclic_harness_loom))]
pub(super) fn acquire(file: File) -> Result<WriterLock> {
    // No nested acquisitions or wait/wakeup protocol: a busy writer returns immediately.
    file.try_lock()
        .map_err(|error| Error::Conflict(format!("client journal is locked: {error}")))?;
    validate(&file)?;
    Ok(file)
}

fn validate(file: &File) -> Result<()> {
    if file
        .metadata()
        .map_err(|error| Error::Storage(error.to_string()))?
        .len()
        != 0
    {
        return Err(Error::Storage("journal lock contains data".into()));
    }
    Ok(())
}

#[cfg(all(acyclic_harness_loom, test))]
pub(super) use modeled::disable_exclusion;
#[cfg(acyclic_harness_loom)]
pub(super) use modeled::{WriterLock, acquire};

#[cfg(acyclic_harness_loom)]
mod modeled {
    use super::*;
    use loom::sync::Mutex;
    use std::{collections::BTreeMap, os::unix::fs::MetadataExt};

    struct Registry {
        held: BTreeMap<(u64, u64), bool>,
        enforce: bool,
    }

    loom::lazy_static! {
        // Lock order: registry only, serializing the nonblocking inode check.
        // Never nested or held across filesystem I/O. Plain values can unwind
        // without leaving Loom-owned references in a static registry.
        static ref LOCKS: Mutex<Registry> = Mutex::new(Registry {
            held: BTreeMap::new(), enforce: true,
        });
    }

    pub(crate) struct WriterLock {
        _file: File,
        identity: (u64, u64),
    }

    pub(crate) fn acquire(file: File) -> Result<WriterLock> {
        let metadata = file
            .metadata()
            .map_err(|error| Error::Storage(error.to_string()))?;
        let identity = (metadata.dev(), metadata.ino());
        {
            let mut locks = LOCKS
                .lock()
                .map_err(|error| Error::Storage(error.to_string()))?;
            if locks.enforce && locks.held.get(&identity) == Some(&true) {
                return Err(Error::Conflict("client journal is locked".into()));
            }
            locks.held.insert(identity, true);
        }
        let lock = WriterLock {
            _file: file,
            identity,
        };
        validate(&lock._file)?;
        Ok(lock)
    }

    impl Drop for WriterLock {
        fn drop(&mut self) {
            if let Ok(mut locks) = LOCKS.lock() {
                locks.held.insert(self.identity, false);
            }
        }
    }

    // Negative control, absent from normal builds: bypass the actual acquisition path.
    #[cfg(test)]
    pub(crate) fn disable_exclusion() {
        if let Ok(mut locks) = LOCKS.lock() {
            locks.enforce = false;
        }
    }
}

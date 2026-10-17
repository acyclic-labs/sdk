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

#[cfg(acyclic_harness_loom)]
pub(super) use modeled::disable_exclusion;
#[cfg(acyclic_harness_loom)]
pub(super) use modeled::{WriterLock, acquire};

#[cfg(acyclic_harness_loom)]
mod modeled {
    use super::*;
    use loom::sync::Mutex;
    use same_file::Handle;
    use std::{collections::HashMap, sync::Arc};

    struct Registry {
        held: HashMap<Arc<Handle>, bool>,
        enforce: bool,
    }

    loom::lazy_static! {
        // Lock order: registry only, serializing the nonblocking inode check.
        // Never nested or held across filesystem I/O. Plain values can unwind
        // without leaving Loom-owned references in a static registry.
        static ref LOCKS: Mutex<Registry> = Mutex::new(Registry {
            held: HashMap::new(), enforce: true,
        });
    }

    pub(crate) struct WriterLock {
        _file: File,
        identity: Arc<Handle>,
    }

    pub(crate) fn acquire(file: File) -> Result<WriterLock> {
        let identity = Arc::new(
            Handle::from_file(
                file.try_clone()
                    .map_err(|error| Error::Storage(error.to_string()))?,
            )
            .map_err(|error| Error::Storage(error.to_string()))?,
        );
        {
            let mut locks = LOCKS
                .lock()
                .map_err(|error| Error::Storage(error.to_string()))?;
            if locks.enforce && locks.held.get(&identity) == Some(&true) {
                return Err(Error::Conflict("client journal is locked".into()));
            }
            locks.held.insert(identity.clone(), true);
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
                locks.held.insert(self.identity.clone(), false);
            }
        }
    }

    // Negative control, absent from normal builds: bypass the actual acquisition path.
    pub(crate) fn disable_exclusion() {
        if let Ok(mut locks) = LOCKS.lock() {
            locks.enforce = false;
        }
    }
}

//! Native positional transfer and its original observation/retirement boundary.
use super::*;
use crate::provider::deadline::GuestDeadline;
use futures::FutureExt;
use std::fs::{File, OpenOptions};

impl LocalBackend {
    pub(super) async fn file<T: Send + 'static>(
        &self,
        access: FileAccess,
        write: bool,
        deadline: &GuestDeadline,
        transfer: impl FnOnce(&File) -> io::Result<T> + Send + 'static,
    ) -> Result<Result<T, NonZeroU32>, ProviderError> {
        admission(&access)?;
        let mut owner = self.execution()?;
        let mut observed = owner.state.subscribe();
        let mut wait = PhysicalWait {
            state: owner.state.clone(),
            control: owner.control.clone(),
            completed: false,
        };
        let machine = self.machine.id;
        let operation = move || {
            let result = (|| {
                owner
                    .check()
                    .map_err(|_| ProviderError::FileIndeterminate(machine))?;
                admission(&access)?;
                let result = (|| {
                    let file = open(&access.path, write)?;
                    // Never interpret a pipe/device as an offset-addressed file.
                    if !file.metadata()?.is_file() {
                        return Err(io::ErrorKind::Unsupported.into());
                    }
                    admission(&access).map_err(io::Error::other)?;
                    owner.check()?;
                    transfer(&file)
                })();
                match result {
                    Ok(value) => Ok(Ok(value)),
                    Err(error) => errno(error).map(Err),
                }
            })();
            owner.retired(true);
            result
        };
        let submitted = acyclic_native_runtime::run_blocking_io(operation, None)
            .map_err(|_| ProviderError::FileIndeterminate(self.machine.id))?;
        // The native worker retains its physical lease even if this wait ends.
        // Deadline, stop or observer loss latches uncertainty; only actual
        // worker completion can release its original cleanup guard.
        let result = deadline
            .run(async {
                tokio::select! {
                    biased;
                    _ = observed.wait_for(|state| *state != PhysicalState::Live) => None,
                    result = std::panic::AssertUnwindSafe(submitted).catch_unwind() => Some(result),
                }
            })
            .await
            .flatten();
        match result {
            Some(Ok(Ok(value))) => {
                wait.completed = true;
                value
            }
            _ => Err(ProviderError::FileIndeterminate(self.machine.id)),
        }
    }
}

fn admission(access: &FileAccess) -> Result<(), ProviderError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| ProviderError::Unavailable)?
        .as_millis();
    if now >= u128::from(access.deadline_unix_ms)
        || !std::path::Path::new(&access.path).is_absolute()
    {
        return Err(ProviderError::Rejected("native file admission expired or its path is relative; recovery: retain an absolute host path and live original deadline".into()));
    }
    Ok(())
}

fn open(path: &str, write: bool) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(!write).write(write).create(write);
    // O_NONBLOCK prevents FIFO open from waiting for another process. Regular
    // files keep native semantics; slow filesystem work retains its lease.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(rustix::fs::OFlags::NONBLOCK.bits() as i32);
    }
    options.open(path)
}

fn errno(error: io::Error) -> Result<NonZeroU32, ProviderError> {
    error.raw_os_error().and_then(|code| u32::try_from(code).ok()).and_then(NonZeroU32::new)
        .ok_or_else(|| ProviderError::Rejected(format!("native file IO failed: {error}; recovery: inspect the original path and OS access authority")))
}

#[cfg(unix)]
pub(super) fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, bytes, offset)
}
#[cfg(windows)]
pub(super) fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, bytes, offset)
}
#[cfg(unix)]
pub(super) fn write_at(file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::write_at(file, bytes, offset)
}
#[cfg(windows)]
pub(super) fn write_at(file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_write(file, bytes, offset)
}

#[cfg(test)]
mod tests;

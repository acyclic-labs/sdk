//! Host process and file primitives for one source-admitted workspace.
use super::*;
use crate::provider::admitted::{AdmittedMachine, NativeMachine};
use acyclic_native_runtime::{CaptureWakes, ProcessControl, ProcessInput, ProcessOutput};
use acyclic_stream::{StreamPath, StreamProvider};
use std::{
    fs::OpenOptions,
    io,
    sync::atomic::{AtomicBool, Ordering},
};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

/// Opens a public provider for the original host-owned workspace incarnation.
/// The caller supplies its actual image/volume contract and already-admitted
/// native configuration. This factory neither grants filesystem authority nor
/// creates another view. Results live on the supplied canonical Streams owner.
/// No checkpoint, memory-resume, billing or tenant-isolation guarantee is made.
pub async fn provider(
    observation: MachineObservation,
    options: LocalOptions,
    streams: Arc<dyn StreamProvider>,
    journal: StreamPath,
) -> Result<Arc<dyn MachinesProvider>, ProviderError> {
    if !observation.contract.capabilities.is_empty()
        || observation.contract.suspension != SuspensionPolicy::Manual
        || observation.contract.expiration != ExpirationPolicy::Never
    {
        return Err(ProviderError::Unsupported("native enrollment claims an absent lifecycle producer; recovery: enroll manual suspension, no automatic expiry and no snapshot/VM capabilities".into()));
    }
    let backend = LocalBackend {
        machine: LocalMachine::new(observation.id, options)?,
        generation: std::sync::Mutex::new(Generation::new()?),
    };
    Ok(Arc::new(
        AdmittedMachine::open(backend, observation, streams, journal).await?,
    ))
}

struct Generation {
    control: ProcessControl,
    live: Arc<RwLock<()>>,
    failed: Arc<AtomicBool>,
}

impl Generation {
    fn new() -> Result<Self, ProviderError> {
        Ok(Self {
            control: ProcessControl::unbounded().map_err(|_| ProviderError::Unavailable)?,
            live: Arc::new(RwLock::new(())),
            failed: Arc::new(AtomicBool::new(false)),
        })
    }
}

struct LocalBackend {
    machine: LocalMachine,
    generation: std::sync::Mutex<Generation>,
}

impl LocalBackend {
    fn execution(&self) -> Result<Execution, ProviderError> {
        let generation = self
            .generation
            .lock()
            .map_err(|_| ProviderError::Unavailable)?;
        generation.control.check().map_err(|_| {
            ProviderError::Conflict(
                "native Machine is stopped; recovery: wake its original lifecycle before IO".into(),
            )
        })?;
        let live = generation
            .live
            .clone()
            .try_read_owned()
            .map_err(|_| ProviderError::Unavailable)?;
        Ok(Execution {
            control: generation.control.clone(),
            live: Some(live),
            failed: generation.failed.clone(),
        })
    }
}

impl Drop for LocalBackend {
    fn drop(&mut self) {
        if let Ok(generation) = self.generation.get_mut() {
            generation.control.stop();
        }
    }
}

// This lease covers physical work, not its disposable output observation.
// An unwind releases admission but latches unknown physical retirement.
struct Execution {
    control: ProcessControl,
    live: Option<OwnedRwLockReadGuard<()>>,
    failed: Arc<AtomicBool>,
}

impl CaptureHandler for Execution {
    fn take_wakes(&mut self) -> io::Result<CaptureWakes> {
        Ok(Some(self.control.wake()).into())
    }
    fn check(&mut self) -> io::Result<()> {
        self.control.check()
    }
    fn observe(&mut self, _: ProcessOutput<'_>) -> io::Result<ProcessInput> {
        Ok(ProcessInput::default())
    }
    fn retired(&mut self, completed: bool) {
        if !completed {
            self.failed.store(true, Ordering::Release);
        }
        self.live.take();
    }
}

impl Drop for Execution {
    fn drop(&mut self) {
        if self.live.is_some() {
            self.failed.store(true, Ordering::Release);
        }
    }
}

#[async_trait]
impl NativeMachine for LocalBackend {
    async fn exec(&self, command: ExecCommand) -> Result<ExecStream, ProviderError> {
        let owner = self.execution()?;
        Ok(Box::pin(self.machine.exec(command, owner).await))
    }
    async fn stop(&self) -> Result<(), ProviderError> {
        let (live, failed) = {
            let generation = self
                .generation
                .lock()
                .map_err(|_| ProviderError::Unavailable)?;
            generation.control.stop();
            (generation.live.clone(), generation.failed.clone())
        };
        // Only CaptureOutcome's original physical join releases readers.
        // Slow readers and a retained terminal stream cannot delay this wait.
        let _retired = live.write_owned().await;
        if failed.load(Ordering::Acquire) {
            return Err(ProviderError::CommandIndeterminate(self.machine.id));
        }
        Ok(())
    }
    async fn wake(&self) -> Result<(), ProviderError> {
        let next = Generation::new()?;
        *self
            .generation
            .lock()
            .map_err(|_| ProviderError::Unavailable)? = next;
        Ok(())
    }
    async fn read(&self, access: FileAccess, limit: u32) -> Result<FileRead, ProviderError> {
        if limit == 0 {
            return Err(ProviderError::Invalid(
                "zero file read credit; recovery: supply positive chunk credit".into(),
            ));
        }
        access.validate_range(limit as usize)?;
        let mut owner = self.execution()?;
        let operation = move || {
            let result = (|| {
                owner.check().map_err(|_| {
                    ProviderError::Rejected(
                        "file owner stopped; recovery: recover its original Machine lifecycle"
                            .into(),
                    )
                })?;
                file_admission(&access)?;
                let result = (|| {
                    let file = std::fs::File::open(&access.path)?;
                    let mut bytes = vec![0; limit as usize];
                    let count = read_at(&file, &mut bytes, access.offset)?;
                    bytes.truncate(count);
                    Ok::<_, io::Error>(bytes)
                })();
                match result {
                    Ok(bytes) => Ok(FileRead::Data(bytes)),
                    Err(error) => errno(error).map(FileRead::Errno),
                }
            })();
            owner.retired(true);
            result
        };
        acyclic_native_runtime::run_blocking_io(operation, None)
            .map_err(|_| ProviderError::Unavailable)?
            .await
            .map_err(|_| ProviderError::FileIndeterminate(self.machine.id))?
    }
    async fn write(
        &self,
        access: FileAccess,
        data: Vec<u8>,
        truncate: bool,
    ) -> Result<FileWrite, ProviderError> {
        access.validate_range(data.len())?;
        if truncate && access.offset != 0 {
            return Err(ProviderError::Invalid("truncation has a nonzero offset; recovery: truncate only the original offset-zero chunk".into()));
        }
        let mut owner = self.execution()?;
        let operation = move || {
            let result = (|| {
                owner.check().map_err(|_| {
                    ProviderError::Rejected(
                        "file owner stopped; recovery: recover its original Machine lifecycle"
                            .into(),
                    )
                })?;
                file_admission(&access)?;
                let result = (|| {
                    let file = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(truncate)
                        .open(&access.path)?;
                    write_at(&file, &data, access.offset)
                })();
                match result {
                    Ok(count) => Ok(FileWrite::Written(count as u32)),
                    Err(error) => errno(error).map(FileWrite::Errno),
                }
            })();
            owner.retired(true);
            result
        };
        acyclic_native_runtime::run_blocking_io(operation, None)
            .map_err(|_| ProviderError::Unavailable)?
            .await
            .map_err(|_| ProviderError::FileIndeterminate(self.machine.id))?
    }
}

fn file_admission(access: &FileAccess) -> Result<(), ProviderError> {
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

fn errno(error: io::Error) -> Result<NonZeroU32, ProviderError> {
    error.raw_os_error().and_then(|code| u32::try_from(code).ok()).and_then(NonZeroU32::new)
        .ok_or_else(|| ProviderError::Rejected(format!("native file IO failed: {error}; recovery: inspect the original path and OS access authority")))
}

#[cfg(unix)]
fn read_at(file: &std::fs::File, bytes: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, bytes, offset)
}
#[cfg(windows)]
fn read_at(file: &std::fs::File, bytes: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, bytes, offset)
}
#[cfg(unix)]
fn write_at(file: &std::fs::File, bytes: &[u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::write_at(file, bytes, offset)
}
#[cfg(windows)]
fn write_at(file: &std::fs::File, bytes: &[u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_write(file, bytes, offset)
}

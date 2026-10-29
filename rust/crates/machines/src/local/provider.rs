//! Host process and file primitives for one source-admitted workspace.
use super::*;
use crate::provider::admitted::{AdmittedMachine, NativeMachine};
use acyclic_native_runtime::{CaptureWakes, ProcessControl, ProcessInput, ProcessOutput};
use acyclic_stream::{StreamPath, StreamProvider};
use std::io;
use tokio::sync::{OwnedRwLockReadGuard, RwLock, watch};

mod file;
#[cfg(test)]
mod tests;

/// Opens a public provider for the original host-owned workspace incarnation.
/// The caller supplies its actual image/volume contract and already-admitted
/// native configuration. This factory neither grants filesystem authority nor
/// creates another view. Results live on the supplied canonical Streams owner.
/// The canonical journal admits one physical owner. Share its returned Arc;
/// reopening a live incarnation refuses rather than inventing a replacement.
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum PhysicalState {
    Live,
    Stopping,
    Uncertain,
}

struct Generation {
    control: ProcessControl,
    live: Arc<RwLock<()>>,
    state: watch::Sender<PhysicalState>,
}

impl Generation {
    fn new() -> Result<Self, ProviderError> {
        Ok(Self {
            control: ProcessControl::unbounded().map_err(|_| ProviderError::Unavailable)?,
            live: Arc::new(RwLock::new(())),
            state: watch::channel(PhysicalState::Live).0,
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
        if *generation.state.borrow() != PhysicalState::Live {
            return Err(ProviderError::CommandIndeterminate(self.machine.id));
        }
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
            state: generation.state.clone(),
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
    state: watch::Sender<PhysicalState>,
}

// A disposable observer never releases the worker's physical lease. Losing
// that observer stops its incarnation and preserves uncertain retirement.
struct PhysicalWait {
    state: watch::Sender<PhysicalState>,
    control: ProcessControl,
    completed: bool,
}
impl Drop for PhysicalWait {
    fn drop(&mut self) {
        if !self.completed {
            self.control.stop();
            self.state.send_replace(PhysicalState::Uncertain);
        }
    }
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
            self.state.send_replace(PhysicalState::Uncertain);
        }
        self.live.take();
    }
}

impl Drop for Execution {
    fn drop(&mut self) {
        if self.live.is_some() {
            self.state.send_replace(PhysicalState::Uncertain);
        }
    }
}

#[async_trait]
impl NativeMachine for LocalBackend {
    async fn exec(
        &self,
        command: ExecCommand,
        until: std::time::Instant,
    ) -> Result<ExecStream, ProviderError> {
        let owner = self.execution()?;
        let mut observed = owner.state.subscribe();
        let mut wait = PhysicalWait {
            state: owner.state.clone(),
            control: owner.control.clone(),
            completed: false,
        };
        tokio::select! {
            biased;
            _ = observed.wait_for(|state| *state != PhysicalState::Live) => {
                Ok(Box::pin(futures::stream::iter([
                    Err(ProviderError::CommandIndeterminate(self.machine.id)),
                ])) as ExecStream)
            }
            execution = self.machine.exec(command, owner, until) => {
                wait.completed = true;
                Ok(Box::pin(execution) as ExecStream)
            }
        }
    }
    async fn stop(&self) -> Result<(), ProviderError> {
        let (live, state) = {
            let generation = self
                .generation
                .lock()
                .map_err(|_| ProviderError::Unavailable)?;
            generation.control.stop();
            generation.state.send_if_modified(|state| {
                if *state == PhysicalState::Live {
                    *state = PhysicalState::Stopping;
                    true
                } else {
                    false
                }
            });
            (generation.live.clone(), generation.state.clone())
        };
        // Only CaptureOutcome's original physical join releases readers.
        // Slow readers and a retained terminal stream cannot delay this wait.
        let mut observed = state.subscribe();
        tokio::select! {
            biased;
            _ = observed.wait_for(|state| *state == PhysicalState::Uncertain) => {
                Err(ProviderError::CommandIndeterminate(self.machine.id))
            }
            _retired = live.write_owned() => {
                if *state.borrow() == PhysicalState::Uncertain {
                    Err(ProviderError::CommandIndeterminate(self.machine.id))
                } else { Ok(()) }
            }
        }
    }
    async fn wake(&self) -> Result<(), ProviderError> {
        let next = Generation::new()?;
        *self
            .generation
            .lock()
            .map_err(|_| ProviderError::Unavailable)? = next;
        Ok(())
    }
    async fn read(
        &self,
        access: FileAccess,
        limit: u32,
        deadline: &crate::provider::deadline::GuestDeadline,
    ) -> Result<FileRead, ProviderError> {
        if limit == 0 {
            return Err(ProviderError::Invalid(
                "zero file read credit; recovery: supply positive chunk credit".into(),
            ));
        }
        access.validate_range(limit as usize)?;
        let offset = access.offset;
        self.file(access, false, deadline, move |file| {
            let mut bytes = vec![0; limit as usize];
            let count = file::read_at(file, &mut bytes, offset)?;
            bytes.truncate(count);
            Ok(bytes)
        })
        .await?
        .map_or_else(
            |code| Ok(FileRead::Errno(code)),
            |bytes| Ok(FileRead::Data(bytes)),
        )
    }
    async fn write(
        &self,
        access: FileAccess,
        data: Vec<u8>,
        truncate: bool,
        deadline: &crate::provider::deadline::GuestDeadline,
    ) -> Result<FileWrite, ProviderError> {
        access.validate_range(data.len())?;
        if truncate && access.offset != 0 {
            return Err(ProviderError::Invalid("truncation has a nonzero offset; recovery: truncate only the original offset-zero chunk".into()));
        }
        let offset = access.offset;
        self.file(access, true, deadline, move |file| {
            if truncate {
                file.set_len(0)?;
            }
            file::write_at(file, &data, offset)
        })
        .await?
        .map_or_else(
            |code| Ok(FileWrite::Errno(code)),
            |count| Ok(FileWrite::Written(count as u32)),
        )
    }
}

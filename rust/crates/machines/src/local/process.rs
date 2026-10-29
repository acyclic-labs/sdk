//! One spawn using the original host workspace and native process owner.
use super::*;
use acyclic_native_runtime::{
    CaptureSchedule, ControlledCapture, OwnedCaptureInput, ProcessControl, ProcessCustody,
    ProcessTree, TaskSpawner,
};
use futures::FutureExt;
use std::{
    io,
    path::PathBuf,
    process::{Command, Stdio},
};

/// Host-owned execution configuration, derived from its admitted workspace.
/// This is not a filesystem authority or a serialized guest enrollment.
pub struct LocalOptions {
    pub cwd: PathBuf,
    pub environment: BTreeMap<String, String>,
    pub inherit_environment: bool,
    pub process_custody: Option<Arc<ProcessCustody>>,
}

/// Execution of one already admitted local Machine incarnation. Processes run
/// with the host user's OS privileges; this is not a tenant isolation boundary.
/// Lifecycle and selected-volume admission remain with the Machine owner.
pub struct LocalMachine {
    pub(super) id: MachineId,
    options: Arc<LocalOptions>,
}

/// Original command stream and callback custody. Neither observing completion
/// nor dropping it re-dispatches a command. Consume output before joining its
/// custody: output credit is intentionally required for a running producer.
pub struct LocalExecution<H: CaptureHandler>(Output<ControlledCapture<H>, H>);

impl<H: CaptureHandler> LocalExecution<H> {
    /// Joins returned callback custody after consuming the original output.
    /// Producer loss remains uncertainty on the original Machine identity.
    pub async fn finish(self) -> Result<H, ProviderError> {
        self.0.finish().await
    }
}

impl<H: CaptureHandler> Stream for LocalExecution<H> {
    type Item = Result<ExecOutput, ProviderError>;
    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().0).poll_next(context)
    }
}

impl LocalMachine {
    /// Binds host configuration once. The caller retains the original verified
    /// workspace, path policy and physical custody in each capture handler.
    pub fn new(id: MachineId, options: LocalOptions) -> Result<Self, ProviderError> {
        if !options.cwd.is_absolute()
            || options.environment.iter().any(|(key, value)| {
                key.is_empty() || key.contains(['=', '\0']) || value.contains('\0')
            })
        {
            return Err(ProviderError::Invalid("local workspace configuration failed; recovery: bind an absolute admitted directory and valid native environment".into()));
        }
        Ok(Self {
            id,
            options: Arc::new(options),
        })
    }

    /// Dispatches exactly once on the shared native worker. Command input is
    /// moved into the original pipe writer; output uses the existing pump and
    /// one credit channel. The handler checks source custody before spawning
    /// and on every physical turn, including original expiry and owner wakes.
    /// Admission refusal is the first stream error; `finish` still returns the
    /// original callback custody. Physical owner loss returns uncertainty.
    pub async fn exec<H: CaptureHandler>(
        &self,
        command: ExecCommand,
        mut handler: H,
    ) -> LocalExecution<H> {
        let machine = self.id;
        let (deadline, maximum, control) = match admit(&command) {
            Ok(admitted) => admitted,
            Err(error) => {
                handler.retired(true);
                return LocalExecution(Output::failed(
                    machine,
                    error,
                    Some(handler),
                    ControlledCapture::into_inner,
                ));
            }
        };
        let mut admission = StopOnDrop(Some(control.clone()));
        let options = self.options.clone();
        let spawner = TaskSpawner::current();
        let submitted = acyclic_native_runtime::run_blocking_io(
            move || {
                let mut handler = control.bind(handler);
                let started = (|| {
                    handler.check().map_err(StartFailure::Refused)?;
                    if std::time::Instant::now() >= deadline {
                        return Err(StartFailure::Refused(io::ErrorKind::TimedOut.into()));
                    }
                    let mut argv = command.argv.into_iter();
                    let executable = argv
                        .next()
                        .ok_or_else(|| StartFailure::Refused(io::ErrorKind::InvalidInput.into()))?;
                    let mut process = Command::new(executable);
                    if !options.inherit_environment {
                        process.env_clear();
                    }
                    process
                        .args(argv)
                        .current_dir(
                            command
                                .cwd
                                .map(|cwd| options.cwd.join(cwd))
                                .as_ref()
                                .unwrap_or(&options.cwd),
                        )
                        .envs(&options.environment)
                        .stdin(if command.stdin.is_empty() {
                            Stdio::null()
                        } else {
                            Stdio::piped()
                        })
                        .stdout(Stdio::piped())
                        .stderr(Stdio::piped());
                    let tree = match &options.process_custody {
                        Some(custody) => ProcessTree::spawn_in_custody(&mut process, custody),
                        None => ProcessTree::spawn(&mut process),
                    }
                    .map_err(|_| StartFailure::Dispatched)?;
                    let input = (!command.stdin.is_empty())
                        .then_some(OwnedCaptureInput::Literal(command.stdin));
                    Ok::<_, StartFailure>((tree, input))
                })();
                match started {
                    Ok((tree, input)) => Ok(tree.stream_capture(
                        &spawner,
                        CaptureSchedule::until(deadline),
                        maximum,
                        input,
                        handler,
                    )),
                    Err(failure) => {
                        handler.retired(matches!(&failure, StartFailure::Refused(_)));
                        Err((
                            match failure {
                                StartFailure::Refused(error) => ProviderError::Rejected(format!(
                                    "local command refused before spawn: {error}; recovery: restore original workspace authority before a new command"
                                )),
                                StartFailure::Dispatched => {
                                    ProviderError::CommandIndeterminate(machine)
                                }
                            },
                            Some(handler.into_inner()),
                        ))
                    }
                }
            },
            None,
        );
        let result = match submitted {
            Ok(submitted) => std::panic::AssertUnwindSafe(submitted)
                .catch_unwind()
                .await
                .ok()
                .and_then(Result::ok)
                .unwrap_or(Err((ProviderError::CommandIndeterminate(machine), None))),
            Err(_) => Err((ProviderError::CommandIndeterminate(machine), None)),
        };
        LocalExecution(match result {
            Ok(capture) => {
                admission.0 = None;
                Output::new(machine, capture, ControlledCapture::into_inner)
            }
            Err((error, custody)) => {
                Output::failed(machine, error, custody, ControlledCapture::into_inner)
            }
        })
    }
}

fn admit(
    command: &ExecCommand,
) -> Result<(std::time::Instant, usize, ProcessControl), ProviderError> {
    let deadline = std::time::Instant::now().checked_add(Duration::from_millis(u64::from(command.timeout_ms)))
        .ok_or_else(|| ProviderError::Invalid("local command deadline overflow; recovery: supply a representable original timeout".into()))?;
    let maximum = command.maximum_output_bytes.map_or(Ok(usize::MAX), usize::try_from)
        .map_err(|_| ProviderError::Invalid("local output allowance is not representable; recovery: select an addressable byte allowance".into()))?;
    let control = match command.expiry_unix_ms {
        Some(expiry) => ProcessControl::open(expiry),
        None => ProcessControl::unbounded(),
    }.map_err(|error| ProviderError::Rejected(format!("local control admission failed: {error}; recovery: restore native wake and expiry services before dispatch")))?;
    Ok((deadline, maximum, control))
}

enum StartFailure {
    Refused(io::Error),
    Dispatched,
}

struct StopOnDrop(Option<ProcessControl>);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        if let Some(control) = &self.0 {
            control.stop();
        }
    }
}

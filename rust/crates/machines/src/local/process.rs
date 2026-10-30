//! One spawn through the shared native launch core, over the original host
//! workspace and native process owner.
use super::*;
use acyclic_native_runtime::{
    ControlledCapture, LaunchFailure, NativeCommand, ProcessControl,
};

/// Host-owned execution configuration, derived from its admitted workspace.
/// This is not a filesystem authority or a serialized guest enrollment.
pub use acyclic_native_runtime::NativeHost as LocalOptions;

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
        options.validate().map_err(|_| ProviderError::Invalid("local workspace configuration failed; recovery: bind an absolute admitted directory and valid native environment".into()))?;
        Ok(Self {
            id,
            options: Arc::new(options),
        })
    }

    /// Dispatches exactly once through the shared native launch core. Command
    /// input moves into the original pipe writer; output uses the existing
    /// pump and one credit channel. The handler checks source custody before
    /// spawning and on every physical turn, including original expiry and
    /// owner wakes. Admission refusal is the first stream error; `finish`
    /// still returns the original callback custody. Physical owner loss
    /// returns uncertainty. `deadline` is the original admission's absolute
    /// monotonic deadline; queueing and worker setup never renew it.
    pub async fn exec<H: CaptureHandler>(
        &self,
        command: ExecCommand,
        mut handler: H,
        deadline: std::time::Instant,
    ) -> LocalExecution<H> {
        let machine = self.id;
        let (maximum, control) = match admit(&command) {
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
        let launched = NativeCommand {
            argv: command.argv.into_iter().map(Into::into).collect(),
            cwd: command.cwd.map(Into::into),
            stdin: (!command.stdin.is_empty()).then_some(command.stdin),
            deadline,
            maximum_output_bytes: maximum,
        }
        .stream(Arc::clone(&self.options), control.bind(handler))
        .await;
        LocalExecution(match launched {
            Ok(capture) => {
                admission.0 = None;
                Output::new(machine, capture, ControlledCapture::into_inner)
            }
            Err(LaunchFailure::Refused { failure, handler }) => Output::failed(
                machine,
                ProviderError::Rejected(format!(
                    "local command refused before spawn: {failure}; recovery: restore original workspace authority before a new command"
                )),
                Some(ControlledCapture::into_inner(handler)),
                ControlledCapture::into_inner,
            ),
            Err(LaunchFailure::Indeterminate { handler }) => Output::failed(
                machine,
                ProviderError::CommandIndeterminate(machine),
                handler.map(ControlledCapture::into_inner),
                ControlledCapture::into_inner,
            ),
        })
    }
}

fn admit(command: &ExecCommand) -> Result<(usize, ProcessControl), ProviderError> {
    let maximum = command.maximum_output_bytes.map_or(Ok(usize::MAX), usize::try_from)
        .map_err(|_| ProviderError::Invalid("local output allowance is not representable; recovery: select an addressable byte allowance".into()))?;
    let control = match command.expiry_unix_ms {
        Some(expiry) => ProcessControl::open(expiry),
        None => ProcessControl::unbounded(),
    }.map_err(|error| ProviderError::Rejected(format!("local control admission failed: {error}; recovery: restore native wake and expiry services before dispatch")))?;
    Ok((maximum, control))
}

/// Stops a command whose launch did not hand its control to a capture.
struct StopOnDrop(Option<ProcessControl>);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        if let Some(control) = &self.0 {
            control.stop();
        }
    }
}

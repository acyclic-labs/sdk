//! The process boundary inherits the same scope as native workers and entropy.
use super::*;
use crate::{CaptureInput, ProcessCaptureFailure, ProcessInput, ProcessOutput};
use std::{io, process::Command, process::Output, time::Duration};

/// Original capture arguments, after command and ownership validation.
pub struct ProcessCapture<'a> {
    /// Fully composed executable, arguments, environment and working directory.
    pub command: &'a Command,
    /// Original monotonic deadline allowance.
    pub timeout: Duration,
    /// Shared stdout and stderr bound.
    pub maximum: usize,
    /// Original literal input or exchange allowance.
    pub input: Option<CaptureInput<'a>>,
}

pub(super) type Provider = dyn Fn(
        ProcessCapture<'_>,
        &mut dyn FnMut() -> io::Result<()>,
        &mut dyn FnMut(ProcessOutput<'_>) -> io::Result<ProcessInput>,
    ) -> Result<Output, ProcessCaptureFailure>
    + Send
    + Sync;

impl Schedule {
    /// Installs one process boundary before dispatch. It cannot change while a
    /// turn or any accepted descendant owns this schedule.
    pub fn set_process_provider(
        &self,
        provider: impl Fn(
            ProcessCapture<'_>,
            &mut dyn FnMut() -> io::Result<()>,
            &mut dyn FnMut(ProcessOutput<'_>) -> io::Result<ProcessInput>,
        ) -> Result<Output, ProcessCaptureFailure>
        + Send
        + Sync
        + 'static,
    ) {
        let mut state = self.0.state();
        assert!(
            state.trace.is_empty(),
            "process provider must precede dispatch"
        );
        assert_eq!(state.active, 0, "cannot replace a live process provider");
        assert!(
            state.process.is_none(),
            "process provider already installed"
        );
        state.process = Some(Arc::new(provider));
    }
}

/// Calls the scoped boundary on the original blocking worker. `None` keeps the
/// ordinary OS provider; an installed provider's failure never falls through.
pub fn capture_process(
    request: ProcessCapture<'_>,
    check: &mut dyn FnMut() -> io::Result<()>,
    observe: &mut dyn FnMut(ProcessOutput<'_>) -> io::Result<ProcessInput>,
) -> Option<Result<Output, ProcessCaptureFailure>> {
    let context = Context::simulation()?;
    let provider = {
        let mut state = context.schedule.0.state();
        let provider = state.process.clone()?;
        state.trace.push(format!("process:{:?}", request.command));
        provider
    };
    // No schedule lock spans provider calls, ownership waits or observers.
    Some(provider(request, check, observe))
}

#[cfg(test)]
#[cfg(not(acyclic_native_runtime_loom))]
mod tests {
    use super::*;

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(32))]
        #[test]
        fn an_installed_process_failure_retains_its_cause(code in 1i32..4096) {
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
            let (schedule, _) = Schedule::seeded(71);
            schedule.set_process_provider(move |_, _, _| Err(ProcessCaptureFailure {
                error: io::Error::from_raw_os_error(code),
                status: None,
                stdout: b"retained".to_vec(),
                stderr: Vec::new(),
                cleanup_completed: false,
            }));
            let command = Command::new("must-not-spawn");
            let capture = || ProcessCapture {
                command: &command,
                timeout: Duration::from_millis(10),
                maximum: 8,
                input: None,
            };
            assert!(capture_process(capture(), &mut || Ok(()), &mut |_| Ok(ProcessInput::default())).is_none());
            let result = runtime.block_on(schedule.scope(async {
                capture_process(capture(), &mut || Ok(()), &mut |_| Ok(ProcessInput::default()))
            }));
            let failure = result.expect("installed failure fell through to OS").unwrap_err();
            proptest::prop_assert_eq!(failure.error.raw_os_error(), Some(code));
            proptest::prop_assert_eq!(failure.stdout, b"retained");
            proptest::prop_assert!(!failure.cleanup_completed);
            proptest::prop_assert_eq!(schedule.trace().len(), 1);
        }
    }
}

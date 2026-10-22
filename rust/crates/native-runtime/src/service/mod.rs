use crate::*;

#[cfg(test)]
mod tests;

/// Selects standard output as the spawned service's readiness channel.
pub const SERVICE_READY_ARGUMENT: &str = "--signal-ready";

/// The starter's end of a service's readiness channel.
#[derive(Debug)]
pub struct ServiceReadiness(File);

/// How a started service left its readiness channel.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceStartOutcome {
    /// The service answers requests.
    Ready,
    /// The service exited without starting, as it does when another service
    /// holds the lock; that service answers instead.
    Exited,
    /// The service could not start, for the reason it reported.
    Failed(String),
}

/// The bytes a failure report may carry; a longer reason is truncated.
const MAXIMUM_FAILURE_BYTES: u64 = 4096;
const READY_FRAME: u8 = b'\n';
const FAILURE_FRAME: u8 = b'!';

impl ServiceReadiness {
    /// Blocks until the service signals that it answers requests, reports why
    /// it could not start, or exits without either.
    pub fn wait(mut self) -> io::Result<ServiceStartOutcome> {
        let operation = obs::span!(
            INFO,
            "acyclic.runtime.service_ready",
            ready = obs::Empty,
            outcome = obs::Empty,
            error.kind = obs::Empty
        );
        operation.in_scope(|| {
            let mut frame = [0_u8; 1];
            let outcome = loop {
                match self.0.read(&mut frame) {
                    Ok(0) => break Ok(ServiceStartOutcome::Exited),
                    Ok(_) if frame[0] == FAILURE_FRAME => {
                        let mut reason = Vec::new();
                        // The failing service closes the channel as it exits.
                        match (&mut self.0)
                            .take(MAXIMUM_FAILURE_BYTES)
                            .read_to_end(&mut reason)
                        {
                            Ok(_) => {}
                            Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {}
                            Err(error) => break Err(error),
                        }
                        break Ok(ServiceStartOutcome::Failed(
                            String::from_utf8_lossy(&reason).into_owned(),
                        ));
                    }
                    Ok(_) => break Ok(ServiceStartOutcome::Ready),
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {
                        break Ok(ServiceStartOutcome::Exited);
                    }
                    Err(error) => break Err(error),
                }
            };
            if let Ok(outcome) = &outcome {
                operation.record("ready", *outcome == ServiceStartOutcome::Ready);
            }
            operation.finish(outcome)
        })
    }
}

/// The service's end of its readiness channel.
#[derive(Debug)]
pub struct ServiceReadySignal(File);

impl ServiceReadySignal {
    /// Tells a connected starter that the service answers requests, and closes the channel.
    /// A starter that has already exited does not prevent the service from running.
    pub fn signal(self) -> io::Result<()> {
        self.send(&[READY_FRAME])
    }

    /// Tells a connected starter why the service cannot start, and closes the
    /// channel, so the starter reports the cause instead of waiting for a
    /// service that will never answer.
    pub fn fail(self, reason: &str) -> io::Result<()> {
        let mut frame = vec![FAILURE_FRAME];
        frame.extend(
            reason
                .bytes()
                .take(usize::try_from(MAXIMUM_FAILURE_BYTES).unwrap_or(usize::MAX)),
        );
        self.send(&frame)
    }

    fn send(mut self, frame: &[u8]) -> io::Result<()> {
        match io::Write::write_all(&mut self.0, frame) {
            Err(error) if error.kind() == io::ErrorKind::BrokenPipe => Ok(()),
            result => result,
        }
    }
}

/// Takes the readiness channel and redirects subsequent standard output to discard.
pub fn take_service_ready_signal() -> io::Result<Option<ServiceReadySignal>> {
    if !std::env::args_os()
        .skip(2)
        .any(|argument| argument == SERVICE_READY_ARGUMENT)
    {
        return Ok(None);
    }
    #[cfg(windows)]
    {
        windows::take_standard_output().map(|file| file.map(ServiceReadySignal))
    }
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    {
        take_standard_output().map(|file| Some(ServiceReadySignal(file)))
    }
    #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
    {
        Ok(None)
    }
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
#[allow(
    unsafe_code,
    reason = "dup2 replaces one descriptor number with an open one"
)]
pub(crate) fn take_standard_output() -> io::Result<File> {
    use std::os::fd::{AsFd as _, AsRawFd as _};

    let taken = io::stdout().as_fd().try_clone_to_owned()?;
    let discard = std::fs::OpenOptions::new().write(true).open("/dev/null")?;
    // SAFETY: both descriptors are open for the duration of the call.
    if unsafe { libc::dup2(discard.as_raw_fd(), libc::STDOUT_FILENO) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(File::from(taken))
}

/// Starts the current executable's `__service` mode without inheriting host standard-I/O handles, and returns the channel on which the service signals that it answers requests.
pub fn spawn_service_process(executable: &Path) -> io::Result<ServiceReadiness> {
    let operation = obs::span!(
        INFO,
        "acyclic.runtime.spawn_service",
        outcome = obs::Empty,
        error.kind = obs::Empty
    );
    operation.in_scope(|| {
        let spawn = || {
            #[cfg(windows)]
            {
                windows::spawn_service_process(executable).map(ServiceReadiness)
            }
            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt as _;

                let mut command = std::process::Command::new(executable);
                command
                    .args(["__service", SERVICE_READY_ARGUMENT])
                    .stdin(std::process::Stdio::null())
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::null());
                command.process_group(0);
                let mut child = command.spawn()?;
                let readiness = child
                    .stdout
                    .take()
                    .ok_or_else(|| io::Error::other("service readiness channel is missing"))?;
                Ok(ServiceReadiness(File::from(std::os::fd::OwnedFd::from(
                    readiness,
                ))))
            }
        };
        operation.finish(spawn())
    })
}

/// Runs a trusted host command with closed stdin and bounded captured output.
pub fn process_output(
    command: &mut std::process::Command,
    timeout: std::time::Duration,
    max_bytes: usize,
) -> io::Result<std::process::Output> {
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    ProcessTree::spawn(command)?
        .wait_with_output(
            timeout,
            max_bytes,
            None,
            || Ok(()),
            |_| Ok(ProcessInput::default()),
            None,
            None,
        )
        .map_err(|failure| failure.error)
}

/// Runs a trusted host command with its configured streams and a bounded wait.
pub fn process_status(
    command: &mut std::process::Command,
    timeout: std::time::Duration,
) -> io::Result<std::process::ExitStatus> {
    let mut tree = ProcessTree::spawn(command)?;
    let result = tree.wait(timeout);
    tree.terminate(None)?;
    result
}

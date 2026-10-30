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
            let outcome = receive(&mut self.0);
            if let Ok(outcome) = &outcome {
                operation.record("ready", *outcome == ServiceStartOutcome::Ready);
            }
            operation.finish(outcome)
        })
    }
}

fn receive(reader: &mut impl Read) -> io::Result<ServiceStartOutcome> {
    let mut frame = [0];
    match reader.read(&mut frame) {
        Ok(0) => Ok(ServiceStartOutcome::Exited),
        Ok(_) if frame[0] == FAILURE_FRAME => {
            let mut reason = Vec::new();
            // The original failing service closes its channel on exit.
            match crate::io_progress::read_to_end(reader, &mut reason) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {}
                Err(error) => return Err(error),
            }
            let reason = String::from_utf8(reason)
                .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned());
            Ok(ServiceStartOutcome::Failed(reason))
        }
        Ok(_) => Ok(ServiceStartOutcome::Ready),
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => Ok(ServiceStartOutcome::Exited),
        Err(error) => Err(error),
    }
}

fn send(writer: &mut impl io::Write, frame: u8, body: &[u8]) -> io::Result<()> {
    let header = [frame];
    let result = (|| {
        #[cfg(windows)]
        if !body.is_empty() {
            // Windows pipes cannot gather separate buffers in one write.
            // Assemble once to retain the original single-call publication.
            let length = body
                .len()
                .checked_add(1)
                .ok_or(io::ErrorKind::InvalidInput)?;
            let mut frame = Vec::new();
            frame
                .try_reserve_exact(length)
                .map_err(|_| io::ErrorKind::OutOfMemory)?;
            frame.push(header[0]);
            frame.extend_from_slice(body);
            return crate::io_progress::write_all(&mut [io::IoSlice::new(&frame)], |buffers| {
                writer.write(&buffers[0])
            });
        }
        crate::io_progress::write_all(
            &mut [io::IoSlice::new(&header), io::IoSlice::new(body)],
            |buffers| writer.write_vectored(buffers),
        )
    })();
    match result {
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => Ok(()),
        result => result,
    }
}

/// The service's end of its readiness channel.
#[derive(Debug)]
pub struct ServiceReadySignal(File);

impl ServiceReadySignal {
    /// Tells a connected starter that the service answers requests, and closes the channel.
    /// A starter that has already exited does not prevent the service from running.
    pub fn signal(mut self) -> io::Result<()> {
        send(&mut self.0, READY_FRAME, &[])
    }

    /// Tells a connected starter why the service cannot start, and closes the
    /// channel, so the starter reports the cause instead of waiting for a
    /// service that will never answer.
    pub fn fail(mut self, reason: &str) -> io::Result<()> {
        send(&mut self.0, FAILURE_FRAME, reason.as_bytes())
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
/// The caller selects its capture schedule, including an unbounded OS wait.
pub fn process_output(
    command: &mut crate::ProcessCommand,
    schedule: impl Into<CaptureSchedule>,
    max_bytes: usize,
) -> io::Result<std::process::Output> {
    command
        .stdin(crate::ProcessStdio::null())
        .stdout(crate::ProcessStdio::piped())
        .stderr(crate::ProcessStdio::piped());
    ProcessTree::spawn(command)?
        .wait_with_output(
            schedule,
            max_bytes,
            None,
            || Ok(()),
            |_| Ok(ProcessInput::default()),
            None,
            None,
            None,
        )
        .map_err(|failure| failure.error)
}

/// Runs a trusted host command with its configured streams and caller schedule.
pub fn process_status(
    command: &mut crate::ProcessCommand,
    schedule: impl Into<CaptureSchedule>,
) -> io::Result<std::process::ExitStatus> {
    let mut tree = ProcessTree::spawn(command)?;
    let result = tree.wait(schedule);
    tree.terminate(None)?;
    result
}

use crate::*;

/// Selects standard output as the spawned service's readiness channel.
pub const SERVICE_READY_ARGUMENT: &str = "--signal-ready";

/// The starter's end of a service's readiness channel.
#[derive(Debug)]
pub struct ServiceReadiness(File);

impl ServiceReadiness {
    /// Blocks until the service signals that it answers requests (`true`), or exits without signalling (`false`).
    pub fn wait(mut self) -> io::Result<bool> {
        let operation = obs::span!(
            INFO,
            "acyclic.runtime.service_ready",
            ready = obs::Empty,
            outcome = obs::Empty,
            error.kind = obs::Empty
        );
        operation.in_scope(|| {
            let mut signal = [0_u8; 1];
            let ready = loop {
                match self.0.read(&mut signal) {
                    Ok(read) => break Ok(read != 0),
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) if error.kind() == io::ErrorKind::BrokenPipe => break Ok(false),
                    Err(error) => break Err(error),
                }
            };
            if let Ok(ready) = ready {
                operation.record("ready", ready);
            }
            operation.finish(ready)
        })
    }
}

/// The service's end of its readiness channel.
#[derive(Debug)]
pub struct ServiceReadySignal(File);

impl ServiceReadySignal {
    /// Tells the starter that the service answers requests, and closes the channel.
    pub fn signal(mut self) -> io::Result<()> {
        io::Write::write_all(&mut self.0, b"\n")
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
        operation.finish((|| {
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
        })())
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

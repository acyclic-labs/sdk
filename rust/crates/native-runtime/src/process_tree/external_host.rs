//! The executable adapter for the existing shared process-tree owner.
//!
//! This module owns no Job, process-group enumeration or reaper. Both ordinary
//! external commands and `ConPTY` transports enter through `ProcessCustody`.

use super::{
    Child, ProcessCommand, ProcessCustody, ProcessCustodyLease, ProcessStdio, ProcessTree,
};
use std::ffi::OsString;
use std::io::{self, Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

mod receipt;

struct State {
    tree: ProcessTree,
    cancelled: bool,
    failure: Option<io::Error>,
}

// The ordinary entry point has zero observation slots, preserving its original
// shared allocation size. The observed entry point funds one inline slot.
struct Shared<const OBSERVATIONS: usize> {
    state: Mutex<State>,
    control: [OnceLock<ProcessHostControlObservation>; OBSERVATIONS],
}

const _: () = assert!(std::mem::size_of::<Shared<0>>() == std::mem::size_of::<Mutex<State>>());

/// The original host control read, before acquiring the process-state lock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessHostControl {
    /// At least one control byte was read. The byte itself is not retained.
    Input,
    /// The original control handle reached EOF.
    Eof,
    /// The original read failed; this is not evidence of EOF or cancellation.
    Failed {
        /// The original read's IO classification.
        kind: io::ErrorKind,
        /// The original OS error, when one was supplied.
        raw_os_error: Option<i32>,
    },
}

/// One completed original control read, on the host's monotonic clock.
#[derive(Clone, Copy, Debug)]
pub struct ProcessHostControlObservation {
    /// Offset from this invocation's original observed entry.
    pub elapsed: Duration,
    /// The original read's result without retaining its byte payload.
    pub outcome: ProcessHostControl,
}

/// Fixed first observations from the original host's physical operations.
///
/// Durations share only this invocation's clock. Admission is the return from
/// the original custody-bound spawn; it is not a child-consumption event.
/// Control observes the host's cancellation input, not the selected root's
/// stdin. A missing control observation means none was published at the
/// terminal snapshot, not that a control producer completed or failed.
#[derive(Clone, Copy, Debug)]
pub struct ProcessHostProgress {
    /// Return from custody-bound spawn, including any admission/lease work.
    pub root_admitted: Duration,
    /// Return from the original root-exit wait, before the state lock.
    pub root_exit_observed: Duration,
    /// Completion of the original physical cleanup, before receipt writing.
    pub retired: Duration,
    /// First completed control read, if published at the terminal snapshot.
    pub control: Option<ProcessHostControlObservation>,
}

/// A terminal returned only after the original physical retirement succeeds.
#[derive(Debug)]
pub struct ProcessHostCompletion {
    /// The enclosing host result, including its cancellation result 124.
    pub exit_code: i32,
    /// The actual selected root status, before cancellation-result mapping.
    pub root_status: ExitStatus,
    /// Whether the original host control path applied cancellation.
    pub cancelled: bool,
    /// Fixed observations from this same physical invocation.
    pub progress: ProcessHostProgress,
}

/// The original host operation that returned an error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessHostPhase {
    /// Parsing the original invocation.
    Arguments,
    /// Exclusively reserving the original completion file.
    ReceiptReservation,
    /// Acquiring and fencing the original custody.
    CustodyAdmission,
    /// Spawning the selected root through that custody.
    RootAdmission,
    /// Starting the original host control reader.
    ControlAdmission,
    /// Waiting for the original root's exit event.
    RootExit,
    /// Joining the original physical tree retirement.
    Retirement,
    /// Returning the original control read or cancellation failure.
    Control,
    /// Writing and syncing the original completion receipt.
    ReceiptPublication,
}

/// Fixed original observations retained when a host operation fails.
///
/// An absent value is unobserved, never a successful terminal. In particular,
/// a root status does not certify physical tree retirement, and host control
/// input is not the selected root's stdin.
#[derive(Clone, Copy, Debug)]
pub struct ProcessHostPartialProgress {
    /// Original completed lease acquisition, or zero when no lease was requested.
    pub lease_wait: Option<Duration>,
    /// Return from the original custody-bound spawn.
    pub root_admitted: Option<Duration>,
    /// Successful return from the original non-reaping root wait.
    pub root_exit_observed: Option<Duration>,
    /// The selected root status, once the original owner observed it.
    pub root_status: Option<ExitStatus>,
    /// Successful return from the original physical retirement.
    pub retired: Option<Duration>,
    /// Whether the original control path applied cancellation.
    pub cancelled: bool,
    /// First original control read published at the terminal snapshot.
    pub control: Option<ProcessHostControlObservation>,
    phase: ProcessHostPhase,
}

impl ProcessHostPartialProgress {
    fn new() -> Self {
        Self {
            lease_wait: None,
            root_admitted: None,
            root_exit_observed: None,
            root_status: None,
            retired: None,
            cancelled: false,
            control: None,
            phase: ProcessHostPhase::Arguments,
        }
    }
}

/// The original host failure with its completed phase observations.
#[derive(Debug)]
pub struct ProcessHostFailure {
    /// The original error, including its native error code and source.
    pub error: io::Error,
    /// Failure publishing the same terminal receipt after an earlier failure.
    /// The original operation error above remains authoritative.
    pub receipt_error: Option<io::Error>,
    /// The original operation that failed.
    pub phase: ProcessHostPhase,
    /// Only observations already supplied by their original producers.
    pub progress: ProcessHostPartialProgress,
}

impl std::fmt::Display for ProcessHostFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "process host {:?}: {}", self.phase, self.error)?;
        if let Some(error) = &self.receipt_error {
            write!(formatter, "; terminal receipt publication: {error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ProcessHostFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

fn observe<const N: usize>(
    observations: &mut [ProcessHostPartialProgress; N],
    update: impl FnOnce(&mut ProcessHostPartialProgress),
) {
    if let Some(progress) = observations.first_mut() {
        update(progress);
    }
}

struct Directory(PathBuf);

#[derive(Debug)]
struct Invocation {
    executable: OsString,
    arguments: Vec<OsString>,
    inherit_input: bool,
    custody: Option<PathBuf>,
    lease: Option<PathBuf>,
    completion: Option<PathBuf>,
    invocation: Option<String>,
}

impl Invocation {
    fn parse(arguments: impl IntoIterator<Item = OsString>) -> io::Result<Self> {
        let mut arguments = arguments.into_iter();
        let mut inherit_input = false;
        let mut custody = None;
        let mut lease = None;
        let mut completion = None;
        let mut invocation = None;
        while let Some(argument) = arguments.next() {
            if argument == "--inherit-input" && !inherit_input {
                inherit_input = true;
            } else if argument == "--invocation" && invocation.is_none() {
                let value = arguments.next().ok_or_else(invalid_arguments)?;
                let value = value.into_string().map_err(|_| invalid_arguments())?;
                if value.is_empty()
                    || !value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
                {
                    return Err(invalid_arguments());
                }
                invocation = Some(value);
            } else if argument == "--custody" || argument == "--completion" || argument == "--lease"
            {
                let path = arguments.next().ok_or_else(invalid_arguments)?;
                if !Path::new(&path).is_absolute() {
                    return Err(invalid_arguments());
                }
                let destination = if argument == "--custody" {
                    &mut custody
                } else if argument == "--lease" {
                    &mut lease
                } else {
                    &mut completion
                };
                if destination.replace(PathBuf::from(path)).is_some() {
                    return Err(invalid_arguments());
                }
            } else if Path::new(&argument).is_absolute() {
                if (custody.is_some() && lease.is_some())
                    || (invocation.is_some() && completion.is_none())
                {
                    return Err(invalid_arguments());
                }
                return Ok(Self {
                    executable: argument,
                    arguments: arguments.collect(),
                    inherit_input,
                    custody,
                    lease,
                    completion,
                    invocation,
                });
            } else {
                return Err(invalid_arguments());
            }
        }
        Err(invalid_arguments())
    }
}

fn invalid_arguments() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "step=process-host-arguments; recovery=acyclic-process-host [--lease ABSOLUTE_ROOT | --custody ABSOLUTE_DIRECTORY] [--completion ABSOLUTE_FILE [--invocation OPAQUE_ID]] [--inherit-input] ABSOLUTE_EXECUTABLE ARGS",
    )
}

struct Completion(Option<std::fs::File>);

impl Completion {
    fn open(path: Option<&Path>) -> io::Result<Self> {
        path.map(|path| {
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
        })
        .transpose()
        .map(Self)
    }

    fn retired(
        &mut self,
        code: i32,
        cancelled: bool,
        lease_wait: std::time::Duration,
    ) -> io::Result<()> {
        if let Some(file) = &mut self.0 {
            // Windows exposes the process status as a DWORD, including when
            // ExitStatus and process::exit carry its bits in an i32.
            #[cfg(windows)]
            let code = u32::from_ne_bytes(code.to_ne_bytes());
            let lease_wait_seconds = lease_wait.as_secs_f64();
            writeln!(
                file,
                "{{\"schema\":\"acyclic.process-custody-completion.v2\",\"process_tree_retired\":true,\"exit_code\":{code},\"cancelled\":{cancelled},\"lease_wait_seconds\":{lease_wait_seconds}}}"
            )?;
            file.sync_all()?;
        }
        Ok(())
    }
}

impl Directory {
    fn create() -> io::Result<Self> {
        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "acyclic-process-host-{}-{epoch}",
            std::process::id()
        ));
        // An existing name is rejected before any command starts.
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        // Never erase an outstanding custody record on failed cleanup.
        let _ = std::fs::remove_dir(&self.0);
    }
}

/// Runs one external command through the native runtime's process custody.
///
/// Arguments are an absolute executable followed by its literal arguments.
/// With `--inherit-input` before that executable, the child receives stdin
/// unchanged (for a `ConPTY` transport). Otherwise the child receives EOF and
/// input or EOF on the host's stdin requests cancellation. Output is inherited
/// unchanged; on Windows the custodian keeps its pipe leases through physical
/// retirement of the admitted root and watched descendants, including when
/// this host is killed outright. Terminal adapters admit their suspended root
/// through the same custodian before resuming it.
///
/// `--completion ABSOLUTE_FILE` exclusively reserves a receipt before launch
/// and writes its v2 completion only after physical process-tree retirement.
/// Its exit code is the unsigned native process status on Windows and the
/// shell exit code on Unix, matching the enclosing process's observed exit.
/// Failed admission or drain never produces a completed receipt. It confers
/// no mount-retirement capability. `--custody ABSOLUTE_DIRECTORY` reuses the
/// shared custody fence before admission; the caller must already serialize
/// this directory's owners. Without it the invocation has private custody.
/// `--lease ABSOLUTE_ROOT` instead uses the shared native file-lock owner and
/// fences its durable custody before any child launch. It is mutually exclusive
/// with `--custody`. Lease wait is reported separately from retirement work.
/// `--invocation OPAQUE_ID` requires `--completion` and explicitly selects the
/// v3 receipt, bound to that caller-supplied nonsecret identifier. It retains
/// original phase observations and failure metadata; unobserved retirement is
/// never certified. Without this option, the exact v2 receipt remains unchanged.
///
/// This is an executable entry point: the caller must exit after it returns.
/// The control reader can still be blocked on stdin after a normal command
/// exit, and belongs to that process rather than a reusable runtime service.
pub fn run_external_process_host(arguments: impl IntoIterator<Item = OsString>) -> io::Result<i32> {
    let invocation = Invocation::parse(arguments)?;
    if invocation.invocation.is_some() {
        return observed_host(Ok(invocation), Instant::now())
            .map(|completion| completion.exit_code)
            .map_err(|failure| io::Error::new(failure.error.kind(), failure));
    }
    run_host(invocation, [], &mut [])
        .map(|(code, _, _, _)| code)
        .map_err(|(error, _)| error)
}

/// Runs the same host owner and returns its original physical observations.
///
/// This has the same executable-only lifetime contract as
/// [`run_external_process_host`]. It adds no reader, callback, timer or sink to
/// the physical path. The existing v2 receipt is unchanged. Failure retains
/// the original IO error and only the phases already observed at its producer.
/// Output remains inherited and has no arrival/EOF observations here.
pub fn run_external_process_host_with_progress(
    arguments: impl IntoIterator<Item = OsString>,
) -> Result<ProcessHostCompletion, ProcessHostFailure> {
    let origin = Instant::now();
    observed_host(Invocation::parse(arguments), origin)
}

fn observed_host(
    invocation: io::Result<Invocation>,
    origin: Instant,
) -> Result<ProcessHostCompletion, ProcessHostFailure> {
    let mut observations = [ProcessHostPartialProgress::new()];
    let result = invocation
        .map_err(|error| (error, None))
        .and_then(|invocation| run_host(invocation, [origin], &mut observations));
    let (exit_code, root_status, cancelled, progress) =
        result.map_err(|(error, receipt_error)| {
            let [progress] = observations;
            ProcessHostFailure {
                error,
                receipt_error,
                phase: progress.phase,
                progress,
            }
        })?;
    Ok(ProcessHostCompletion {
        exit_code,
        root_status,
        cancelled,
        progress: progress.expect("observed host retains its original clock"),
    })
}

fn run_host<const OBSERVATIONS: usize>(
    mut invocation: Invocation,
    origins: [Instant; OBSERVATIONS],
    observations: &mut [ProcessHostPartialProgress; OBSERVATIONS],
) -> Result<(i32, ExitStatus, bool, Option<ProcessHostProgress>), (io::Error, Option<io::Error>)> {
    let origin = origins.first().copied();
    let binding = invocation.invocation.take();
    observe(observations, |progress| {
        progress.phase = ProcessHostPhase::ReceiptReservation
    });
    let mut completion =
        Completion::open(invocation.completion.as_deref()).map_err(|error| (error, None))?;
    // Keep the original lease until the same completion sink has settled,
    // including when physical admission or retirement returned an error.
    let mut lease = None;
    let result = (|| -> io::Result<_> {
        observe(observations, |progress| {
            progress.phase = ProcessHostPhase::CustodyAdmission
        });
        lease = invocation
            .lease
            .as_deref()
            .map(ProcessCustodyLease::acquire)
            .transpose()?;
        observe(observations, |progress| {
            progress.lease_wait = Some(
                lease
                    .as_ref()
                    .map_or(Duration::ZERO, ProcessCustodyLease::waited),
            );
        });
        let directory = (invocation.custody.is_none() && lease.is_none())
            .then(Directory::create)
            .transpose()?;
        let custody = invocation
            .custody
            .as_deref()
            .or_else(|| directory.as_ref().map(|directory| directory.0.as_path()))
            .map(ProcessCustody::acquire)
            .transpose()?;
        let custody = lease
            .as_ref()
            .map(ProcessCustodyLease::custody)
            .or(custody.as_deref())
            .ok_or_else(invalid_arguments)?;
        let mut command = ProcessCommand::new(invocation.executable);
        command
            .args(invocation.arguments)
            .stdin(if invocation.inherit_input {
                ProcessStdio::inherit()
            } else {
                ProcessStdio::null()
            })
            .stdout(ProcessStdio::inherit())
            .stderr(ProcessStdio::inherit());
        observe(observations, |progress| {
            progress.phase = ProcessHostPhase::RootAdmission
        });
        let mut tree = ProcessTree::spawn_in_custody(&mut command, custody)?;
        let root_admitted = origin.map(|origin| origin.elapsed());
        observe(observations, |progress| {
            progress.root_admitted = root_admitted
        });
        // The sole waiter owns the actual Child. Cancellation only asks the same
        // tree's containment owner to terminate; it never substitutes PID polling.
        let child = tree.child.take().ok_or(io::ErrorKind::NotFound)?;
        tree.cleanup_on_drop = false;
        let shared = Arc::new(Shared::<OBSERVATIONS> {
            state: Mutex::new(State {
                tree,
                cancelled: false,
                failure: None,
            }),
            control: std::array::from_fn(|_| OnceLock::new()),
        });
        if !invocation.inherit_input {
            observe(observations, |progress| {
                progress.phase = ProcessHostPhase::ControlAdmission
            });
            let control = Arc::clone(&shared);
            let started = std::thread::Builder::new()
                .name("process-host-control".into())
                .spawn(move || {
                    let input = io::stdin().read(&mut [0]);
                    if let Some(origin) = origin {
                        let outcome = match &input {
                            Ok(0) => ProcessHostControl::Eof,
                            Ok(_) => ProcessHostControl::Input,
                            Err(error) => ProcessHostControl::Failed {
                                kind: error.kind(),
                                raw_os_error: error.raw_os_error(),
                            },
                        };
                        if let Some(slot) = control.control.first() {
                            let _ = slot.set(ProcessHostControlObservation {
                                elapsed: origin.elapsed(),
                                outcome,
                            });
                        }
                    }
                    let mut state = control
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state.cancelled = true;
                    state.failure = state
                        .tree
                        .terminate_descendants()
                        .err()
                        .or_else(|| input.err());
                });
            if let Err(admission) = started {
                let mut state = shared
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                return Err(reject_control_admission(&mut state.tree, child, admission));
            }
        }
        observe(observations, |progress| {
            progress.phase = ProcessHostPhase::RootExit
        });
        let waited = super::platform::wait_without_reaping(&child);
        let root_exit_observed = waited
            .as_ref()
            .ok()
            .and(origin)
            .map(|origin| origin.elapsed());
        observe(observations, |progress| {
            progress.root_exit_observed = root_exit_observed
        });
        let mut state = shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        observe(observations, |progress| {
            progress.cancelled = state.cancelled;
            progress.control = shared.control.first().and_then(OnceLock::get).copied();
        });
        if waited.is_ok() {
            observe(observations, |progress| {
                progress.phase = ProcessHostPhase::Retirement
            });
        }
        let status = retire_after_observation(&mut state.tree, child, waited, |status| {
            observe(observations, |progress| progress.root_status = Some(status));
        })?;
        let retired = origin.map(|origin| origin.elapsed());
        observe(observations, |progress| progress.retired = retired);
        if let Some(failure) = state.failure.take() {
            observe(observations, |progress| {
                progress.phase = ProcessHostPhase::Control
            });
            return Err(failure);
        }
        let code = if state.cancelled {
            124
        } else {
            exit_code(status)
        };
        observe(observations, |progress| {
            progress.phase = ProcessHostPhase::ReceiptPublication
        });
        let progress = origin.map(|_| ProcessHostProgress {
            root_admitted: root_admitted.expect("admitted on the original clock"),
            root_exit_observed: root_exit_observed.expect("observed on the original clock"),
            retired: retired.expect("retired on the original clock"),
            control: shared.control.first().and_then(OnceLock::get).copied(),
        });
        Ok((code, status, state.cancelled, progress))
    })();
    let publication = if let Some(binding) = &binding {
        match observations.first() {
            Some(progress) => completion.terminal(
                binding,
                result.as_ref().map_or(125, |(code, _, _, _)| *code),
                progress,
                result.as_ref().err(),
            ),
            None => Err(io::Error::from(io::ErrorKind::InvalidInput)),
        }
    } else if let Ok((code, _, cancelled, _)) = &result {
        completion.retired(
            *code,
            *cancelled,
            lease
                .as_ref()
                .map_or(Duration::ZERO, ProcessCustodyLease::waited),
        )
    } else {
        Ok(())
    };
    match (result, publication) {
        (Err(error), publication) => Err((error, publication.err())),
        (Ok(_), Err(error)) => {
            observe(observations, |progress| {
                progress.phase = ProcessHostPhase::ReceiptPublication
            });
            Err((error, None))
        }
        (Ok(result), Ok(())) => Ok(result),
    }
}

fn reject_control_admission(
    tree: &mut ProcessTree,
    child: Child,
    admission: io::Error,
) -> io::Error {
    // A reader that failed to start has no owner to request cancellation.
    // Restore the original child before the same physical retirement API.
    tree.child = Some(child);
    match tree.terminate(None) {
        Ok(()) => admission,
        Err(retirement) => io::Error::new(
            admission.kind(),
            format!(
                "step=process-host-control-admission: {admission}; retirement unresolved: {retirement}; recovery=acyclic-process-host --custody ABSOLUTE_DIRECTORY ABSOLUTE_EXECUTABLE ARGS"
            ),
        ),
    }
}

fn retire_after_observation(
    tree: &mut ProcessTree,
    child: Child,
    observed: io::Result<()>,
    observed_status: impl FnOnce(ExitStatus),
) -> io::Result<ExitStatus> {
    // Restore the original child before entering the one ordered retirement
    // API. Failed observation leaves both child and durable recovery record
    // owned; the adapter never removes records or closes containment itself.
    tree.child = Some(child);
    observed?;
    let status = tree.try_wait(None)?.ok_or(io::ErrorKind::WouldBlock)?;
    observed_status(status);
    tree.terminate(None)?;
    Ok(status)
}

fn exit_code(status: ExitStatus) -> i32 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        status
            .code()
            .unwrap_or_else(|| 128 + status.signal().unwrap_or(1))
    }
    #[cfg(windows)]
    status.code().unwrap_or(125)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOST_TEST: &str = "process_tree::external_host::tests::original_host_phases_preserve_root_status_and_control_custody";
    const ROOT_TEST: &str = "process_tree::external_host::tests::host_phase_root_fixture";
    const PHASE_ROOT: &str = "ACYCLIC_HOST_PHASE_ROOT";
    const PHASE_MODE: &str = "ACYCLIC_HOST_PHASE_MODE";
    const PHASE_RECEIPT: &str = "ACYCLIC_HOST_PHASE_RECEIPT";
    const PHASE_BINDING: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn host_phase_root_fixture() {
        let Some(root) = std::env::var_os(PHASE_ROOT) else {
            return;
        };
        println!("phase-root-ready");
        io::stdout().flush().unwrap();
        if std::env::var(PHASE_MODE).unwrap() == "exit" {
            std::process::exit(124);
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(Path::new(&root).join("blocked.lock"))
            .unwrap();
        let _held = crate::FileLock::acquire(file, crate::FileLockMode::Exclusive).unwrap();
        panic!("the original control must retire the root before its blocker releases");
    }

    #[test]
    fn original_host_phases_preserve_root_status_and_control_custody() {
        use std::io::BufRead as _;
        if let Some(root) = std::env::var_os(PHASE_ROOT) {
            let root = Path::new(&root);
            #[cfg(unix)]
            if std::env::var(PHASE_MODE).unwrap() == "failed" {
                use std::os::fd::AsRawFd as _;
                let directory = std::fs::File::open(root).unwrap();
                // SAFETY: this fixture process owns stdin. Its original control
                // read receives the actual directory-read error. Unlike EBADF,
                // std's stdin wrapper does not turn this error into EOF.
                assert_eq!(unsafe { libc::dup2(directory.as_raw_fd(), 0) }, 0);
            }
            let mut arguments = vec![
                "--custody".into(),
                root.join("custody").into_os_string(),
                "--completion".into(),
                root.join("completion.json").into_os_string(),
            ];
            if std::env::var(PHASE_RECEIPT).as_deref() == Ok("3") {
                arguments.extend(["--invocation".into(), PHASE_BINDING.into()]);
            }
            arguments.extend([
                std::env::current_exe().unwrap().into_os_string(),
                "--exact".into(),
                ROOT_TEST.into(),
                "--nocapture".into(),
            ]);
            let result = run_external_process_host_with_progress(arguments);
            if let Err(failure) = result {
                let progress = failure.progress;
                std::fs::write(
                    root.join("observations.json"),
                    serde_json::to_vec(&serde_json::json!({
                        "failed": true,
                        "phase": format!("{:?}", failure.phase),
                        "raw_os_error": failure.error.raw_os_error(),
                        "root_code": progress.root_status.and_then(|status| status.code()),
                        "root_status_observed": progress.root_status.is_some(),
                        "cancelled": progress.cancelled,
                        "admitted": progress.root_admitted.map(|elapsed| elapsed.as_secs_f64()),
                        "exit_observed": progress.root_exit_observed.map(|elapsed| elapsed.as_secs_f64()),
                        "retired": progress.retired.map(|elapsed| elapsed.as_secs_f64()),
                        "control_failed": progress.control.is_some_and(|control| matches!(
                            control.outcome, ProcessHostControl::Failed { raw_os_error: Some(_), .. }
                        )),
                    })).unwrap(),
                ).unwrap();
                std::process::exit(125);
            }
            let result = result.unwrap();
            let progress = result.progress;
            let control = progress.control.map(|control| {
                serde_json::json!({
                    "elapsed": control.elapsed.as_secs_f64(),
                    "outcome": match control.outcome {
                        ProcessHostControl::Input => "input",
                        ProcessHostControl::Eof => "eof",
                        ProcessHostControl::Failed { .. } => "failed",
                    },
                })
            });
            std::fs::write(
                root.join("observations.json"),
                serde_json::to_vec(&serde_json::json!({
                    "exit_code": result.exit_code,
                    "root_code": result.root_status.code(),
                    "cancelled": result.cancelled,
                    "admitted": progress.root_admitted.as_secs_f64(),
                    "exit_observed": progress.root_exit_observed.as_secs_f64(),
                    "retired": progress.retired.as_secs_f64(),
                    "control": control,
                }))
                .unwrap(),
            )
            .unwrap();
            std::process::exit(result.exit_code);
        }
        let failure = run_external_process_host_with_progress([]).unwrap_err();
        assert_eq!(failure.phase, ProcessHostPhase::Arguments);
        assert_eq!(failure.error.kind(), io::ErrorKind::InvalidInput);
        assert!(failure.progress.root_admitted.is_none());
        assert!(failure.progress.root_exit_observed.is_none());
        assert!(failure.progress.root_status.is_none());
        assert!(failure.progress.retired.is_none());
        let absent = tempfile::tempdir().unwrap();
        let failure = run_external_process_host_with_progress([
            "--inherit-input".into(),
            "--completion".into(),
            absent.path().join("failed.json").into_os_string(),
            "--invocation".into(),
            PHASE_BINDING.into(),
            "--custody".into(),
            absent.path().join("custody").into_os_string(),
            absent.path().join("absent-program").into_os_string(),
        ])
        .unwrap_err();
        assert_eq!(failure.phase, ProcessHostPhase::RootAdmission);
        assert_eq!(failure.error.kind(), io::ErrorKind::NotFound);
        assert!(failure.error.raw_os_error().is_some());
        assert!(failure.progress.root_admitted.is_none());
        assert!(failure.progress.root_exit_observed.is_none());
        assert!(failure.progress.root_status.is_none());
        assert!(failure.progress.retired.is_none());
        assert!(failure.receipt_error.is_none());
        let refused: serde_json::Value =
            serde_json::from_slice(&std::fs::read(absent.path().join("failed.json")).unwrap())
                .unwrap();
        assert_eq!(refused["schema"], "acyclic.process-custody-completion.v3");
        assert_eq!(refused["invocation"], PHASE_BINDING);
        assert_eq!(refused["failure"]["phase"], "root-admission");
        assert_eq!(
            refused["failure"]["error"]["raw_os_error"].as_i64(),
            failure.error.raw_os_error().map(i64::from)
        );
        assert_eq!(refused["process_tree_retired"], false);
        assert!(refused["root_status"].is_null());
        assert!(refused["phases"]["root_admitted_seconds"].is_null());
        assert!(refused["phases"]["retired_seconds"].is_null());
        let mut unavailable = Completion(Some(
            std::fs::File::open(absent.path().join("failed.json")).unwrap(),
        ));
        let publication = unavailable
            .terminal(PHASE_BINDING, 125, &failure.progress, Some(&failure.error))
            .unwrap_err();
        let direct = std::fs::File::open(absent.path().join("failed.json"))
            .unwrap()
            .write_all(b"refused write")
            .unwrap_err();
        assert_eq!(publication.raw_os_error(), direct.raw_os_error());
        assert_eq!(failure.phase, ProcessHostPhase::RootAdmission);
        assert!(failure.progress.retired.is_none());
        for version in ["2", "3"] {
            for mode in ["exit", "input", "eof"]
                .into_iter()
                .chain(cfg!(unix).then_some("failed"))
            {
                let root = tempfile::tempdir().unwrap();
                let file = std::fs::OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .open(root.path().join("blocked.lock"))
                    .unwrap();
                let _held = crate::FileLock::acquire(file, crate::FileLockMode::Exclusive).unwrap();
                let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
                command
                    .args(["--exact", HOST_TEST, "--nocapture"])
                    .env(PHASE_ROOT, root.path())
                    .env(PHASE_MODE, mode)
                    .env(PHASE_RECEIPT, version)
                    .stdin(ProcessStdio::piped())
                    .stdout(ProcessStdio::piped())
                    .stderr(ProcessStdio::inherit());
                let mut owner = ProcessTree::spawn(&mut command).unwrap();
                let child = owner.child.as_mut().unwrap();
                let mut control = child.stdin.take();
                let output = child.stdout.take().unwrap();
                let mut output = io::BufReader::new(output);
                let ready = mode == "failed"
                    || output.by_ref().lines().any(|line| {
                        line.expect("read original root readiness")
                            .ends_with("phase-root-ready")
                    });
                assert!(ready, "original root exited before signalling readiness");
                match mode {
                    "input" => control.as_mut().unwrap().write_all(b"x").unwrap(),
                    "eof" => drop(control.take()),
                    _ => {}
                }
                super::super::platform::wait_without_reaping(owner.child.as_ref().unwrap())
                    .unwrap();
                let status = owner.try_wait(None).unwrap().unwrap();
                owner.terminate(None).unwrap();
                drop(control);
                assert_eq!(
                    status.code(),
                    Some(if mode == "failed" { 125 } else { 124 })
                );
                assert!(owner.is_reaped());
                let observation: serde_json::Value = serde_json::from_slice(
                    &std::fs::read(root.path().join("observations.json")).unwrap(),
                )
                .unwrap();
                assert_eq!(observation["cancelled"], mode != "exit");
                let admitted = observation["admitted"].as_f64().unwrap();
                let exited = observation["exit_observed"].as_f64().unwrap();
                let retired = observation["retired"].as_f64().unwrap();
                assert!(admitted <= exited && exited <= retired);
                if version == "3" {
                    let receipt: serde_json::Value = serde_json::from_slice(
                        &std::fs::read(root.path().join("completion.json")).unwrap(),
                    )
                    .unwrap();
                    assert_eq!(receipt["schema"], "acyclic.process-custody-completion.v3");
                    assert_eq!(receipt["invocation"], PHASE_BINDING);
                    assert_eq!(receipt["platform"], std::env::consts::OS);
                    assert_eq!(
                        receipt["host_exit_code"],
                        if mode == "failed" { 125 } else { 124 }
                    );
                    assert_eq!(receipt["cancelled"], mode != "exit");
                    assert_eq!(receipt["root_status"]["code"], observation["root_code"]);
                    assert!(receipt["root_status"]["native"].is_number());
                    assert_eq!(receipt["process_tree_retired"], true);
                    assert_eq!(
                        receipt["phases"]["root_admitted_seconds"],
                        observation["admitted"]
                    );
                    assert_eq!(
                        receipt["phases"]["root_exit_observed_seconds"],
                        observation["exit_observed"]
                    );
                    assert_eq!(receipt["phases"]["retired_seconds"], observation["retired"]);
                    if mode == "failed" {
                        assert_eq!(receipt["failure"]["phase"], "control");
                        assert_eq!(
                            receipt["failure"]["error"]["raw_os_error"],
                            observation["raw_os_error"]
                        );
                    } else {
                        assert!(receipt["failure"].is_null());
                    }
                    println!("host-receipt-capture:{receipt}");
                }
                if mode == "failed" {
                    assert_eq!(observation["failed"], true);
                    assert_eq!(observation["phase"], "Control");
                    #[cfg(unix)]
                    {
                        let expected = std::fs::File::open(root.path())
                            .unwrap()
                            .read(&mut [0])
                            .unwrap_err();
                        assert_eq!(
                            observation["raw_os_error"].as_i64(),
                            expected.raw_os_error().map(i64::from)
                        );
                    }
                    assert_eq!(observation["control_failed"], true);
                    assert_eq!(observation["root_status_observed"], true);
                    assert_ne!(observation["root_code"], 124);
                    if version == "2" {
                        assert!(
                            std::fs::read(root.path().join("completion.json"))
                                .unwrap()
                                .is_empty()
                        );
                    }
                    assert_eq!(
                        std::fs::read_dir(root.path().join("custody"))
                            .unwrap()
                            .count(),
                        0
                    );
                    continue;
                }
                assert_eq!(observation["exit_code"], 124);
                if mode == "exit" {
                    assert_eq!(observation["root_code"], 124);
                    assert!(observation["control"].is_null());
                } else {
                    assert_ne!(
                        observation["root_code"], 124,
                        "host cancellation must not invent the root status"
                    );
                    assert_eq!(observation["control"]["outcome"], mode);
                    let control = observation["control"]["elapsed"].as_f64().unwrap();
                    assert!(admitted <= control && control <= retired);
                }
                assert_eq!(
                    std::fs::read_dir(root.path().join("custody"))
                        .unwrap()
                        .count(),
                    0
                );
                if version == "2" {
                    let receipt: serde_json::Value = serde_json::from_slice(
                        &std::fs::read(root.path().join("completion.json")).unwrap(),
                    )
                    .unwrap();
                    assert_eq!(receipt["schema"], "acyclic.process-custody-completion.v2");
                    assert_eq!(
                        receipt.as_object().unwrap().len(),
                        5,
                        "the original receipt field set stays exact"
                    );
                    assert_eq!(receipt["cancelled"], mode != "exit");
                    assert_eq!(receipt["process_tree_retired"], true);
                }
            }
        }
    }

    #[test]
    fn native_lease_excludes_successors_until_the_custody_owner_releases_it() {
        let root = tempfile::tempdir().unwrap();
        let lease = ProcessCustodyLease::acquire(root.path()).unwrap();
        let open = || {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(root.path().join("owner.lock"))
                .unwrap()
        };
        assert_eq!(
            crate::FileLock::try_acquire(open(), crate::FileLockMode::Exclusive)
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(lease.custody().fence_stale().unwrap(), 0);
        drop(lease);
        let successor = ProcessCustodyLease::acquire(root.path()).unwrap();
        assert_eq!(successor.custody().fence_stale().unwrap(), 0);
    }

    #[test]
    fn serial_admission_rejects_failed_stale_recovery_before_launch() {
        let root = tempfile::tempdir().unwrap();
        let lease = ProcessCustodyLease::acquire(root.path()).unwrap();
        let mut tree = ProcessTree::spawn_in_custody(
            &mut super::super::tests::command("partial", root.path()),
            lease.custody(),
        )
        .unwrap();
        super::super::tests::ready(root.path(), "partial-ready", || {
            tree.try_wait(None).map_or(true, |status| status.is_some())
        });
        let records = root.path().join("trees");
        let path = std::fs::read_dir(&records)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let captured = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        drop(lease);
        // Former caller-only serialization had no recovery admission. Its
        // open succeeds at this real failed-unlink boundary; the shared
        // serial entry point must reject before admitting any child.
        let former_admitted = ProcessCustody::open(&records).is_ok();
        let rejected = ProcessCustodyLease::acquire(root.path()).is_err();
        std::fs::remove_dir(&path).unwrap();
        std::fs::write(&path, captured).unwrap();
        tree.terminate(None).unwrap();
        assert!(former_admitted);
        assert!(rejected);
        assert!(tree.is_reaped());
    }

    #[test]
    fn leased_host_rejects_ambiguous_custody_and_preserves_literal_arguments() {
        let executable = std::env::current_exe().unwrap().into_os_string();
        let root = tempfile::tempdir().unwrap();
        let path = root.path().as_os_str().to_owned();
        let parsed = Invocation::parse([
            "--lease".into(),
            path.clone(),
            executable.clone(),
            "--lease".into(),
        ])
        .unwrap();
        assert_eq!(parsed.lease.as_deref(), Some(root.path()));
        assert_eq!(parsed.arguments, ["--lease"]);
        for arguments in [
            vec!["--lease".into(), "relative".into(), executable.clone()],
            vec![
                "--lease".into(),
                path.clone(),
                "--lease".into(),
                path.clone(),
                executable.clone(),
            ],
            vec![
                "--custody".into(),
                path.clone(),
                "--lease".into(),
                path,
                executable,
            ],
        ] {
            assert_eq!(
                Invocation::parse(arguments).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
    }

    #[test]
    fn failed_control_admission_retires_through_the_original_process_owner() {
        for former_order in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let records = root.path().join("custody");
            let custody = ProcessCustody::open(&records).unwrap();
            let mut tree = ProcessTree::spawn_in_custody(
                &mut super::super::tests::command("partial", root.path()),
                &custody,
            )
            .unwrap();
            super::super::tests::ready(root.path(), "partial-ready", || {
                tree.try_wait(None).map_or(true, |status| status.is_some())
            });
            let child = tree.child.take().unwrap();
            tree.cleanup_on_drop = false;
            let admission = io::Error::from(io::ErrorKind::WouldBlock);
            let (error, retained_for_fixture_cleanup) = if former_order {
                // The former spawn-? cut propagated without returning Child
                // to its tree or invoking retirement. Keep that Child only
                // in the fixture so this negative can explicitly drain it.
                (admission, Some(child))
            } else {
                (reject_control_admission(&mut tree, child, admission), None)
            };
            let completed = tree.is_reaped() && std::fs::read_dir(&records).unwrap().count() == 0;
            if let Some(child) = retained_for_fixture_cleanup {
                tree.child = Some(child);
            }
            tree.terminate(None).unwrap();
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(completed, !former_order);
            assert!(tree.is_reaped());
            assert_eq!(std::fs::read_dir(&records).unwrap().count(), 0);
        }
    }

    #[test]
    fn failed_control_admission_retains_recovery_when_retirement_fails() {
        let root = tempfile::tempdir().unwrap();
        let records = root.path().join("custody");
        let custody = ProcessCustody::open(&records).unwrap();
        let mut tree = ProcessTree::spawn_in_custody(
            &mut super::super::tests::command("partial", root.path()),
            &custody,
        )
        .unwrap();
        super::super::tests::ready(root.path(), "partial-ready", || {
            tree.try_wait(None).map_or(true, |status| status.is_some())
        });
        let record = std::fs::read_dir(&records)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let captured = std::fs::read(&record).unwrap();
        std::fs::remove_file(&record).unwrap();
        std::fs::create_dir(&record).unwrap();
        let child = tree.child.take().unwrap();
        tree.cleanup_on_drop = false;
        let error =
            reject_control_admission(&mut tree, child, io::Error::from(io::ErrorKind::WouldBlock));
        let retained = tree.custody.is_some() && !tree.is_reaped();
        std::fs::remove_dir(&record).unwrap();
        std::fs::write(&record, captured).unwrap();
        tree.terminate(None).unwrap();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(error.to_string().contains("retirement unresolved"));
        assert!(retained);
        assert!(tree.is_reaped());
        assert_eq!(std::fs::read_dir(&records).unwrap().count(), 0);
    }

    #[test]
    fn external_retirement_preserves_the_observed_native_exit_status() {
        let root = tempfile::tempdir().unwrap();
        let custody = ProcessCustody::open(&root.path().join("custody")).unwrap();
        let mut tree = ProcessTree::spawn_in_custody(
            &mut super::super::tests::command("exit-code", root.path()),
            &custody,
        )
        .unwrap();
        let child = tree.child.take().unwrap();
        super::super::platform::wait_without_reaping(&child).unwrap();
        let mut state = State {
            tree,
            cancelled: false,
            failure: None,
        };
        assert_eq!(
            retire_after_observation(&mut state.tree, child, Ok(()), |_| {})
                .unwrap()
                .code(),
            Some(7)
        );
        assert!(state.tree.is_reaped());
    }

    #[test]
    fn failed_external_retirement_keeps_the_canonical_recovery_owner() {
        for former_order in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let records = root.path().join("custody");
            let custody = ProcessCustody::open(&records).unwrap();
            let mut tree = ProcessTree::spawn_in_custody(
                &mut super::super::tests::command("partial", root.path()),
                &custody,
            )
            .unwrap();
            super::super::tests::ready(root.path(), "partial-ready", || {
                tree.try_wait(None).map_or(true, |status| status.is_some())
            });
            let record = std::fs::read_dir(&records)
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path();
            let record_bytes = std::fs::read(&record).unwrap();
            // A real failed unlink, rather than a synthetic process result.
            std::fs::remove_file(&record).unwrap();
            std::fs::create_dir(&record).unwrap();
            tree.terminate_descendants().unwrap();
            let mut child = tree.child.take().unwrap();
            super::super::platform::wait_without_reaping(&child).unwrap();
            let mut state = State {
                tree,
                cancelled: false,
                failure: None,
            };
            let failed = if former_order {
                // Actual pre-integration ordering from 579657faa7: reap the
                // child, close its Job, then attempt record removal directly.
                child.wait().unwrap();
                #[cfg(windows)]
                state.tree.guard.close(None).unwrap();
                state.tree.custody.as_ref().unwrap().remove().is_err()
            } else {
                retire_after_observation(&mut state.tree, child, Ok(()), |_| {}).is_err()
            };
            let retained = state.tree.custody.is_some() && !state.tree.is_reaped();
            #[cfg(windows)]
            let former_completion = state.tree.child.is_none() && !state.tree.guard.pending();
            #[cfg(unix)]
            let unreaped = state.tree.child.is_some();
            // Restore the filesystem boundary and explicitly retire the old
            // control's record before assertions; neither case leaves work.
            std::fs::remove_dir(&record).unwrap();
            std::fs::write(&record, record_bytes).unwrap();
            if former_order {
                super::super::custody::Record::retire(&mut state.tree.custody, None).unwrap();
            }
            state.tree.terminate(None).unwrap();
            assert!(failed);
            assert!(retained);
            #[cfg(windows)]
            assert!(
                former_completion,
                "the former completion predicate loses pending recovery work"
            );
            #[cfg(unix)]
            assert_eq!(
                unreaped, !former_order,
                "failed record retirement must retain the unreaped child"
            );
            assert!(state.tree.is_reaped());
            assert_eq!(std::fs::read_dir(&records).unwrap().count(), 0);
        }
    }

    #[test]
    fn failed_host_observation_keeps_the_live_child_and_recovery_authority() {
        let root = tempfile::tempdir().unwrap();
        let records = root.path().join("custody");
        let custody = ProcessCustody::open(&records).unwrap();
        let mut tree = ProcessTree::spawn_in_custody(
            &mut super::super::tests::command("partial", root.path()),
            &custody,
        )
        .unwrap();
        super::super::tests::ready(root.path(), "partial-ready", || {
            tree.try_wait(None).map_or(true, |status| status.is_some())
        });
        let child = tree.child.take().unwrap();
        let failure = retire_after_observation(
            &mut tree,
            child,
            Err(io::ErrorKind::Interrupted.into()),
            |_| {},
        );
        let retained = tree.child.is_some()
            && tree.custody.is_some()
            && std::fs::read_dir(&records).unwrap().count() == 1;
        tree.terminate(None).unwrap();
        assert_eq!(failure.unwrap_err().kind(), io::ErrorKind::Interrupted);
        assert!(retained);
        assert!(tree.is_reaped());
        assert_eq!(std::fs::read_dir(&records).unwrap().count(), 0);
    }

    // Captured from 579657faa74dc6a3a6334cf97e059a70d0724c03's external
    // adapter ordering. Only state.tree is passed as an argument here; the
    // native operations and the error propagation cuts are unchanged.
    fn retire_in_former_order(
        tree: &mut ProcessTree,
        mut child: Child,
        waited: io::Result<()>,
    ) -> io::Result<ExitStatus> {
        let terminated = tree.terminate_descendants();
        let status = child.wait();
        #[cfg(windows)]
        let closed = tree.guard.close(None);
        let retired = tree
            .custody
            .as_ref()
            .map_or(Ok(()), super::super::custody::Record::remove);
        waited?;
        terminated?;
        #[cfg(windows)]
        closed?;
        retired?;
        status
    }

    #[test]
    fn failed_observation_retains_recovery_authority_against_actual_former_order() {
        for former_order in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let records = root.path().join("custody");
            let custody = ProcessCustody::open(&records).unwrap();
            let mut tree = ProcessTree::spawn_in_custody(
                &mut super::super::tests::command("partial", root.path()),
                &custody,
            )
            .unwrap();
            super::super::tests::ready(root.path(), "partial-ready", || {
                tree.try_wait(None).map_or(true, |status| status.is_some())
            });
            let child = tree.child.take().unwrap();
            let observed = Err(io::ErrorKind::Interrupted.into());
            let failure = if former_order {
                retire_in_former_order(&mut tree, child, observed)
            } else {
                retire_after_observation(&mut tree, child, observed, |_| {})
            };
            let retained = tree.child.is_some()
                && tree.custody.is_some()
                && std::fs::read_dir(&records).unwrap().count() == 1;
            // Always drain through the sole production owner before asserting.
            tree.terminate(None).unwrap();
            if former_order {
                // The captured former ordering removed the record directly
                // after losing Child. Retire its abandoned fixture handle;
                // the recovery observation above must remain unchanged.
                super::super::custody::Record::retire(&mut tree.custody, None).unwrap();
            }
            assert_eq!(failure.unwrap_err().kind(), io::ErrorKind::Interrupted);
            assert_eq!(
                retained, !former_order,
                "the same failed observation must retain recovery authority"
            );
            assert!(tree.is_reaped());
            assert_eq!(std::fs::read_dir(&records).unwrap().count(), 0);
        }
    }

    #[test]
    fn process_host_options_stop_at_the_literal_executable() {
        let executable = std::env::current_exe().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let receipt = directory.path().join("completion.json");
        let parsed = Invocation::parse([
            "--completion".into(),
            receipt.clone().into_os_string(),
            "--custody".into(),
            directory.path().as_os_str().to_owned(),
            "--inherit-input".into(),
            executable.clone().into_os_string(),
            "--completion".into(),
            "literal & % child argument".into(),
        ])
        .unwrap();
        assert_eq!(parsed.executable, executable.as_os_str());
        assert_eq!(parsed.completion.as_deref(), Some(receipt.as_path()));
        assert_eq!(parsed.custody.as_deref(), Some(directory.path()));
        assert!(parsed.inherit_input);
        assert_eq!(
            parsed.arguments,
            ["--completion", "literal & % child argument"]
        );
    }

    #[test]
    fn process_host_rejects_relative_and_duplicate_receipt_admission() {
        let executable = std::env::current_exe().unwrap().into_os_string();
        let receipt = std::env::temp_dir().join("receipt.json").into_os_string();
        for arguments in [
            vec!["--completion".into(), "relative".into(), executable.clone()],
            vec!["--custody".into(), "relative".into(), executable.clone()],
            vec![
                "--completion".into(),
                receipt.clone(),
                "--completion".into(),
                receipt,
                executable,
            ],
        ] {
            assert_eq!(
                Invocation::parse(arguments).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
    }

    #[test]
    fn process_completion_is_exclusive_and_never_publishes_before_retirement() {
        #[cfg(windows)]
        let codes = [0, 17, 124, 0x7fff_ffff, 0x8000_0000, u32::MAX];
        #[cfg(not(windows))]
        let codes = [17_u32];
        for raw in codes {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("completion.json");
            let mut completion = Completion::open(Some(&path)).unwrap();
            assert!(std::fs::read(&path).unwrap().is_empty());
            assert_eq!(
                Completion::open(Some(&path)).err().unwrap().kind(),
                io::ErrorKind::AlreadyExists
            );
            #[cfg(windows)]
            let code = {
                use std::os::windows::process::ExitStatusExt as _;
                exit_code(ExitStatus::from_raw(raw))
            };
            #[cfg(not(windows))]
            let code = 17;
            completion
                .retired(code, false, std::time::Duration::ZERO)
                .unwrap();
            let receipt = std::fs::read_to_string(path).unwrap();
            assert!(receipt.contains(&format!("\"exit_code\":{raw},")));
            assert!(receipt.contains("\"process_tree_retired\":true"));
            assert!(receipt.contains("\"schema\":\"acyclic.process-custody-completion.v2\""));
            assert!(receipt.contains("\"lease_wait_seconds\":0"));
            assert!(!receipt.contains("mount"));
        }
    }
}

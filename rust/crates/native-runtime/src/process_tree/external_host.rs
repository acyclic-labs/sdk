//! The executable adapter for the existing shared process-tree owner.
//!
//! This module owns no Job, process-group enumeration or reaper. Both ordinary
//! external commands and `ConPTY` transports enter through `ProcessCustody`.

use super::{ProcessCustody, ProcessCustodyLease, ProcessTree};
use std::ffi::OsString;
use std::io::{self, Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};

struct State {
    tree: ProcessTree,
    cancelled: bool,
    failure: Option<io::Error>,
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
}

impl Invocation {
    fn parse(arguments: impl IntoIterator<Item = OsString>) -> io::Result<Self> {
        let mut arguments = arguments.into_iter();
        let mut inherit_input = false;
        let mut custody = None;
        let mut lease = None;
        let mut completion = None;
        while let Some(argument) = arguments.next() {
            if argument == "--inherit-input" && !inherit_input {
                inherit_input = true;
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
                if custody.is_some() && lease.is_some() {
                    return Err(invalid_arguments());
                }
                return Ok(Self {
                    executable: argument,
                    arguments: arguments.collect(),
                    inherit_input,
                    custody,
                    lease,
                    completion,
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
        "step=process-host-arguments; recovery=acyclic-process-host [--lease ABSOLUTE_ROOT | --custody ABSOLUTE_DIRECTORY] [--completion ABSOLUTE_FILE] [--inherit-input] ABSOLUTE_EXECUTABLE ARGS",
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
/// Failed admission or drain never produces a completed receipt. It confers
/// no mount-retirement capability. `--custody ABSOLUTE_DIRECTORY` reuses the
/// shared custody fence before admission; the caller must already serialize
/// this directory's owners. Without it the invocation has private custody.
/// `--lease ABSOLUTE_ROOT` instead uses the shared native file-lock owner and
/// fences its durable custody before any child launch. It is mutually exclusive
/// with `--custody`. Lease wait is reported separately from retirement work.
///
/// This is an executable entry point: the caller must exit after it returns.
/// The control reader can still be blocked on stdin after a normal command
/// exit, and belongs to that process rather than a reusable runtime service.
pub fn run_external_process_host(arguments: impl IntoIterator<Item = OsString>) -> io::Result<i32> {
    let invocation = Invocation::parse(arguments)?;
    let mut completion = Completion::open(invocation.completion.as_deref())?;
    let lease = invocation
        .lease
        .as_deref()
        .map(ProcessCustodyLease::acquire)
        .transpose()?;
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
    let mut command = Command::new(invocation.executable);
    command
        .args(invocation.arguments)
        .stdin(if invocation.inherit_input {
            Stdio::inherit()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let mut tree = ProcessTree::spawn_in_custody(&mut command, custody)?;
    // The sole waiter owns the actual Child. Cancellation only asks the same
    // tree's containment owner to terminate; it never substitutes PID polling.
    let child = tree.child.take().ok_or(io::ErrorKind::NotFound)?;
    tree.cleanup_on_drop = false;
    let state = Arc::new(Mutex::new(State {
        tree,
        cancelled: false,
        failure: None,
    }));
    if !invocation.inherit_input {
        let control = Arc::clone(&state);
        let started = std::thread::Builder::new()
            .name("process-host-control".into())
            .spawn(move || {
                let input = io::stdin().read(&mut [0]);
                let mut state = control
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
            let mut state = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            return Err(reject_control_admission(&mut state.tree, child, admission));
        }
    }
    let waited = super::platform::wait_without_reaping(&child);
    let mut state = state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let status = retire_after_observation(&mut state.tree, child, waited)?;
    if let Some(failure) = state.failure.take() {
        return Err(failure);
    }
    let code = if state.cancelled {
        124
    } else {
        exit_code(status)
    };
    completion.retired(
        code,
        state.cancelled,
        lease
            .as_ref()
            .map_or(std::time::Duration::ZERO, ProcessCustodyLease::waited),
    )?;
    Ok(code)
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
) -> io::Result<ExitStatus> {
    // Restore the original child before entering the one ordered retirement
    // API. Failed observation leaves both child and durable recovery record
    // owned; the adapter never removes records or closes containment itself.
    tree.child = Some(child);
    observed?;
    let status = tree.try_wait(None)?.ok_or(io::ErrorKind::WouldBlock)?;
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
            retire_after_observation(&mut state.tree, child, Ok(()))
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
                retire_after_observation(&mut state.tree, child, Ok(())).is_err()
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
        let failure =
            retire_after_observation(&mut tree, child, Err(io::ErrorKind::Interrupted.into()));
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
                retire_after_observation(&mut tree, child, observed)
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
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("completion.json");
        let mut completion = Completion::open(Some(&path)).unwrap();
        assert!(std::fs::read(&path).unwrap().is_empty());
        assert_eq!(
            Completion::open(Some(&path)).err().unwrap().kind(),
            io::ErrorKind::AlreadyExists
        );
        completion
            .retired(17, false, std::time::Duration::ZERO)
            .unwrap();
        let receipt = std::fs::read_to_string(path).unwrap();
        assert!(receipt.contains("\"exit_code\":17"));
        assert!(receipt.contains("\"process_tree_retired\":true"));
        assert!(receipt.contains("\"schema\":\"acyclic.process-custody-completion.v2\""));
        assert!(receipt.contains("\"lease_wait_seconds\":0"));
        assert!(!receipt.contains("mount"));
    }
}

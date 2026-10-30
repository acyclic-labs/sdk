//! Job membership is rechecked after a pinned member exits, never on a timer.
use super::super::capture_wait::{CaptureWait, pending::Cleanup};
use super::*;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::time::Instant;
use windows_sys::Win32::Foundation::{ERROR_INVALID_PARAMETER, ERROR_MORE_DATA};
use windows_sys::Win32::System::JobObjects::{
    IsProcessInJob, JOBOBJECT_BASIC_PROCESS_ID_LIST, JobObjectBasicProcessIdList,
};
use windows_sys::Win32::System::Threading::{
    OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE,
};

pub(super) fn wait_empty(
    job: HANDLE,
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<()> {
    let deadline = Instant::now() + super::super::ProcessTree::TERMINATION_TIMEOUT;
    let mut wait = None;
    loop {
        let Some(pid) = first_member(job, meter)? else {
            return Ok(());
        };
        if Instant::now() >= deadline {
            return Err(io::ErrorKind::TimedOut.into());
        }
        let Some(process) = Member::open(pid, job, meter)? else {
            // The listed process exited or its PID was reused before OpenProcess.
            continue;
        };
        if wait.is_none() {
            wait = Some(CaptureWait::new(meter)?);
        }
        wait.as_mut()
            .ok_or(io::ErrorKind::InvalidData)?
            .wait_handle(&process.handle, deadline)?;
    }
}

fn first_member(
    job: HANDLE,
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<Option<u32>> {
    // One listed member suffices: no allocation proportional to the tree and no
    // incomplete-prefix interpretation as an empty job.
    // SAFETY: this fixed output structure accepts zero for every field.
    let mut list: JOBOBJECT_BASIC_PROCESS_ID_LIST = unsafe { zeroed() };
    super::super::process_control_attempt(meter, crate::NativeOperationKind::Read, || {
        // SAFETY: a live owned Job and the complete writable fixed ABI structure.
        let complete = unsafe {
            QueryInformationJobObject(
                job,
                JobObjectBasicProcessIdList,
                (&raw mut list).cast(),
                u32::try_from(size_of::<JOBOBJECT_BASIC_PROCESS_ID_LIST>())
                    .map_err(io::Error::other)?,
                std::ptr::null_mut(),
            )
        } != 0;
        if !complete {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_MORE_DATA.cast_signed()) {
                return Err(error);
            }
        }
        super::super::job_members::first_reported_member(
            super::super::job_members::Page {
                assigned: usize::try_from(list.NumberOfAssignedProcesses)
                    .map_err(io::Error::other)?,
                listed: usize::try_from(list.NumberOfProcessIdsInList).map_err(io::Error::other)?,
                complete,
            },
            list.ProcessIdList[0],
        )
    })
}

struct Member<'a> {
    handle: OwnedHandle,
    cleanup: Cleanup<'a>,
}

impl<'a> Member<'a> {
    fn open(
        pid: u32,
        job: HANDLE,
        meter: Option<&'a dyn crate::NativeReadMeter>,
    ) -> io::Result<Option<Self>> {
        let cleanup = Cleanup::new(meter)?;
        let (opened, recorded) =
            super::super::pipe_operation_attempt(meter, crate::NativeOperationKind::Read, || {
                // SAFETY: a noninherited query/synchronize handle to this PID, owned on success.
                let handle = unsafe {
                    OpenProcess(
                        PROCESS_SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION,
                        0,
                        pid,
                    )
                };
                if handle.is_null() {
                    let error = io::Error::last_os_error();
                    return if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER.cast_signed()) {
                        Ok(None)
                    } else {
                        Err(error)
                    };
                }
                // SAFETY: OpenProcess transferred this fresh owned handle.
                let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
                Ok(Some(handle))
            })?;
        let process = opened?.map(|handle| Self { handle, cleanup });
        recorded?;
        let Some(process) = process else {
            return Ok(None);
        };
        let member =
            super::super::process_control_attempt(meter, crate::NativeOperationKind::Read, || {
                let mut member = 0;
                // SAFETY: both handles remain live; the process handle pins its identity.
                if unsafe { IsProcessInJob(process.handle.as_raw_handle(), job, &raw mut member) }
                    == 0
                {
                    return Err(io::Error::last_os_error());
                }
                Ok(member != 0)
            })?;
        Ok(member.then_some(process))
    }
}

impl Drop for Member<'_> {
    fn drop(&mut self) {
        let _ = self.cleanup.record();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reused_pid_outside_the_job_is_never_waited_on() -> io::Result<()> {
        let guard = Guard::new()?;
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args([
                "--exact",
                "process_tree::capture_events_tests::event_capture_child",
                "--nocapture",
            ])
            .env("ACYCLIC_TEST_EVENT_CAPTURE", "quiet")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        let mut foreign = command.spawn()?;
        let meter = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
        let result = Member::open(foreign.id(), guard.job, Some(&meter));
        let _ = foreign.kill();
        foreign.wait()?;
        assert!(
            result?.is_none(),
            "a PID alone cannot grant membership in this Job"
        );
        assert_eq!(
            meter
                .write_attempts
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );
        Ok(())
    }

    #[test]
    fn an_empty_job_requires_only_one_query() -> io::Result<()> {
        let guard = Guard::new()?;
        let meter = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
        wait_empty(guard.job, Some(&meter))?;
        assert_eq!(
            meter
                .read_attempts
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );
        assert_eq!(
            meter
                .write_attempts
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
        Ok(())
    }
    #[test]
    fn the_one_slot_query_waits_on_a_live_member_before_proving_empty() -> io::Result<()> {
        use std::io::BufRead as _;
        use std::process::Stdio;
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args(["--exact", member_child_name(), "--nocapture"])
            .env("ACYCLIC_JOB_MEMBER_PAGE_CHILD", "parent")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut tree = crate::ProcessTree::spawn(&mut command)?;
        let mut output =
            std::io::BufReader::new(tree.take_stdout().ok_or(io::ErrorKind::BrokenPipe)?);
        let mut line = String::new();
        let descendant = loop {
            line.clear();
            if output.read_line(&mut line)? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            if let Some((_, pid)) = line.trim().rsplit_once("JOB_PAGE_CHILD=") {
                break pid.parse::<u32>().map_err(io::Error::other)?;
            }
        };
        let root = tree.child.as_ref().ok_or(io::ErrorKind::NotFound)?.id();
        // Retain independent query custody across ProcessTree::terminate,
        // which closes the tree's own Job handle after observing cleanup.
        // SAFETY: tree owns this live Job throughout the duplicate operation.
        let job = unsafe { std::os::windows::io::BorrowedHandle::borrow_raw(tree.guard.job) }
            .try_clone_to_owned()?;
        let members = crate::process_tree::custody::members(job.as_raw_handle())?;
        assert!(members.contains(&root) && members.contains(&descendant));
        let selected = first_member(job.as_raw_handle(), None)?;
        assert!(selected.is_some_and(|pid| members.contains(&pid)));
        let terminated = tree.terminate(None);
        eprintln!(
            "JOB_FIRST_TERMINATE result={terminated:?} tree_handle_closed={}",
            tree.guard.job.is_null()
        );
        terminated?;
        assert!(tree.guard.job.is_null(), "the tree releases its own handle");
        assert_eq!(first_member(job.as_raw_handle(), None)?, None);
        eprintln!(
            "JOB_FIRST_NATIVE root={root} descendant={descendant} selected={selected:?} empty_after_cleanup=true"
        );
        Ok(())
    }

    fn member_child_name() -> &'static str {
        std::any::type_name_of_val(&job_member_page_child)
            .split_once("::")
            .expect("a test function belongs to its crate")
            .1
    }

    #[test]
    fn job_member_page_child() -> io::Result<()> {
        use std::io::{Read as _, Write as _};
        use std::process::Stdio;
        let Ok(mode) = std::env::var("ACYCLIC_JOB_MEMBER_PAGE_CHILD") else {
            return Ok(());
        };
        let mut descendant = if mode == "parent" {
            let child = Command::new(std::env::current_exe()?)
                .args(["--exact", member_child_name(), "--nocapture"])
                .env("ACYCLIC_JOB_MEMBER_PAGE_CHILD", "child")
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()?;
            println!("JOB_PAGE_CHILD={}", child.id());
            std::io::stdout().flush()?;
            Some(child)
        } else {
            None
        };
        // Each real process remains alive on its original input producer.
        std::io::stdin().read(&mut [0])?;
        if let Some(child) = descendant.as_mut() {
            drop(child.stdin.take());
            child.wait()?;
        }
        Ok(())
    }
}

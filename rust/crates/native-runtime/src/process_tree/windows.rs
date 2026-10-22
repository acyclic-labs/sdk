use std::io;
use std::io::{Read, Write};
use std::mem::{size_of, zeroed};
use std::os::windows::process::CommandExt as _;
#[cfg(test)]
use std::process::Child;
use std::process::Command;
use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_BROKEN_PIPE, HANDLE, NTSTATUS, RtlNtStatusToDosError,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
    QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
};
use windows_sys::Win32::System::Pipes::{PIPE_NOWAIT, PeekNamedPipe, SetNamedPipeHandleState};
use windows_sys::Win32::System::Threading::{
    CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW, CREATE_SUSPENDED,
};

pub(super) use std::os::windows::io::AsRawHandle as Pipe;

#[path = "windows_wait.rs"]
pub(super) mod wait;
pub(super) use wait::wait_for_exit;
#[cfg(test)]
pub(super) use wait::wait_for_exit_coarse;

pub(super) fn write_pipe(
    pipe: &mut (impl Write + Pipe),
    bytes: &[u8],
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<(Option<usize>, io::Result<()>)> {
    let mode = PIPE_NOWAIT;
    let (changed, recorded) =
        super::pipe_operation_attempt(meter, crate::NativeOperationKind::Write, || {
            // SAFETY: this exclusively owned byte pipe has no pending I/O.
            if unsafe {
                SetNamedPipeHandleState(
                    pipe.as_raw_handle().cast(),
                    &mode,
                    std::ptr::null(),
                    std::ptr::null(),
                )
            } == 0
            {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        })?;
    if let Err(error) = changed {
        if error.kind() == io::ErrorKind::BrokenPipe {
            return Ok((Some(0), recorded));
        }
        recorded?;
        return Err(error);
    }
    recorded?;
    let mut candidate = bytes;
    loop {
        let mut actual = 0;
        let (written, recorded) =
            super::pipe_operation_attempt(meter, crate::NativeOperationKind::Write, || {
                // SAFETY: synchronous WriteFile borrows the existing input
                // only for this call and reports its physical prefix.
                if unsafe {
                    windows_sys::Win32::Storage::FileSystem::WriteFile(
                        pipe.as_raw_handle().cast(),
                        candidate.as_ptr(),
                        u32::try_from(candidate.len()).unwrap_or(u32::MAX),
                        &raw mut actual,
                        std::ptr::null_mut(),
                    )
                } == 0
                {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(actual as usize)
                }
            })?;
        if actual != 0 {
            return Ok((Some(actual as usize), recorded.and(written.map(|_| ()))));
        }
        match written {
            Ok(0) if candidate.len() > 1 => {
                recorded?;
                candidate = candidate
                    .get(..candidate.len() / 2)
                    .ok_or_else(|| io::Error::other("invalid process write prefix"))?;
            }
            Ok(0) => return Ok((None, recorded)),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                return Ok((None, recorded));
            }
            Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {
                return Ok((Some(0), recorded));
            }
            Err(error) => {
                recorded?;
                return Err(error);
            }
            Ok(written) => return Ok((Some(written), recorded)),
        }
    }
}

pub(super) fn read_pipe(
    pipe: &mut (impl Read + Pipe),
    buffer: &mut [std::mem::MaybeUninit<u8>],
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<(Option<usize>, io::Result<()>)> {
    let mut available = 0;
    let (peek, recorded) =
        super::pipe_operation_attempt(meter, crate::NativeOperationKind::Read, || {
            // SAFETY: peek observes this live exclusively owned pipe.
            if unsafe {
                PeekNamedPipe(
                    pipe.as_raw_handle().cast(),
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null_mut(),
                    &raw mut available,
                    std::ptr::null_mut(),
                )
            } == 0
            {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        })?;
    match peek {
        Err(error) if error.raw_os_error() == Some(ERROR_BROKEN_PIPE.cast_signed()) => {
            return Ok((Some(0), recorded));
        }
        Err(error) => {
            recorded?;
            return Err(error);
        }
        Ok(()) => {}
    }
    if available == 0 {
        return Ok((None, recorded));
    }
    recorded?;
    let length = buffer.len().min(available as usize).min(u32::MAX as usize);
    let mut actual = 0;
    let (read, recorded) =
        super::pipe_operation_attempt(meter, crate::NativeOperationKind::Read, || {
            // SAFETY: synchronous ReadFile initializes only the returned
            // prefix of this exclusively borrowed writable allocation.
            if unsafe {
                windows_sys::Win32::Storage::FileSystem::ReadFile(
                    pipe.as_raw_handle().cast(),
                    buffer.as_mut_ptr().cast(),
                    u32::try_from(length).map_err(|_| io::ErrorKind::InvalidInput)?,
                    &raw mut actual,
                    std::ptr::null_mut(),
                )
            } == 0
            {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        })?;
    let recorded = recorded.and(read);
    Ok((Some(actual as usize), recorded))
}

pub(super) struct Guard {
    job: HANDLE,
    active: bool,
    pub(super) assigned: bool,
    termination_sent: bool,
    pub(super) direct_termination_sent: bool,
    pub(super) cleanup_on_drop: bool,
}

// SAFETY: these exclusively owned kernel handles have no thread affinity.
// Mutation and every native call require the unique mutable Guard owner.
// No Sync implementation permits unsynchronized native handle operations.
unsafe impl Send for Guard {}

pub(super) fn spawn_controlled(
    command: &mut Command,
    meter: Option<&dyn crate::NativeReadMeter>,
    custody: &mut Option<super::ProcessTree>,
) -> io::Result<()> {
    spawn_in_job(command, meter, custody, None)
}

/// Spawns into a Job created under `name`, which must not already exist.
pub(super) fn spawn_named(
    command: &mut Command,
    custody: &mut Option<super::ProcessTree>,
    name: &[u16],
    record: &mut super::custody::Record,
) -> io::Result<()> {
    spawn_in_job(command, None, custody, Some((name, record)))
}

fn spawn_in_job(
    command: &mut Command,
    meter: Option<&dyn crate::NativeReadMeter>,
    custody: &mut Option<super::ProcessTree>,
    mut named: Option<(&[u16], &mut super::custody::Record)>,
) -> io::Result<()> {
    *custody = Some(super::ProcessTree {
        child: None,
        guard: Guard::empty(meter.is_none()),
        output_taken: false,
        cleanup_on_drop: meter.is_none(),
        custody: None,
    });
    let initialized = custody
        .as_mut()
        .ok_or(io::ErrorKind::NotFound)?
        .guard
        .initialize(meter, named.as_ref().map(|(name, _)| *name));
    if initialized.is_err() && custody.as_ref().is_some_and(super::ProcessTree::is_reaped) {
        custody.take();
    }
    initialized?;
    let tree = custody.as_mut().ok_or(io::ErrorKind::NotFound)?;
    // A named tree's custodian holds its Job until every member has exited.
    let custodian = named
        .as_mut()
        .map(|(_, record)| super::custodian::launch(tree.guard.job, record))
        .transpose()?;
    let started = start_in_job(command, meter, tree);
    if started.is_err()
        && let Some(mut custodian) = custodian
    {
        // Never started, the tree has no member for the custodian to await.
        let _ = custodian.kill();
        let _ = custodian.wait();
    }
    started
}

fn start_in_job(
    command: &mut Command,
    meter: Option<&dyn crate::NativeReadMeter>,
    tree: &mut super::ProcessTree,
) -> io::Result<()> {
    command.creation_flags(CREATE_SUSPENDED | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
    super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
        tree.child = Some(command.spawn()?);
        Ok(())
    })?;
    let child = tree.child.as_ref().ok_or(io::ErrorKind::NotFound)?;
    super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
        // SAFETY: both handles are owned and the child is still suspended.
        if unsafe { AssignProcessToJobObject(tree.guard.job, child.as_raw_handle().cast()) } == 0 {
            Err(io::Error::last_os_error())
        } else {
            tree.guard.assigned = true;
            Ok(())
        }
    })?;
    super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
        #[link(name = "ntdll")]
        unsafe extern "system" {
            fn NtResumeProcess(process: HANDLE) -> NTSTATUS;
        }
        // SAFETY: the owned child is suspended and already assigned to this Job.
        let status = unsafe { NtResumeProcess(child.as_raw_handle().cast()) };
        if status < 0 {
            // SAFETY: the returned NTSTATUS is a valid input to this conversion.
            let error = unsafe { RtlNtStatusToDosError(status) };
            Err(io::Error::from_raw_os_error(error.cast_signed()))
        } else {
            Ok(())
        }
    })
}

#[cfg(test)]
fn failed_admission(child: &mut Child, guard: &mut Guard, admission: io::Error) -> io::Error {
    let job = guard.terminate();
    let killed = child.kill();
    let cleanup = (|| {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if child.try_wait()?.is_some() {
                return job;
            }
            if let Err(error) = &killed {
                return Err(io::Error::new(error.kind(), error.to_string()));
            }
            if std::time::Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "unadmitted child termination is unresolved",
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    })();
    match cleanup {
        Ok(()) => admission,
        Err(error) => io::Error::new(
            error.kind(),
            format!("process admission failed ({admission}); cleanup unresolved ({error})"),
        ),
    }
}

impl Guard {
    const fn empty(cleanup_on_drop: bool) -> Self {
        Self {
            job: std::ptr::null_mut(),
            active: false,
            assigned: false,
            termination_sent: false,
            direct_termination_sent: false,
            cleanup_on_drop,
        }
    }

    #[cfg(test)]
    fn new() -> io::Result<Self> {
        let mut guard = Self::empty(true);
        guard.initialize(None, None)?;
        Ok(guard)
    }

    fn initialize(
        &mut self,
        meter: Option<&dyn crate::NativeReadMeter>,
        name: Option<&[u16]>,
    ) -> io::Result<()> {
        super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
            let name = name.map_or(std::ptr::null(), <[u16]>::as_ptr);
            // SAFETY: a Job with default security and either no name or a
            // NUL-terminated one; owned on success.
            let job = unsafe { CreateJobObjectW(std::ptr::null(), name) };
            if job.is_null() {
                return Err(io::Error::last_os_error());
            }
            // A recorded name must name only this tree's Job.
            if !name.is_null()
                && io::Error::last_os_error().raw_os_error()
                    == Some(windows_sys::Win32::Foundation::ERROR_ALREADY_EXISTS.cast_signed())
            {
                // SAFETY: the handle CreateJobObjectW just returned.
                unsafe { CloseHandle(job) };
                return Err(io::ErrorKind::AlreadyExists.into());
            }
            self.job = job;
            self.active = true;
            Ok(())
        })?;
        // SAFETY: the job limit structure accepts zero for every field.
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { zeroed() };
        if meter.is_none() {
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        }
        super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
            // SAFETY: initialized fixed ABI structure and owned Job.
            if unsafe {
                SetInformationJobObject(
                    self.job,
                    JobObjectExtendedLimitInformation,
                    (&raw const limits).cast(),
                    u32::try_from(size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>())
                        .map_err(|_| io::ErrorKind::InvalidInput)?,
                )
            } == 0
            {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        })
    }

    pub(super) const fn pending(&self) -> bool {
        !self.job.is_null()
    }

    pub(super) fn close(&mut self, meter: Option<&dyn crate::NativeReadMeter>) -> io::Result<()> {
        if !self.job.is_null() {
            super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                // SAFETY: cleanup observed an empty Job; this is its owned handle.
                if unsafe { CloseHandle(self.job) } == 0 {
                    return Err(io::Error::last_os_error());
                }
                self.job = std::ptr::null_mut();
                Ok(())
            })?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn terminate(&mut self) -> io::Result<()> {
        self.terminate_with_meter(None)
    }

    pub(super) fn terminate_with_meter(
        &mut self,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<()> {
        if !self.active {
            return Ok(());
        }
        if !self.termination_sent {
            super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                // SAFETY: the exclusively owned live Job contains this tree.
                if unsafe { TerminateJobObject(self.job, 1) } == 0 {
                    return Err(io::Error::last_os_error());
                }
                self.termination_sent = true;
                Ok(())
            })?;
        }
        let deadline = std::time::Instant::now() + super::ProcessTree::TERMINATION_TIMEOUT;
        loop {
            // SAFETY: this plain output structure accepts zero for every field.
            let mut accounting: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { zeroed() };
            super::process_control_attempt(meter, crate::NativeOperationKind::Read, || {
                // SAFETY: writable fixed ABI structure and owned live Job.
                if unsafe {
                    QueryInformationJobObject(
                        self.job,
                        JobObjectBasicAccountingInformation,
                        (&raw mut accounting).cast(),
                        u32::try_from(size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>())
                            .map_err(|_| io::ErrorKind::InvalidInput)?,
                        std::ptr::null_mut(),
                    )
                } == 0
                {
                    return Err(io::Error::last_os_error());
                }
                if accounting.ActiveProcesses == 0 {
                    self.active = false;
                }
                Ok(())
            })?;
            if !self.active {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(io::ErrorKind::TimedOut.into());
            }
            super::process_control_attempt(meter, crate::NativeOperationKind::Read, || {
                std::thread::sleep(super::ProcessTree::TERMINATION_POLL_INTERVAL);
                Ok(())
            })?;
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if !self.cleanup_on_drop {
            return;
        }
        let _ = self.close(None);
    }
}

#[test]
fn denied_job_cleanup_retains_child_and_reports_failure() {
    use windows_sys::Win32::Foundation::DuplicateHandle;
    use windows_sys::Win32::System::Threading::GetCurrentProcess;
    let temporary = tempfile::tempdir().expect("temporary directory");
    let mut tree = super::ProcessTree::spawn(&mut super::tests::command("child", temporary.path()))
        .expect("spawn tree");
    super::tests::ready(temporary.path(), "tree-ready", || {
        tree.try_wait(None).map_or(true, |status| status.is_some())
    });
    let mut restricted = std::ptr::null_mut();
    // SAFETY: both source and target are this process; the duplicate has
    // zero access rights. Keep the original handle for mandatory cleanup.
    let duplicated = unsafe {
        DuplicateHandle(
            GetCurrentProcess(),
            tree.guard.job,
            GetCurrentProcess(),
            &raw mut restricted,
            0,
            0,
            0,
        )
    };
    assert_ne!(duplicated, 0);
    let original = std::mem::replace(
        &mut tree.guard,
        Guard {
            job: restricted,
            active: true,
            assigned: true,
            ..Guard::empty(true)
        },
    );
    let started = std::time::Instant::now();
    let result = tree
        .wait_with_output(
            std::time::Duration::ZERO,
            4096,
            None,
            || Ok(()),
            |_| Ok(crate::ProcessInput::default()),
            None,
            None,
        )
        .map_err(|failure| failure.error);
    let retained = tree.child.is_some() && tree.guard.active;
    let recollection = tree
        .wait_with_output(
            std::time::Duration::ZERO,
            4096,
            None,
            || Ok(()),
            |_| Ok(crate::ProcessInput::default()),
            None,
            None,
        )
        .map_err(|failure| failure.error);
    let restricted = std::mem::replace(&mut tree.guard, original);
    drop(restricted);
    tree.terminate(None)
        .expect("cleanup with original authority");
    assert_eq!(
        result.expect_err("cleanup must fail").kind(),
        io::ErrorKind::PermissionDenied
    );
    assert!(retained);
    assert!(
        recollection
            .expect_err("consumed pipes cannot be collected again")
            .to_string()
            .contains("already collected")
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    super::tests::assert_no_escape(temporary.path());
}

#[test]
fn denied_admission_still_observes_unassigned_child_exit() {
    use windows_sys::Win32::Foundation::DuplicateHandle;
    use windows_sys::Win32::System::Threading::GetCurrentProcess;
    let temporary = tempfile::tempdir().expect("temporary directory");
    let guard = Guard::new().expect("create Job");
    let mut restricted = std::ptr::null_mut();
    // SAFETY: duplicate the live Job into this process without assign or
    // terminate authority; the original guard remains independently owned.
    let duplicated = unsafe {
        DuplicateHandle(
            GetCurrentProcess(),
            guard.job,
            GetCurrentProcess(),
            &raw mut restricted,
            0,
            0,
            0,
        )
    };
    assert_ne!(duplicated, 0);
    let mut restricted = Guard {
        job: restricted,
        active: true,
        ..Guard::empty(true)
    };
    let mut command = super::tests::command("child", temporary.path());
    command.creation_flags(CREATE_SUSPENDED | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
    let mut child = command.spawn().expect("spawn suspended child");
    // SAFETY: live handles; this real assignment must fail for lack of rights.
    let assigned =
        unsafe { AssignProcessToJobObject(restricted.job, child.as_raw_handle().cast()) };
    assert_eq!(assigned, 0);
    let admission = io::Error::last_os_error();
    let error = failed_admission(&mut child, &mut restricted, admission);
    assert!(
        child
            .try_wait()
            .expect("observe unassigned child")
            .is_some()
    );
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert!(!temporary.path().join("tree-ready").exists());
}

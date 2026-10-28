use std::io;
use std::io::{Read, Write};
use std::mem::{size_of, zeroed};
use std::os::windows::io::{FromRawHandle as _, OwnedHandle};
use std::os::windows::process::CommandExt as _;
#[cfg(test)]
use std::process::Child;
use std::process::Command;
use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_BROKEN_PIPE, ERROR_IO_PENDING, HANDLE, NTSTATUS, RtlNtStatusToDosError,
    WAIT_TIMEOUT,
};
use windows_sys::Win32::Storage::FileSystem::ReadFile;
use windows_sys::Win32::System::IO::{GetOverlappedResult, OVERLAPPED};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
    QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
};
use windows_sys::Win32::System::Pipes::PeekNamedPipe;
use windows_sys::Win32::System::Threading::{
    CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW, CREATE_SUSPENDED, CreateEventW, WaitForSingleObject,
};

pub(super) use std::os::windows::io::AsRawHandle as Pipe;

#[path = "windows_job_wait.rs"]
mod job_wait;

#[path = "windows_wait.rs"]
pub(super) mod wait;
pub(super) use wait::wait_for_exit;

pub(super) fn wait_without_reaping(child: &std::process::Child) -> io::Result<()> {
    crate::process_identity::ProcessProbe::from_process(child)?.await_exit()
}
#[cfg(test)]
pub(super) use wait::wait_for_exit_coarse;
#[cfg(test)]
#[path = "windows_job_wait_tests.rs"]
mod job_wait_tests;

/// The one overlapped stdin write a capture keeps in flight.
#[derive(Default)]
pub(super) struct Writer(pub(super) Option<std::sync::Arc<super::events::PipeOperation>>);

/// Writes a prefix of `bytes` as one overlapped write. `Some(n)` reports `n`
/// bytes the pipe took, either now or by a write that was pending until the
/// child read; `None` while a write is still pending; `Some(0)` once the
/// child's end is closed.
pub(super) fn write_pipe(
    pipe: &mut (impl Write + Pipe),
    writer: &mut Writer,
    bytes: &[u8],
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<(Option<usize>, io::Result<()>)> {
    let operation = if let Some(operation) = &writer.0 {
        std::sync::Arc::clone(operation)
    } else {
        let operation =
            super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                super::events::PipeOperation::writer(pipe)
            })?;
        writer.0 = Some(std::sync::Arc::clone(&operation));
        operation
    };
    match operation.take_written() {
        Some(Ok(written)) => return Ok((Some(written), Ok(()))),
        Some(Err(error)) if error.kind() == io::ErrorKind::BrokenPipe => {
            return Ok((Some(0), Ok(())));
        }
        Some(Err(error)) => return Err(error),
        None if operation.pending() => return Ok((None, Ok(()))),
        None => {}
    }
    // A pending write can outlive this poll and its caller's cancellation.
    // Retain the original ledger, never a replacement allowance, until IOCP
    // reports the physical transfer, even if capture stops observing it.
    let reservation = meter
        .map(|meter| {
            let owner = meter.allocation_owner().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "asynchronous process input requires the original meter owner",
                )
            })?;
            crate::NativeByteReservation::new(
                owner,
                crate::NativeOperationKind::Write,
                super::events::PipeOperation::write_length(bytes.len()),
            )
        })
        .transpose()?;
    let (written, recorded) =
        super::pipe_operation_attempt(meter, crate::NativeOperationKind::Write, || {
            operation.write(bytes, reservation)
        })?;
    match written {
        Ok(written) => Ok((written, recorded)),
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => Ok((Some(0), recorded)),
        Err(error) => {
            recorded?;
            Err(error)
        }
    }
}

/// Reads bytes the pipe already holds. The OVERLAPPED's event, with its low
/// bit set, keeps the read off the completion port that the capture's
/// zero-byte readiness reads use.
fn read_available(pipe: &impl Pipe, buffer: &mut [std::mem::MaybeUninit<u8>]) -> io::Result<usize> {
    // SAFETY: an unnamed manual-reset event, owned on success.
    let event = unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
    if event.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful creation transfers one owned event handle.
    let event = unsafe { OwnedHandle::from_raw_handle(event) };
    // SAFETY: an all-zero OVERLAPPED is its documented initial value.
    let mut overlapped: OVERLAPPED = unsafe { zeroed() };
    overlapped.hEvent = event.as_raw_handle().map_addr(|address| address | 1);
    let length = u32::try_from(buffer.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
    // SAFETY: the buffer and OVERLAPPED outlive the transfer, which this
    // frame waits for below before either is released.
    if unsafe {
        ReadFile(
            pipe.as_raw_handle().cast(),
            buffer.as_mut_ptr().cast(),
            length,
            std::ptr::null_mut(),
            &raw mut overlapped,
        )
    } == 0
    {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(ERROR_IO_PENDING.cast_signed()) {
            return Err(error);
        }
    }
    let mut actual = 0;
    // SAFETY: the transfer started above on this live pipe; waiting keeps
    // its buffer and OVERLAPPED borrowed until it completes.
    if unsafe {
        GetOverlappedResult(
            pipe.as_raw_handle().cast(),
            &raw const overlapped,
            &raw mut actual,
            1,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(actual as usize)
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
    let buffer = buffer
        .get_mut(..length)
        .ok_or(io::ErrorKind::InvalidInput)?;
    let (read, recorded) =
        super::pipe_operation_attempt(meter, crate::NativeOperationKind::Read, || {
            read_available(pipe, buffer)
        })?;
    let actual = read.as_ref().map_or(0, |actual| *actual);
    Ok((Some(actual), recorded.and(read.map(drop))))
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
    meter: Option<&dyn crate::NativeReadMeter>,
    custody: &mut Option<super::ProcessTree>,
    name: &[u16],
    record: super::custody::Record,
    recorded: io::Result<()>,
    events: super::events::Events,
) -> io::Result<()> {
    let job = name.strip_suffix(&[0]).ok_or(io::ErrorKind::InvalidInput)?;
    command.env(
        "ACYCLIC_PROCESS_CUSTODY_JOB",
        String::from_utf16(job).map_err(|_| io::ErrorKind::InvalidInput)?,
    );
    spawn_in_job(
        command,
        meter,
        custody,
        Some((name, record, recorded, events)),
    )
}

fn spawn_in_job(
    command: &mut Command,
    meter: Option<&dyn crate::NativeReadMeter>,
    custody: &mut Option<super::ProcessTree>,
    named: Option<(
        &[u16],
        super::custody::Record,
        io::Result<()>,
        super::events::Events,
    )>,
) -> io::Result<()> {
    let (name, record, recorded, events) = match named {
        Some((name, record, recorded, events)) => (Some(name), Some(record), recorded, events),
        None => (None, None, Ok(()), super::events::Events::start()?),
    };
    *custody = Some(super::ProcessTree {
        child: None,
        guard: Guard::empty(meter.is_none()),
        output_taken: false,
        cleanup_on_drop: meter.is_none(),
        custody: record,
        writer: Writer::default(),
        queued_stdin: None,
        readers: [None, None],
        _events: events,
    });
    // The physical record may already exist when completion accounting fails.
    // Put its recovery authority in the caller's slot before propagating that
    // failure, just as for later Job initialization or process admission.
    recorded?;
    let initialized = custody
        .as_mut()
        .ok_or(io::ErrorKind::NotFound)?
        .guard
        .initialize(meter, name);
    if initialized.is_err() && custody.as_ref().is_some_and(super::ProcessTree::is_reaped) {
        custody.take();
    }
    initialized?;
    let tree = custody.as_mut().ok_or(io::ErrorKind::NotFound)?;
    // A named tree's custodian holds its Job until every member has exited.
    let mut custodian = tree
        .custody
        .as_mut()
        .map(|record| super::custodian::launch(tree.guard.job, record))
        .transpose()?;
    if let Some(custodian) = &custodian {
        command.env(super::custodian_admission::ENV, &custodian.endpoint);
    }
    let started = start_in_job(command, meter, tree, custodian.as_ref());
    if started.is_err()
        && let Some(custodian) = custodian.as_mut()
    {
        // Never started, the tree has no member for the custodian to await.
        let _ = custodian.child.kill();
        let _ = custodian.child.wait();
    }
    started
}

fn start_in_job(
    command: &mut Command,
    meter: Option<&dyn crate::NativeReadMeter>,
    tree: &mut super::ProcessTree,
    custodian: Option<&super::custodian::Custodian>,
) -> io::Result<()> {
    command.creation_flags(CREATE_SUSPENDED | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
    super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
        tree.child = Some(super::spawner::spawn(command)?);
        Ok(())
    })?;
    let child = tree.child.as_ref().ok_or(io::ErrorKind::NotFound)?;
    // Packaged applications can use desktop-app breakaway even when their
    // assigned Job disallows ordinary breakaway. Check the actual suspended
    // process, so aliases and a replaced executable cannot bypass admission.
    super::process_control_attempt(meter, crate::NativeOperationKind::Read, || {
        crate::executable::require_unpackaged(child.as_raw_handle().cast())
    })?;
    super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
        // SAFETY: both handles are owned and the child is still suspended.
        if unsafe { AssignProcessToJobObject(tree.guard.job, child.as_raw_handle().cast()) } == 0 {
            Err(io::Error::last_os_error())
        } else {
            tree.guard.assigned = true;
            Ok(())
        }
    })?;
    if let Some(custodian) = custodian {
        super::custodian_admission::admit(&custodian.endpoint, child.id())?;
    }
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
    pub(super) const fn needs_kill(&self) -> bool {
        self.active && !self.termination_sent
    }

    pub(super) fn kill(&mut self) -> io::Result<()> {
        if !self.active || self.termination_sent {
            return Ok(());
        }
        // SAFETY: the exclusively owned live Job contains this tree.
        if unsafe { TerminateJobObject(self.job, 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        self.termination_sent = true;
        Ok(())
    }

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
                self.kill()
            })?;
        }
        job_wait::wait_empty(self.job, meter)?;
        self.active = false;
        Ok(())
    }
}

// Kill the Job, kill an unassigned partial child, and release the Job handle.
pub(super) const RETIREMENT_WRITES: u64 = 3;

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
    let mut restricted_guard = Guard::empty(true);
    restricted_guard.job = restricted;
    restricted_guard.active = true;
    restricted_guard.assigned = true;
    let original = std::mem::replace(&mut tree.guard, restricted_guard);
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
    let mut restricted_guard = Guard::empty(true);
    restricted_guard.job = restricted;
    restricted_guard.active = true;
    let mut restricted = restricted_guard;
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

#[cfg(test)]
#[path = "termination_wait_tests.rs"]
mod termination_wait_tests;
/// Trees started from several threads are all created on the one spawner
/// thread, none on a caller's: process creation's per-thread state (RPC's,
/// for an app execution alias) is then held once, not once per caller thread.
#[test]
fn trees_started_from_many_threads_are_created_on_one() -> io::Result<()> {
    let callers = (0..4)
        .map(|_| {
            std::thread::spawn(|| -> io::Result<std::thread::ThreadId> {
                let mut command = Command::new("cmd.exe");
                command
                    .args(["/d", "/c", "exit 0"])
                    .stdin(std::process::Stdio::null())
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null());
                let mut tree = super::ProcessTree::spawn(&mut command)?;
                while tree.try_wait(None)?.is_none() {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                Ok(std::thread::current().id())
            })
        })
        .collect::<Vec<_>>();
    let callers = callers
        .into_iter()
        .map(|caller| {
            caller
                .join()
                .map_err(|_| io::Error::other("caller panicked"))?
        })
        .collect::<io::Result<Vec<_>>>()?;
    let created = super::spawner::CREATED_ON
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert!(created.len() >= callers.len(), "{created:?}");
    assert!(
        created.iter().all(|thread| *thread == created[0]),
        "every process is created on one thread: {created:?}"
    );
    assert!(!callers.contains(&created[0]), "and on none of its callers");
    Ok(())
}

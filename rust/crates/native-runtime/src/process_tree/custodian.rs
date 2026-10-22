//! The Windows custodian of a custodied tree: a small process that holds the
//! tree's Job, and so its name, until every member has fully exited, and
//! kills the tree when its owner exits. A reopened owner's fence therefore
//! finds a Job by name for as long as any member may still run, and "not
//! found" means the tree is gone. The tree's record names its custodian, so
//! the fence also awaits the custodian's exit. Records are never the
//! custodian's to remove: each has one remover, its live owner or the
//! reopened owner's fence.
//!
//! The custodian is the owner's own executable, started with [`SWITCH`] set:
//! a CRT initializer runs it before the host's `main`, in every executable
//! that links this crate, so no host has to dispatch it. Kill-on-close stays
//! on the Job, so a killed custodian leaves the owner's handle to kill the
//! tree when the owner exits.

use std::io::{self, BufRead as _, BufReader, Read as _, Write as _};
use std::os::windows::io::AsRawHandle as _;
use std::os::windows::process::CommandExt as _;
use std::process::{Child, Command, Stdio};
use windows_sys::Win32::Foundation::{
    CloseHandle, DUPLICATE_SAME_ACCESS, DuplicateHandle, FILETIME, HANDLE, INVALID_HANDLE_VALUE,
    WAIT_FAILED,
};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatus, OVERLAPPED, PostQueuedCompletionStatus,
};
use windows_sys::Win32::System::JobObjects::{
    IsProcessInJob, JOBOBJECT_ASSOCIATE_COMPLETION_PORT, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
    JobObjectAssociateCompletionPortInformation, JobObjectBasicAccountingInformation,
    QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
};
use windows_sys::Win32::System::Threading::{
    CREATE_NO_WINDOW, ExitProcess, GetCurrentProcess, GetProcessId, GetProcessTimes, INFINITE,
    OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE, WaitForMultipleObjects,
    WaitForSingleObject,
};

/// The environment variable that makes a process a custodian.
const SWITCH: &str = "ACYCLIC_PROCESS_CUSTODIAN";
/// The line a custodian writes once it watches the Job.
const READY: &str = "ready";
/// The Job message that its last active process has exited (winnt.h).
const JOB_OBJECT_MSG_ACTIVE_PROCESS_ZERO: u32 = 4;
/// The Job message that a process joined it; its value is the PID.
const JOB_OBJECT_MSG_NEW_PROCESS: u32 = 6;
/// The completion key the owner watch posts once the owner has exited.
const OWNER_EXITED: usize = usize::MAX;

#[used]
#[unsafe(link_section = ".CRT$XCU")]
static ENTRY: extern "C" fn() = entry;

/// Runs before `main`: serves as a custodian only when started as one.
extern "C" fn entry() {
    if std::env::var_os(SWITCH).is_none() {
        return;
    }
    let code = u32::from(serve().is_err());
    // SAFETY: ends this custodian without returning to the host's startup.
    unsafe { ExitProcess(code) }
}

/// Starts the custodian of `job` and names it in the tree's `record`, then
/// returns once it watches the still-empty Job.
pub(super) fn launch(job: HANDLE, record: &mut super::custody::Record) -> io::Result<Child> {
    // Keeps the initializer, and so the entry, in every linked executable
    // that can start a custodian.
    std::hint::black_box(&ENTRY);
    let mut custodian = Command::new(std::env::current_exe()?)
        .env(SWITCH, "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .creation_flags(CREATE_NO_WINDOW)
        .spawn()?;
    let handed = hand_over(&mut custodian, job)
        .and_then(|()| record.name_custodian(&identity(custodian.as_raw_handle().cast())?));
    if handed.is_err() {
        let _ = custodian.kill();
        let _ = custodian.wait();
    }
    handed.map(|()| custodian)
}

fn hand_over(custodian: &mut Child, job: HANDLE) -> io::Result<()> {
    let target: HANDLE = custodian.as_raw_handle().cast();
    // SAFETY: duplicating this process's owned Job handle into the custodian.
    let job = unsafe { duplicate(job, target, 0, DUPLICATE_SAME_ACCESS) }?;
    // SAFETY: a synchronize-only handle to this process, for the custodian.
    let owner = unsafe { duplicate(GetCurrentProcess(), target, PROCESS_SYNCHRONIZE, 0) }?;
    let mut input = custodian.stdin.take().ok_or(io::ErrorKind::BrokenPipe)?;
    write!(
        input,
        "{}\n{}\n",
        job.expose_provenance(),
        owner.expose_provenance()
    )?;
    drop(input);
    let mut ready = String::new();
    BufReader::new(custodian.stdout.take().ok_or(io::ErrorKind::BrokenPipe)?)
        .read_line(&mut ready)?;
    if ready.trim_end() == READY {
        Ok(())
    } else {
        Err(io::Error::other("the process custodian did not start"))
    }
}

/// A process's PID and creation time, which no later process shares.
fn identity(process: HANDLE) -> io::Result<String> {
    // SAFETY: a live process handle.
    let pid = unsafe { GetProcessId(process) };
    if pid == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(format!("{pid}-{}", creation_time(process)?))
}

fn creation_time(process: HANDLE) -> io::Result<u64> {
    let mut times = [FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    }; 4];
    let [creation, exit, kernel, user] = &mut times;
    // SAFETY: a live process handle and four writable times.
    if unsafe { GetProcessTimes(process, creation, exit, kernel, user) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(u64::from(creation.dwHighDateTime) << 32 | u64::from(creation.dwLowDateTime))
}

/// Waits for the custodian of `identity`, as a record names it, if it still
/// runs: it exits only once every member of its tree has.
pub(super) fn await_custodian(identity: &str) -> io::Result<()> {
    let Some((pid, created)) = identity
        .split_once('-')
        .and_then(|(pid, created)| Some((pid.parse::<u32>().ok()?, created.parse::<u64>().ok()?)))
    else {
        return Err(io::ErrorKind::InvalidData.into());
    };
    // SAFETY: opening a handle to the recorded PID; owned below.
    let process = unsafe {
        OpenProcess(
            PROCESS_SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION,
            0,
            pid,
        )
    };
    if process.is_null() {
        return Ok(());
    }
    let waited = creation_time(process).and_then(|time| {
        // Another process with a reused PID is no custodian of this tree.
        // SAFETY: the owned process handle.
        if time == created && unsafe { WaitForSingleObject(process, INFINITE) } == WAIT_FAILED {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    });
    // SAFETY: the handle opened above.
    unsafe { CloseHandle(process) };
    waited
}

/// Duplicates `source` into `target`, returning its value there.
///
/// # Safety
///
/// `source` must be a live handle of this process and `target` a live
/// process handle with duplicate access.
unsafe fn duplicate(
    source: HANDLE,
    target: HANDLE,
    access: u32,
    options: u32,
) -> io::Result<HANDLE> {
    let mut duplicated: HANDLE = std::ptr::null_mut();
    // SAFETY: the caller's handles and a writable output.
    if unsafe {
        DuplicateHandle(
            GetCurrentProcess(),
            source,
            target,
            &raw mut duplicated,
            access,
            0,
            options,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(duplicated)
}

/// The custodian: watches the Job, kills it when the owner exits, and
/// exits once no member remains.
fn serve() -> io::Result<()> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let mut lines = input.lines();
    let mut handle = || -> io::Result<HANDLE> {
        let value = lines
            .next()
            .and_then(|line| line.parse::<usize>().ok())
            .ok_or(io::ErrorKind::InvalidInput)?;
        Ok(std::ptr::with_exposed_provenance_mut(value))
    };
    let job = handle()?;
    let owner = handle()?;
    // SAFETY: a new completion port, not yet tied to any file.
    let port = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
    if port.is_null() {
        return Err(io::Error::last_os_error());
    }
    let association = JOBOBJECT_ASSOCIATE_COMPLETION_PORT {
        CompletionKey: job,
        CompletionPort: port,
    };
    // SAFETY: the handed-over Job and an initialized association.
    if unsafe {
        SetInformationJobObject(
            job,
            JobObjectAssociateCompletionPortInformation,
            (&raw const association).cast(),
            u32::try_from(size_of::<JOBOBJECT_ASSOCIATE_COMPLETION_PORT>())
                .map_err(|_| io::ErrorKind::InvalidInput)?,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let mut output = io::stdout();
    writeln!(output, "{READY}")?;
    output.flush()?;
    let (owner_watch, job_watch, port_watch) = (
        owner.expose_provenance(),
        job.expose_provenance(),
        port.expose_provenance(),
    );
    std::thread::spawn(move || {
        let owner: HANDLE = std::ptr::with_exposed_provenance_mut(owner_watch);
        let job: HANDLE = std::ptr::with_exposed_provenance_mut(job_watch);
        let port: HANDLE = std::ptr::with_exposed_provenance_mut(port_watch);
        // SAFETY: handles this custodian owns for its whole life.
        unsafe {
            WaitForSingleObject(owner, INFINITE);
            TerminateJobObject(job, 1);
            PostQueuedCompletionStatus(port, 0, OWNER_EXITED, std::ptr::null());
        }
    });
    let mut members = Members::default();
    let mut owner_exited = false;
    loop {
        let mut message = 0;
        let mut key = 0;
        let mut overlapped: *mut OVERLAPPED = std::ptr::null_mut();
        // SAFETY: the owned port and writable outputs.
        if unsafe {
            GetQueuedCompletionStatus(
                port,
                &raw mut message,
                &raw mut key,
                &raw mut overlapped,
                INFINITE,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        if key == job.addr() && message == JOB_OBJECT_MSG_NEW_PROCESS {
            // The message carries the new member's PID.
            members.admit(job, u32::try_from(overlapped.addr()).unwrap_or(0));
        }
        if key == job.addr() && message == JOB_OBJECT_MSG_ACTIVE_PROCESS_ZERO {
            break;
        }
        if key == OWNER_EXITED {
            owner_exited = true;
        }
        // Job messages are notifications whose delivery is not guaranteed,
        // and a Job that never gained a member posts none: once the owner
        // has exited, each listed member is awaited, until none is active.
        if owner_exited {
            for pid in super::custody::members(job)? {
                members.admit(job, pid);
            }
            members.await_exits()?;
            if active_processes(job)? == 0 {
                break;
            }
        }
    }
    // A member stops counting as active before its process has fully
    // exited; the Job goes only once each known member has.
    members.await_exits()?;
    // SAFETY: the Job handle; closing it lets the name go only now.
    unsafe { CloseHandle(job) };
    Ok(())
}

/// Handles to the Job's members, held so their exits can be awaited.
#[derive(Default)]
struct Members(Vec<HANDLE>);

impl Members {
    /// Holds process `pid` if it is a member of `job`. A PID whose process
    /// already exited is either gone or reused by a process outside `job`.
    fn admit(&mut self, job: HANDLE, pid: u32) {
        // SAFETY: opening a handle to a PID; owned below.
        let process = unsafe {
            OpenProcess(
                PROCESS_SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION,
                0,
                pid,
            )
        };
        if process.is_null() {
            return;
        }
        let mut member = 0;
        // SAFETY: owned process and Job handles and a writable result.
        if unsafe { IsProcessInJob(process, job, &raw mut member) } != 0 && member != 0 {
            self.0.push(process);
        } else {
            // SAFETY: the handle opened above.
            unsafe { CloseHandle(process) };
        }
    }

    /// Waits until every held member has exited.
    fn await_exits(&mut self) -> io::Result<()> {
        for chunk in self.0.chunks(64) {
            let count = u32::try_from(chunk.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
            // SAFETY: up to 64 owned process handles; waits for all.
            if unsafe { WaitForMultipleObjects(count, chunk.as_ptr(), 1, INFINITE) } == WAIT_FAILED
            {
                return Err(io::Error::last_os_error());
            }
        }
        for process in self.0.drain(..) {
            // SAFETY: each handle admitted above.
            unsafe { CloseHandle(process) };
        }
        Ok(())
    }
}

fn active_processes(job: HANDLE) -> io::Result<u32> {
    // SAFETY: this plain output structure accepts zero for every field.
    let mut accounting: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: the owned Job and writable output of the stated size.
    if unsafe {
        QueryInformationJobObject(
            job,
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
    Ok(accounting.ActiveProcesses)
}

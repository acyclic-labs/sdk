//! The Windows custodian of a custodied tree: a small process that holds the
//! tree's Job, and so its name, until every member has fully exited, and
//! kills the tree when its owner exits. A reopened owner's fence therefore
//! finds a Job by name for as long as any member may still run, and "not
//! found" means the tree is gone. The tree's record names its custodian, so
//! the fence also awaits the custodian's exit. Records are never the
//! custodian's to remove: each has one remover, its live owner or the
//! reopened owner's fence.
//!
//! The custodian is the owner's own executable, started with [`SWITCH`] as
//! its first argument: a CRT initializer runs it before the host's `main`,
//! in every executable that links this crate, so no host has to dispatch it.
//! It inherits no handle: it duplicates the Job and its ready event from the
//! owner, so it never holds the owner's output or any other inherited
//! handle. Kill-on-close stays on the Job, so a killed custodian leaves the
//! owner's handle to kill the tree when the owner exits.

use std::collections::BTreeMap;
use std::ffi::c_void;
use std::io;
use std::os::windows::ffi::OsStrExt as _;
use std::os::windows::io::IntoRawHandle as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use windows_sys::Win32::Foundation::{
    CloseHandle, DUPLICATE_SAME_ACCESS, DuplicateHandle, FILETIME, HANDLE, INVALID_HANDLE_VALUE,
    WAIT_FAILED, WAIT_OBJECT_0,
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
    CREATE_NO_WINDOW, CreateEventW, ExitProcess, GetCurrentProcess, GetProcessId, GetProcessTimes,
    INFINITE, OpenProcess, PROCESS_DUP_HANDLE, PROCESS_QUERY_LIMITED_INFORMATION,
    PROCESS_SYNCHRONIZE, RegisterWaitForSingleObject, SetEvent, TerminateProcess, UnregisterWaitEx,
    WT_EXECUTEONLYONCE, WaitForMultipleObjects, WaitForSingleObject,
};

/// The first argument that makes a process a custodian.
const SWITCH: &str = "--acyclic-process-custodian";
/// The Job message that a process joined it; its value is the PID.
const JOB_OBJECT_MSG_NEW_PROCESS: u32 = 6;
/// The completion key the owner watch posts once the owner has exited.
const OWNER_EXITED: usize = usize::MAX;
/// The completion key a member's wait posts once it has exited; the
/// message carries its PID.
const MEMBER_EXITED: usize = usize::MAX - 1;

#[used]
#[unsafe(link_section = ".CRT$XCU")]
static ENTRY: extern "C" fn() = entry;

/// Runs before `main`: serves as a custodian only when started as one.
extern "C" fn entry() {
    if std::env::args_os()
        .nth(1)
        .is_none_or(|argument| argument != SWITCH)
    {
        return;
    }
    let code = u32::from(serve().is_err());
    // SAFETY: ends this custodian without returning to the host's startup.
    unsafe { ExitProcess(code) }
}

/// A started custodian, which this owner leaves running once its tree has
/// a member, or stops when the tree never starts.
pub(super) struct Custodian(HANDLE);

impl Custodian {
    /// Ends a custodian whose tree never started, and awaits its exit.
    pub(super) fn stop(self) {
        // SAFETY: the owned process handle; a failed termination leaves a
        // custodian that exits on its own with this owner.
        unsafe {
            TerminateProcess(self.0, 1);
            WaitForSingleObject(self.0, INFINITE);
        }
    }
}

impl Drop for Custodian {
    fn drop(&mut self) {
        // SAFETY: the process handle CreateProcessW returned.
        unsafe { CloseHandle(self.0) };
    }
}

/// Starts the custodian of `job` and names it in the tree's `record`, then
/// returns once it watches the still-empty Job.
pub(super) fn launch(job: HANDLE, record: &mut super::custody::Record) -> io::Result<Custodian> {
    // Keeps the initializer, and so the entry, in every linked executable
    // that can start a custodian.
    std::hint::black_box(&ENTRY);
    // SAFETY: an unnamed manual-reset event, owned below; not inheritable.
    let ready = unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
    if ready.is_null() {
        return Err(io::Error::last_os_error());
    }
    let started = start(job, ready).and_then(|custodian| {
        let named = await_ready(&custodian, ready)
            .and_then(|()| record.name_custodian(&identity(custodian.0)?));
        match named {
            Ok(()) => Ok(custodian),
            Err(error) => {
                custodian.stop();
                Err(error)
            }
        }
    });
    // SAFETY: the event created above; the custodian holds its own copy.
    unsafe { CloseHandle(ready) };
    started
}

/// Creates the custodian process, inheriting no handle: it duplicates the
/// Job and `ready` from this owner, which it names by PID and creation time.
fn start(job: HANDLE, ready: HANDLE) -> io::Result<Custodian> {
    let program = std::env::current_exe()?;
    let mut application: Vec<u16> = program.as_os_str().encode_wide().collect();
    application.push(0);
    // SAFETY: the pseudo handle of this process.
    let owner_created = creation_time(unsafe { GetCurrentProcess() })?;
    let mut line = std::ffi::OsString::from("\"");
    line.push(&program);
    line.push(format!(
        "\" {SWITCH} {} {owner_created} {} {}",
        std::process::id(),
        job.expose_provenance(),
        ready.expose_provenance(),
    ));
    let mut line: Vec<u16> = line.encode_wide().collect();
    line.push(0);
    let started = crate::command::start::start(crate::command::start::Start {
        application: &application,
        command_line: &mut line,
        flags: CREATE_NO_WINDOW,
        environment: None,
        directory: None,
        stdio: [std::ptr::null_mut(); 3],
    })?;
    Ok(Custodian(started.process.into_raw_handle()))
}

/// Returns once the custodian signals `ready`, or fails if it exits first.
fn await_ready(custodian: &Custodian, ready: HANDLE) -> io::Result<()> {
    let handles = [ready, custodian.0];
    // SAFETY: two owned handles; waits for either.
    match unsafe { WaitForMultipleObjects(2, handles.as_ptr(), 0, INFINITE) } {
        WAIT_OBJECT_0 => Ok(()),
        WAIT_FAILED => Err(io::Error::last_os_error()),
        _ => Err(io::Error::other("the process custodian did not start")),
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

/// Duplicates the owner's handle `value` into this process.
fn duplicate_from(owner: HANDLE, value: usize) -> io::Result<HANDLE> {
    let mut duplicated: HANDLE = std::ptr::null_mut();
    // SAFETY: the owner's process handle with duplicate access, a value the
    // owner named on the command line, and a writable output.
    if unsafe {
        DuplicateHandle(
            owner,
            std::ptr::with_exposed_provenance_mut(value),
            GetCurrentProcess(),
            &raw mut duplicated,
            0,
            0,
            DUPLICATE_SAME_ACCESS,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(duplicated)
}

/// Opens the owner `pid` created at `created`; a reused PID is not it.
fn open_owner(pid: u32, created: u64) -> io::Result<HANDLE> {
    // SAFETY: opening a handle to the owner's PID; owned by the caller.
    let owner = unsafe {
        OpenProcess(
            PROCESS_DUP_HANDLE | PROCESS_SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION,
            0,
            pid,
        )
    };
    if owner.is_null() {
        return Err(io::Error::last_os_error());
    }
    if creation_time(owner)? == created {
        return Ok(owner);
    }
    // SAFETY: the handle opened above, to another process.
    unsafe { CloseHandle(owner) };
    Err(io::ErrorKind::NotFound.into())
}

/// The custodian: watches the Job, kills it when the owner exits, and
/// exits once no member remains.
fn serve() -> io::Result<()> {
    let arguments: Vec<String> = std::env::args().skip(2).collect();
    let [pid, created, job, ready] = arguments.as_slice() else {
        return Err(io::ErrorKind::InvalidInput.into());
    };
    let number = |value: &String| {
        value
            .parse::<u64>()
            .map_err(|_| io::ErrorKind::InvalidInput)
    };
    let owner = open_owner(
        u32::try_from(number(pid)?).map_err(|_| io::ErrorKind::InvalidInput)?,
        number(created)?,
    )?;
    let handle = |value: &String| {
        usize::try_from(number(value)?).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))
    };
    let job = duplicate_from(owner, handle(job)?)?;
    let ready = duplicate_from(owner, handle(ready)?)?;
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
    // SAFETY: the owner's event, duplicated above and closed once set.
    let signaled = unsafe { SetEvent(ready) };
    // SAFETY: as above.
    unsafe { CloseHandle(ready) };
    if signaled == 0 {
        return Err(io::Error::last_os_error());
    }
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
    PORT.store(port.expose_provenance(), Ordering::Relaxed);
    let mut members = Members::default();
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
            members.admit(job, u32::try_from(overlapped.addr()).unwrap_or(0))?;
        }
        if key == MEMBER_EXITED {
            members.release(message);
        }
        // Job messages are notifications whose delivery is not guaranteed:
        // once the owner has exited, every listed member is admitted too.
        if key == OWNER_EXITED {
            for pid in super::custody::members(job)? {
                members.admit(job, pid)?;
            }
        }
        // A member stops counting as active before its process has fully
        // exited, so the Job goes only once no member is active and every
        // admitted one has signaled its exit.
        if members.is_empty() && active_processes(job)? == 0 {
            break;
        }
    }
    // SAFETY: the Job handle; closing it lets the name go only now.
    unsafe { CloseHandle(job) };
    Ok(())
}

/// The completion port that member exit waits post to; one per custodian.
static PORT: AtomicUsize = AtomicUsize::new(0);

/// Posts the exit of the member whose PID is `context`.
unsafe extern "system" fn member_exited(context: *mut c_void, _timed_out: bool) {
    let port: HANDLE = std::ptr::with_exposed_provenance_mut(PORT.load(Ordering::Relaxed));
    let pid = u32::try_from(context.addr()).unwrap_or(0);
    // SAFETY: the custodian's port, which lives as long as the process.
    unsafe { PostQueuedCompletionStatus(port, pid, MEMBER_EXITED, std::ptr::null()) };
}

/// The Job's live members: each holds its process handle and a thread pool
/// wait that reports its exit, and is released on that report, so the
/// custodian's cost tracks live members, never every member there was.
#[derive(Default)]
struct Members(BTreeMap<u32, (HANDLE, HANDLE)>);

impl Members {
    /// Watches process `pid` if it is a member of `job`. A PID whose
    /// process already exited is either gone or reused outside `job`; a
    /// held handle keeps a watched member's PID from being reused.
    fn admit(&mut self, job: HANDLE, pid: u32) -> io::Result<()> {
        if self.0.contains_key(&pid) {
            return Ok(());
        }
        // SAFETY: opening a handle to a PID; owned below.
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
        let mut member = 0;
        let mut wait: HANDLE = std::ptr::null_mut();
        // SAFETY: owned process and Job handles and a writable result; the
        // wait reports once, with the PID as its context.
        let watched = unsafe {
            IsProcessInJob(process, job, &raw mut member) != 0
                && member != 0
                && RegisterWaitForSingleObject(
                    &raw mut wait,
                    process,
                    Some(member_exited),
                    std::ptr::with_exposed_provenance(pid as usize),
                    INFINITE,
                    WT_EXECUTEONLYONCE,
                ) != 0
        };
        if watched {
            self.0.insert(pid, (process, wait));
            return Ok(());
        }
        let error = io::Error::last_os_error();
        // SAFETY: the handle opened above.
        unsafe { CloseHandle(process) };
        // Only a failed registration of a member is an error.
        if member != 0 { Err(error) } else { Ok(()) }
    }

    /// Releases the member `pid`, whose exit its wait reported.
    fn release(&mut self, pid: u32) {
        if let Some((process, wait)) = self.0.remove(&pid) {
            // SAFETY: the reported wait, which never runs again, and the
            // handle it watched. A null event returns without waiting for
            // the reporting callback, which no longer uses either.
            unsafe {
                UnregisterWaitEx(wait, std::ptr::null_mut());
                CloseHandle(process);
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
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

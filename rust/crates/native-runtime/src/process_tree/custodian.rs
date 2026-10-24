//! One lazy custodian per host keeps independent Jobs alive until their members
//! have fully exited. Each Job has its own completion event, so fencing one
//! owner never waits for an unrelated live Job or for the shared daemon to exit.
use std::collections::BTreeMap;
use std::ffi::c_void;
use std::io::{self, BufRead as _, BufReader, Write as _};
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use std::os::windows::process::CommandExt as _;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use windows_sys::Win32::Foundation::*;
use windows_sys::Win32::System::IO::*;
use windows_sys::Win32::System::JobObjects::*;
use windows_sys::Win32::System::Threading::*;

mod server;
const SWITCH: &str = "ACYCLIC_PROCESS_CUSTODIAN";
const READY: &str = "ready";
const JOB_OBJECT_MSG_NEW_PROCESS: u32 = 6;
const OWNER_EXITED: usize = usize::MAX;
const MEMBER_EXITED: usize = usize::MAX - 1;
const COMMAND: usize = usize::MAX - 2;
static HOST: Mutex<Option<Host>> = Mutex::new(None);
static SEQUENCE: AtomicUsize = AtomicUsize::new(0);

#[used]
#[unsafe(link_section = ".CRT$XCU")]
static ENTRY: extern "C" fn() = entry;

extern "C" fn entry() {
    if std::env::var_os(SWITCH).is_none() {
        return;
    }
    let code = u32::from(server::serve().is_err());
    // SAFETY: the daemon must not return into its host executable's main.
    unsafe { ExitProcess(code) }
}

struct Host {
    child: Child,
    output: BufReader<ChildStdout>,
    identity: String,
}
struct Starting(Option<Child>);
impl Drop for Starting {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Host {
    fn start() -> io::Result<Self> {
        std::hint::black_box(&ENTRY);
        let child = Command::new(std::env::current_exe()?)
            .env(SWITCH, "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .creation_flags(CREATE_NO_WINDOW)
            .spawn()?;
        let mut starting = Starting(Some(child));
        let admitted = (|| {
            let child = starting.0.as_mut().ok_or(io::ErrorKind::BrokenPipe)?;
            // SAFETY: this process and the fresh daemon are live; synchronize
            // access lets that single daemon watch the host for its lifetime.
            let owner = unsafe {
                duplicate(
                    GetCurrentProcess(),
                    child.as_raw_handle(),
                    PROCESS_SYNCHRONIZE,
                    0,
                )
            }?;
            writeln!(
                child.stdin.as_mut().ok_or(io::ErrorKind::BrokenPipe)?,
                "{}",
                owner.expose_provenance()
            )?;
            let output = BufReader::new(child.stdout.take().ok_or(io::ErrorKind::BrokenPipe)?);
            let identity = identity(child.as_raw_handle())?;
            let child = starting.0.take().ok_or(io::ErrorKind::BrokenPipe)?;
            let mut host = Self {
                child,
                output,
                identity,
            };
            host.ready()?;
            Ok(host)
        })();
        // A failed startup has accepted no Jobs. Host owns any successfully
        // admitted child; before that point the local startup guard reaps it.
        admitted
    }
    fn ready(&mut self) -> io::Result<()> {
        let mut line = String::new();
        self.output.read_line(&mut line)?;
        if line.trim_end() == READY {
            Ok(())
        } else {
            Err(io::Error::other(
                "the process custodian did not acknowledge admission",
            ))
        }
    }
    fn send(&mut self, message: &str) -> io::Result<()> {
        let input = self.child.stdin.as_mut().ok_or(io::ErrorKind::BrokenPipe)?;
        writeln!(input, "{message}")?;
        input.flush()?;
        self.ready()
    }
}

/// A completion handle stays with the parent's Job handle, including while a
/// completed Job remains named. The daemon retains its copy through reaping.
pub(super) struct Completion(OwnedHandle);
impl Completion {
    fn create(name: &[u16]) -> io::Result<Self> {
        let name = completion_name(name);
        // SAFETY: a fresh named manual-reset event, owned on success.
        let event = unsafe { CreateEventW(std::ptr::null(), 1, 0, name.as_ptr()) };
        if event.is_null() {
            return Err(io::Error::last_os_error());
        }
        let error = io::Error::last_os_error();
        // SAFETY: CreateEventW transferred this handle.
        let event = Self(unsafe { OwnedHandle::from_raw_handle(event) });
        if error.raw_os_error() == Some(ERROR_ALREADY_EXISTS.cast_signed()) {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        Ok(event)
    }
    pub(super) fn open(name: &[u16]) -> io::Result<Option<Self>> {
        let name = completion_name(name);
        // SAFETY: opens the exact recorded Job's completion event.
        let event = unsafe { OpenEventW(SYNCHRONIZATION_SYNCHRONIZE, 0, name.as_ptr()) };
        if event.is_null() {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(ERROR_FILE_NOT_FOUND.cast_signed()) {
                Ok(None)
            } else {
                Err(error)
            };
        }
        // SAFETY: OpenEventW transferred this handle.
        Ok(Some(Self(unsafe { OwnedHandle::from_raw_handle(event) })))
    }
}

fn completion_name(name: &[u16]) -> Vec<u16> {
    name.iter()
        .copied()
        .take_while(|unit| *unit != 0)
        .chain(".complete".encode_utf16())
        .chain(std::iter::once(0))
        .collect()
}

pub(super) struct Registration {
    key: usize,
    host: String,
    completion: Option<Completion>,
    activated: bool,
}
impl Registration {
    fn activate_inner(&mut self) -> io::Result<()> {
        if self.activated {
            return Ok(());
        }
        let mut slot = HOST
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let host = slot
            .as_mut()
            .filter(|host| host.identity == self.host)
            .ok_or(io::ErrorKind::BrokenPipe)?;
        host.send(&format!("A {}", self.key))?;
        self.activated = true;
        Ok(())
    }
    pub(super) fn activate(mut self) -> io::Result<Completion> {
        self.activate_inner()?;
        self.completion
            .take()
            .ok_or_else(|| io::ErrorKind::BrokenPipe.into())
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        let _ = self.activate_inner();
    }
}

/// Registers an empty Job before any member can start. A separate activation
/// after the spawn attempt lets the daemon retire even an aborted empty Job.
pub(super) fn launch(
    name: &[u16],
    record: &mut super::custody::Record,
) -> io::Result<Registration> {
    let completion = Completion::create(name)?;
    let key = SEQUENCE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |key| {
            key.checked_add(1).filter(|next| *next < COMMAND)
        })
        .map_err(|_| io::ErrorKind::OutOfMemory)?;
    let name =
        String::from_utf16(name.strip_suffix(&[0]).unwrap_or(name)).map_err(io::Error::other)?;
    if name.chars().any(char::is_whitespace) {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    let mut slot = HOST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(host) = slot.as_mut()
        && host.child.try_wait()?.is_some()
    {
        *slot = None;
    }
    if slot.is_none() {
        *slot = Some(Host::start()?);
    }
    let host = slot.as_mut().ok_or(io::ErrorKind::BrokenPipe)?;
    let identity = host.identity.clone();
    let registered = host.send(&format!("R {key} {name}"));
    drop(slot);
    // An uncertain acknowledgement still activates the empty registration
    // during rollback, so it cannot retain one Job forever in a live daemon.
    let registration = Registration {
        key,
        host: identity.clone(),
        completion: Some(completion),
        activated: false,
    };
    registered?;
    record.name_custodian(&identity)?;
    Ok(registration)
}

/// A fence waits for this Job's reaping event. Daemon death retains the old
/// kill-on-close recovery path after the fence's direct member wait, without
/// ever waiting for the lifetime of an unrelated shared daemon.
pub(super) fn await_completion(completion: Option<&Completion>, identity: &str) -> io::Result<()> {
    let Some(process) = recorded_process(identity)? else {
        return Ok(());
    };
    let Some(completion) = completion else {
        // SAFETY: the pinned recorded daemon process.
        return if unsafe { WaitForSingleObject(process.as_raw_handle(), 0) } == WAIT_OBJECT_0 {
            Ok(())
        } else {
            Err(io::Error::other(
                "live custodian has no Job completion event",
            ))
        };
    };
    let handles = [completion.0.as_raw_handle(), process.as_raw_handle()];
    // SAFETY: these two owned handles remain live throughout the wait.
    match unsafe { WaitForMultipleObjects(2, handles.as_ptr(), 0, INFINITE) } {
        WAIT_OBJECT_0 => Ok(()),
        value if value == WAIT_OBJECT_0 + 1 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

fn recorded_process(identity: &str) -> io::Result<Option<OwnedHandle>> {
    let Some((pid, created)) = identity
        .split_once('-')
        .and_then(|(pid, created)| Some((pid.parse::<u32>().ok()?, created.parse::<u64>().ok()?)))
    else {
        return Err(io::ErrorKind::InvalidData.into());
    };
    let Some(process) = open_process(PROCESS_SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION, pid)?
    else {
        return Ok(None);
    };
    if creation_time(process.as_raw_handle())? == created {
        Ok(Some(process))
    } else {
        Ok(None)
    }
}

fn open_process(access: u32, pid: u32) -> io::Result<Option<OwnedHandle>> {
    // SAFETY: opens an unshared handle, owned on success.
    let process = unsafe { OpenProcess(access, 0, pid) };
    if process.is_null() {
        let error = io::Error::last_os_error();
        return if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER.cast_signed()) {
            Ok(None)
        } else {
            Err(error)
        };
    }
    // SAFETY: OpenProcess transferred this handle.
    Ok(Some(unsafe { OwnedHandle::from_raw_handle(process) }))
}

/// Pins a listed PID only if the opened process still belongs to this Job.
/// Both live admission and stale-owner fencing use the same failure policy.
pub(in crate::process_tree) fn member(job: HANDLE, pid: u32) -> io::Result<Option<OwnedHandle>> {
    let Some(process) = open_process(PROCESS_SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION, pid)?
    else {
        return Ok(None);
    };
    let mut member = 0;
    // SAFETY: the retained process handle and supplied Job handle, with a
    // writable membership result. Failure conveys no membership evidence.
    if unsafe { IsProcessInJob(process.as_raw_handle(), job, &raw mut member) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((member != 0).then_some(process))
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
        let Some(process) = member(job, pid)? else {
            return Ok(());
        };
        let mut wait: HANDLE = std::ptr::null_mut();
        // SAFETY: this retained member's handle and a writable wait handle;
        // the wait reports once, with the PID as its context.
        if unsafe {
            RegisterWaitForSingleObject(
                &raw mut wait,
                process.as_raw_handle(),
                Some(member_exited),
                std::ptr::with_exposed_provenance(pid as usize),
                INFINITE,
                WT_EXECUTEONLYONCE,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        self.0.insert(
            pid,
            (
                std::os::windows::io::IntoRawHandle::into_raw_handle(process),
                wait,
            ),
        );
        Ok(())
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

#[cfg(test)]
mod admission_tests {
    use super::*;

    #[test]
    fn opening_failure_is_not_a_process_exit_witness() {
        // SAFETY: this test's thread-local last-error slot.
        unsafe { SetLastError(0) };
        // Unsupported access bits cause a real OpenProcess failure against
        // this still-running process, without changing its security policy.
        let opened = open_process(u32::MAX, std::process::id());
        // SAFETY: read this thread's actual OpenProcess failure cause.
        let code = unsafe { GetLastError() };
        eprintln!("CUSTODIAN_OPEN_FAILURE native_error={code} result={opened:?}");
        assert_ne!(code, 0, "the OS must reject the requested access");
        assert!(
            opened.is_err(),
            "an inaccessible live process must not be classified as exited"
        );
    }

    #[test]
    fn an_invalid_job_cannot_prove_a_live_process_is_not_a_member() {
        let mut members = Members::default();
        let admitted = members.admit(INVALID_HANDLE_VALUE, std::process::id());
        eprintln!("CUSTODIAN_MEMBERSHIP_FAILURE result={admitted:?}");
        assert!(
            admitted.is_err(),
            "a failed Job query must not classify a live process as a nonmember"
        );
        assert!(members.is_empty());
    }

    #[test]
    fn a_recorded_process_requires_both_its_pid_and_creation_time() -> io::Result<()> {
        let process = open_process(
            PROCESS_SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION,
            std::process::id(),
        )?
        .ok_or(io::ErrorKind::NotFound)?;
        let recorded = identity(process.as_raw_handle())?;
        assert!(recorded_process(&recorded)?.is_some());
        let created = creation_time(process.as_raw_handle())?;
        let reused = format!("{}-{}", std::process::id(), created ^ 1);
        assert!(recorded_process(&reused)?.is_none());
        Ok(())
    }
}

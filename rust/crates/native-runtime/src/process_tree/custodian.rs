//! The Windows custodian of an owner's custodied trees: one small process per
//! owner that holds each tree's Job, and so its name, until every member of
//! that tree has fully exited, and kills every tree when the owner exits. A
//! reopened owner's fence therefore finds a Job by name for as long as any
//! member may still run, and "not found" means the tree is gone. Each record
//! names the custodian to detect producer loss while awaiting the selected
//! tree's original retirement event. Unrelated trees are never barriers.
//! Records are never the custodian's to remove: each has one remover, its
//! live owner or the reopened owner's fence.
//!
//! The custodian is the owner's own executable, started once, detached from
//! any console, with [`SWITCH`] set: a CRT initializer runs it before the
//! host's `main`, in every executable that links this crate, so no host has
//! to dispatch it. A tree costs the owner one line on the custodian's input
//! and one wait for its acknowledgment, which arrives before the tree's first
//! instruction runs. Kill-on-close stays on the Job, so a killed custodian
//! leaves the owner's handle to kill the tree when the owner exits, and the
//! owner's next tree starts a new custodian.

use super::custodian_admission::{self as admission, Secret};
use indexmap::IndexMap;
use std::collections::BTreeMap;
use std::ffi::c_void;
use std::io::{self, BufRead as _, BufReader, Write as _};
use std::os::windows::io::{
    AsHandle as _, AsRawHandle as _, BorrowedHandle, FromRawHandle as _, OwnedHandle,
};
use std::os::windows::process::CommandExt as _;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};
use windows_sys::Win32::Foundation::{
    CloseHandle, DUPLICATE_CLOSE_SOURCE, DUPLICATE_SAME_ACCESS, DuplicateHandle,
    ERROR_ALREADY_EXISTS, ERROR_INVALID_HANDLE, ERROR_INVALID_PARAMETER, GetLastError, HANDLE,
    INVALID_HANDLE_VALUE, WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::System::Console::{GetStdHandle, STD_ERROR_HANDLE, STD_OUTPUT_HANDLE};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatus, OVERLAPPED,
};
use windows_sys::Win32::System::JobObjects::{
    IsProcessInJob, JOBOBJECT_ASSOCIATE_COMPLETION_PORT, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
    JobObjectAssociateCompletionPortInformation, JobObjectBasicAccountingInformation,
    QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
};
use windows_sys::Win32::System::Threading::{
    CreateEventW, DETACHED_PROCESS, ExitProcess, GetCurrentProcess, GetProcessId, INFINITE,
    OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE, SetEvent,
    WaitForMultipleObjects, WaitForSingleObject,
};

mod input;
mod wait;
use input::Input;
use wait::{ExitWait, Producer};

/// The environment variable that makes a process a custodian.
const SWITCH: &str = "ACYCLIC_PROCESS_CUSTODIAN";
/// The line a custodian writes once it can take trees.
const READY: &str = "ready";
/// A producer failure, on the same pipe as the readiness response.
const FAILED: &str = "failed ";
const FAILURE_KINDS: [(&str, io::ErrorKind); 3] = [
    ("invalid-data", io::ErrorKind::InvalidData),
    ("broken-pipe", io::ErrorKind::BrokenPipe),
    ("other", io::ErrorKind::Other),
];
/// The Job message that a process joined it; its value is the PID.
const JOB_OBJECT_MSG_NEW_PROCESS: u32 = 6;
/// The kernel completion key delivered once the owner has exited.
const OWNER_EXITED: usize = usize::MAX;
/// The kernel completion key delivered once a member has exited; the
/// message carries its PID.
const MEMBER_EXITED: usize = usize::MAX - 1;
/// The completion key the input reader posts for each tree it reads.
const OPENED: usize = usize::MAX - 3;
/// The kernel completion key delivered when the original input thread exits.
const INPUT_EXITED: usize = usize::MAX - 4;
/// The kernel completion key for the existing admission service's terminal.
const ADMISSION_EXITED: usize = usize::MAX - 5;

/// The owner's handle on its running custodian.
pub(super) struct Custodian {
    child: Child,
    input: Mutex<Option<ChildStdin>>,
    identity: String,
    endpoint: String,
    health: Mutex<Health>,
}

/// The original startup pipe stays with this exact custodian. Only after its
/// process exits may a waiter consume the failure response; every later waiter
/// retains that same outcome. Exit without a complete response remains loss.
#[derive(Debug)]
struct Health {
    output: BufReader<ChildStdout>,
    failure: Option<Arc<io::Error>>,
}

/// One tree's standing with the custodian: `done` is set once every member
/// of the tree has fully exited and the custodian has let go of its Job.
pub(super) struct Link {
    custodian: Arc<Custodian>,
    done: OwnedHandle,
}

/// This owner's custodian, started by its first custodied tree.
static CURRENT: Mutex<Option<Arc<Custodian>>> = Mutex::new(None);

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

fn poisoned() -> io::Error {
    io::Error::other("process custodian registry poisoned")
}

/// This owner's running custodian, started if it has none or its last one
/// has exited.
pub(super) fn current() -> io::Result<Arc<Custodian>> {
    let mut current = CURRENT.lock().map_err(|_| poisoned())?;
    if let Some(custodian) = current.as_ref()
        && process_running(custodian.child.as_handle())?
    {
        return Ok(Arc::clone(custodian));
    }
    let custodian = Arc::new(launch()?);
    *current = Some(Arc::clone(&custodian));
    Ok(custodian)
}

fn launch() -> io::Result<Custodian> {
    let mut child = launch_child()?;
    let started = hand_over(&mut child).and_then(|(input, endpoint, health)| {
        Ok((
            input,
            endpoint,
            health,
            identity(child.as_raw_handle().cast())?,
        ))
    });
    match started {
        Ok((input, endpoint, health, identity)) => Ok(Custodian {
            child,
            input: Mutex::new(Some(input)),
            identity,
            endpoint,
            health: Mutex::new(health),
        }),
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            Err(error)
        }
    }
}

fn launch_child() -> io::Result<Child> {
    // Keeps the initializer, and so the entry, in every linked executable
    // that can start a custodian.
    std::hint::black_box(&ENTRY);
    // Detached, the custodian starts no console host of its own.
    super::spawner::spawn(
        Command::new(std::env::current_exe()?)
            .env(SWITCH, "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .creation_flags(DETACHED_PROCESS),
    )
}

fn hand_over(custodian: &mut Child) -> io::Result<(ChildStdin, String, Health)> {
    let target: HANDLE = custodian.as_raw_handle().cast();
    // SAFETY: a synchronize-only handle to this process, for the custodian.
    let owner = unsafe { duplicate(GetCurrentProcess(), target, PROCESS_SYNCHRONIZE, 0) }?;
    hand_over_owner(custodian, owner)
}

/// `owner` is the original process handle already transferred to this child.
fn hand_over_owner(
    custodian: &mut Child,
    owner: HANDLE,
) -> io::Result<(ChildStdin, String, Health)> {
    let target: HANDLE = custodian.as_raw_handle().cast();
    let output = duplicate_output(target, STD_OUTPUT_HANDLE)?;
    let error = duplicate_output(target, STD_ERROR_HANDLE)?;
    let mut input = custodian.stdin.take().ok_or(io::ErrorKind::BrokenPipe)?;
    writeln!(
        input,
        "{} {} {}",
        owner.expose_provenance(),
        output.expose_provenance(),
        error.expose_provenance()
    )?;
    let (endpoint, health) = startup_response(custodian)?;
    Ok((input, endpoint, health))
}

/// Reads this original child's one complete startup response.
fn startup_response(custodian: &mut Child) -> io::Result<(String, Health)> {
    let mut ready = String::new();
    let mut output = BufReader::new(custodian.stdout.take().ok_or(io::ErrorKind::BrokenPipe)?);
    output.read_line(&mut ready)?;
    let response = ready.strip_suffix('\n').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            "process custodian exited before a complete startup response",
        )
    })?;
    // A complete failure response precedes any tree admission. A truncated
    // response remains producer loss, never an inferred native cause.
    if let Some(code) = response.strip_prefix(FAILED) {
        return Err(failure_response(code)?);
    }
    let Some(endpoint) = response
        .strip_prefix(READY)
        .and_then(|line| line.strip_prefix(' '))
    else {
        return Err(io::ErrorKind::InvalidData.into());
    };
    let address = endpoint
        .parse::<std::net::SocketAddrV4>()
        .map_err(|_| io::ErrorKind::InvalidData)?;
    if !address.ip().is_loopback() {
        return Err(io::ErrorKind::InvalidData.into());
    }
    Ok((
        endpoint.to_owned(),
        Health {
            output,
            failure: None,
        },
    ))
}

/// Native failures retain their original numeric startup representation. The
/// reader's non-native failures retain their kind. Arbitrary cause text cannot
/// be written before exit because the observer reads only after that exit.
fn failure_response(code: &str) -> io::Result<io::Error> {
    for (name, kind) in FAILURE_KINDS {
        if code == name {
            return Ok(io::Error::new(
                kind,
                "process custodian producer failed; original detail unavailable",
            ));
        }
    }
    let code = code
        .parse::<i32>()
        .map_err(|_| io::ErrorKind::InvalidData)?;
    if code == 0 {
        return Err(io::ErrorKind::InvalidData.into());
    }
    Ok(io::Error::from_raw_os_error(code))
}

impl Health {
    /// Called only after the retained process handle signals, so its original
    /// private response pipe cannot wait for a still-running response writer.
    fn exited(&mut self) -> io::Error {
        let failure = self.failure.get_or_insert_with(|| {
            let mut read = || -> io::Result<io::Error> {
                let mut line = String::new();
                self.output.read_line(&mut line)?;
                let response = line.strip_suffix('\n').ok_or_else(|| io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "process custodian exited without a complete failure response; retirement is unresolved",
                ))?;
                failure_response(response.strip_prefix(FAILED).ok_or(io::ErrorKind::InvalidData)?)
            };
            Arc::new(read().unwrap_or_else(|error| error))
        });
        failure.raw_os_error().map_or_else(
            || io::Error::new(failure.kind(), Arc::clone(failure)),
            io::Error::from_raw_os_error,
        )
    }
}

/// Observes the original process once. A refused wait cannot admit a new
/// custodian as though the retained producer had exited.
fn process_running(process: BorrowedHandle<'_>) -> io::Result<bool> {
    // SAFETY: the caller retains this process handle through the observation.
    match unsafe { WaitForSingleObject(process.as_raw_handle(), 0) } {
        WAIT_TIMEOUT => Ok(true),
        WAIT_OBJECT_0 => Ok(false),
        WAIT_FAILED => Err(io::Error::last_os_error()),
        status => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected custodian process wait status {status}"),
        )),
    }
}

impl Custodian {
    fn exited(&self) -> io::Error {
        self.health
            .lock()
            .map_or_else(|_| poisoned(), |mut health| health.exited())
    }
    /// The PID and creation time that name this custodian in each record.
    pub(super) fn identity(&self) -> &str {
        &self.identity
    }

    /// Where a member admits a suspended process of `tree` it started
    /// itself, with the tree's admission secret.
    pub(super) fn admission(&self, tree: usize, secret: &Secret) -> String {
        format!("{}/{tree}/{}", self.endpoint, admission::encode(secret))
    }

    /// Hands `tree`'s Job and its suspended first member, `process`, to the
    /// custodian, and returns once the custodian watches both, so every later
    /// member is seen joining. The caller resumes `process` only after this
    /// succeeds: on any failure the custodian has killed the Job.
    pub(super) fn open(
        self: &Arc<Self>,
        tree: usize,
        secret: &Secret,
        job: HANDLE,
        process: HANDLE,
        retirement: &[u16],
    ) -> io::Result<Link> {
        let ready = event(false, None)?;
        let done = event(true, Some(retirement))?;
        let target: HANDLE = self.child.as_raw_handle().cast();
        let mut handoff = Handoff::new(target);
        handoff.duplicate(job, 0, DUPLICATE_SAME_ACCESS)?;
        handoff.duplicate(
            process,
            PROCESS_SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION,
            0,
        )?;
        handoff.duplicate(ready.as_raw_handle().cast(), 0, DUPLICATE_SAME_ACCESS)?;
        handoff.duplicate(done.as_raw_handle().cast(), 0, DUPLICATE_SAME_ACCESS)?;
        {
            let mut input = self.input.lock().map_err(|_| poisoned())?;
            let output = input.as_mut().ok_or(io::ErrorKind::BrokenPipe)?;
            if let Err(error) = handoff.publish(output, tree, secret) {
                // A partial line is never followed by another handover. EOF
                // withdraws that publication; subsequent callers fail closed.
                drop(input.take());
                return Err(error);
            }
        }
        // Done before ready is the custodian's refusal; its exit is its loss.
        match wait_any(&[
            ready.as_raw_handle().cast(),
            done.as_raw_handle().cast(),
            target,
        ])? {
            0 => Ok(Link {
                custodian: Arc::clone(self),
                done,
            }),
            1 => Err(io::Error::other("the process custodian refused the tree")),
            _ => Err(self.exited()),
        }
    }
}

/// Owns remote duplicates until the complete original handover is published.
/// A failed later duplicate, lock, or write must not leave earlier handles in
/// the shared custodian without a tree that can close them.
struct Handoff {
    target: HANDLE,
    handles: [HANDLE; 4],
    count: usize,
}

impl Handoff {
    fn new(target: HANDLE) -> Self {
        Self {
            target,
            handles: [std::ptr::null_mut(); 4],
            count: 0,
        }
    }

    fn duplicate(&mut self, source: HANDLE, access: u32, options: u32) -> io::Result<()> {
        let slot = self
            .handles
            .get_mut(self.count)
            .ok_or(io::ErrorKind::InvalidInput)?;
        // SAFETY: the caller retains its source and the custodian process.
        *slot = unsafe { duplicate(source, self.target, access, options)? };
        self.count += 1;
        Ok(())
    }

    fn publish(
        mut self,
        output: &mut impl io::Write,
        tree: usize,
        secret: &Secret,
    ) -> io::Result<()> {
        if self.count != self.handles.len() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        writeln!(
            output,
            "{tree} {} {} {} {} {}",
            self.handles[0].expose_provenance(),
            self.handles[1].expose_provenance(),
            self.handles[2].expose_provenance(),
            self.handles[3].expose_provenance(),
            admission::encode(secret)
        )?;
        self.count = 0;
        Ok(())
    }
}

impl Drop for Handoff {
    fn drop(&mut self) {
        for handle in &self.handles[..self.count] {
            let mut local = std::ptr::null_mut();
            // SAFETY: these are our unpublished duplicates in the retained
            // target. CLOSE_SOURCE closes each there, including on failure.
            // If the target has exited its handle table is already retired.
            if unsafe {
                DuplicateHandle(
                    self.target,
                    *handle,
                    GetCurrentProcess(),
                    &raw mut local,
                    0,
                    0,
                    DUPLICATE_CLOSE_SOURCE | DUPLICATE_SAME_ACCESS,
                )
            } != 0
            {
                // SAFETY: the successful transfer returned one local handle.
                unsafe { CloseHandle(local) };
            }
        }
    }
}

impl Link {
    /// Returns once every member has fully exited. Loss of the original
    /// custodian without that terminal event retains unresolved custody.
    pub(super) fn await_members(&self) -> io::Result<()> {
        match wait_any(&[
            self.done.as_raw_handle().cast(),
            self.custodian.child.as_raw_handle().cast(),
        ])? {
            0 => Ok(()),
            _ => Err(self.custodian.exited()),
        }
    }
}

/// A new event, auto-reset unless `manual`. Only the original tree's
/// retirement event has a name so the successor can retain that same source.
fn event(manual: bool, name: Option<&[u16]>) -> io::Result<OwnedHandle> {
    // SAFETY: an unsignaled event; any supplied name is NUL-terminated and
    // belongs to this original tree identity. Owned on success.
    let event = unsafe {
        CreateEventW(
            std::ptr::null(),
            i32::from(manual),
            0,
            name.map_or(std::ptr::null(), <[u16]>::as_ptr),
        )
    };
    if event.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: captured immediately after the original successful creation.
    let existed = name.is_some() && unsafe { GetLastError() } == ERROR_ALREADY_EXISTS;
    // SAFETY: a successful creation transfers one owned handle.
    let event = unsafe { OwnedHandle::from_raw_handle(event) };
    if existed {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "process tree retirement identity already exists",
        ));
    }
    Ok(event)
}

/// Waits for the first of `handles` to be signaled, and returns its index.
fn wait_any(handles: &[HANDLE]) -> io::Result<usize> {
    let count = u32::try_from(handles.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
    // SAFETY: live waitable handles the caller holds through the wait.
    let signaled = unsafe { WaitForMultipleObjects(count, handles.as_ptr(), 0, INFINITE) };
    if signaled == WAIT_FAILED {
        return Err(io::Error::last_os_error());
    }
    signaled
        .checked_sub(WAIT_OBJECT_0)
        .and_then(|index| usize::try_from(index).ok())
        .filter(|index| *index < handles.len())
        .ok_or_else(|| io::Error::other("abandoned process custodian wait"))
}

/// Retain an outer transport's pipe through physical tree retirement. The
/// custodian does not write it: EOF must imply that no custodied member lives.
fn duplicate_output(target: HANDLE, kind: u32) -> io::Result<HANDLE> {
    // SAFETY: the current process's standard handle is borrowed for duplication.
    let source = unsafe { GetStdHandle(kind) };
    if source.is_null() || source == INVALID_HANDLE_VALUE {
        return Ok(std::ptr::null_mut());
    }
    // SAFETY: a borrowed standard handle and the owned custodian process.
    match unsafe { duplicate(source, target, 0, DUPLICATE_SAME_ACCESS) } {
        Err(error) if error.raw_os_error() == Some(ERROR_INVALID_HANDLE.cast_signed()) => {
            Ok(std::ptr::null_mut())
        }
        result => result,
    }
}

/// A process's PID and creation time, which no later process shares.
fn identity(process: HANDLE) -> io::Result<String> {
    // SAFETY: a live process handle.
    let pid = unsafe { GetProcessId(process) };
    if pid == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: this caller holds the live process handle for the query.
    let borrowed = unsafe { std::os::windows::io::BorrowedHandle::borrow_raw(process.cast()) };
    Ok(format!(
        "{pid}-{}",
        crate::process_identity::process_started(&borrowed)?
    ))
}

/// Waits for this tree's original physical retirement. Custodian exit is
/// producer loss, never a successful substitute for that terminal event.
pub(super) fn await_tree(done: &OwnedHandle, expected: &str) -> io::Result<()> {
    // One admission observation handles a terminal retained after its
    // custodian exited. This is not a readiness loop.
    // SAFETY: the caller retains the event through this observation and wait.
    match unsafe { WaitForSingleObject(done.as_raw_handle().cast(), 0) } {
        WAIT_OBJECT_0 => return Ok(()),
        WAIT_TIMEOUT => {}
        _ => return Err(io::Error::last_os_error()),
    }
    let Some(pid) = expected
        .split_once('-')
        .and_then(|(pid, _)| pid.parse::<u32>().ok())
    else {
        return Err(io::ErrorKind::InvalidData.into());
    };
    // SAFETY: a process identity, queried through the same owned handle that
    // remains in the wait. No replacement PID can become its producer.
    let process = unsafe {
        OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
            0,
            pid,
        )
    };
    if process.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: OpenProcess transferred one owned handle.
    let process = unsafe { OwnedHandle::from_raw_handle(process.cast()) };
    if identity(process.as_raw_handle().cast())? != expected {
        return Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "process tree retirement producer was replaced",
        ));
    }
    match wait_any(&[done.as_raw_handle().cast(), process.as_raw_handle().cast()])? {
        0 => Ok(()),
        _ => Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "process tree retirement producer exited before completion",
        )),
    }
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

/// A tree the owner handed over: the handles it duplicated into this
/// custodian, which owns each from here on.
#[derive(Clone, Copy)]
struct Opened {
    tree: usize,
    job: HANDLE,
    process: HANDLE,
    ready: HANDLE,
    done: HANDLE,
    secret: Secret,
}

// SAFETY: plain kernel handle values this custodian owns, with no thread
// affinity; the reader only passes them to the serving loop.
unsafe impl Send for Opened {}

impl Opened {
    fn parse(line: &str) -> Option<Self> {
        let mut fields = line.split(' ');
        let tree = fields.next()?.parse().ok()?;
        let mut handle = || {
            let value = fields.next()?.parse::<usize>().ok()?;
            Some(std::ptr::with_exposed_provenance_mut(value))
        };
        let opened = Self {
            tree,
            job: handle()?,
            process: handle()?,
            ready: handle()?,
            done: handle()?,
            secret: admission::decode(fields.next()?)?,
        };
        fields.next().is_none().then_some(opened)
    }
}

/// One live tree: its Job, its count of watched members and the event its
/// owner awaits.
struct Tree {
    job: HANDLE,
    done: HANDLE,
    members: usize,
    /// What an external admission of a member of this tree must present.
    secret: Secret,
}

/// A partial readiness write cannot be followed by a failure response.
enum Response {
    Unstarted,
    Writing,
    Ready,
}

fn write_failure(output: &mut impl io::Write, error: &io::Error) -> io::Result<()> {
    if let Some(code) = error.raw_os_error() {
        writeln!(output, "{FAILED}{code}")?;
    } else if let Some((name, _)) = FAILURE_KINDS.iter().find(|(_, kind)| *kind == error.kind()) {
        // At most 20 bytes, independent of arbitrary diagnostic text. The
        // original pipe carries READY plus this one fixed terminal only.
        writeln!(output, "{FAILED}{name}")?;
    }
    output.flush()
}

/// Watches each tree's Job, kills every tree when the owner exits, and exits
/// once the owner has and no member of any tree remains, or on producer failure.
fn serve() -> io::Result<()> {
    let mut response = Response::Unstarted;
    let result = run(&mut response);
    if !matches!(response, Response::Writing)
        && let Err(error) = &result
    {
        // The same pipe carries startup refusal or the running producer's
        // original native failure. A partial READY cannot acquire a suffix.
        // No response means incomplete observation, never inferred retirement.
        write_failure(&mut io::stdout(), error)?;
    }
    result
}

fn run(response: &mut Response) -> io::Result<()> {
    let mut input = BufReader::new(io::stdin());
    let mut line = String::new();
    input.read_line(&mut line)?;
    let mut handles = line.trim_end().split(' ').map(|value| {
        value
            .parse::<usize>()
            .map(std::ptr::with_exposed_provenance_mut::<c_void>)
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))
    });
    let mut handle = || handles.next().ok_or(io::ErrorKind::InvalidInput)?;
    let owner = handle()?;
    let output = handle()?;
    let error = handle()?;
    // These original pipe handles stay live until every member has physically
    // exited. In particular, killing the owner cannot publish an early EOF.
    let _output = transport_lease(output);
    let _error = transport_lease(error);
    // SAFETY: a new completion port, not yet tied to any file.
    let port = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
    if port.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the original owned port and transferred owner process handle
    // remain live throughout this custodian. A failed association is refused
    // before READY, so it cannot become an owner-death event.
    let _owner_wait = ExitWait::new(
        unsafe { BorrowedHandle::borrow_raw(port) },
        unsafe { BorrowedHandle::borrow_raw(owner) },
        OWNER_EXITED,
        0,
    )?;
    let admission = super::custodian_admission::Server::bind()?;
    let endpoint = admission.endpoint.clone();
    let (mut requests, thread) = admission.start(port)?;
    // SAFETY: the original port lives through this existing producer's dequeue.
    let mut admission = Producer::new(
        thread,
        unsafe { BorrowedHandle::borrow_raw(port) },
        ADMISSION_EXITED,
    )?;
    let mut opened = Input::start(input, port)?;
    let mut output = io::stdout();
    // A partial readiness write must never be followed by a failure frame.
    *response = Response::Writing;
    writeln!(output, "{READY} {endpoint}")?;
    output.flush()?;
    *response = Response::Ready;
    let mut state = State {
        trees: IndexMap::new(),
        members: Members::new(port),
        owner_exited: false,
        input_closed: false,
        #[cfg(test)]
        visits: 0,
    };
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
        let touched = match key {
            OPENED => {
                if let Ok(tree) = opened.receive.try_recv() {
                    state.open(tree, port)?;
                    Some(tree.tree)
                } else {
                    // The terminal packet may already have consumed the one
                    // credited record. Never wait for a producer that exited.
                    None
                }
            }
            INPUT_EXITED => {
                opened.finish()?;
                // One record can remain regardless of packet ordering. Its
                // original handles transfer before admitting ordinary EOF.
                if let Ok(tree) = opened.receive.try_recv() {
                    state.open(tree, port)?;
                    state.settle(Some(tree.tree))?;
                }
                state.input_closed = true;
                None
            }
            super::custodian_admission::ADMIT => {
                let request = requests.try_recv().map_err(|_| io::ErrorKind::BrokenPipe)?;
                // Only a holder of the tree's secret may name its members.
                let admitted = match state.trees.get(&request.tree) {
                    Some(tree) if admission::matches(&tree.secret, &request.secret) => {
                        state.admit(request.tree, request.pid)
                    }
                    _ => Ok(false),
                };
                request.complete(admitted)
            }
            ADMISSION_EXITED => {
                admission.finish()?;
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "process custodian admission producer exited",
                ));
            }
            MEMBER_EXITED => state.members.release(message).inspect(|id| {
                if let Some(tree) = state.trees.get_mut(id) {
                    tree.members = tree.members.saturating_sub(1);
                }
            }),
            OWNER_EXITED => {
                state.owner_exited = true;
                state.kill_all()?;
                None
            }
            // A Job message, whose completion key is its tree.
            id => {
                if message == JOB_OBJECT_MSG_NEW_PROCESS {
                    // The message carries the new member's PID.
                    state.admit(id, u32::try_from(overlapped.addr()).unwrap_or(0))?;
                }
                Some(id)
            }
        };
        if state.settle(touched)? {
            return Ok(());
        }
    }
}

/// Every tree a custodian watches, and what it knows of its owner.
struct State {
    // Dense iteration visits only live trees, including after a much larger
    // population has retired. Keyed retirement swaps out one entry.
    trees: IndexMap<usize, Tree>,
    members: Members,
    owner_exited: bool,
    input_closed: bool,
    #[cfg(test)]
    visits: usize,
}

impl State {
    /// Starts watching a handed-over tree, then acknowledges it. A tree this
    /// custodian cannot watch is killed before its first member resumes,
    /// and its owner told so.
    fn open(&mut self, opened: Opened, port: HANDLE) -> io::Result<()> {
        let association = JOBOBJECT_ASSOCIATE_COMPLETION_PORT {
            CompletionKey: std::ptr::with_exposed_provenance_mut(opened.tree),
            CompletionPort: port,
        };
        let mut tree = Tree {
            job: opened.job,
            done: opened.done,
            members: 0,
            secret: opened.secret,
        };
        // SAFETY: the handed-over Job and an initialized association.
        let associated = u32::try_from(size_of::<JOBOBJECT_ASSOCIATE_COMPLETION_PORT>()).is_ok_and(
            |size| unsafe {
                SetInformationJobObject(
                    opened.job,
                    JobObjectAssociateCompletionPortInformation,
                    (&raw const association).cast(),
                    size,
                ) != 0
            },
        );
        let watched = if associated {
            self.members
                .watch(&mut tree, opened.tree, opened.process)
                .is_ok()
        } else {
            false
        };
        if !watched {
            // Before acknowledgement the sole initial member is suspended:
            // no untrusted code or descendant can have started. Keep its
            // original handle through termination before signalling done.
            // SAFETY: the handed-over Job and still-owned suspended member.
            if unsafe { TerminateJobObject(opened.job, 1) } == 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: watch retains ownership on failure; association failure
            // did not transfer the original member handle either.
            if unsafe { WaitForSingleObject(opened.process, INFINITE) } != WAIT_OBJECT_0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: the failed watch never took this handle.
            unsafe { CloseHandle(opened.process) };
        }
        // SAFETY: the handed-over Job with terminate access, and its events,
        // each signaled at most once here and the ready event then released.
        unsafe {
            if watched && self.owner_exited {
                TerminateJobObject(opened.job, 1);
            }
            if watched {
                SetEvent(opened.ready);
            } else {
                // The suspended member has physically exited before refusal.
                SetEvent(opened.done);
                CloseHandle(opened.done);
                CloseHandle(opened.job);
            }
            CloseHandle(opened.ready);
        }
        if watched {
            self.trees.insert(opened.tree, tree);
        }
        Ok(())
    }

    /// Watches process `pid` if it is a member of tree `id`.
    fn admit(&mut self, id: usize, pid: u32) -> io::Result<bool> {
        self.trees
            .get_mut(&id)
            .map_or(Ok(false), |tree| self.members.admit(tree, id, pid))
    }

    /// Kills every tree after admitting each member its Job lists, so their
    /// live handles are held before termination can drop them from the list.
    fn kill_all(&mut self) -> io::Result<()> {
        let mut index = 0;
        while let Some((&id, tree)) = self.trees.get_index_mut(index) {
            for pid in super::custody::members(tree.job)? {
                self.members.admit(tree, id, pid)?;
            }
            // SAFETY: the handed-over Job with terminate access.
            if unsafe { TerminateJobObject(tree.job, 1) } == 0 {
                return Err(io::Error::last_os_error());
            }
            // A tree with no remaining member producer may already be done.
            // Settle it in this one required owner-death pass. A swap leaves
            // the next live tree at the same index; survivors advance once.
            let count = self.trees.len();
            self.settle_tree(id)?;
            if self.trees.len() == count {
                index += 1;
            }
        }
        Ok(())
    }

    /// Settles only the tree this event touched. The live count and original
    /// input terminal decide completion without querying any unrelated Job.
    fn settle(&mut self, touched: Option<usize>) -> io::Result<bool> {
        if let Some(id) = touched {
            self.settle_tree(id)?;
        }
        Ok(self.owner_exited && self.input_closed && self.trees.is_empty())
    }

    /// Lets go of tree `id` once no member is watched and none is active: a
    /// member stops counting as active before its process has fully exited,
    /// so both must hold. The owner learns of it from the tree's event.
    fn settle_tree(&mut self, id: usize) -> io::Result<()> {
        #[cfg(test)]
        {
            self.visits += 1;
        }
        let Some(tree) = self.trees.get(&id) else {
            return Ok(());
        };
        if tree.members == 0 && active_processes(tree.job)? == 0 {
            // The terminal event follows release of the original Job. Its
            // producer must fail rather than publish retirement on OS error.
            // SAFETY: the tree's handed-over Job and event remain owned here.
            if unsafe { CloseHandle(tree.job) } == 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: the original retirement event remains owned here.
            if unsafe { SetEvent(tree.done) } == 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: terminal publication completes this event's custody.
            unsafe { CloseHandle(tree.done) };
            self.trees.swap_remove(&id);
        }
        Ok(())
    }
}

fn transport_lease(handle: HANDLE) -> Option<OwnedHandle> {
    if handle.is_null() {
        None
    } else {
        // SAFETY: hand_over transferred this distinct handle into this process.
        Some(unsafe { OwnedHandle::from_raw_handle(handle.cast()) })
    }
}

/// Every tree's live members: each holds its process handle, a kernel wait
/// packet that reports its exit and its tree, and is released on dequeue,
/// so the custodian's cost tracks live members, never every member there was.
struct Members {
    port: HANDLE,
    live: BTreeMap<u32, (HANDLE, ExitWait, usize)>,
}

impl Members {
    fn new(port: HANDLE) -> Self {
        Self {
            port,
            live: BTreeMap::new(),
        }
    }

    /// Watches process `pid` if it is a member of `tree`'s Job. A PID whose
    /// process already exited is either gone or reused outside the Job; a
    /// held handle keeps a watched member's PID from being reused.
    fn admit(&mut self, tree: &mut Tree, id: usize, pid: u32) -> io::Result<bool> {
        if self.live.contains_key(&pid) {
            return Ok(true);
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
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER.cast_signed()) {
                Ok(false)
            } else {
                Err(error)
            };
        }
        let mut member = 0;
        // SAFETY: owned process and Job handles and a writable result.
        let queried = unsafe { IsProcessInJob(process, tree.job, &raw mut member) };
        if queried != 0 && member != 0 {
            return self
                .watch(tree, id, process)
                .map(|()| true)
                .inspect_err(|_| {
                    // SAFETY: a failed watch leaves ownership with this caller.
                    unsafe { CloseHandle(process) };
                });
        }
        let error = io::Error::last_os_error();
        // SAFETY: the handle opened above.
        unsafe { CloseHandle(process) };
        if queried == 0 { Err(error) } else { Ok(false) }
    }

    /// Watches `process`, taking ownership only on success. On failure the
    /// caller retains the same handle for physical cleanup.
    fn watch(&mut self, tree: &mut Tree, id: usize, process: HANDLE) -> io::Result<()> {
        // SAFETY: an owned process handle.
        let pid = unsafe { GetProcessId(process) };
        if pid == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: this custodian retains its original port and process handle
        // through the exact exit packet. No user callback can lose a post.
        let wait = ExitWait::new(
            unsafe { BorrowedHandle::borrow_raw(self.port) },
            unsafe { BorrowedHandle::borrow_raw(process) },
            MEMBER_EXITED,
            pid,
        )?;
        self.live.insert(pid, (process, wait, id));
        tree.members += 1;
        Ok(())
    }

    /// Releases the member `pid`, whose exit its wait reported, and answers
    /// its tree.
    fn release(&mut self, pid: u32) -> Option<usize> {
        let (process, wait, tree) = self.live.remove(&pid)?;
        // Dequeue ended this association before either original handle retires.
        drop(wait);
        // SAFETY: the retained process whose kernel exit packet was dequeued.
        unsafe { CloseHandle(process) };
        Some(tree)
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
mod handoff_tests {
    use super::*;
    use std::os::windows::io::IntoRawHandle as _;
    use windows_sys::Win32::Foundation::{
        ERROR_ACCESS_DENIED, ERROR_INVALID_DATA, GetHandleInformation,
    };
    use windows_sys::Win32::System::JobObjects::CreateJobObjectW;
    use windows_sys::Win32::System::Pipes::{
        GetNamedPipeInfo, PIPE_NOWAIT, SetNamedPipeHandleState,
    };

    #[test]
    fn admission_terminal_survives_failed_post_and_full_credit_loss() -> io::Result<()> {
        use std::io::Read as _;
        use std::net::TcpStream;

        for failed_post in [true, false] {
            // SAFETY: one original completion port, owned through dequeue.
            let port =
                unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
            if port.is_null() {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: successful creation transferred this owned handle.
            let port = unsafe { OwnedHandle::from_raw_handle(port) };
            let server = admission::Server::bind()?;
            let address = server.endpoint.clone();
            let (requests, thread) = server.start(if failed_post {
                std::ptr::null_mut()
            } else {
                port.as_raw_handle()
            })?;
            let mut producer = wait::Producer::new(thread, port.as_handle(), ADMISSION_EXITED)?;
            let bytes = [
                7_u32.to_le_bytes().as_slice(),
                9_u64.to_le_bytes().as_slice(),
                &[13; 16],
            ]
            .concat();
            let mut first = TcpStream::connect(&address)?;
            first.write_all(&bytes)?;
            let mut requests = Some(requests);
            let mut second = None;
            if !failed_post {
                let (mut message, mut key, mut context) = (0, 0, std::ptr::null_mut());
                // SAFETY: the original port and writable packet outputs.
                // The first real post proves its request occupies the sole
                // credit; no handler or reply has consumed that request.
                if unsafe {
                    GetQueuedCompletionStatus(
                        port.as_raw_handle(),
                        &mut message,
                        &mut key,
                        &mut context,
                        INFINITE,
                    )
                } == 0
                {
                    return Err(io::Error::last_os_error());
                }
                assert_eq!((key, message), (admission::ADMIT, 0));
                assert_eq!(requests.as_ref().unwrap().len(), 1);
                let mut waiting = TcpStream::connect(&address)?;
                waiting.write_all(&bytes)?;
                second = Some(waiting);
                // The same async dispatch must fail on receiver loss even
                // when its original request credit is occupied.
                drop(requests.take());
            }
            let (mut message, mut key, mut context) = (0, 0, std::ptr::null_mut());
            // SAFETY: this same original producer's kernel exit packet does
            // not depend on its own failed PostQueuedCompletionStatus.
            if unsafe {
                GetQueuedCompletionStatus(
                    port.as_raw_handle(),
                    &mut message,
                    &mut key,
                    &mut context,
                    INFINITE,
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            assert_eq!((key, message), (ADMISSION_EXITED, 0));
            let error = producer.finish().unwrap_err();
            if failed_post {
                assert_eq!(
                    error.raw_os_error(),
                    Some(ERROR_INVALID_HANDLE.cast_signed())
                );
                let original = requests.as_mut().unwrap().try_recv().unwrap();
                assert_eq!(
                    (original.pid, original.tree, original.secret),
                    (7, 9, [13; 16])
                );
            } else {
                assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
                assert_eq!(error.raw_os_error(), None);
            }
            // Dropping a queued reply may send a refusal before service exit.
            // Neither that refusal nor EOF may become an admission ACK.
            for mut peer in [Some(first), second].into_iter().flatten() {
                let mut reply = [1];
                let count = peer.read(&mut reply)?;
                assert!(count == 0 || reply == [0]);
            }
        }
        Ok(())
    }

    #[test]
    fn failure_response_does_not_wait_for_diagnostic_pipe_credit() -> io::Result<()> {
        let (read, mut write) = io::pipe()?;
        let mode = PIPE_NOWAIT;
        // SAFETY: the original writable pipe and a valid mode. This control
        // makes excess output fail immediately, rather than hang a test whose
        // observer deliberately cannot read until its producer exits.
        if unsafe {
            SetNamedPipeHandleState(
                write.as_raw_handle(),
                &mode,
                std::ptr::null(),
                std::ptr::null(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let mut capacity = 0;
        // SAFETY: the same retained pipe and one writable size output.
        if unsafe {
            GetNamedPipeInfo(
                write.as_raw_handle(),
                std::ptr::null_mut(),
                &mut capacity,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        assert_ne!(capacity, 0);
        let error = io::Error::other("x".repeat(usize::try_from(capacity).unwrap() + 1));
        writeln!(write, "{READY}")?;
        // Retaining arbitrary error text here would exhaust the real pipe
        // before this producer could exit and enable its original observer.
        write_failure(&mut write, &error)?;
        drop(write);
        let mut read = BufReader::new(read);
        let mut response = String::new();
        read.read_line(&mut response)?;
        assert_eq!(response.trim_end(), READY);
        response.clear();
        read.read_line(&mut response)?;
        let retained = failure_response(response.trim_end().strip_prefix(FAILED).unwrap())?;
        assert_eq!(retained.kind(), error.kind());
        assert_eq!(retained.raw_os_error(), None);
        assert!(retained.to_string().contains("original detail unavailable"));

        let (read, mut write) = io::pipe()?;
        drop(read);
        // The original physical sink loss still fails the write; a fixed
        // response is not evidence of delivery when the observer is gone.
        assert_eq!(
            write_failure(&mut write, &error).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        Ok(())
    }

    #[test]
    fn startup_preserves_the_original_native_wait_refusal() -> io::Result<()> {
        // Use the actual initializer and handover, denying only the original
        // owner's SYNCHRONIZE right before any tree can be admitted.
        let mut child = launch_child()?;
        // SAFETY: both original process handles remain live. This transfers
        // the deliberately restricted owner handle to the actual custodian.
        let owner = unsafe {
            duplicate(
                GetCurrentProcess(),
                child.as_raw_handle().cast(),
                PROCESS_QUERY_LIMITED_INFORMATION,
                0,
            )
        };
        let result = owner.and_then(|owner| hand_over_owner(&mut child, owner));
        // No tree was submitted, so the original direct child's exit is the
        // entire physical cleanup. Also clean up an unexpected READY result.
        let _ = child.kill();
        child.wait()?;
        let failure = result.expect_err("the native owner wait must refuse missing SYNCHRONIZE");
        assert_eq!(
            failure.raw_os_error(),
            Some(ERROR_ACCESS_DENIED.cast_signed())
        );
        assert_eq!(failure.kind(), io::ErrorKind::PermissionDenied);

        // Withdraw the actual startup request producer. The initializer
        // exits without a response; the same reader must report producer
        // loss rather than accept EOF as readiness or invent a native cause.
        let mut lost = launch_child()?;
        drop(lost.stdin.take());
        let reply = startup_response(&mut lost);
        let _ = lost.kill();
        lost.wait()?;
        let failure = reply.expect_err("the actual initializer lost its startup input");
        assert_eq!(failure.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(failure.raw_os_error(), None);

        // The same launch/handshake still admits a valid native owner. Its
        // original direct process is retained through the cleanup wait.
        let mut running = launch()?;
        let live = process_running(running.child.as_handle());
        let _ = running.child.kill();
        running.child.wait()?;
        assert!(live?);
        assert_eq!(running.exited().kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(running.exited().raw_os_error(), None);

        // A running reader's native framing refusal travels over that SAME
        // original pipe. Active and late Link waiters retain the first cause;
        // neither may mistake process loss for the tree's retirement event.
        for (bytes, native) in [
            (
                b"invalid handover\n".as_slice(),
                Some(ERROR_INVALID_DATA.cast_signed()),
            ),
            (b"\xff\n".as_slice(), None),
        ] {
            let running = Arc::new(launch()?);
            let done = event(true, None)?;
            let active = Link {
                custodian: Arc::clone(&running),
                done: done.try_clone()?,
            };
            let waiter = std::thread::spawn(move || active.await_members());
            running
                .input
                .lock()
                .map_err(|_| poisoned())?
                .as_mut()
                .ok_or(io::ErrorKind::BrokenPipe)?
                .write_all(bytes)?;
            let error = waiter.join().unwrap().unwrap_err();
            assert_eq!(error.raw_os_error(), native);
            if native.is_none() {
                assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            }
            let late = Link {
                custodian: Arc::clone(&running),
                done: done.try_clone()?,
            };
            let retained = late.await_members().unwrap_err();
            assert_eq!(retained.raw_os_error(), error.raw_os_error());
            assert_eq!(retained.kind(), error.kind());
            assert_eq!(retained.to_string(), error.to_string());
            // SAFETY: this exact retained event never received retirement.
            assert_eq!(
                unsafe { WaitForSingleObject(done.as_raw_handle(), 0) },
                WAIT_TIMEOUT
            );
            drop(late);
            let mut running = Arc::try_unwrap(running)
                .ok()
                .expect("all original observers retired");
            assert!(!running.child.wait()?.success());
        }
        Ok(())
    }

    #[test]
    fn recovery_uses_the_selected_retirement_and_refuses_original_producer_loss() -> io::Result<()>
    {
        let root = tempfile::tempdir()?;
        let mut random = [0; 8];
        getrandom::fill(&mut random).map_err(io::Error::other)?;
        // SAFETY: the current-process pseudo-handle is borrowed for identity.
        let producer = identity(unsafe { GetCurrentProcess() })?;
        // SAFETY: the running current process retains its pseudo-handle.
        assert!(process_running(unsafe {
            BorrowedHandle::borrow_raw(GetCurrentProcess())
        })?);
        // Retain a real process handle without SYNCHRONIZE. Its refused wait
        // must remain the native error, not a false producer-exit observation.
        // SAFETY: both pseudo-handles are live and duplication transfers the
        // deliberately restricted handle to this same process.
        let restricted = unsafe {
            duplicate(
                GetCurrentProcess(),
                GetCurrentProcess(),
                PROCESS_QUERY_LIMITED_INFORMATION,
                0,
            )?
        };
        // SAFETY: the successful duplication transferred one owned handle.
        let restricted = unsafe { OwnedHandle::from_raw_handle(restricted) };
        assert_eq!(
            process_running(restricted.as_handle())
                .unwrap_err()
                .raw_os_error(),
            Some(ERROR_ACCESS_DENIED.cast_signed()),
        );
        let name = format!(
            "acyclic-process-tree-{}-{:016x}",
            producer,
            u64::from_ne_bytes(random)
        );
        let job_name = super::super::custody::job_name(&name);
        let event_name = super::super::custody::job_name(&format!("{name}-retired"));
        let done = event(true, Some(&event_name))?;
        assert_eq!(
            event(true, Some(&event_name)).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        let unrelated_done = event(true, None)?;
        let job = |name: *const u16| -> io::Result<OwnedHandle> {
            // SAFETY: an optional NUL-terminated name, owned on success.
            let handle = unsafe { CreateJobObjectW(std::ptr::null(), name) };
            if handle.is_null() {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: CreateJobObjectW transferred one owned handle.
            Ok(unsafe { OwnedHandle::from_raw_handle(handle.cast()) })
        };
        let selected = job(job_name.as_ptr())?;
        let unrelated = job(std::ptr::null())?;
        // Hold the unrelated original producer before its settle transition.
        // These are actual native Jobs and the SAME production State, not a
        // synthesized member count or process-exit receipt.
        let tree = |job: &OwnedHandle, done: &OwnedHandle| -> io::Result<Tree> {
            Ok(Tree {
                job: job.try_clone()?.into_raw_handle().cast(),
                done: done.try_clone()?.into_raw_handle().cast(),
                members: 0,
                secret: [0; 16],
            })
        };
        let mut state = State {
            trees: [
                (0, tree(&selected, &done)?),
                (1, tree(&unrelated, &unrelated_done)?),
            ]
            .into_iter()
            .collect(),
            members: Members::new(std::ptr::null_mut()),
            owner_exited: true,
            input_closed: true,
            visits: 0,
        };
        std::fs::write(root.path().join(format!("{name}-custodian-{producer}")), [])?;
        assert!(!state.settle(Some(0))?);
        assert!(!state.settle(None)?);
        assert_eq!(state.visits, 1, "only the selected tree was visited");
        assert_eq!(
            super::super::ProcessCustody::open(root.path())?.fence_stale()?,
            1
        );
        assert!(state.trees.contains_key(&1));
        // SAFETY: one observation of the retained original unrelated event.
        assert_eq!(
            unsafe { WaitForSingleObject(unrelated_done.as_raw_handle().cast(), 0) },
            WAIT_TIMEOUT
        );
        let mut child = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "process_tree::custody_tests::custody_owner_helper",
                "--nocapture",
            ])
            .env_remove("ACYCLIC_CUSTODY_TEST_MODE")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let lost = identity(child.as_raw_handle().cast())?;
        assert!(child.wait()?.success());
        // Query the same retained process handle, never a replacement PID.
        assert!(!process_running(child.as_handle())?);
        assert_eq!(
            await_tree(&unrelated_done, &lost).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        let (pid, started) = producer.split_once('-').unwrap();
        let replaced = format!("{pid}-{}", started.parse::<u64>().unwrap() ^ 1);
        assert_eq!(
            await_tree(&unrelated_done, &replaced).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert!(state.settle(Some(1))?);
        await_tree(&unrelated_done, &producer)?;
        assert!(state.trees.is_empty());

        // The original owner-death pass must also retire already-empty Jobs.
        // Removing one live slot swaps another into it: neither may be skipped
        // or left waiting for a member-exit producer that no longer exists.
        let empty = [job(std::ptr::null())?, job(std::ptr::null())?];
        let completion = [event(true, None)?, event(true, None)?];
        let mut state = State {
            trees: [
                (0, tree(&empty[0], &completion[0])?),
                (1, tree(&empty[1], &completion[1])?),
            ]
            .into_iter()
            .collect(),
            members: Members::new(std::ptr::null_mut()),
            owner_exited: true,
            input_closed: true,
            visits: 0,
        };
        state.kill_all()?;
        assert!(state.settle(None)?);
        assert_eq!(state.visits, empty.len());
        for completed in completion {
            // SAFETY: this retained event belongs to the exact original Job.
            assert_eq!(
                unsafe { WaitForSingleObject(completed.as_raw_handle().cast(), 0) },
                WAIT_OBJECT_0
            );
        }
        Ok(())
    }

    fn assert_closed(handle: HANDLE) {
        let mut flags = 0;
        // SAFETY: queries a numeric handle without dereferencing it.
        assert_eq!(unsafe { GetHandleInformation(handle, &raw mut flags) }, 0);
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(ERROR_INVALID_HANDLE.cast_signed())
        );
    }

    #[test]
    fn failed_handover_retires_every_unpublished_duplicate() {
        let source = event(false, None).expect("source event");
        // SAFETY: the current process pseudo-handle is always live.
        let target = unsafe { GetCurrentProcess() };
        let copied;
        {
            let mut handoff = Handoff::new(target);
            handoff
                .duplicate(source.as_raw_handle().cast(), 0, DUPLICATE_SAME_ACCESS)
                .expect("first duplicate");
            copied = handoff.handles[0];
            assert!(
                handoff
                    .duplicate(std::ptr::null_mut(), 0, DUPLICATE_SAME_ACCESS)
                    .is_err()
            );
        }
        assert_closed(copied);

        let (read, mut output) = io::pipe().expect("handover pipe");
        drop(read);
        let mut handoff = Handoff::new(target);
        for _ in 0..4 {
            handoff
                .duplicate(source.as_raw_handle().cast(), 0, DUPLICATE_SAME_ACCESS)
                .expect("duplicate before write");
        }
        let copied = handoff.handles;
        assert!(handoff.publish(&mut output, 1, &[0; 16]).is_err());
        for handle in copied {
            assert_closed(handle);
        }
    }
}

//! The Windows custodian of an owner's custodied trees: one small process per
//! owner that holds each tree's Job, and so its name, until every member of
//! that tree has fully exited, and kills every tree when the owner exits. A
//! reopened owner's fence therefore finds a Job by name for as long as any
//! member may still run, and "not found" means the tree is gone. Each record
//! names the custodian, so the fence also awaits the custodian's exit, which
//! follows the last member of the dead owner's last tree. Records are never
//! the custodian's to remove: each has one remover, its live owner or the
//! reopened owner's fence.
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
use std::collections::{BTreeMap, HashMap};
use std::ffi::c_void;
use std::io::{self, BufRead as _, BufReader, Write as _};
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use std::os::windows::process::CommandExt as _;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use windows_sys::Win32::Foundation::{
    CloseHandle, DUPLICATE_SAME_ACCESS, DuplicateHandle, ERROR_INVALID_HANDLE,
    ERROR_INVALID_PARAMETER, HANDLE, INVALID_HANDLE_VALUE, WAIT_FAILED, WAIT_OBJECT_0,
    WAIT_TIMEOUT,
};
use windows_sys::Win32::System::Console::{GetStdHandle, STD_ERROR_HANDLE, STD_OUTPUT_HANDLE};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatus, OVERLAPPED, PostQueuedCompletionStatus,
};
use windows_sys::Win32::System::JobObjects::{
    IsProcessInJob, JOBOBJECT_ASSOCIATE_COMPLETION_PORT, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
    JobObjectAssociateCompletionPortInformation, JobObjectBasicAccountingInformation,
    QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
};
use windows_sys::Win32::System::Threading::{
    CreateEventW, DETACHED_PROCESS, ExitProcess, GetCurrentProcess, GetProcessId, INFINITE,
    OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE,
    RegisterWaitForSingleObject, SetEvent, UnregisterWaitEx, WT_EXECUTEONLYONCE,
    WaitForMultipleObjects, WaitForSingleObject,
};

/// The environment variable that makes a process a custodian.
const SWITCH: &str = "ACYCLIC_PROCESS_CUSTODIAN";
/// The line a custodian writes once it can take trees.
const READY: &str = "ready";
/// The Job message that a process joined it; its value is the PID.
const JOB_OBJECT_MSG_NEW_PROCESS: u32 = 6;
/// The completion key the owner watch posts once the owner has exited.
const OWNER_EXITED: usize = usize::MAX;
/// The completion key a member's wait posts once it has exited; the
/// message carries its PID.
const MEMBER_EXITED: usize = usize::MAX - 1;
/// The completion key the input reader posts for each tree it reads.
const OPENED: usize = usize::MAX - 3;
/// The completion key the input reader posts once the owner's input ends.
const INPUT_CLOSED: usize = usize::MAX - 4;

/// The owner's handle on its running custodian.
pub(super) struct Custodian {
    child: Child,
    input: Mutex<ChildStdin>,
    identity: String,
    endpoint: String,
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
    if let Some(custodian) = current.as_ref().filter(|custodian| custodian.running()) {
        return Ok(Arc::clone(custodian));
    }
    let custodian = Arc::new(launch()?);
    *current = Some(Arc::clone(&custodian));
    Ok(custodian)
}

/// Whether `identity` names this owner's running custodian, which exits only
/// after the owner does and so can never be awaited by it.
pub(super) fn is_current(identity: &str) -> io::Result<bool> {
    Ok(CURRENT
        .lock()
        .map_err(|_| poisoned())?
        .as_ref()
        .is_some_and(|custodian| custodian.identity == identity))
}

fn launch() -> io::Result<Custodian> {
    // Keeps the initializer, and so the entry, in every linked executable
    // that can start a custodian.
    std::hint::black_box(&ENTRY);
    // Detached, the custodian starts no console host of its own.
    let mut child = super::spawner::spawn(
        Command::new(std::env::current_exe()?)
            .env(SWITCH, "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .creation_flags(DETACHED_PROCESS),
    )?;
    let started = hand_over(&mut child).and_then(|(input, endpoint)| {
        Ok((input, endpoint, identity(child.as_raw_handle().cast())?))
    });
    match started {
        Ok((input, endpoint, identity)) => Ok(Custodian {
            child,
            input: Mutex::new(input),
            identity,
            endpoint,
        }),
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            Err(error)
        }
    }
}

fn hand_over(custodian: &mut Child) -> io::Result<(ChildStdin, String)> {
    let target: HANDLE = custodian.as_raw_handle().cast();
    // SAFETY: a synchronize-only handle to this process, for the custodian.
    let owner = unsafe { duplicate(GetCurrentProcess(), target, PROCESS_SYNCHRONIZE, 0) }?;
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
    let mut ready = String::new();
    BufReader::new(custodian.stdout.take().ok_or(io::ErrorKind::BrokenPipe)?)
        .read_line(&mut ready)?;
    let Some(endpoint) = ready.trim_end().strip_prefix(&format!("{READY} ")) else {
        return Err(io::Error::other("the process custodian did not start"));
    };
    let address = endpoint
        .parse::<std::net::SocketAddrV4>()
        .map_err(|_| io::ErrorKind::InvalidData)?;
    if !address.ip().is_loopback() {
        return Err(io::ErrorKind::InvalidData.into());
    }
    Ok((input, endpoint.to_owned()))
}

impl Custodian {
    fn running(&self) -> bool {
        // SAFETY: the owned custodian process handle; a zero wait only reads
        // whether it has exited.
        unsafe { WaitForSingleObject(self.child.as_raw_handle().cast(), 0) == WAIT_TIMEOUT }
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
    ) -> io::Result<Link> {
        let ready = event(false)?;
        let done = event(true)?;
        let target: HANDLE = self.child.as_raw_handle().cast();
        // SAFETY: owned Job, process and event handles of this process, and
        // the live custodian with duplicate access. Each lands in the
        // custodian, which owns and closes it.
        let handed = unsafe {
            [
                duplicate(job, target, 0, DUPLICATE_SAME_ACCESS)?,
                duplicate(
                    process,
                    target,
                    PROCESS_SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION,
                    0,
                )?,
                duplicate(
                    ready.as_raw_handle().cast(),
                    target,
                    0,
                    DUPLICATE_SAME_ACCESS,
                )?,
                duplicate(
                    done.as_raw_handle().cast(),
                    target,
                    0,
                    DUPLICATE_SAME_ACCESS,
                )?,
            ]
        };
        writeln!(
            self.input.lock().map_err(|_| poisoned())?,
            "{tree} {} {} {} {} {}",
            handed[0].expose_provenance(),
            handed[1].expose_provenance(),
            handed[2].expose_provenance(),
            handed[3].expose_provenance(),
            admission::encode(secret)
        )?;
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
            _ => Err(io::Error::other("the process custodian exited")),
        }
    }
}

impl Link {
    /// Returns once every member of the tree has fully exited, or the
    /// custodian has, as it does only after killing the tree's members.
    pub(super) fn await_members(&self) -> io::Result<()> {
        wait_any(&[
            self.done.as_raw_handle().cast(),
            self.custodian.child.as_raw_handle().cast(),
        ])
        .map(drop)
    }
}

/// A new unnamed event, auto-reset unless `manual`.
fn event(manual: bool) -> io::Result<OwnedHandle> {
    // SAFETY: an unnamed, unsignaled event; owned on success.
    let event = unsafe { CreateEventW(std::ptr::null(), i32::from(manual), 0, std::ptr::null()) };
    if event.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful creation transfers one owned handle.
    Ok(unsafe { OwnedHandle::from_raw_handle(event) })
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

/// Waits for the custodian of `identity`, as a record names it, if it still
/// runs: it exits only once every member of each of its trees has.
pub(super) fn await_custodian(identity: &str) -> io::Result<()> {
    let Some((pid, created)) = identity
        .split_once('-')
        .and_then(|(pid, created)| Some((pid.parse::<u32>().ok()?, created.parse::<u64>().ok()?)))
    else {
        return Err(io::ErrorKind::InvalidData.into());
    };
    use crate::process_identity::{
        IncarnationState, ProcessIncarnation, ProcessObservation, ProcessProbe,
    };
    let owner = ProcessIncarnation::new(pid, created).ok_or(io::ErrorKind::InvalidData)?;
    let Some(process) = ProcessProbe::open(owner.pid())? else {
        return Ok(());
    };
    match owner.observe(ProcessObservation::Present {
        started: process.started()?,
        exited: false,
    }) {
        IncarnationState::Running => process.await_exit(),
        IncarnationState::Replaced | IncarnationState::Exited | IncarnationState::Absent => Ok(()),
        IncarnationState::Unknown => Err(io::ErrorKind::InvalidData.into()),
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
        Some(Self {
            tree,
            job: handle()?,
            process: handle()?,
            ready: handle()?,
            done: handle()?,
            secret: admission::decode(fields.next()?)?,
        })
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

/// The custodian: watches each tree's Job, kills every tree when the owner
/// exits, and exits once the owner has and no member of any tree remains.
fn serve() -> io::Result<()> {
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
    PORT.store(port.expose_provenance(), Ordering::Relaxed);
    let admission = super::custodian_admission::Server::bind()?;
    let endpoint = admission.endpoint.clone();
    let requests = admission.start(port)?;
    let opened = read_trees(input, port)?;
    let mut output = io::stdout();
    writeln!(output, "{READY} {endpoint}")?;
    output.flush()?;
    let (owner_watch, port_watch) = (owner.expose_provenance(), port.expose_provenance());
    std::thread::spawn(move || {
        let owner: HANDLE = std::ptr::with_exposed_provenance_mut(owner_watch);
        let port: HANDLE = std::ptr::with_exposed_provenance_mut(port_watch);
        // SAFETY: handles this custodian owns for its whole life.
        unsafe {
            WaitForSingleObject(owner, INFINITE);
            PostQueuedCompletionStatus(port, 0, OWNER_EXITED, std::ptr::null());
        }
    });
    let mut state = State {
        trees: HashMap::new(),
        members: Members::default(),
        owner_exited: false,
        input_closed: false,
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
                let tree = opened.recv().map_err(|_| io::ErrorKind::BrokenPipe)?;
                state.open(tree, port);
                Some(tree.tree)
            }
            INPUT_CLOSED => {
                state.input_closed = true;
                None
            }
            super::custodian_admission::ADMIT => {
                let request = requests.recv().map_err(|_| io::ErrorKind::BrokenPipe)?;
                // Only a holder of the tree's secret may name its members.
                let admitted = match state.trees.get(&request.tree) {
                    Some(tree) if admission::matches(&tree.secret, &request.secret) => {
                        state.admit(request.tree, request.pid)
                    }
                    _ => Ok(false),
                };
                let _ = request
                    .reply
                    .send(admitted.as_ref().is_ok_and(|admitted| *admitted));
                admitted?;
                Some(request.tree)
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
    trees: HashMap<usize, Tree>,
    members: Members,
    owner_exited: bool,
    input_closed: bool,
}

impl State {
    /// Starts watching a handed-over tree, then acknowledges it. A tree this
    /// custodian cannot watch is killed before its first member resumes,
    /// and its owner told so.
    fn open(&mut self, opened: Opened, port: HANDLE) {
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
            // SAFETY: the handed-over process handle, which nothing watches.
            unsafe { CloseHandle(opened.process) };
            false
        };
        // SAFETY: the handed-over Job with terminate access, and its events,
        // each signaled at most once here and the ready event then released.
        unsafe {
            if !watched || self.owner_exited {
                TerminateJobObject(opened.job, 1);
            }
            if watched {
                SetEvent(opened.ready);
            } else {
                // Unwatched, the tree is the owner's to kill and await.
                SetEvent(opened.done);
                CloseHandle(opened.done);
                CloseHandle(opened.job);
            }
            CloseHandle(opened.ready);
        }
        if watched {
            self.trees.insert(opened.tree, tree);
        }
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
        for (id, tree) in &mut self.trees {
            for pid in super::custody::members(tree.job)? {
                self.members.admit(tree, *id, pid)?;
            }
            // SAFETY: the handed-over Job with terminate access.
            if unsafe { TerminateJobObject(tree.job, 1) } == 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }

    /// Settles the tree an event touched, and once the owner has exited
    /// every tree; answers whether the custodian is done: an owner that
    /// exited and closed its input can hand over no further tree.
    fn settle(&mut self, touched: Option<usize>) -> io::Result<bool> {
        if self.owner_exited {
            let ids: Vec<usize> = self.trees.keys().copied().collect();
            for id in ids {
                self.settle_tree(id)?;
            }
            return Ok(self.input_closed && self.trees.is_empty());
        }
        if let Some(id) = touched {
            self.settle_tree(id)?;
        }
        Ok(false)
    }

    /// Lets go of tree `id` once no member is watched and none is active: a
    /// member stops counting as active before its process has fully exited,
    /// so both must hold. The owner learns of it from the tree's event.
    fn settle_tree(&mut self, id: usize) -> io::Result<()> {
        let Some(tree) = self.trees.get(&id) else {
            return Ok(());
        };
        if tree.members == 0 && active_processes(tree.job)? == 0 {
            // SAFETY: the tree's handed-over Job and event; closing the Job
            // lets the name go only now.
            unsafe {
                SetEvent(tree.done);
                CloseHandle(tree.done);
                CloseHandle(tree.job);
            }
            self.trees.remove(&id);
        }
        Ok(())
    }
}

/// Reads each tree the owner hands over and posts it to `port`; posts the
/// end of the owner's input once it closes.
fn read_trees(mut input: BufReader<io::Stdin>, port: HANDLE) -> io::Result<mpsc::Receiver<Opened>> {
    let (send, receive) = mpsc::channel();
    let port = port.expose_provenance();
    std::thread::Builder::new()
        .name("custodian-input".into())
        .spawn(move || {
            let port: HANDLE = std::ptr::with_exposed_provenance_mut(port);
            let mut line = String::new();
            loop {
                line.clear();
                let tree = match input.read_line(&mut line) {
                    Ok(0) | Err(_) => None,
                    Ok(_) => Opened::parse(line.trim_end()),
                };
                let key = if let Some(tree) = tree {
                    if send.send(tree).is_err() {
                        return;
                    }
                    OPENED
                } else {
                    INPUT_CLOSED
                };
                // SAFETY: the custodian owns this completion port until exit.
                unsafe { PostQueuedCompletionStatus(port, 0, key, std::ptr::null()) };
                if key == INPUT_CLOSED {
                    return;
                }
            }
        })?;
    Ok(receive)
}

fn transport_lease(handle: HANDLE) -> Option<OwnedHandle> {
    if handle.is_null() {
        None
    } else {
        // SAFETY: hand_over transferred this distinct handle into this process.
        Some(unsafe { OwnedHandle::from_raw_handle(handle.cast()) })
    }
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

/// Every tree's live members: each holds its process handle, a thread pool
/// wait that reports its exit and its tree, and is released on that report,
/// so the custodian's cost tracks live members, never every member there was.
#[derive(Default)]
struct Members(BTreeMap<u32, (HANDLE, HANDLE, usize)>);

impl Members {
    /// Watches process `pid` if it is a member of `tree`'s Job. A PID whose
    /// process already exited is either gone or reused outside the Job; a
    /// held handle keeps a watched member's PID from being reused.
    fn admit(&mut self, tree: &mut Tree, id: usize, pid: u32) -> io::Result<bool> {
        if self.0.contains_key(&pid) {
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
            return self.watch(tree, id, process).map(|()| true);
        }
        let error = io::Error::last_os_error();
        // SAFETY: the handle opened above.
        unsafe { CloseHandle(process) };
        if queried == 0 { Err(error) } else { Ok(false) }
    }

    /// Watches `process`, a member of `tree`, taking ownership of its handle.
    fn watch(&mut self, tree: &mut Tree, id: usize, process: HANDLE) -> io::Result<()> {
        // SAFETY: an owned process handle.
        let pid = unsafe { GetProcessId(process) };
        let mut wait: HANDLE = std::ptr::null_mut();
        // SAFETY: the owned process handle and a one-shot callback context;
        // the wait reports once, with the PID as its context.
        let registered = pid != 0
            && unsafe {
                RegisterWaitForSingleObject(
                    &raw mut wait,
                    process,
                    Some(member_exited),
                    std::ptr::with_exposed_provenance(pid as usize),
                    INFINITE,
                    WT_EXECUTEONLYONCE,
                )
            } != 0;
        if !registered {
            let error = io::Error::last_os_error();
            // SAFETY: the owned handle, which nothing watches.
            unsafe { CloseHandle(process) };
            return Err(error);
        }
        self.0.insert(pid, (process, wait, id));
        tree.members += 1;
        Ok(())
    }

    /// Releases the member `pid`, whose exit its wait reported, and answers
    /// its tree.
    fn release(&mut self, pid: u32) -> Option<usize> {
        let (process, wait, tree) = self.0.remove(&pid)?;
        // SAFETY: the reported wait, which never runs again, and the
        // handle it watched. A null event returns without waiting for
        // the reporting callback, which no longer uses either.
        unsafe {
            UnregisterWaitEx(wait, std::ptr::null_mut());
            CloseHandle(process);
        }
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

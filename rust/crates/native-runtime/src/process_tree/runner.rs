//! The Linux launcher of custodied trees: one small process per owner, from
//! which every custodied tree's warden is cloned.
//!
//! Forking the owner copies its whole address space, once for the warden and
//! again for each process the warden forks, and leaves the copy holding
//! whatever the host's other threads held: its allocator, locks and fork
//! handlers. The runner is instead a separate static image, built with this
//! library (`runner/main.rs`), that the owner execs once from a sealed memory
//! file: its clones and the warden's forks copy only the runner's few pages,
//! and hold nothing of the host, whatever host loaded this library.
//!
//! Each warden is cloned with `CLONE_PARENT`: it is the owner's own child,
//! watches the owner itself and is reaped by it, exactly as one the owner
//! forked. The runner leads a process group of its own, which each warden
//! joins once its tree's group exists, so a kill of the tree never reaches
//! it. The runner holds no tree: killing it ends no tree and breaks no
//! custody, and the owner's next spawn starts another.
//!
//! A request carries the custody directory, a reply pipe, the owner's
//! working directory and the child's standard streams as descriptors, then
//! the owner's PID, the program, its arguments and its whole environment.
//! The cloned warden writes its PID to the reply pipe first, and an exec
//! failure after it; the reply ends once the program has exec'd and the
//! warden closed its copy, as a forked spawn's status pipe does.

use super::ProcessCommand;
use super::command::Stdio as Stream;
use super::platform::Child;
use libc::pid_t;
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{self, Read as _, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt as _;
use std::process::Command;
use std::sync::{Mutex, OnceLock};

/// The runner's image, built with this library.
static IMAGE: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/runner"));

/// Reply frame tags, each followed by a native-endian 32-bit value.
const STARTED: u8 = b'P';
const FAILED: u8 = b'E';
/// Stream slots in a request.
const NULL: u8 = 0;
const SENT: u8 = 1;
const CLOSED: u8 = 2;
/// Request flags.
const HAS_CWD: u8 = 1;
const HAS_UID: u8 = 2;
const HAS_GID: u8 = 4;

/// Resource limits a request carries: every Linux limit, 0..`RLIM_NLIMITS`.
const LIMITS: u32 = 16;

/// Appends what a fork of the owner would have inherited but the long-lived
/// runner cannot know: the owner's current file-creation mask and resource
/// limits, both changeable after the runner started.
fn put_settings(body: &mut Vec<u8>) -> io::Result<()> {
    // Read without changing it; umask(2) can only set it.
    let status = std::fs::read("/proc/self/status")?;
    let mask = status
        .split(|byte| *byte == b'\n')
        .find_map(|line| line.strip_prefix(b"Umask:"))
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| u32::from_str_radix(value.trim(), 8).ok())
        .ok_or(io::ErrorKind::InvalidData)?;
    body.extend_from_slice(&mask.to_ne_bytes());
    for resource in 0..LIMITS {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: writable limit storage; reads this process's limit.
        if unsafe { libc::getrlimit(resource, &raw mut limit) } != 0 {
            return Err(io::Error::last_os_error());
        }
        body.extend_from_slice(&limit.rlim_cur.to_ne_bytes());
        body.extend_from_slice(&limit.rlim_max.to_ne_bytes());
    }
    Ok(())
}

/// Appends a slot per standard stream, and the descriptor each sent one
/// carries. Returns the child's ends of new pipes, which the request
/// carries, and the owner's ends.
fn slots(
    command: &ProcessCommand,
    open_streams: [bool; 3],
    descriptors: &mut Vec<RawFd>,
    body: &mut Vec<u8>,
) -> io::Result<(Vec<OwnedFd>, [Option<OwnedFd>; 3])> {
    let mut theirs = Vec::with_capacity(3);
    let mut ours: [Option<OwnedFd>; 3] = [None, None, None];
    let streams = [&command.stdin, &command.stdout, &command.stderr];
    for (index, stream) in streams.into_iter().enumerate() {
        let slot = match stream.as_ref().map(|stdio| &stdio.0) {
            Some(Stream::Null) => NULL,
            None | Some(Stream::Inherit) => {
                if open_streams.get(index).copied().unwrap_or(false) {
                    descriptors
                        .push(RawFd::try_from(index).map_err(|_| io::ErrorKind::InvalidInput)?);
                    SENT
                } else {
                    CLOSED
                }
            }
            Some(Stream::Owned(fd)) => {
                descriptors.push(fd.as_raw_fd());
                SENT
            }
            Some(Stream::Piped) => {
                let (read, write) = pipe()?;
                let (child, parent) = if index == 0 {
                    (read, write)
                } else {
                    (write, read)
                };
                descriptors.push(child.as_raw_fd());
                theirs.push(child);
                if let Some(end) = ours.get_mut(index) {
                    *end = Some(parent);
                }
                SENT
            }
        };
        body.push(slot);
    }
    Ok((theirs, ours))
}

/// Appends a request's fixed fields after its stream slots, then its
/// strings: program, directory, arg0, arguments and environment.
fn encode(
    body: &mut Vec<u8>,
    command: &ProcessCommand,
    environment: &BTreeMap<&OsStr, &OsStr>,
) -> io::Result<()> {
    let identity = &command.unix;
    let flags = [
        (command.cwd.is_some(), HAS_CWD),
        (identity.uid.is_some(), HAS_UID),
        (identity.gid.is_some(), HAS_GID),
    ]
    .iter()
    .filter(|(set, _)| *set)
    .fold(0, |flags, (_, flag)| flags | flag);
    body.push(flags);
    body.extend_from_slice(&identity.uid.unwrap_or_default().to_ne_bytes());
    body.extend_from_slice(&identity.gid.unwrap_or_default().to_ne_bytes());
    body.extend_from_slice(&std::process::id().to_ne_bytes());
    put_settings(body)?;
    put_count(body, 1 + command.args.len());
    put_count(body, environment.len());
    put_string(body, &[command.program.as_bytes()])?;
    if let Some(cwd) = &command.cwd {
        put_string(body, &[cwd.as_os_str().as_bytes()])?;
    }
    put_string(
        body,
        &[identity
            .arg0
            .as_ref()
            .unwrap_or(&command.program)
            .as_bytes()],
    )?;
    for argument in command.get_args() {
        put_string(body, &[argument.as_bytes()])?;
    }
    for (key, value) in environment {
        put_string(body, &[key.as_bytes(), b"=", value.as_bytes()])?;
    }
    Ok(())
}

/// Has the runner start `command` in `directory`'s custody. Returns the
/// warden once one exists, with the exec failure it reported.
pub(super) fn start(
    command: &ProcessCommand,
    directory: &File,
) -> io::Result<(Child, io::Result<()>)> {
    let identity = &command.unix;
    // The tree always leads its own group, which the warden's PID names.
    if identity.process_group.is_some_and(|group| group != 0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a custodied tree leads its own process group",
        ));
    }
    let mut environment: BTreeMap<&OsStr, &OsStr> = BTreeMap::new();
    let inherited: Vec<(OsString, OsString)> = if command.env.clear {
        Vec::new()
    } else {
        std::env::vars_os().collect()
    };
    for (key, value) in &inherited {
        environment.insert(key, value);
    }
    for (key, value) in &command.env.vars {
        match value {
            Some(value) => environment.insert(&key.name, value),
            None => environment.remove(key.name.as_os_str()),
        };
    }
    // Which standard streams the owner has open, decided before this
    // function allocates a descriptor that could take a closed one's number.
    // The custody directory was opened by the caller, so its number is
    // excluded too.
    let open_streams = [0, 1, 2].map(|fd: RawFd| {
        // SAFETY: F_GETFD only reads this process's descriptor flags.
        fd != directory.as_raw_fd() && unsafe { libc::fcntl(fd, libc::F_GETFD) } >= 0
    });
    let (reply, reply_end) = pipe()?;
    // The child starts where the owner is now, then in its own directory.
    // A path descriptor is enough for fchdir and needs no read access.
    let here = {
        use std::os::unix::fs::OpenOptionsExt as _;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_PATH | libc::O_DIRECTORY)
            .open(".")?
    };
    let mut descriptors = vec![
        directory.as_raw_fd(),
        reply_end.as_raw_fd(),
        here.as_raw_fd(),
    ];
    let mut body = Vec::new();
    let (theirs, ours) = slots(command, open_streams, &mut descriptors, &mut body)?;
    encode(&mut body, command, &environment)?;
    let mut message = Vec::with_capacity(8 + body.len());
    put_count(&mut message, body.len());
    message.extend_from_slice(&body);
    send(&descriptors, &message)?;
    // Only the runner and its clone hold the child's ends now.
    drop(theirs);
    drop(reply_end);
    drop(here);
    let mut replied = Vec::new();
    File::from(reply).read_to_end(&mut replied)?;
    let mut pid = None;
    let mut failure = None;
    for frame in replied.chunks(5) {
        let (tag, value) = frame.split_first().ok_or(io::ErrorKind::InvalidData)?;
        let value = i32::from_ne_bytes(value.try_into().map_err(|_| io::ErrorKind::InvalidData)?);
        match *tag {
            STARTED => pid = pid.or(Some(value)),
            FAILED => failure = failure.or(Some(io::Error::from_raw_os_error(value))),
            _ => return Err(io::ErrorKind::InvalidData.into()),
        }
    }
    let Some(pid) = pid else {
        // Without a failure, the runner exited holding this request; the
        // next spawn observes its exit and starts another.
        return Err(failure.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "the process runner exited")
        }));
    };
    let [stdin, stdout, stderr] = ours;
    let child = Child::new(
        pid,
        stdin.map(Into::into),
        stdout.map(Into::into),
        stderr.map(Into::into),
    );
    Ok((child, failure.map_or(Ok(()), Err)))
}

fn put_count(body: &mut Vec<u8>, count: usize) {
    // usize is at most 64 bits on every supported target.
    body.extend_from_slice(&u64::try_from(count).unwrap_or(u64::MAX).to_ne_bytes());
}

/// Appends `parts` as one NUL-terminated string.
fn put_string(body: &mut Vec<u8>, parts: &[&[u8]]) -> io::Result<()> {
    for part in parts {
        if part.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "nul byte found in provided data",
            ));
        }
        body.extend_from_slice(part);
    }
    body.push(0);
    Ok(())
}

fn pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut ends = [0; 2];
    // SAFETY: writable storage for two descriptors.
    if unsafe { libc::pipe2(ends.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let [read, write] = ends;
    // SAFETY: pipe2 transferred both descriptors to this caller.
    Ok(unsafe { (OwnedFd::from_raw_fd(read), OwnedFd::from_raw_fd(write)) })
}

/// This owner's runner: its control socket and process.
struct Runner {
    owner: u32,
    control: UnixStream,
    pid: pid_t,
    reaped: bool,
}

static RUNNER: Mutex<Option<Runner>> = Mutex::new(None);

impl Runner {
    fn start() -> io::Result<Self> {
        let image = image()?;
        let (control, theirs) = UnixStream::pair()?;
        // The runner's standard input is its control socket.
        // The runner's null device becomes a null child's standard input
        // too, so it is open for reading as well as writing.
        let null = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/null")?;
        let runner = Command::new(format!("/proc/self/fd/{}", image.as_raw_fd()))
            .env_clear()
            .stdin(OwnedFd::from(theirs))
            .stdout(null.try_clone()?)
            .stderr(null)
            .process_group(0)
            .spawn()?;
        let pid = pid_t::try_from(runner.id()).map_err(|_| io::ErrorKind::InvalidData)?;
        Ok(Self {
            owner: std::process::id(),
            control,
            pid,
            reaped: false,
        })
    }

    /// Whether this runner still serves this process: it is alive, and this
    /// is the process that started it, not a fork of it.
    fn serves(&mut self) -> bool {
        if self.owner != std::process::id() {
            return false;
        }
        let mut status = 0;
        // SAFETY: this process's own unreaped child and writable storage.
        if unsafe { libc::waitpid(self.pid, &raw mut status, libc::WNOHANG) } == self.pid {
            self.reaped = true;
        }
        !self.reaped
    }

    fn retire(self) {
        if self.owner == std::process::id() && !self.reaped {
            // SAFETY: this process's own unreaped child, so the PID is its.
            unsafe { libc::kill(self.pid, libc::SIGKILL) };
            let mut status = 0;
            // SAFETY: as above, with writable status storage.
            while unsafe { libc::waitpid(self.pid, &raw mut status, 0) } < 0
                && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted
            {}
        }
    }
}

/// The runner's image in a sealed memory file, made once per process.
fn image() -> io::Result<&'static OwnedFd> {
    static FILE: OnceLock<OwnedFd> = OnceLock::new();
    static MAKING: Mutex<()> = Mutex::new(());
    let _making = MAKING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(file) = FILE.get() {
        return Ok(file);
    }
    let file = make_image()?;
    Ok(FILE.get_or_init(|| file))
}

/// Writes the runner's image to a new memory file, seals it, and returns a
/// read-only description of it. Exec refuses a file any process holds open
/// for writing, so the writable description lives only from its creation to
/// the reopen: one write, the seals, the reopen, then its close.
fn make_image() -> io::Result<OwnedFd> {
    // SAFETY: a NUL-terminated name and flags.
    let fd = unsafe {
        libc::memfd_create(
            c"acyclic-runner".as_ptr(),
            libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: memfd_create transferred this descriptor to this caller.
    let writable = unsafe { OwnedFd::from_raw_fd(fd) };
    let path = format!("/proc/self/fd/{fd}");
    // SAFETY: readable storage of the stated length.
    let written = unsafe { libc::pwrite(fd, IMAGE.as_ptr().cast(), IMAGE.len(), 0) };
    if usize::try_from(written).ok() != Some(IMAGE.len()) {
        return Err(if written < 0 {
            io::Error::last_os_error()
        } else {
            io::ErrorKind::WriteZero.into()
        });
    }
    let seals = libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
    // SAFETY: adds seals to this process's own memory file.
    if unsafe { libc::fcntl(fd, libc::F_ADD_SEALS, seals) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let file = File::open(path)?;
    drop(writable);
    Ok(file.into())
}

fn runner() -> std::sync::MutexGuard<'static, Option<Runner>> {
    RUNNER
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Sends one request, starting a runner when none serves this process.
fn send(descriptors: &[RawFd], message: &[u8]) -> io::Result<()> {
    let mut slot = runner();
    if slot.as_mut().is_some_and(|runner| !runner.serves())
        && let Some(stale) = slot.take()
    {
        stale.retire();
    }
    if slot.is_none() {
        *slot = Some(Runner::start()?);
    }
    let runner = slot.as_mut().ok_or(io::ErrorKind::NotFound)?;
    let sent = send_message(&runner.control, descriptors, message);
    if sent.is_err()
        && let Some(failed) = slot.take()
    {
        failed.retire();
    }
    sent
}

/// Writes `message`, carrying `descriptors` with its first byte.
fn send_message(control: &UnixStream, descriptors: &[RawFd], message: &[u8]) -> io::Result<()> {
    let mut space = [0_u64; 8];
    let length = u32::try_from(std::mem::size_of_val(descriptors))
        .map_err(|_| io::ErrorKind::InvalidInput)?;
    let mut vector = libc::iovec {
        iov_base: message.as_ptr().cast_mut().cast(),
        iov_len: message.len(),
    };
    // SAFETY: plain fields; all-zero is a valid empty header.
    let mut header: libc::msghdr = unsafe { std::mem::zeroed() };
    header.msg_iov = &raw mut vector;
    header.msg_iovlen = 1;
    header.msg_control = space.as_mut_ptr().cast();
    // SAFETY: CMSG_SPACE only computes a size.
    let space_used = unsafe { libc::CMSG_SPACE(length) };
    if usize::try_from(space_used).map_or(true, |used| used > std::mem::size_of_val(&space)) {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    header.msg_controllen = space_used as _;
    // SAFETY: the control buffer above holds one header of this length.
    unsafe {
        let control = libc::CMSG_FIRSTHDR(&raw const header);
        (*control).cmsg_level = libc::SOL_SOCKET;
        (*control).cmsg_type = libc::SCM_RIGHTS;
        (*control).cmsg_len = libc::CMSG_LEN(length) as _;
        std::ptr::copy_nonoverlapping(
            descriptors.as_ptr().cast::<u8>(),
            libc::CMSG_DATA(control),
            std::mem::size_of_val(descriptors),
        );
    }
    let sent = loop {
        // SAFETY: an initialized header naming live buffers and descriptors.
        let sent =
            unsafe { libc::sendmsg(control.as_raw_fd(), &raw const header, libc::MSG_NOSIGNAL) };
        if let Ok(sent) = usize::try_from(sent) {
            break sent;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    };
    // The descriptors went with the first byte; the rest is plain data.
    (&*control).write_all(message.get(sent..).unwrap_or_default())
}

#[cfg(test)]
#[path = "runner_tests.rs"]
mod tests;

/// The running runner's PID, for tests that kill it.
#[cfg(test)]
pub(super) fn runner_pid() -> Option<u32> {
    runner()
        .as_ref()
        .and_then(|runner| u32::try_from(runner.pid).ok())
}

/// Retires this owner's runner, so the next spawn starts another.
#[cfg(test)]
pub(super) fn retire_runner() {
    if let Some(runner) = runner().take() {
        runner.retire();
    }
}

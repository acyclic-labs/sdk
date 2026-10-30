//! The direct child of a custodied tree. It records and locks the tree in
//! the owner's custody directory, watches the owner, and forks the real
//! program, which returns from [`enter`] and execs. The warden itself never
//! returns.
//!
//! On Linux the runner clones the warden and this runs there, in an image
//! without the standard library; on macOS `Command::spawn` forks it and this
//! runs before exec, in a copy of a multithreaded host. Either way it makes
//! only system calls: no allocation, no locks and no unwinding.

use super::record::{self, BOOT_BYTES, START_BYTES};
use super::sys::{self, Errno, Fd, Pid};
use core::ffi::CStr;

/// Runs in the owner's direct child. On macOS it already leads its own
/// process group; on Linux it creates that group here and later returns to
/// `home`, the runner's group. Returns only in the program's process, which
/// then execs.
pub(super) fn enter(
    directory: Fd,
    owner: Pid,
    #[cfg(target_os = "linux")] home: Pid,
) -> Result<(), Errno> {
    // An inherited host handler must not run in the warden.
    sys::set_signal(sys::SIGCHLD, false);
    // The tree's group, which this warden's PID names.
    #[cfg(target_os = "linux")]
    sys::setpgid(0, 0)?;
    let warden = sys::getpid();
    let mut storage = [0; 24];
    let name = record::name(warden, &mut storage).ok_or(Errno(sys::EINVAL))?;
    // The record exists before any of the program's code runs.
    let record = sys::openat(
        directory,
        name,
        sys::O_CREAT | sys::O_EXCL | sys::O_RDWR | sys::O_CLOEXEC,
        0o600,
    )?;
    let unlink = || {
        let _ = sys::unlinkat(directory, name);
    };
    let watch = match arm(record, owner, warden) {
        Ok(watch) => watch,
        Err(error) => {
            unlink();
            return Err(error);
        }
    };
    // The program waits on this pipe until the warden has left the group.
    let [admission, admit] = match sys::pipe() {
        Ok(ends) => ends,
        Err(error) => {
            unlink();
            return Err(error);
        }
    };
    let program = match sys::fork() {
        Ok(0) => {
            // Descriptors the program never uses.
            sys::close(record);
            sys::close(watch);
            sys::close(admit);
            return admitted_by_warden(admission);
        }
        Ok(program) => program,
        Err(error) => {
            sys::close(admission);
            sys::close(admit);
            unlink();
            return Err(error);
        }
    };
    sys::close(admission);
    #[cfg(target_os = "linux")]
    let left = leave_group(home);
    #[cfg(target_vendor = "apple")]
    let left = leave_group();
    if left.is_ok() {
        // A failed write leaves the program unadmitted, which fails the spawn.
        let _ = write_full(admit, &[1]);
    }
    sys::close(admit);
    supervise(directory, record, watch, owner, warden, program, name)
}

fn write_full(file: Fd, mut bytes: &[u8]) -> Result<(), Errno> {
    while !bytes.is_empty() {
        match sys::write(file, bytes) {
            Ok(0) => return Err(Errno(sys::EPIPE)),
            Ok(written) => bytes = bytes.get(written..).unwrap_or_default(),
            Err(Errno(sys::EINTR)) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// In the program: waits until the warden has left the tree's group.
/// Without that, a kill of the tree would also kill its custodian, so the
/// spawn fails instead.
fn admitted_by_warden(admission: Fd) -> Result<(), Errno> {
    let mut byte = [0_u8];
    let read = loop {
        match sys::read(admission, &mut byte) {
            Err(Errno(sys::EINTR)) => {}
            read => break read,
        }
    };
    sys::close(admission);
    if read == Ok(1) {
        Ok(())
    } else {
        Err(Errno(sys::EPERM))
    }
}

/// Moves the warden out of the tree's process group back into the runner's,
/// so a kill of the tree's group never reaches the warden, while the
/// warden's live PID keeps the tree's group ID from naming any other group.
/// Fails, failing the spawn, once no member of the runner's group lives.
#[cfg(target_os = "linux")]
fn leave_group(home: Pid) -> Result<(), Errno> {
    sys::setpgid(0, home)
}

/// Moves the warden out of the tree's process group into a group of its
/// own, so a kill of the tree's group never reaches the warden, while the
/// warden's live PID keeps the tree's group ID from naming any other group.
#[cfg(target_vendor = "apple")]
fn leave_group() -> Result<(), Errno> {
    // A new group needs a leader to name it: a short-lived child.
    let warden = sys::getpid();
    let anchor = sys::fork()?;
    if anchor == 0 {
        anchor_until_warden_exits(warden);
    }
    // The anchor is this warden's own unexeced child in its session, and the
    // warden then joins that group in the same session.
    let moved = sys::setpgid(anchor, anchor).and_then(|()| sys::setpgid(0, anchor));
    // The group lives on with the warden as its only member.
    let _ = sys::kill(anchor, sys::SIGKILL);
    while sys::wait(anchor, 0) == Err(Errno(sys::EINTR)) {}
    moved
}

/// In the anchor: holds none of the warden's descriptors, so neither the
/// record's lock nor the program's admission outlives the warden, and exits
/// when the warden does, if the warden's SIGKILL has not come first.
#[cfg(target_vendor = "apple")]
fn anchor_until_warden_exits(warden: Pid) -> ! {
    sys::block_all();
    close_except([-1; 3], sys::getpid());
    // The watch names the warden only while it is still the parent.
    if let Ok(watch) = owner_watch(warden)
        && sys::getppid() == warden
    {
        let _ = next_exit(watch);
    }
    sys::exit(0)
}

/// Locks the record, writes the warden's identity, and returns the owner's
/// exit watch.
fn arm(record: Fd, owner: Pid, warden: Pid) -> Result<Fd, Errno> {
    // Held until the warden exits: the program closes its copy before exec.
    sys::try_lock(record)?;
    let mut boot = [0; BOOT_BYTES];
    record::boot_identity(&mut boot)?;
    let mut start = [0; START_BYTES];
    let length = record::start_identity(warden, &mut start)?;
    // The boot, zero-padded to its fixed width, then the warden's start.
    write_full(record, &boot)?;
    write_full(record, start.get(..length).unwrap_or_default())?;
    #[cfg(target_os = "linux")]
    sys::subreaper()?;
    let watch = owner_watch(owner)?;
    // The watch names the owner only while it is still this process's
    // parent; otherwise the owner already exited and its PID may be reused.
    if sys::getppid() != owner {
        sys::close(watch);
        return Err(Errno(sys::EPIPE));
    }
    Ok(watch)
}

#[cfg(target_os = "linux")]
fn owner_watch(owner: Pid) -> Result<Fd, Errno> {
    sys::pidfd(owner)
}

#[cfg(target_vendor = "apple")]
fn owner_watch(owner: Pid) -> Result<Fd, Errno> {
    // SAFETY: kqueue has no preconditions.
    let queue = unsafe { libc::kqueue() };
    if queue < 0 {
        return Err(last_error());
    }
    match watch_exit(queue, owner) {
        Ok(true) => Ok(queue),
        result => {
            sys::close(queue);
            Err(result.err().unwrap_or(Errno(sys::EPIPE)))
        }
    }
}

#[cfg(target_vendor = "apple")]
fn last_error() -> Errno {
    Errno(
        std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO),
    )
}

/// Registers `NOTE_EXIT` for `pid`. False when it has already exited.
#[cfg(target_vendor = "apple")]
fn watch_exit(queue: Fd, pid: Pid) -> Result<bool, Errno> {
    let change = libc::kevent {
        ident: usize::try_from(pid).unwrap_or_default(),
        filter: libc::EVFILT_PROC,
        flags: libc::EV_ADD | libc::EV_ONESHOT,
        fflags: libc::NOTE_EXIT,
        data: 0,
        udata: core::ptr::null_mut(),
    };
    // SAFETY: one initialized change and no event output.
    if unsafe {
        libc::kevent(
            queue,
            &raw const change,
            1,
            core::ptr::null_mut(),
            0,
            core::ptr::null(),
        )
    } == 0
    {
        return Ok(true);
    }
    match last_error() {
        Errno(libc::ESRCH) => Ok(false),
        error => Err(error),
    }
}

/// Blocks for one event. Returns the PID it names.
#[cfg(target_vendor = "apple")]
fn next_exit(queue: Fd) -> Result<Pid, Errno> {
    // SAFETY: plain fields; all-zero is a valid output value.
    let mut event: libc::kevent = unsafe { core::mem::zeroed() };
    loop {
        // SAFETY: no changes and one writable event.
        let ready = unsafe {
            libc::kevent(
                queue,
                core::ptr::null(),
                0,
                &raw mut event,
                1,
                core::ptr::null(),
            )
        };
        if ready > 0 {
            return Pid::try_from(event.ident).map_err(|_| Errno(sys::EINVAL));
        }
        let error = last_error();
        if ready < 0 && error != Errno(sys::EINTR) {
            return Err(error);
        }
    }
}

fn supervise(
    directory: Fd,
    record: Fd,
    watch: Fd,
    owner: Pid,
    warden: Pid,
    program: Pid,
    name: &CStr,
) -> ! {
    // Graceful requests to the group reach the program; the warden reports
    // the program's own outcome.
    for signal in [
        sys::SIGTERM,
        sys::SIGINT,
        sys::SIGHUP,
        sys::SIGQUIT,
        sys::SIGPIPE,
    ] {
        sys::set_signal(signal, true);
    }
    // Inherited descriptors, including the reply pipe and the program's
    // standard streams, close here.
    close_except([directory, record, watch], warden);
    let status = await_exit(watch, owner, program);
    let status = drain(watch, warden, program, status);
    let _ = sys::unlinkat(directory, name);
    exit_as(status)
}

/// Waits for the program to exit, returning its status, or for the owner to
/// exit, returning `None`.
#[cfg(target_os = "linux")]
fn await_exit(watch: Fd, _owner: Pid, program: Pid) -> Option<i32> {
    // An unreaped child's PID cannot be reused.
    let child = sys::pidfd(program).ok()?;
    let mut ready = [sys::PollFd::readable(watch), sys::PollFd::readable(child)];
    loop {
        match sys::poll(&mut ready) {
            Ok(_) => {}
            Err(Errno(sys::EINTR)) => continue,
            Err(_) => return None,
        }
        let [owner, child] = &ready;
        if child.ready() {
            return reap(program);
        }
        if owner.ready() {
            return None;
        }
    }
}

#[cfg(target_vendor = "apple")]
fn await_exit(watch: Fd, owner: Pid, program: Pid) -> Option<i32> {
    match watch_exit(watch, program) {
        Ok(true) => {}
        Ok(false) => return reap(program),
        Err(_) => return None,
    }
    loop {
        match next_exit(watch) {
            Ok(pid) if pid == program => return reap(program),
            Ok(pid) if pid == owner => return None,
            Ok(_) => {}
            Err(_) => return None,
        }
    }
}

fn reap(program: Pid) -> Option<i32> {
    loop {
        match sys::wait(program, 0) {
            Ok((reaped, status)) if reaped == program => return Some(status),
            Err(Errno(sys::EINTR)) | Ok(_) => {}
            Err(_) => return None,
        }
    }
}

/// Kills and reaps every remaining descendant. As child subreaper, every
/// orphaned descendant becomes this warden's child, so an empty wait means
/// the whole tree is gone.
#[cfg(target_os = "linux")]
fn drain(_watch: Fd, warden: Pid, program: Pid, mut status: Option<i32>) -> Option<i32> {
    let mut path = [0_u8; 64];
    let Some(children) = children_path(warden, &mut path) else {
        return status;
    };
    loop {
        // The warden left this group, and its live PID keeps the group's ID
        // from naming any other group: this reaches only the tree.
        let _ = sys::kill(-warden, sys::SIGKILL);
        if !kill_children(children) {
            // Without the children listing, the tree still dies as a group.
            return status;
        }
        match sys::wait(-1, 0) {
            Ok((reaped, raw)) if reaped == program => status = status.or(Some(raw)),
            Ok(_) | Err(Errno(sys::EINTR)) => {}
            Err(_) => return status,
        }
    }
}

#[cfg(target_os = "linux")]
fn children_path(warden: Pid, buffer: &mut [u8; 64]) -> Option<&CStr> {
    let mut name = [0; 24];
    let pid = record::name(warden, &mut name)?.to_bytes();
    let parts: [&[u8]; 4] = [b"/proc/self/task/", pid, b"/children", b"\0"];
    let mut length = 0;
    for byte in parts.iter().flat_map(|part| part.iter()) {
        *buffer.get_mut(length)? = *byte;
        length += 1;
    }
    CStr::from_bytes_with_nul(buffer.get(..length)?).ok()
}

/// Sends SIGKILL to each listed child. False when the listing is unavailable.
#[cfg(target_os = "linux")]
fn kill_children(path: &CStr) -> bool {
    let Ok(file) = sys::open(path, sys::O_RDONLY | sys::O_CLOEXEC) else {
        return false;
    };
    let mut buffer = [0_u8; 512];
    let mut current: Option<Pid> = None;
    while let Ok(read) = record::read_full(file, &mut buffer) {
        for byte in buffer.iter().take(read) {
            if byte.is_ascii_digit() {
                current = Some(
                    current
                        .unwrap_or(0)
                        .saturating_mul(10)
                        .saturating_add(Pid::from(byte - b'0')),
                );
            } else if let Some(child) = current.take() {
                let _ = sys::kill(child, sys::SIGKILL);
            }
        }
        if read < buffer.len() {
            break;
        }
    }
    if let Some(child) = current {
        let _ = sys::kill(child, sys::SIGKILL);
    }
    sys::close(file);
    true
}

/// Kills the tree's process group and waits for each member to exit.
#[cfg(target_vendor = "apple")]
fn drain(queue: Fd, warden: Pid, program: Pid, mut status: Option<i32>) -> Option<i32> {
    let mut members = [0 as Pid; 256];
    loop {
        // The warden left this group, and its live PID keeps the group's ID
        // from naming any other group: one signal reaches exactly the tree,
        // never a listed PID that may since have been reused.
        let _ = sys::kill(-warden, sys::SIGKILL);
        reap_ready(program, &mut status);
        let Ok(count) = group_members(warden, &mut members) else {
            return status;
        };
        let mut live = false;
        for member in members.iter().take(count).copied() {
            if member <= 0 {
                continue;
            }
            // A listed PID may since have been reused; only a watch that
            // still names a member counts. No process joins the group now.
            // SAFETY: getpgid only reads the named process's group.
            live |= watch_exit(queue, member).unwrap_or(false)
                && unsafe { libc::getpgid(member) } == warden;
        }
        if !live {
            reap_ready(program, &mut status);
            return status;
        }
        if next_exit(queue).is_err() {
            return status;
        }
    }
}

/// Lists the PIDs of process group `group`.
#[cfg(target_vendor = "apple")]
fn group_members(group: Pid, members: &mut [Pid; 256]) -> Result<usize, Errno> {
    let bytes =
        libc::c_int::try_from(core::mem::size_of_val(members)).map_err(|_| Errno(sys::EINVAL))?;
    let group = u32::try_from(group).map_err(|_| Errno(sys::EINVAL))?;
    // SAFETY: writable PID storage of the stated size; 2 is PROC_PGRP_ONLY.
    let listed = unsafe { libc::proc_listpids(2, group, members.as_mut_ptr().cast(), bytes) };
    usize::try_from(listed)
        .map(|listed| listed / core::mem::size_of::<Pid>())
        .map_err(|_| last_error())
}

#[cfg(target_vendor = "apple")]
fn reap_ready(program: Pid, status: &mut Option<i32>) {
    while let Ok((reaped, raw)) = sys::wait(-1, sys::WNOHANG) {
        if reaped <= 0 {
            return;
        }
        if reaped == program {
            *status = status.or(Some(raw));
        }
    }
}

/// Closes every descriptor but `keep`.
#[cfg(target_os = "linux")]
pub(super) fn close_except(mut keep: [Fd; 3], _warden: Pid) {
    keep.sort_unstable();
    let mut first: u32 = 0;
    for fd in keep {
        let Ok(fd) = u32::try_from(fd) else {
            continue;
        };
        if fd > first {
            sys::close_range(first, fd - 1);
        }
        first = fd.saturating_add(1);
    }
    sys::close_range(first, u32::MAX);
}

#[cfg(target_vendor = "apple")]
fn close_except(keep: [Fd; 3], warden: Pid) {
    // SAFETY: plain fields; all-zero is a valid value.
    let mut listed: [libc::proc_fdinfo; 256] = unsafe { core::mem::zeroed() };
    let Ok(bytes) = libc::c_int::try_from(core::mem::size_of_val(&listed)) else {
        return;
    };
    loop {
        // SAFETY: writable descriptor-info storage of the stated size.
        let filled = unsafe {
            libc::proc_pidinfo(
                warden,
                libc::PROC_PIDLISTFDS,
                0,
                listed.as_mut_ptr().cast(),
                bytes,
            )
        };
        let Ok(filled) = usize::try_from(filled) else {
            return;
        };
        let mut closed = false;
        for info in listed
            .iter()
            .take(filled / core::mem::size_of::<libc::proc_fdinfo>())
        {
            if !keep.contains(&info.proc_fd) {
                sys::close(info.proc_fd);
                closed = true;
            }
        }
        if !closed {
            return;
        }
    }
}

/// Exits as the program did: with its code, or by its signal.
fn exit_as(status: Option<i32>) -> ! {
    if let Some(status) = status {
        // The same status encoding on Linux and macOS.
        let signal = status & 0x7f;
        if signal == 0 {
            sys::exit((status >> 8) & 0xff);
        }
        if signal != 0x7f {
            sys::set_signal(signal, false);
            sys::unblock(signal);
            let _ = sys::kill(sys::getpid(), signal);
            sys::exit(128 + signal);
        }
    }
    // The owner exited: the tree was killed.
    sys::exit(128 + sys::SIGKILL)
}

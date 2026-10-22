//! The direct child of a custodied tree. It is forked by `Command::spawn` and
//! runs here before exec: it records and locks the tree in the owner's
//! custody directory, watches the owner, and forks the real program, which
//! returns from [`enter`] and execs. The warden itself never returns.
//!
//! Everything here runs between `fork` and `_exit` in a copy of a
//! multithreaded host, so it makes only async-signal-safe system calls: no
//! allocation, no locks and no unwinding.

use libc::{c_int, pid_t};
use std::ffi::CStr;
use std::io;

/// Bytes of boot identity a record holds.
pub(super) const BOOT_BYTES: usize = 64;

/// Formats `pid` as the NUL-terminated decimal name of its record.
pub(super) fn record_name(pid: pid_t, buffer: &mut [u8; 24]) -> Option<&CStr> {
    let mut value = u32::try_from(pid).ok()?;
    let mut digits = [0_u8; 10];
    let mut count = 0;
    for digit in &mut digits {
        *digit = b'0' + u8::try_from(value % 10).ok()?;
        value /= 10;
        count += 1;
        if value == 0 {
            break;
        }
    }
    let (name, _) = buffer.split_at_mut(count + 1);
    for (slot, digit) in name.iter_mut().zip(digits.iter().take(count).rev()) {
        *slot = *digit;
    }
    *name.last_mut()? = 0;
    CStr::from_bytes_with_nul(name).ok()
}

/// The identity of the current boot, so a record never outlives its boot.
pub(super) fn boot_identity(buffer: &mut [u8; BOOT_BYTES]) -> io::Result<usize> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: a static NUL-terminated path.
        let file = unsafe {
            libc::open(
                c"/proc/sys/kernel/random/boot_id".as_ptr(),
                libc::O_RDONLY | libc::O_CLOEXEC,
            )
        };
        if file < 0 {
            return Err(io::Error::last_os_error());
        }
        let read = read_full(file, buffer);
        // SAFETY: the descriptor opened above.
        unsafe { libc::close(file) };
        read
    }
    #[cfg(target_vendor = "apple")]
    {
        let mut names = [libc::CTL_KERN, libc::KERN_BOOTTIME];
        // SAFETY: plain integer fields; all-zero is a valid value.
        let mut boot: libc::timeval = unsafe { std::mem::zeroed() };
        let mut size = std::mem::size_of::<libc::timeval>();
        // SAFETY: a two-name MIB and writable storage of the stated size.
        if unsafe {
            libc::sysctl(
                names.as_mut_ptr(),
                2,
                (&raw mut boot).cast(),
                &raw mut size,
                std::ptr::null_mut(),
                0,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        let seconds = boot.tv_sec.to_le_bytes();
        let micros = boot.tv_usec.to_le_bytes();
        let mut written = 0;
        for (slot, byte) in buffer.iter_mut().zip(seconds.iter().chain(micros.iter())) {
            *slot = *byte;
            written += 1;
        }
        Ok(written)
    }
}

#[cfg(target_os = "linux")]
fn read_full(file: c_int, buffer: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while let Some(rest) = buffer.get_mut(filled..).filter(|rest| !rest.is_empty()) {
        // SAFETY: writable storage of the stated length.
        let read = unsafe { libc::read(file, rest.as_mut_ptr().cast(), rest.len()) };
        match usize::try_from(read) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(_) if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return Err(io::Error::last_os_error()),
        }
    }
    Ok(filled)
}

fn write_full(file: c_int, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        // SAFETY: readable storage of the stated length.
        let written = unsafe { libc::write(file, bytes.as_ptr().cast(), bytes.len()) };
        match usize::try_from(written) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => bytes = bytes.get(written..).unwrap_or_default(),
            Err(_) if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return Err(io::Error::last_os_error()),
        }
    }
    Ok(())
}

/// Runs in the forked direct child, which already leads its own process
/// group. Returns only in the program's process, which then execs.
pub(super) fn enter(directory: c_int, owner: pid_t) -> io::Result<()> {
    // An inherited host handler must not run in the warden.
    // SAFETY: resetting one disposition to its default.
    unsafe { libc::signal(libc::SIGCHLD, libc::SIG_DFL) };
    // SAFETY: getpid has no preconditions.
    let warden = unsafe { libc::getpid() };
    let mut storage = [0; 24];
    let name = record_name(warden, &mut storage).ok_or(io::ErrorKind::InvalidInput)?;
    // The record exists before any of the program's code runs.
    // SAFETY: a live directory descriptor and a NUL-terminated name.
    let record = unsafe {
        libc::openat(
            directory,
            name.as_ptr(),
            libc::O_CREAT | libc::O_EXCL | libc::O_RDWR | libc::O_CLOEXEC,
            0o600,
        )
    };
    if record < 0 {
        return Err(io::Error::last_os_error());
    }
    let unlink = || {
        // SAFETY: the same directory and name as the record created above.
        unsafe { libc::unlinkat(directory, name.as_ptr(), 0) };
    };
    let watch = match arm(record, owner) {
        Ok(watch) => watch,
        Err(error) => {
            unlink();
            return Err(error);
        }
    };
    // SAFETY: fork in the single-threaded child; both branches below make
    // only async-signal-safe calls until exec or _exit.
    let program = unsafe { libc::fork() };
    if program == 0 {
        // SAFETY: descriptors this process owns and the program never uses.
        unsafe {
            libc::close(record);
            libc::close(watch);
        }
        return Ok(());
    }
    if program < 0 {
        let error = io::Error::last_os_error();
        unlink();
        return Err(error);
    }
    supervise(directory, record, watch, owner, warden, program, name)
}

/// Locks the record, writes its boot, and returns the owner's exit watch.
fn arm(record: c_int, owner: pid_t) -> io::Result<c_int> {
    // The kernel releases this lock only when the warden's last descriptor
    // of the record closes: when the warden has exited.
    // SAFETY: the record descriptor opened by the caller.
    if unsafe { libc::flock(record, libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let mut boot = [0; BOOT_BYTES];
    let length = boot_identity(&mut boot)?;
    write_full(record, boot.get(..length).unwrap_or_default())?;
    #[cfg(target_os = "linux")]
    // SAFETY: marks this process as the reaper of its orphaned descendants.
    if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let watch = owner_watch(owner)?;
    // The watch names the owner only while it is still this process's
    // parent; otherwise the owner already exited and its PID may be reused.
    // SAFETY: getppid has no preconditions.
    if unsafe { libc::getppid() } != owner {
        // SAFETY: the watch descriptor opened above.
        unsafe { libc::close(watch) };
        return Err(io::ErrorKind::BrokenPipe.into());
    }
    Ok(watch)
}

#[cfg(target_os = "linux")]
fn pidfd(pid: pid_t) -> io::Result<c_int> {
    // SAFETY: pidfd_open takes a PID and zero flags.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    c_int::try_from(fd)
        .ok()
        .filter(|fd| *fd >= 0)
        .ok_or_else(io::Error::last_os_error)
}

#[cfg(target_os = "linux")]
fn owner_watch(owner: pid_t) -> io::Result<c_int> {
    pidfd(owner)
}

#[cfg(target_vendor = "apple")]
fn exit_event(pid: pid_t) -> libc::kevent {
    libc::kevent {
        ident: usize::try_from(pid).unwrap_or_default(),
        filter: libc::EVFILT_PROC,
        flags: libc::EV_ADD | libc::EV_ONESHOT,
        fflags: libc::NOTE_EXIT,
        data: 0,
        udata: std::ptr::null_mut(),
    }
}

/// Registers `NOTE_EXIT` for `pid`. False when it has already exited.
#[cfg(target_vendor = "apple")]
pub(super) fn watch_exit(queue: c_int, pid: pid_t) -> io::Result<bool> {
    let change = exit_event(pid);
    // SAFETY: one initialized change and no event output.
    if unsafe {
        libc::kevent(
            queue,
            &raw const change,
            1,
            std::ptr::null_mut(),
            0,
            std::ptr::null(),
        )
    } == 0
    {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(false)
    } else {
        Err(error)
    }
}

/// Blocks for one event. Returns the PID it names.
#[cfg(target_vendor = "apple")]
pub(super) fn next_exit(queue: c_int) -> io::Result<pid_t> {
    // SAFETY: plain fields; all-zero is a valid output value.
    let mut event: libc::kevent = unsafe { std::mem::zeroed() };
    loop {
        // SAFETY: no changes and one writable event.
        let ready = unsafe {
            libc::kevent(
                queue,
                std::ptr::null(),
                0,
                &raw mut event,
                1,
                std::ptr::null(),
            )
        };
        if ready > 0 {
            return pid_t::try_from(event.ident).map_err(|_| io::ErrorKind::InvalidData.into());
        }
        let error = io::Error::last_os_error();
        if ready < 0 && error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(target_vendor = "apple")]
fn owner_watch(owner: pid_t) -> io::Result<c_int> {
    // SAFETY: kqueue has no preconditions.
    let queue = unsafe { libc::kqueue() };
    if queue < 0 {
        return Err(io::Error::last_os_error());
    }
    match watch_exit(queue, owner) {
        Ok(true) => Ok(queue),
        result => {
            // SAFETY: the queue opened above.
            unsafe { libc::close(queue) };
            Err(result
                .err()
                .unwrap_or_else(|| io::ErrorKind::BrokenPipe.into()))
        }
    }
}

fn supervise(
    directory: c_int,
    record: c_int,
    watch: c_int,
    owner: pid_t,
    warden: pid_t,
    program: pid_t,
    name: &CStr,
) -> ! {
    // Graceful requests to the group reach the program; the warden reports
    // the program's own outcome.
    for signal in [
        libc::SIGTERM,
        libc::SIGINT,
        libc::SIGHUP,
        libc::SIGQUIT,
        libc::SIGPIPE,
    ] {
        // SAFETY: setting one disposition.
        unsafe { libc::signal(signal, libc::SIG_IGN) };
    }
    // Inherited host descriptors, including the spawn's exec-status pipe and
    // the program's standard streams, close here.
    close_except([directory, record, watch], warden);
    let status = await_exit(watch, owner, program);
    let status = drain(watch, warden, program, status);
    // SAFETY: the record this warden created.
    unsafe { libc::unlinkat(directory, name.as_ptr(), 0) };
    exit_as(status)
}

/// Waits for the program to exit, returning its status, or for the owner to
/// exit, returning `None`.
#[cfg(target_os = "linux")]
fn await_exit(watch: c_int, _owner: pid_t, program: pid_t) -> Option<c_int> {
    // An unreaped child's PID cannot be reused.
    let Ok(child) = pidfd(program) else {
        return None;
    };
    let mut ready = [
        libc::pollfd {
            fd: watch,
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: child,
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    loop {
        // SAFETY: two initialized pollfds.
        if unsafe { libc::poll(ready.as_mut_ptr(), 2, -1) } < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return None;
        }
        let [owner, child] = &ready;
        if child.revents != 0 {
            return reap(program);
        }
        if owner.revents != 0 {
            return None;
        }
    }
}

#[cfg(target_vendor = "apple")]
fn await_exit(watch: c_int, owner: pid_t, program: pid_t) -> Option<c_int> {
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

fn reap(program: pid_t) -> Option<c_int> {
    let mut status = 0;
    loop {
        // SAFETY: this process's own child and writable status storage.
        let reaped = unsafe { libc::waitpid(program, &raw mut status, 0) };
        if reaped == program {
            return Some(status);
        }
        if reaped < 0 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            return None;
        }
    }
}

/// Kills and reaps every remaining descendant. As child subreaper, every
/// orphaned descendant becomes this warden's child, so an empty wait means
/// the whole tree is gone.
#[cfg(target_os = "linux")]
fn drain(_watch: c_int, warden: pid_t, program: pid_t, mut status: Option<c_int>) -> Option<c_int> {
    let mut path = [0_u8; 64];
    let Some(children) = children_path(warden, &mut path) else {
        return status;
    };
    loop {
        if !kill_children(children) {
            // Without the children listing, an orphaned owner's tree still
            // dies as a group, this warden included; reopen fences the rest.
            if status.is_none() {
                // SAFETY: this warden's own process group.
                unsafe { libc::kill(-warden, libc::SIGKILL) };
            }
            return status;
        }
        let mut raw = 0;
        // SAFETY: writable status storage.
        let reaped = unsafe { libc::waitpid(-1, &raw mut raw, 0) };
        if reaped == program {
            status = status.or(Some(raw));
        }
        if reaped < 0 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            return status;
        }
    }
}

#[cfg(target_os = "linux")]
fn children_path(warden: pid_t, buffer: &mut [u8; 64]) -> Option<&CStr> {
    let mut name = [0; 24];
    let pid = record_name(warden, &mut name)?.to_bytes();
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
    // SAFETY: a NUL-terminated path.
    let file = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
    if file < 0 {
        return false;
    }
    let mut buffer = [0_u8; 512];
    let mut current: Option<pid_t> = None;
    while let Ok(read) = read_full(file, &mut buffer) {
        for byte in buffer.iter().take(read) {
            if byte.is_ascii_digit() {
                current = Some(
                    current
                        .unwrap_or(0)
                        .saturating_mul(10)
                        .saturating_add(pid_t::from(byte - b'0')),
                );
            } else if let Some(child) = current.take() {
                // SAFETY: a listed child of this process.
                unsafe { libc::kill(child, libc::SIGKILL) };
            }
        }
        if read < buffer.len() {
            break;
        }
    }
    if let Some(child) = current {
        // SAFETY: a listed child of this process.
        unsafe { libc::kill(child, libc::SIGKILL) };
    }
    // SAFETY: the descriptor opened above.
    unsafe { libc::close(file) };
    true
}

/// Kills every other member of this warden's process group and waits for
/// each to exit.
#[cfg(target_vendor = "apple")]
fn drain(queue: c_int, warden: pid_t, program: pid_t, mut status: Option<c_int>) -> Option<c_int> {
    let mut members = [0 as pid_t; 256];
    loop {
        reap_ready(program, &mut status);
        let Ok(count) = group_members(warden, &mut members) else {
            return status;
        };
        let mut live = false;
        for member in members.iter().take(count).copied() {
            if member == warden || member <= 0 {
                continue;
            }
            // SAFETY: a member of this warden's own process group.
            unsafe { libc::kill(member, libc::SIGKILL) };
            live |= watch_exit(queue, member).unwrap_or(false);
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
pub(super) fn group_members(group: pid_t, members: &mut [pid_t; 256]) -> io::Result<usize> {
    let bytes =
        c_int::try_from(std::mem::size_of_val(members)).map_err(|_| io::ErrorKind::InvalidInput)?;
    let group = u32::try_from(group).map_err(|_| io::ErrorKind::InvalidInput)?;
    // SAFETY: writable PID storage of the stated size; 2 is PROC_PGRP_ONLY.
    let listed = unsafe { libc::proc_listpids(2, group, members.as_mut_ptr().cast(), bytes) };
    usize::try_from(listed)
        .map(|listed| listed / std::mem::size_of::<pid_t>())
        .map_err(|_| io::Error::last_os_error())
}

#[cfg(target_vendor = "apple")]
fn reap_ready(program: pid_t, status: &mut Option<c_int>) {
    loop {
        let mut raw = 0;
        // SAFETY: writable status storage.
        let reaped = unsafe { libc::waitpid(-1, &raw mut raw, libc::WNOHANG) };
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
fn close_except(mut keep: [c_int; 3], _warden: pid_t) {
    keep.sort_unstable();
    let mut first: u32 = 0;
    for fd in keep {
        let Ok(fd) = u32::try_from(fd) else {
            continue;
        };
        if fd > first {
            close_range(first, fd - 1);
        }
        first = fd.saturating_add(1);
    }
    close_range(first, u32::MAX);
}

#[cfg(target_os = "linux")]
fn close_range(first: u32, last: u32) {
    // SAFETY: closes only descriptors in this range of this process.
    if unsafe { libc::syscall(libc::SYS_close_range, first, last, 0) } == 0 {
        return;
    }
    // Kernels before close_range: close up to the descriptor limit.
    // SAFETY: plain fields; all-zero is a valid output value.
    let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
    // SAFETY: writable limit storage.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut limit) } != 0 {
        return;
    }
    let end = u32::try_from(limit.rlim_cur).unwrap_or(u32::MAX).min(last);
    for fd in first..=end {
        if let Ok(fd) = c_int::try_from(fd) {
            // SAFETY: closing a descriptor of this process.
            unsafe { libc::close(fd) };
        }
    }
}

#[cfg(target_vendor = "apple")]
fn close_except(keep: [c_int; 3], warden: pid_t) {
    // SAFETY: plain fields; all-zero is a valid value.
    let mut listed: [libc::proc_fdinfo; 256] = unsafe { std::mem::zeroed() };
    let Ok(bytes) = c_int::try_from(std::mem::size_of_val(&listed)) else {
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
            .take(filled / std::mem::size_of::<libc::proc_fdinfo>())
        {
            if !keep.contains(&info.proc_fd) {
                // SAFETY: closing a descriptor of this process.
                unsafe { libc::close(info.proc_fd) };
                closed = true;
            }
        }
        if !closed {
            return;
        }
    }
}

/// Exits as the program did: with its code, or by its signal.
fn exit_as(status: Option<c_int>) -> ! {
    if let Some(status) = status {
        if libc::WIFEXITED(status) {
            // SAFETY: immediate exit without running host destructors.
            unsafe { libc::_exit(libc::WEXITSTATUS(status)) };
        }
        if libc::WIFSIGNALED(status) {
            let signal = libc::WTERMSIG(status);
            // SAFETY: restoring the default action, unblocking it and
            // sending it to this process; all async-signal-safe.
            unsafe {
                libc::signal(signal, libc::SIG_DFL);
                let mut set: libc::sigset_t = std::mem::zeroed();
                libc::sigemptyset(&raw mut set);
                libc::sigaddset(&raw mut set, signal);
                libc::sigprocmask(libc::SIG_UNBLOCK, &raw const set, std::ptr::null_mut());
                libc::kill(libc::getpid(), signal);
                libc::_exit(128 + signal);
            }
        }
    }
    // The owner exited: the tree was killed.
    // SAFETY: immediate exit without running host destructors.
    unsafe { libc::_exit(128 + libc::SIGKILL) }
}

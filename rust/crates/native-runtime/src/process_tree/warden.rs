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

/// The packet-device hosts this tree's program started. They are children of
/// the warden only because `CLONE_PARENT` makes them siblings of the program;
/// they serve every owner in the namespace, so tree retirement must not kill
/// them. The program registers each host's PID over a pipe before it exits.
#[cfg(target_os = "linux")]
const HOST_PIDS: Fd = 3;
#[cfg(target_os = "linux")]
const HOST_MAGIC: [u8; 4] = *b"ACH0";
#[cfg(target_os = "linux")]
const MAXIMUM_HOSTS: usize = 8;

#[cfg(target_os = "linux")]
struct Hosts {
    pids: [Pid; MAXIMUM_HOSTS],
    count: usize,
}

/// A host is a direct child of this warden whose executable is this same
/// runner image. A program cannot make an arbitrary descendant satisfy both:
/// its descendants have another parent until the program dies, and only the
/// runner image is the host. This is the trusted path that grants the
/// retirement exemption.
#[cfg(target_os = "linux")]
fn host_is_owned(pid: Pid) -> bool {
    if pid <= 0 {
        return false;
    }
    let mut name = [0; 24];
    let Some(name) = record::name(pid, &mut name).map(CStr::to_bytes) else {
        return false;
    };
    let mut path = [0; 64];
    let Some(status) = host_path(name, b"/status\0", &mut path) else {
        return false;
    };
    let Ok(file) = sys::open(status, sys::O_RDONLY | sys::O_CLOEXEC) else {
        return false;
    };
    let mut buffer = [0_u8; 1024];
    let mut parent = None;
    let mut read = 0;
    while read < buffer.len() {
        let Ok(count) = sys::read(file, &mut buffer[read..]) else {
            break;
        };
        if count == 0 {
            break;
        }
        read += count;
    }
    sys::close(file);
    let bytes = buffer.get(..read).unwrap_or_default();
    let mut index = 0;
    while index < bytes.len() {
        if bytes.get(index..index + 5) == Some(b"PPid:") {
            let mut value: Pid = 0;
            let mut digits = 0;
            for byte in bytes.get(index + 5..).unwrap_or_default() {
                if byte.is_ascii_digit() {
                    value = value
                        .saturating_mul(10)
                        .saturating_add(Pid::from(byte - b'0'));
                    digits += 1;
                } else if digits > 0 {
                    break;
                }
            }
            parent = Some(value);
            break;
        }
        index += 1;
    }
    if parent != Some(sys::getpid()) {
        return false;
    }
    // The packet-device host leads its own process group. A nested warden
    // runs the same image but belongs to the runner's group, so it is not a
    // host and must still be retired with the tree.
    if sys::getpgid(pid) != Ok(pid) {
        return false;
    }
    let Some(exe) = host_path(name, b"/exe\0", &mut path) else {
        return false;
    };
    let Ok(host_exe) = sys::open(exe, sys::O_RDONLY | sys::O_CLOEXEC) else {
        return false;
    };
    let Ok(self_exe) = sys::open(c"/proc/self/exe", sys::O_RDONLY | sys::O_CLOEXEC) else {
        sys::close(host_exe);
        return false;
    };
    let same = same_executable(host_exe, self_exe);
    sys::close(host_exe);
    sys::close(self_exe);
    same
}

/// The runner image is a sealed memory file created independently by each
/// launch, so its inode is not shared. Its bytes are: compare the executable
/// contents the kernel exposes for both processes.
#[cfg(target_os = "linux")]
fn same_executable(left: Fd, right: Fd) -> bool {
    let mut left_buffer = [0_u8; 4096];
    let mut right_buffer = [0_u8; 4096];
    loop {
        let left_read = match sys::read(left, &mut left_buffer) {
            Ok(read) => read,
            Err(Errno(sys::EINTR)) => continue,
            Err(_) => return false,
        };
        let mut right_read = 0;
        while right_read < left_read {
            match sys::read(right, &mut right_buffer[right_read..left_read]) {
                Ok(0) => return false,
                Ok(read) => right_read += read,
                Err(Errno(sys::EINTR)) => {}
                Err(_) => return false,
            }
        }
        if left_buffer.get(..left_read) != right_buffer.get(..left_read) {
            return false;
        }
        if left_read == 0 {
            let mut extra = [0_u8; 1];
            return matches!(sys::read(right, &mut extra), Ok(0));
        }
    }
}

#[cfg(target_os = "linux")]
fn host_path<'a>(pid: &[u8], suffix: &[u8], buffer: &'a mut [u8; 64]) -> Option<&'a CStr> {
    let parts: [&[u8]; 4] = [b"/proc/", pid, suffix, b""];
    let mut length = 0;
    for byte in parts.iter().flat_map(|part| part.iter()) {
        *buffer.get_mut(length)? = *byte;
        length += 1;
    }
    CStr::from_bytes_with_nul(buffer.get(..length)?).ok()
}

#[cfg(target_os = "linux")]
impl Hosts {
    const EMPTY: Self = Self {
        pids: [0; MAXIMUM_HOSTS],
        count: 0,
    };

    fn contains(&self, pid: Pid) -> bool {
        self.pids
            .get(..self.count)
            .is_some_and(|pids| pids.contains(&pid))
    }

    fn add(&mut self, pid: Pid) {
        if pid > 0
            && host_is_owned(pid)
            && !self.contains(pid)
            && let Some(slot) = self.pids.get_mut(self.count)
        {
            *slot = pid;
            self.count += 1;
        }
    }

    fn remove(&mut self, pid: Pid) {
        let Some(index) = self
            .pids
            .get(..self.count)
            .and_then(|pids| pids.iter().position(|&candidate| candidate == pid))
        else {
            return;
        };
        let last = self.count.saturating_sub(1);
        let tail = self.pids.get(last).copied();
        if let (Some(slot), Some(tail)) = (self.pids.get_mut(index), tail) {
            *slot = tail;
        }
        self.count = last;
    }
}

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
    let record = if directory < 0 {
        -1
    } else {
        sys::openat(
            directory,
            name,
            sys::O_CREAT | sys::O_EXCL | sys::O_RDWR | sys::O_CLOEXEC,
            0o600,
        )?
    };
    let unlink = || {
        if directory >= 0 {
            let _ = sys::unlinkat(directory, name);
        }
    };
    let watch = match arm(record, owner, warden) {
        Ok(watch) => watch,
        Err(error) => {
            unlink();
            return Err(error);
        }
    };
    #[cfg(target_os = "linux")]
    let control = match sys::tree_control() {
        Ok(control) => control,
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
    // The program registers packet-device hosts it starts, so retirement
    // leaves them alive and reaps them when they exit.
    #[cfg(target_os = "linux")]
    let [host_pids, host_pids_out] = match sys::socketpair() {
        Ok(ends) => ends,
        Err(error) => {
            sys::close(admission);
            sys::close(admit);
            unlink();
            return Err(error);
        }
    };
    #[cfg(target_os = "linux")]
    if let Err(error) = sys::write(host_pids, &HOST_MAGIC) {
        sys::close(host_pids);
        sys::close(host_pids_out);
        sys::close(admission);
        sys::close(admit);
        unlink();
        return Err(error);
    }
    let program = match sys::fork() {
        Ok(0) => {
            // Descriptors the program never uses.
            sys::close(record);
            sys::close(watch);
            sys::close(admit);
            #[cfg(target_os = "linux")]
            sys::close(control);
            #[cfg(target_os = "linux")]
            {
                sys::close(host_pids);
                sys::dup_onto(host_pids_out, HOST_PIDS, false)?;
                sys::close(host_pids_out);
            }
            admitted_by_warden(admission)?;
            return Ok(());
        }
        Ok(program) => program,
        Err(error) => {
            sys::close(admission);
            sys::close(admit);
            #[cfg(target_os = "linux")]
            {
                sys::close(host_pids);
                sys::close(host_pids_out);
            }
            unlink();
            return Err(error);
        }
    };
    sys::close(admission);
    #[cfg(target_os = "linux")]
    sys::close(host_pids_out);
    // A warden forked from a multithreaded owner holds every descriptor the
    // owner had, including other trees' pipe ends. Release them before the
    // group move, so no other tree's peer outlives that tree's program
    // because this one is still starting.
    #[cfg(target_vendor = "apple")]
    close_except([directory, record, watch, admit], warden);
    #[cfg(target_os = "linux")]
    let left = leave_group(home);
    #[cfg(target_vendor = "apple")]
    let left = leave_group();
    if left.is_ok() {
        // A failed write leaves the program unadmitted, which fails the spawn.
        let _ = write_full(admit, &[1]);
    }
    sys::close(admit);
    supervise(
        directory,
        record,
        watch,
        owner,
        warden,
        program,
        name,
        #[cfg(target_os = "linux")]
        control,
        #[cfg(target_os = "linux")]
        host_pids,
    )
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
    if record >= 0 {
        sys::try_lock(record)?;
        let mut boot = [0; BOOT_BYTES];
        record::boot_identity(&mut boot)?;
        let mut start = [0; START_BYTES];
        let length = record::start_identity(warden, &mut start)?;
        // The boot, zero-padded to its fixed width, then the warden's start.
        write_full(record, &boot)?;
        write_full(record, start.get(..length).unwrap_or_default())?;
    }
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
    #[cfg(target_os = "linux")] control: Fd,
    #[cfg(target_os = "linux")] host_pids: Fd,
) -> ! {
    // Graceful requests to the group reach the program; the warden reports
    // the program's own outcome.
    #[cfg(target_vendor = "apple")]
    sys::set_signal(sys::SIGTERM, true);
    for signal in [sys::SIGINT, sys::SIGHUP, sys::SIGQUIT, sys::SIGPIPE] {
        sys::set_signal(signal, true);
    }
    // Inherited descriptors, including the reply pipe and the program's
    // standard streams, close here.
    #[cfg(target_os = "linux")]
    close_except([directory, record, watch, control, host_pids], warden);
    #[cfg(target_vendor = "apple")]
    close_except([directory, record, watch], warden);
    #[cfg(target_os = "linux")]
    let mut hosts = Hosts::EMPTY;
    let status = await_exit(
        watch,
        owner,
        program,
        #[cfg(target_os = "linux")]
        control,
        #[cfg(target_os = "linux")]
        warden,
        #[cfg(target_os = "linux")]
        host_pids,
        #[cfg(target_os = "linux")]
        &mut hosts,
    );
    let status = drain(
        watch,
        warden,
        program,
        status,
        #[cfg(target_os = "linux")]
        &hosts,
    );
    if directory >= 0 {
        let _ = sys::unlinkat(directory, name);
    }
    exit_as(status)
}

/// Waits for the program to exit, returning its status, or for the owner to
/// exit, returning `None`.
#[cfg(target_os = "linux")]
fn await_exit(
    watch: Fd,
    _owner: Pid,
    program: Pid,
    control: Fd,
    warden: Pid,
    host_pids: Fd,
    hosts: &mut Hosts,
) -> Option<i32> {
    // An unreaped child's PID cannot be reused.
    let child = sys::pidfd(program).ok()?;
    let mut ready = [
        sys::PollFd::readable(watch),
        sys::PollFd::readable(child),
        sys::PollFd::readable(control),
        sys::PollFd::readable(host_pids),
    ];
    loop {
        match sys::poll(&mut ready) {
            Ok(_) => {}
            Err(Errno(sys::EINTR)) => {
                if let Some(status) = reap_spare(program, hosts) {
                    return Some(status);
                }
                continue;
            }
            Err(_) => return None,
        }
        // Read host registrations before the program's exit: the program
        // writes them before it exits, and retirement must not kill a host
        // whose registration is already in the pipe.
        if ready[3].ready() {
            let mut buffer = [0_u8; 4 * MAXIMUM_HOSTS];
            let closed = match sys::read(host_pids, &mut buffer) {
                Ok(0) => true,
                Ok(read) => {
                    for chunk in buffer.get(..read).unwrap_or_default().as_chunks::<4>().0 {
                        hosts.add(Pid::from_ne_bytes(*chunk));
                    }
                    false
                }
                Err(Errno(sys::EINTR)) => continue,
                Err(_) => true,
            };
            if closed {
                // The program closed the registration pipe. Stop polling a
                // hung-up descriptor on every wake.
                ready[3].disable();
            }
            // The intermediate clone exits at once; reap it here rather than
            // leaving one zombie per host launch until the tree retires.
            if let Some(status) = reap_spare(program, hosts) {
                return Some(status);
            }
        }
        if ready[1].ready() {
            return reap(program);
        }
        if ready[0].ready() {
            return None;
        }
        if ready[2].ready() {
            let mut signal = [0; 128];
            match sys::read(control, &mut signal) {
                Err(Errno(sys::EINTR)) => continue,
                Ok(128) => {}
                _ => return None,
            }
            if u32::from_ne_bytes([signal[0], signal[1], signal[2], signal[3]])
                != sys::SIGTERM as u32
            {
                return None;
            }
            // Both unreaped PIDs reserve groups belonging to this tree. A root
            // which became its own group/session leader took its children with
            // it. Never broadcast to an arbitrary group the root merely joined.
            let group = sys::getpgid(program).ok();
            let _ = sys::kill(-warden, sys::SIGTERM);
            if group == Some(program) {
                let _ = sys::kill(-program, sys::SIGTERM);
            } else if group != Some(warden) {
                let _ = sys::kill(program, sys::SIGTERM);
            }
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

/// Reaps every exited child that is not the program, returning the program's
/// status if it was among them. The intermediate host clone exits at once;
/// leaving it unreaped would keep one zombie per host launch.
#[cfg(target_os = "linux")]
fn reap_spare(program: Pid, hosts: &mut Hosts) -> Option<i32> {
    loop {
        match sys::wait(-1, 1) {
            Ok((reaped, raw)) if reaped == program => return Some(raw),
            Ok((reaped, _)) if reaped > 0 => hosts.remove(reaped),
            Ok(_) => return None,
            Err(Errno(sys::EINTR)) => continue,
            Err(_) => return None,
        }
    }
}

/// Kills and reaps every remaining descendant. As child subreaper, every
/// orphaned descendant becomes this warden's child, so an empty wait means
/// the whole tree is gone.
#[cfg(target_os = "linux")]
fn drain(
    _watch: Fd,
    warden: Pid,
    program: Pid,
    mut status: Option<i32>,
    hosts: &Hosts,
) -> Option<i32> {
    let mut path = [0_u8; 64];
    let Some(children) = children_path(warden, &mut path) else {
        return status;
    };
    loop {
        // Reap completed children before doing any termination work. An empty
        // wait proves there is no tree left: no group kill or proc scan is needed.
        match sys::wait(-1, 1) {
            Ok((reaped, raw)) if reaped > 0 => {
                if reaped == program {
                    status = status.or(Some(raw));
                }
                continue;
            }
            Ok(_) => {}
            Err(Errno(sys::EINTR)) => continue,
            Err(_) => return status,
        }
        // The warden left this group, and its live PID keeps the group's ID
        // from naming any other group: this reaches only the tree.
        let _ = sys::kill(-warden, sys::SIGKILL);
        if !kill_children(children, hosts) {
            // Without the children listing, the tree still dies as a group.
            return status;
        }
        // A registered packet-device host is not part of the tree. Once only
        // hosts remain, the tree is gone and the warden must not wait for a
        // host that other owners still use.
        if only_hosts(children, hosts) {
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

/// Sends SIGKILL to each listed child that is not a registered host. False
/// when the listing is unavailable.
#[cfg(target_os = "linux")]
fn kill_children(path: &CStr, hosts: &Hosts) -> bool {
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
            } else if let Some(child) = current.take()
                && !hosts.contains(child)
                && !host_is_owned(child)
            {
                let _ = sys::kill(child, sys::SIGKILL);
            }
        }
        if read < buffer.len() {
            break;
        }
    }
    if let Some(child) = current
        && !hosts.contains(child)
        && !host_is_owned(child)
    {
        let _ = sys::kill(child, sys::SIGKILL);
    }
    sys::close(file);
    true
}

/// Whether every listed child is a registered packet-device host.
#[cfg(target_os = "linux")]
fn only_hosts(path: &CStr, hosts: &Hosts) -> bool {
    let Ok(file) = sys::open(path, sys::O_RDONLY | sys::O_CLOEXEC) else {
        return false;
    };
    let mut buffer = [0_u8; 512];
    let mut current: Option<Pid> = None;
    let mut only = true;
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
                only &= hosts.contains(child) || host_is_owned(child);
            }
        }
        if read < buffer.len() {
            break;
        }
    }
    if let Some(child) = current {
        only &= hosts.contains(child) || host_is_owned(child);
    }
    sys::close(file);
    only
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
pub(super) fn close_except<const N: usize>(mut keep: [Fd; N], _warden: Pid) {
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
fn close_except<const N: usize>(keep: [Fd; N], warden: Pid) {
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

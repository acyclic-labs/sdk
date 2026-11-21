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

/// The processes this tree's program names to the warden, over a socket that
/// `ACYCLIC_PROCESS_CUSTODY_HOLDERS` names to the program and its descendants
/// (`ProcessCustody::admit`). Each naming is a role and a PID (`record::HOST`)
/// with one descriptor, to which the warden replies with 0 or an error number.
/// A packet-device host is a child of the warden only because `CLONE_PARENT`
/// makes it a sibling of the program; it serves every owner in the namespace,
/// so retirement never kills it. A custody holder acts for this tree: the
/// drain spares and awaits it, and only an explicit cancellation kills it. An
/// owner-exit action is spared and awaited even then. On Linux the runner
/// places the program's end at this descriptor; on macOS the owner chooses it.
#[cfg(target_os = "linux")]
const HOST_PIDS: Fd = 3;
/// What a naming made of its process (`record::HOST`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Host,
    Holder,
    Action,
}

/// One process the program named to the warden.
#[derive(Clone, Copy)]
struct Entry {
    pid: Pid,
    role: Role,
}

/// Every process the program named, in private memory that grows with them:
/// no count of hosts or holders is bounded.
struct Named {
    entries: *mut Entry,
    capacity: usize,
    count: usize,
    /// The owner cancelled the tree: holders die with it.
    cancelled: bool,
}

/// Whether a channel stays open to more namings.
#[derive(PartialEq, Eq)]
enum Channel {
    Open,
    Closed,
}

/// A packet-device host is a direct child in its own group running this same
/// trusted image. It may outlive the warden, named or not.
#[cfg(target_os = "linux")]
fn owned_host(pid: Pid) -> bool {
    if pid <= 0 {
        return false;
    }
    let mut name = [0; 24];
    let Some(name) = record::name(pid, &mut name) else {
        return false;
    };
    let name = name.to_bytes();
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

impl Named {
    const EMPTY: Self = Self {
        entries: core::ptr::null_mut(),
        capacity: 0,
        count: 0,
        cancelled: false,
    };

    fn entries(&self) -> &[Entry] {
        if self.entries.is_null() {
            return &[];
        }
        // SAFETY: the first `count` entries of this mapping are initialized.
        unsafe { core::slice::from_raw_parts(self.entries, self.count) }
    }

    /// The role `pid` was named with.
    fn role(&self, pid: Pid) -> Option<Role> {
        self.entries()
            .iter()
            .find(|entry| entry.pid == pid)
            .map(|entry| entry.role)
    }

    /// Whether the drain spares `child`: a host or an action always, a holder
    /// unless the owner cancelled the tree.
    #[cfg(target_os = "linux")]
    fn spares(&self, child: Pid) -> bool {
        match self.role(child) {
            Some(Role::Holder) => !self.cancelled,
            Some(Role::Host | Role::Action) => true,
            None => owned_host(child),
        }
    }

    /// Whether `child` may outlive the tree: a host.
    #[cfg(target_os = "linux")]
    fn outlives(&self, child: Pid) -> bool {
        match self.role(child) {
            Some(role) => role == Role::Host,
            None => owned_host(child),
        }
    }

    /// Records one naming. A holder is still the program's descendant, so
    /// only a host is checked here; a host not yet the warden's child is
    /// recognized by its image at the drain.
    fn name(
        &mut self,
        naming: [u8; 8],
        #[cfg(target_vendor = "apple")] queue: Fd,
    ) -> Result<(), Errno> {
        let [r0, r1, r2, r3, p0, p1, p2, p3] = naming;
        let pid = Pid::from_ne_bytes([p0, p1, p2, p3]);
        let role = match i32::from_ne_bytes([r0, r1, r2, r3]) {
            record::HOST => {
                return if host(pid)? {
                    self.insert(pid, Role::Host)
                } else {
                    Ok(())
                };
            }
            record::HOLDER => Role::Holder,
            record::ACTION => Role::Action,
            _ => return Err(Errno(sys::EINVAL)),
        };
        // The drain kills the tree's group whole, so a holder inside it
        // could never be spared.
        #[cfg(target_vendor = "apple")]
        {
            // SAFETY: getpgid only reads the named process's group.
            let group = unsafe { libc::getpgid(pid) };
            if group == sys::getpid() {
                return Err(Errno(sys::EINVAL));
            }
        }
        // Its exit removes it; one already gone needs no waiting.
        #[cfg(target_vendor = "apple")]
        if !watch_exit(queue, pid)? {
            return Ok(());
        }
        self.insert(pid, role)
    }

    fn insert(&mut self, pid: Pid, role: Role) -> Result<(), Errno> {
        if pid <= 0 {
            return Err(Errno(sys::EINVAL));
        }
        if self.role(pid).is_some() {
            return Ok(());
        }
        if self.count == self.capacity {
            let size = core::mem::size_of::<Entry>();
            let capacity = self.capacity.saturating_mul(2).max(4096 / size);
            let bytes = capacity.checked_mul(size).ok_or(Errno(sys::EINVAL))?;
            let grown = sys::map(bytes)?.cast::<Entry>();
            if !self.entries.is_null() {
                // SAFETY: both mappings hold at least `count` entries and are
                // distinct; the old one is released only after the copy.
                unsafe {
                    core::ptr::copy_nonoverlapping(self.entries, grown, self.count);
                    sys::unmap(self.entries.cast(), self.capacity * size);
                }
            }
            self.entries = grown;
            self.capacity = capacity;
        }
        // SAFETY: `count` is below the mapping's capacity.
        unsafe { self.entries.add(self.count).write(Entry { pid, role }) };
        self.count += 1;
        Ok(())
    }

    fn remove(&mut self, pid: Pid) {
        let Some(index) = self.entries().iter().position(|entry| entry.pid == pid) else {
            return;
        };
        self.count -= 1;
        // SAFETY: both indices are below the old count, inside the mapping.
        unsafe { *self.entries.add(index) = *self.entries.add(self.count) };
    }

    /// Kills every named holder still running.
    #[cfg(target_vendor = "apple")]
    fn kill_holders(&self) {
        for entry in self.entries() {
            if entry.role == Role::Holder {
                let _ = sys::kill(entry.pid, sys::SIGKILL);
            }
        }
    }
}

/// Whether a positive frame names a packet-device host to record. A host
/// not yet recognized is still spared by its image at the drain.
#[cfg(target_os = "linux")]
fn host(frame: Pid) -> Result<bool, Errno> {
    Ok(owned_host(frame))
}

/// macOS has no packet-device hosts.
#[cfg(target_vendor = "apple")]
fn host(_frame: Pid) -> Result<bool, Errno> {
    Err(Errno(sys::EINVAL))
}

/// Serves every pending naming on `channel`, replying to each namer.
fn serve(channel: Fd, named: &mut Named, #[cfg(target_vendor = "apple")] queue: Fd) -> Channel {
    loop {
        let (naming, reply) = match sys::receive_frame(channel) {
            Ok(Some(naming)) => naming,
            Ok(None) => return Channel::Closed,
            Err(Errno(sys::EAGAIN)) => return Channel::Open,
            // A malformed naming has no namer to answer.
            Err(Errno(sys::EINTR | sys::EINVAL)) => continue,
            Err(_) => return Channel::Closed,
        };
        let code = match named.name(
            naming,
            #[cfg(target_vendor = "apple")]
            queue,
        ) {
            Ok(()) => 0,
            Err(Errno(error)) => error,
        };
        let _ = write_full(reply, &code.to_ne_bytes());
        sys::close(reply);
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
    #[cfg(target_vendor = "apple")] channel: Fd,
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
    // The program names the hosts and holders it starts (`HOST_PIDS`).
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
    if let Err(error) = sys::nonblocking(host_pids) {
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
    // A cancellation can come as soon as the spawn returns; it must reach
    // this queue, never kill the warden.
    #[cfg(target_vendor = "apple")]
    sys::set_signal(sys::SIGUSR1, true);
    #[cfg(target_vendor = "apple")]
    if let Err(error) = watch_channel(watch, channel) {
        sys::close(admit);
        unlink();
        return Err(error);
    }
    #[cfg(target_vendor = "apple")]
    close_except([directory, record, watch, admit, channel], warden);
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
        #[cfg(target_vendor = "apple")]
        channel,
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
    // Shared until the warden retires the tree: the program closes its copy
    // before exec, and each process that takes the tree's custody shares it
    // on a description of its own.
    if record >= 0 {
        sys::try_share(record)?;
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

/// A committed cancellation byte wins even after the owner's exit.
pub(super) fn release_decision(release: Fd, owner_exited: bool) -> Result<bool, Errno> {
    loop {
        match sys::read(release, &mut [0]) {
            Ok(count) => return Ok(count == 0),
            Err(Errno(sys::EINTR)) => {}
            Err(Errno(sys::EAGAIN)) if owner_exited => return Ok(true),
            Err(error) => return Err(error),
        }
    }
}

/// EOF proves that every admitted producer has closed its activity endpoint.
pub(super) fn await_activity(activity: Fd) -> Result<(), Errno> {
    loop {
        match sys::read(activity, &mut [0; 256]) {
            Ok(0) => return Ok(()),
            Ok(_) | Err(Errno(sys::EINTR)) => {}
            Err(error) => return Err(error),
        }
    }
}

/// Arms the action warden, then returns only when it should exec.
#[cfg(target_os = "macos")]
pub(super) fn await_release(
    owner: Pid,
    release: Fd,
    activity: Fd,
    admission: Fd,
) -> Result<(), Errno> {
    if sys::getppid() != owner {
        return Err(Errno(libc::ESRCH));
    }
    // SAFETY: release is a live pipe read end owned by the action.
    if unsafe { libc::fcntl(release, libc::F_SETFL, libc::O_NONBLOCK) } < 0 {
        return Err(last_error());
    }
    let queue = owner_watch(owner)?;
    if sys::getppid() != owner {
        return Err(Errno(libc::ESRCH));
    }
    apply(
        queue,
        &[libc::kevent {
            ident: usize::try_from(release).map_err(|_| Errno(sys::EINVAL))?,
            filter: libc::EVFILT_READ,
            flags: libc::EV_ADD,
            fflags: 0,
            data: 0,
            udata: core::ptr::null_mut(),
        }],
    )?;
    close_except(
        [0, 1, 2, queue, release, activity, admission],
        sys::getpid(),
    );
    let result = (|| {
        write_full(admission, &[1])?;
        sys::close(admission);
        let event = next_event(queue)?;
        sys::close(queue);
        let execute = release_decision(release, matches!(event, Event::Exit(_)))?;
        sys::close(release);
        await_activity(activity)?;
        sys::close(activity);
        Ok(execute)
    })();
    match result {
        Ok(true) => Ok(()),
        Ok(false) => sys::exit(0),
        Err(Errno(_)) => sys::exit(127),
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
    #[cfg(target_vendor = "apple")] channel: Fd,
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
    close_except([directory, record, watch, channel], warden);
    let mut hosts = Named::EMPTY;
    let status = await_exit(
        watch,
        owner,
        program,
        #[cfg(target_vendor = "apple")]
        channel,
        #[cfg(target_vendor = "apple")]
        &mut hosts,
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
        control,
        #[cfg(target_os = "linux")]
        host_pids,
        #[cfg(target_vendor = "apple")]
        channel,
        &mut hosts,
    );
    if directory >= 0 {
        // A process that took the tree's custody (`ProcessCustody::hold`)
        // can still act for it however it set its process group, as a
        // mount custodian does: the tree retires only once each has exited.
        sys::close(record);
        if let Ok(record) = sys::openat(directory, name, sys::O_RDWR | sys::O_CLOEXEC, 0) {
            let _ = sys::lock(record);
        }
        let _ = sys::unlinkat(directory, name);
    }
    exit_as(status)
}

/// Waits for the program to exit, returning its status, or for the owner to
/// exit or cancel the tree, returning `None`.
#[cfg(target_os = "linux")]
fn await_exit(
    watch: Fd,
    _owner: Pid,
    program: Pid,
    control: Fd,
    warden: Pid,
    host_pids: Fd,
    hosts: &mut Named,
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
            Err(Errno(sys::EINTR)) => continue,
            Err(_) => return None,
        }
        // Namings first: each namer waits for its reply, so the program
        // cannot exit with one unserved.
        if ready[3].ready() && serve(host_pids, hosts) == Channel::Closed {
            // Stop polling a hung-up descriptor on every wake.
            ready[3].disable();
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
            let signal = u32::from_ne_bytes([signal[0], signal[1], signal[2], signal[3]]);
            if signal == sys::SIGCHLD as u32 {
                // A host's intermediate clone, or an orphan, exited.
                if let Some(status) = reap_spare(program, hosts) {
                    return Some(status);
                }
                continue;
            }
            if signal != sys::SIGTERM as u32 {
                // The owner cancelled the tree: its holders die with it.
                hosts.cancelled = true;
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
fn await_exit(watch: Fd, owner: Pid, program: Pid, channel: Fd, named: &mut Named) -> Option<i32> {
    match watch_exit(watch, program) {
        Ok(true) => {}
        Ok(false) => return reap(program),
        Err(_) => return None,
    }
    loop {
        match next_event(watch) {
            Ok(Event::Exit(pid)) if pid == program => return reap(program),
            Ok(Event::Exit(pid)) if pid == owner => return None,
            Ok(Event::Exit(pid)) => named.remove(pid),
            Ok(Event::Readable(end)) => receive_namings(watch, channel, named, end),
            Ok(Event::Cancelled) => {
                named.cancelled = true;
                return None;
            }
            Err(_) => return None,
        }
    }
}

/// Serves the channel's namings, and stops watching it once it has ended.
#[cfg(target_vendor = "apple")]
fn receive_namings(queue: Fd, channel: Fd, named: &mut Named, end: bool) {
    if serve(channel, named, queue) == Channel::Closed || end {
        let _ = unwatch_channel(queue, channel);
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
fn reap_spare(program: Pid, hosts: &mut Named) -> Option<i32> {
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

/// Kills and reaps every remaining descendant but the tree's custody
/// holders, whose exits it awaits until the owner cancels the tree. As child
/// subreaper, every orphaned descendant becomes this warden's child, so an
/// empty wait means the whole tree is gone.
#[cfg(target_os = "linux")]
fn drain(
    _watch: Fd,
    warden: Pid,
    program: Pid,
    mut status: Option<i32>,
    control: Fd,
    channel: Fd,
    hosts: &mut Named,
) -> Option<i32> {
    let mut path = [0_u8; 64];
    let Some(children) = children_path(warden, &mut path) else {
        return status;
    };
    // Holders the drain spares can still name more.
    let mut open = Channel::Open;
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
        // Shared packet-device hosts serve other trees. Holders act for this
        // tree and must finish before it reports its status.
        if only_hosts(children, hosts) {
            return status;
        }
        // A child's exit, a naming, or the owner's cancellation, which kills
        // holders.
        let mut ready = [
            sys::PollFd::readable(control),
            sys::PollFd::readable(channel),
        ];
        if open == Channel::Closed {
            ready[1].disable();
        }
        match sys::poll(&mut ready) {
            Ok(_) | Err(Errno(sys::EINTR)) => {}
            Err(_) => return status,
        }
        if ready[1].ready() {
            open = serve(channel, hosts);
        }
        if !ready[0].ready() {
            continue;
        }
        let mut signal = [0; 128];
        match sys::read(control, &mut signal) {
            Ok(128) => {
                hosts.cancelled |= u32::from_ne_bytes([signal[0], signal[1], signal[2], signal[3]])
                    == sys::SIGUSR1 as u32;
            }
            Err(Errno(sys::EINTR)) => {}
            _ => return status,
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

/// Sends SIGKILL to each listed child the drain does not spare. False when
/// the listing is unavailable.
#[cfg(target_os = "linux")]
fn kill_children(path: &CStr, hosts: &Named) -> bool {
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
                && !hosts.spares(child)
            {
                let _ = sys::kill(child, sys::SIGKILL);
            }
        }
        if read < buffer.len() {
            break;
        }
    }
    if let Some(child) = current
        && !hosts.spares(child)
    {
        let _ = sys::kill(child, sys::SIGKILL);
    }
    sys::close(file);
    true
}

/// Whether every listed child is a packet-device host.
#[cfg(target_os = "linux")]
fn only_hosts(path: &CStr, hosts: &Named) -> bool {
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
                only &= hosts.outlives(child);
            }
        }
        if read < buffer.len() {
            break;
        }
    }
    if let Some(child) = current {
        only &= hosts.outlives(child);
    }
    sys::close(file);
    only
}

/// What woke the warden's queue.
#[cfg(target_vendor = "apple")]
enum Event {
    /// The process with this PID exited.
    Exit(Pid),
    /// The channel has namings, or has ended when true.
    Readable(bool),
    /// The owner cancelled the tree.
    Cancelled,
}

/// Blocks for one event on the warden's queue.
#[cfg(target_vendor = "apple")]
fn next_event(queue: Fd) -> Result<Event, Errno> {
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
            return Ok(match event.filter {
                libc::EVFILT_READ => Event::Readable(event.flags & libc::EV_EOF != 0),
                libc::EVFILT_SIGNAL => Event::Cancelled,
                _ => Event::Exit(Pid::try_from(event.ident).map_err(|_| Errno(sys::EINVAL))?),
            });
        }
        let error = last_error();
        if ready < 0 && error != Errno(sys::EINTR) {
            return Err(error);
        }
    }
}

/// Watches `channel` for namings and the owner's cancelling SIGUSR1, which
/// the queue records though the warden ignores it.
#[cfg(target_vendor = "apple")]
fn watch_channel(queue: Fd, channel: Fd) -> Result<(), Errno> {
    let change = |ident: usize, filter| libc::kevent {
        ident,
        filter,
        flags: libc::EV_ADD,
        fflags: 0,
        data: 0,
        udata: core::ptr::null_mut(),
    };
    let changes = [
        change(
            usize::try_from(channel).map_err(|_| Errno(sys::EINVAL))?,
            libc::EVFILT_READ,
        ),
        change(
            usize::try_from(sys::SIGUSR1).map_err(|_| Errno(sys::EINVAL))?,
            libc::EVFILT_SIGNAL,
        ),
    ];
    apply(queue, &changes)
}

/// Stops watching an ended channel, so its end of file does not wake the queue.
#[cfg(target_vendor = "apple")]
fn unwatch_channel(queue: Fd, channel: Fd) -> Result<(), Errno> {
    let change = libc::kevent {
        ident: usize::try_from(channel).map_err(|_| Errno(sys::EINVAL))?,
        filter: libc::EVFILT_READ,
        flags: libc::EV_DELETE,
        fflags: 0,
        data: 0,
        udata: core::ptr::null_mut(),
    };
    apply(queue, &[change])
}

#[cfg(target_vendor = "apple")]
fn apply(queue: Fd, changes: &[libc::kevent]) -> Result<(), Errno> {
    let count = libc::c_int::try_from(changes.len()).map_err(|_| Errno(sys::EINVAL))?;
    // SAFETY: initialized changes of the stated count and no event output.
    if unsafe {
        libc::kevent(
            queue,
            changes.as_ptr(),
            count,
            core::ptr::null_mut(),
            0,
            core::ptr::null(),
        )
    } == 0
    {
        Ok(())
    } else {
        Err(last_error())
    }
}

/// Kills the tree's process group and waits for each member to exit, and
/// for each named holder outside it, which only a cancellation kills.
#[cfg(target_vendor = "apple")]
fn drain(
    queue: Fd,
    warden: Pid,
    program: Pid,
    mut status: Option<i32>,
    channel: Fd,
    named: &mut Named,
) -> Option<i32> {
    if status.is_none() {
        // The owner left or cancelled while the program runs. A dying process
        // already fails the group check below while it still holds its
        // descriptors, so only its reap proves they are released. The
        // unreaped child's PID names it even outside the group.
        let _ = sys::kill(program, sys::SIGKILL);
        status = reap(program);
    }
    let mut members = [0 as Pid; 256];
    loop {
        // The warden left this group, and its live PID keeps the group's ID
        // from naming any other group: one signal reaches exactly the tree,
        // never a listed PID that may since have been reused.
        let _ = sys::kill(-warden, sys::SIGKILL);
        if named.cancelled {
            named.kill_holders();
        }
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
        if !live && named.count == 0 {
            reap_ready(program, &mut status);
            return status;
        }
        match next_event(queue) {
            Ok(Event::Exit(pid)) => named.remove(pid),
            Ok(Event::Readable(end)) => receive_namings(queue, channel, named, end),
            Ok(Event::Cancelled) => named.cancelled = true,
            Err(_) => return status,
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

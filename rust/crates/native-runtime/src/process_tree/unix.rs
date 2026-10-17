use std::io;
use std::io::{Read, Write};
pub(super) use std::os::fd::AsRawFd as Pipe;
use std::os::unix::process::CommandExt as _;
use std::os::unix::process::ExitStatusExt as _;
#[cfg(test)]
use std::process::Child;
use std::process::{Command, ExitStatus};

#[cfg(test)]
pub(super) fn observe_exit(child: &Child) -> io::Result<Option<ExitStatus>> {
    observe_exit_id(child.id(), None)
}

pub(super) fn observe_exit_id(
    id: libc::id_t,
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<Option<ExitStatus>> {
    let flags = libc::WEXITED | libc::WNOWAIT | libc::WNOHANG;
    loop {
        // SAFETY: initialized OS output storage and the exclusively owned
        // child's ID. WNOWAIT retains the leader until group termination,
        // preventing its PID/PGID from being reused for an unrelated group.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let (waited, recorded) =
            super::pipe_operation_attempt(meter, crate::NativeOperationKind::Read, || {
                // SAFETY: `info` is writable storage owned by this frame and
                // `id` names the exclusively owned child.
                if unsafe { libc::waitid(libc::P_PID, id, &raw mut info, flags) } == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            })?;
        match waited {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                recorded?;
                continue;
            }
            Err(error) => return Err(error),
            Ok(()) => recorded?,
        }
        // SAFETY: WEXITED waitid initialized the child-status union.
        if unsafe { info.si_pid() } == 0 {
            return Ok(None);
        }
        // SAFETY: the same WEXITED observation initialized the status field.
        let status = unsafe { info.si_status() };
        let raw = match info.si_code {
            libc::CLD_EXITED => status << 8,
            libc::CLD_KILLED => status,
            libc::CLD_DUMPED => status | 0x80,
            _ => return Err(io::ErrorKind::Other.into()),
        };
        return Ok(Some(ExitStatus::from_raw(raw)));
    }
}

pub(super) fn reap_exit_id(
    pid: libc::pid_t,
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<(Option<ExitStatus>, io::Result<()>)> {
    loop {
        let mut status = 0;
        let (result, recorded) =
            super::pipe_operation_attempt(meter, crate::NativeOperationKind::Read, || {
                // SAFETY: exclusively owned child ID and writable status.
                let waited = unsafe { libc::waitpid(pid, &raw mut status, libc::WNOHANG) };
                if waited < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(waited)
                }
            })?;
        match result {
            Ok(0) => return Ok((None, recorded)),
            Ok(_) => return Ok((Some(ExitStatus::from_raw(status)), recorded)),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                recorded?;
            }
            Err(error) => return Err(error),
        }
    }
}

pub(super) fn read_pipe(
    pipe: &mut (impl Read + Pipe),
    buffer: &mut [std::mem::MaybeUninit<u8>],
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<(Option<usize>, io::Result<()>)> {
    nonblocking(pipe, meter)?;
    let (read, recorded) =
        super::pipe_operation_attempt(meter, crate::NativeOperationKind::Read, || {
            // SAFETY: the owned nonblocking fd receives this exclusively
            // borrowed writable allocation; read initializes its prefix.
            let read =
                unsafe { libc::read(pipe.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
            // Only the -1 failure result is negative.
            usize::try_from(read).map_err(|_| io::Error::last_os_error())
        })?;
    match read {
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            ) =>
        {
            Ok((None, recorded))
        }
        Err(error) => {
            recorded?;
            Err(error)
        }
        Ok(read) => Ok((Some(read), recorded)),
    }
}

pub(super) fn write_pipe(
    pipe: &mut (impl Write + Pipe),
    bytes: &[u8],
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<(Option<usize>, io::Result<()>)> {
    nonblocking(pipe, meter)?;
    let (written, recorded) =
        super::pipe_operation_attempt(meter, crate::NativeOperationKind::Write, || {
            pipe.write(bytes)
        })?;
    match written {
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            ) =>
        {
            Ok((None, recorded))
        }
        Ok(0) => {
            recorded?;
            Err(io::Error::from(io::ErrorKind::WriteZero))
        }
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => Ok((Some(0), recorded)),
        Err(error) => {
            recorded?;
            Err(error)
        }
        Ok(written) => Ok((Some(written), recorded)),
    }
}

fn nonblocking(pipe: &impl Pipe, meter: Option<&dyn crate::NativeReadMeter>) -> io::Result<()> {
    let (flags, recorded) =
        super::pipe_operation_attempt(meter, crate::NativeOperationKind::Read, || {
            // SAFETY: this exclusively owned live fd is queried only.
            let flags = unsafe { libc::fcntl(pipe.as_raw_fd(), libc::F_GETFL) };
            if flags == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(flags)
            }
        })?;
    recorded?;
    let flags = flags?;
    if flags & libc::O_NONBLOCK != 0 {
        return Ok(());
    }
    let (result, recorded) =
        super::pipe_operation_attempt(meter, crate::NativeOperationKind::Write, || {
            // SAFETY: add only O_NONBLOCK to this owner's original flags.
            if unsafe { libc::fcntl(pipe.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) }
                == -1
            {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        })?;
    recorded?;
    result
}

pub(super) struct Guard {
    process_group: libc::pid_t,
    active: bool,
}

/// Native spawn objects that a failed SDK destroy did not release.
#[derive(Default)]
pub(super) struct SpawnResources {
    actions: Option<libc::posix_spawn_file_actions_t>,
    attributes: Option<libc::posix_spawnattr_t>,
}

// SAFETY: these initialized SDK objects are exclusively owned values.
// Native mutation requires &mut custody; shared access inspects only the
// inline presence flags and never dereferences or mutates a native handle.
unsafe impl Send for SpawnResources {}
// SAFETY: shared access observes presence only; all SDK calls require an
// exclusive mutable owner, as for Send above.
unsafe impl Sync for SpawnResources {}

impl SpawnResources {
    pub(super) const fn pending(&self) -> bool {
        self.actions.is_some() || self.attributes.is_some()
    }

    pub(super) fn cleanup(&mut self, meter: Option<&dyn crate::NativeReadMeter>) -> io::Result<()> {
        if self.actions.is_some() {
            super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                let actions = self.actions.as_mut().ok_or(io::ErrorKind::NotFound)?;
                // SAFETY: a retained object is initialized and not yet destroyed.
                spawn_status(unsafe { libc::posix_spawn_file_actions_destroy(actions) })?;
                self.actions.take();
                Ok(())
            })?;
        }
        if self.attributes.is_some() {
            super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                let attributes = self.attributes.as_mut().ok_or(io::ErrorKind::NotFound)?;
                // SAFETY: a retained object is initialized and not yet destroyed.
                spawn_status(unsafe { libc::posix_spawnattr_destroy(attributes) })?;
                self.attributes.take();
                Ok(())
            })?;
        }
        Ok(())
    }
}

pub(super) fn spawn_controlled(
    command: &mut Command,
    meter: Option<&dyn crate::NativeReadMeter>,
    custody: &mut Option<super::ProcessTree>,
) -> io::Result<()> {
    command.process_group(0);
    super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
        let child = command.spawn()?;
        // A Unix Child ID originated as pid_t at this same SDK boundary.
        let process_group = child.id() as libc::pid_t;
        *custody = Some(super::ProcessTree {
            child: Some(child),
            guard: Guard {
                process_group,
                active: true,
            },
            output_taken: false,
            native_child: None,
            cleanup_on_drop: meter.is_none(),
            spawn_resources: SpawnResources::default(),
            prepared_spawn: None,
        });
        Ok(())
    })
}

#[cfg(target_vendor = "apple")]
unsafe extern "C" {
    fn _NSGetEnviron() -> *mut *mut *mut libc::c_char;
}

#[cfg(not(target_vendor = "apple"))]
unsafe extern "C" {
    static mut environ: *mut *mut libc::c_char;
}

pub(super) fn inherited_environment() -> *mut *mut libc::c_char {
    // SAFETY: the existing closed-spawn host contract excludes concurrent
    // unsafe native environment mutation while these pointers are borrowed.
    unsafe {
        #[cfg(target_vendor = "apple")]
        return *_NSGetEnviron();
        #[cfg(not(target_vendor = "apple"))]
        return environ;
    }
}

pub(super) fn spawn_closed(
    program: &std::ffi::CStr,
    args: &[&std::ffi::CStr],
    meter: &dyn crate::NativeReadMeter,
    custody: &mut Option<super::ProcessTree>,
) -> io::Result<()> {
    let count = args
        .len()
        .checked_add(2)
        .ok_or(io::ErrorKind::InvalidInput)?;
    let bytes = count
        .checked_mul(std::mem::size_of::<*mut libc::c_char>())
        .ok_or(io::ErrorKind::InvalidInput)?;
    let copied = bytes - std::mem::size_of::<*mut libc::c_char>();
    meter.admit_examination((count - 1) as u64, copied as u64)?;
    meter.allocate(bytes as u64)?;
    let mut arguments = Vec::new();
    if arguments.try_reserve_exact(count).is_err() {
        meter.release(bytes as u64);
        return Err(io::ErrorKind::OutOfMemory.into());
    }
    arguments.push(program.as_ptr().cast_mut());
    arguments.extend(args.iter().map(|argument| argument.as_ptr().cast_mut()));
    arguments.push(std::ptr::null_mut());
    let result = meter
        .record_examination((count - 1) as u64, copied as u64)
        .and_then(|()| spawn_closed_with_arguments(program, &arguments, None, meter, custody));
    drop(arguments);
    meter.release(bytes as u64);
    result
}

pub(super) fn spawn_closed_with_arguments(
    program: &std::ffi::CStr,
    arguments: &[*mut libc::c_char],
    environment: Option<*mut *mut libc::c_char>,
    meter: &dyn crate::NativeReadMeter,
    custody: &mut Option<super::ProcessTree>,
) -> io::Result<()> {
    let attr_cleanup = meter.reserve_cleanup_write()?;
    let actions_cleanup = match meter.reserve_cleanup_write() {
        Ok(cleanup) => cleanup,
        Err(error) => {
            meter.cancel_cleanup_write(attr_cleanup);
            return Err(error);
        }
    };
    let mut objects = SpawnObjects {
        attributes: std::mem::MaybeUninit::uninit(),
        actions: std::mem::MaybeUninit::uninit(),
        attr_initialized: false,
        actions_initialized: false,
    };
    let spawned = objects
        .initialize(meter)
        .and_then(|()| objects.spawn(program, arguments, environment, meter, custody));
    // Retire objects in reverse construction order. Both actual
    // destroys run even after cancellation or another cleanup failure.
    let (actions_result, actions) = retire(
        objects.actions_initialized.then_some(&mut objects.actions),
        libc::posix_spawn_file_actions_destroy,
        actions_cleanup,
        meter,
    );
    let (attr_result, attributes) = retire(
        objects.attr_initialized.then_some(&mut objects.attributes),
        libc::posix_spawnattr_destroy,
        attr_cleanup,
        meter,
    );
    let retained = SpawnResources {
        actions,
        attributes,
    };
    if retained.pending() {
        let owner = custody.get_or_insert_with(|| super::ProcessTree {
            child: None,
            native_child: None,
            guard: Guard {
                process_group: 0,
                active: false,
            },
            output_taken: false,
            cleanup_on_drop: false,
            spawn_resources: SpawnResources::default(),
            prepared_spawn: None,
        });
        owner.spawn_resources = retained;
    }
    spawned.and(actions_result).and(attr_result)
}

fn spawn_status(status: libc::c_int) -> io::Result<()> {
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(status))
    }
}

/// One closed spawn's native objects; each initialized one must be destroyed.
struct SpawnObjects {
    attributes: std::mem::MaybeUninit<libc::posix_spawnattr_t>,
    actions: std::mem::MaybeUninit<libc::posix_spawn_file_actions_t>,
    attr_initialized: bool,
    actions_initialized: bool,
}

impl SpawnObjects {
    fn initialize(&mut self, meter: &dyn crate::NativeReadMeter) -> io::Result<()> {
        let flags = libc::c_short::try_from(libc::POSIX_SPAWN_SETPGROUP)
            .map_err(|_| io::ErrorKind::InvalidInput)?;
        let native = |call: &mut dyn FnMut() -> libc::c_int| {
            super::process_control_attempt(Some(meter), crate::NativeOperationKind::Write, || {
                spawn_status(call())
            })
        };
        native(&mut || {
            // SAFETY: this owner's attribute storage, before its initialization.
            let status = unsafe { libc::posix_spawnattr_init(self.attributes.as_mut_ptr()) };
            self.attr_initialized |= status == 0;
            status
        })?;
        native(&mut || {
            // SAFETY: this owner's file-action storage, before its initialization.
            let status = unsafe { libc::posix_spawn_file_actions_init(self.actions.as_mut_ptr()) };
            self.actions_initialized |= status == 0;
            status
        })?;
        // SAFETY: the attribute object initialized above.
        native(&mut || unsafe {
            libc::posix_spawnattr_setpgroup(self.attributes.as_mut_ptr(), 0)
        })?;
        // SAFETY: the attribute object initialized above.
        native(&mut || unsafe {
            libc::posix_spawnattr_setflags(self.attributes.as_mut_ptr(), flags)
        })?;
        for descriptor in [0, 1, 2] {
            let access = if descriptor == 0 {
                libc::O_RDONLY
            } else {
                libc::O_WRONLY
            };
            // SAFETY: the file-action object initialized above and a static
            // NUL-terminated path, which the call copies.
            native(&mut || unsafe {
                libc::posix_spawn_file_actions_addopen(
                    self.actions.as_mut_ptr(),
                    descriptor,
                    c"/dev/null".as_ptr(),
                    access,
                    0,
                )
            })?;
        }
        Ok(())
    }

    fn spawn(
        &self,
        program: &std::ffi::CStr,
        arguments: &[*mut libc::c_char],
        environment: Option<*mut *mut libc::c_char>,
        meter: &dyn crate::NativeReadMeter,
        custody: &mut Option<super::ProcessTree>,
    ) -> io::Result<()> {
        let mut pid = 0;
        loop {
            let (result, recorded) = super::pipe_operation_attempt(
                Some(meter),
                crate::NativeOperationKind::Write,
                || {
                    let environment = environment.unwrap_or_else(inherited_environment);
                    // SAFETY: both spawn objects are initialized; the program,
                    // the null-terminated argument vector and the environment
                    // stay live and unmodified through this synchronous call.
                    let status = unsafe {
                        libc::posix_spawn(
                            &raw mut pid,
                            program.as_ptr(),
                            self.actions.as_ptr(),
                            self.attributes.as_ptr(),
                            arguments.as_ptr(),
                            environment,
                        )
                    };
                    if status == 0 {
                        *custody = Some(super::ProcessTree {
                            child: None,
                            native_child: Some(pid),
                            guard: Guard {
                                process_group: pid,
                                active: true,
                            },
                            output_taken: false,
                            cleanup_on_drop: false,
                            spawn_resources: SpawnResources::default(),
                            prepared_spawn: None,
                        });
                    }
                    spawn_status(status)
                },
            )?;
            recorded?;
            match result {
                Err(error) if error.kind() == io::ErrorKind::Interrupted && custody.is_none() => {}
                result => return result,
            }
        }
    }
}

/// Destroys one initialized spawn object, returning it when the destroy fails.
fn retire<T>(
    object: Option<&mut std::mem::MaybeUninit<T>>,
    destroy: unsafe extern "C" fn(*mut T) -> libc::c_int,
    cleanup: u64,
    meter: &dyn crate::NativeReadMeter,
) -> (io::Result<()>, Option<T>) {
    let Some(object) = object else {
        meter.cancel_cleanup_write(cleanup);
        return (Ok(()), None);
    };
    let recorded = meter.record_cleanup_write(cleanup);
    // SAFETY: the caller passes only an initialized, not yet destroyed object.
    let status = unsafe { destroy(object.as_mut_ptr()) };
    // SAFETY: a failed destroy leaves the object initialized; this read moves
    // it to its retaining owner, and the frame's storage is not used again.
    let retained = (status != 0).then(|| unsafe { object.assume_init_read() });
    (recorded.and_then(|()| spawn_status(status)), retained)
}

impl Guard {
    fn group_gone(
        &self,
        error: &io::Error,
        _meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<bool> {
        if error.raw_os_error() == Some(libc::ESRCH) {
            return Ok(true);
        }
        #[cfg(target_vendor = "apple")]
        if error.raw_os_error() == Some(libc::EPERM) {
            return group_has_no_live_members(self.process_group, _meter);
        }
        Ok(false)
    }

    pub(super) fn request_termination(
        &mut self,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<()> {
        if !self.active {
            return Ok(());
        }
        let signal =
            super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                // SAFETY: the unreaped owned leader reserves this process group.
                if unsafe { libc::kill(-self.process_group, libc::SIGTERM) } == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            });
        let Err(error) = signal else {
            return Ok(());
        };
        if self.group_gone(&error, meter)? {
            Ok(())
        } else {
            Err(error)
        }
    }

    pub(super) fn terminate_with_meter(
        &mut self,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<()> {
        if !self.active {
            return Ok(());
        }
        let signal =
            super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                // SAFETY: a negative, nonzero pid addresses exactly this owned
                // process group; SIGKILL requires no shared memory or signal data.
                if unsafe { libc::kill(-self.process_group, libc::SIGKILL) } == 0 {
                    self.active = false;
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            });
        let Err(error) = signal else {
            return Ok(());
        };
        if self.group_gone(&error, meter)? {
            self.active = false;
            Ok(())
        } else {
            Err(error)
        }
    }
}

#[cfg(target_vendor = "apple")]
fn group_has_no_live_members(
    group: libc::pid_t,
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<bool> {
    let deadline = std::time::Instant::now() + super::ProcessTree::TERMINATION_TIMEOUT;
    loop {
        if group_snapshot_has_no_live_members(group, meter)? {
            return Ok(true);
        }
        if std::time::Instant::now() >= deadline {
            return Ok(false);
        }
        super::process_control_attempt(meter, crate::NativeOperationKind::Read, || {
            std::thread::sleep(super::ProcessTree::TERMINATION_POLL_INTERVAL);
            Ok(())
        })?;
    }
}

#[cfg(target_vendor = "apple")]
fn group_snapshot_has_no_live_members(
    group: libc::pid_t,
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<bool> {
    let mut members = [0_i32; 2];
    let bytes = i32::try_from(std::mem::size_of_val(&members)).map_err(io::Error::other)?;
    let id = u32::try_from(group).map_err(io::Error::other)?;
    // libproc returns zero both for an empty list and for an errno failure.
    let read = super::process_control_attempt(meter, crate::NativeOperationKind::Read, || {
        // SAFETY: thread-local errno and a writable, correctly sized PID array.
        Ok(unsafe {
            *libc::__error() = 0;
            libc::proc_listpids(2, id, members.as_mut_ptr().cast(), bytes)
        })
    })?;
    if read <= 0 {
        let error = io::Error::last_os_error();
        return if read == 0 && error.raw_os_error() == Some(0) {
            Ok(true)
        } else {
            Err(error)
        };
    }
    if read == i32::try_from(std::mem::size_of::<libc::pid_t>()).map_err(io::Error::other)?
        && members.first() == Some(&group)
    {
        return Ok(observe_exit_id(id, meter)?.is_some());
    }
    Ok(false)
}

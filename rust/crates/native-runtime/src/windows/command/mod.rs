//! The Windows command: `std::process::Command`'s builder and semantics,
//! started by [`start::start`] so the child inherits only its own standard
//! handles. `std` starts every child with `bInheritHandles` set and no
//! handle list, so each one inherits every inheritable handle in the
//! process, the host's standard handles among them.

mod line;
pub(crate) mod start;
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod testing;
#[cfg(test)]
mod tests;

use line::Arg;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::File;
use std::io;
use std::os::windows::ffi::OsStrExt as _;
use std::os::windows::io::{
    AsHandle, AsRawHandle, BorrowedHandle, FromRawHandle as _, IntoRawHandle, OwnedHandle,
    RawHandle,
};
use std::os::windows::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::{ChildStderr, ChildStdin, ChildStdout, ExitStatus, Output};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use windows_sys::Win32::Foundation::{
    ERROR_ACCESS_DENIED, GENERIC_READ, GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE, WAIT_FAILED,
    WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::Globalization::{CSTR_EQUAL, CSTR_LESS_THAN, CompareStringOrdinal};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED, FILE_READ_ATTRIBUTES,
    FILE_WRITE_ATTRIBUTES, OPEN_EXISTING, PIPE_ACCESS_INBOUND, PIPE_ACCESS_OUTBOUND,
};
use windows_sys::Win32::System::Console::{
    GetStdHandle, STD_ERROR_HANDLE, STD_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
};
use windows_sys::Win32::System::Pipes::{
    CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE, PIPE_WAIT,
};
use windows_sys::Win32::System::Threading::{
    GetExitCodeProcess, INFINITE, TerminateProcess, WaitForSingleObject,
};

/// An environment variable name, compared without case as Windows does.
#[derive(Clone, Debug)]
struct EnvKey {
    name: OsString,
    wide: Vec<u16>,
}

impl EnvKey {
    fn new(name: &OsStr) -> Self {
        Self {
            name: name.to_owned(),
            wide: name.encode_wide().collect(),
        }
    }
}

impl Ord for EnvKey {
    fn cmp(&self, other: &Self) -> Ordering {
        let length = |wide: &[u16]| i32::try_from(wide.len()).unwrap_or(i32::MAX);
        // SAFETY: both slices are live for the stated lengths.
        let compared = unsafe {
            CompareStringOrdinal(
                self.wide.as_ptr(),
                length(&self.wide),
                other.wide.as_ptr(),
                length(&other.wide),
                1,
            )
        };
        match compared {
            CSTR_LESS_THAN => Ordering::Less,
            CSTR_EQUAL => Ordering::Equal,
            // Greater, or a failure that valid input never produces.
            _ if compared > 0 => Ordering::Greater,
            _ => self.wide.cmp(&other.wide),
        }
    }
}

impl PartialOrd for EnvKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for EnvKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for EnvKey {}

fn is_path(name: &OsStr) -> bool {
    name.to_str()
        .is_some_and(|name| name.eq_ignore_ascii_case("PATH"))
}

/// What a child receives as one standard handle: a description the
/// builder keeps. Each start creates its own pipe or null device, or
/// duplicates the given handle, so a builder starts every child alike.
pub struct Stdio(Kind);

enum Kind {
    Inherit,
    Null,
    Piped,
    Handle(OwnedHandle),
}

impl Stdio {
    /// A new pipe whose other end the parent holds.
    #[must_use]
    pub fn piped() -> Self {
        Self(Kind::Piped)
    }

    /// The parent's own handle for this stream.
    #[must_use]
    pub fn inherit() -> Self {
        Self(Kind::Inherit)
    }

    /// The null device.
    #[must_use]
    pub fn null() -> Self {
        Self(Kind::Null)
    }
}

impl fmt::Debug for Stdio {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.0 {
            Kind::Inherit => "Inherit",
            Kind::Null => "Null",
            Kind::Piped => "MakePipe",
            Kind::Handle(_) => "Handle",
        })
    }
}

impl From<OwnedHandle> for Stdio {
    fn from(handle: OwnedHandle) -> Self {
        Self(Kind::Handle(handle))
    }
}

impl From<File> for Stdio {
    fn from(file: File) -> Self {
        Self::from(OwnedHandle::from(file))
    }
}

impl From<io::PipeReader> for Stdio {
    fn from(pipe: io::PipeReader) -> Self {
        Self::from(OwnedHandle::from(pipe))
    }
}

impl From<io::PipeWriter> for Stdio {
    fn from(pipe: io::PipeWriter) -> Self {
        Self::from(OwnedHandle::from(pipe))
    }
}

impl From<ChildStdin> for Stdio {
    fn from(pipe: ChildStdin) -> Self {
        Self::from(OwnedHandle::from(pipe))
    }
}

impl From<ChildStdout> for Stdio {
    fn from(pipe: ChildStdout) -> Self {
        Self::from(OwnedHandle::from(pipe))
    }
}

impl From<ChildStderr> for Stdio {
    fn from(pipe: ChildStderr) -> Self {
        Self::from(OwnedHandle::from(pipe))
    }
}

fn wide(text: &str) -> Vec<u16> {
    text.encode_utf16().chain([0]).collect()
}

/// A byte pipe: an overlapped end for this process, as `std` makes its own,
/// and a synchronous end for the child. Neither end is inheritable.
fn pipe(ours_readable: bool) -> io::Result<(OwnedHandle, OwnedHandle)> {
    // Unique among live processes: a server end is never inherited, so a
    // name lives only as long as the process that created it. A name taken
    // by another process fails the start; it cannot be read through, since
    // the one instance is created here before the child's end opens it.
    static NEXT: AtomicU64 = AtomicU64::new(0);
    const CAPACITY: u32 = 64 * 1024;
    let name = wide(&format!(
        "\\\\.\\pipe\\acyclic-command-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, AtomicOrdering::Relaxed),
    ));
    let direction = if ours_readable {
        PIPE_ACCESS_INBOUND
    } else {
        PIPE_ACCESS_OUTBOUND
    };
    // SAFETY: a NUL-terminated name; the one instance this call creates is
    // ours, so no other process already holds the name.
    let ours = unsafe {
        CreateNamedPipeW(
            name.as_ptr(),
            direction | FILE_FLAG_FIRST_PIPE_INSTANCE | FILE_FLAG_OVERLAPPED,
            PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
            1,
            CAPACITY,
            CAPACITY,
            0,
            std::ptr::null(),
        )
    };
    if ours == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreateNamedPipeW returned a handle this process owns.
    let ours = unsafe { OwnedHandle::from_raw_handle(ours) };
    let access = if ours_readable {
        GENERIC_WRITE | FILE_READ_ATTRIBUTES
    } else {
        GENERIC_READ | FILE_WRITE_ATTRIBUTES
    };
    // SAFETY: opens the client end of the instance created above.
    let theirs = unsafe {
        CreateFileW(
            name.as_ptr(),
            access,
            0,
            std::ptr::null(),
            OPEN_EXISTING,
            0,
            std::ptr::null_mut(),
        )
    };
    if theirs == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreateFileW returned a handle this process owns.
    Ok((ours, unsafe { OwnedHandle::from_raw_handle(theirs) }))
}

fn open_null(readable: bool) -> io::Result<OwnedHandle> {
    let name = wide(r"\\.\NUL");
    let access = if readable {
        GENERIC_READ
    } else {
        GENERIC_WRITE
    };
    // SAFETY: a NUL-terminated device name.
    let handle = unsafe {
        CreateFileW(
            name.as_ptr(),
            access | FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES,
            windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_WRITE,
            std::ptr::null(),
            OPEN_EXISTING,
            0,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreateFileW returned a handle this process owns.
    Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
}

/// One standard stream of one start: the child's end, created for this
/// start or borrowed from the host or the builder, and this process's end
/// of its pipe when there is one.
struct Lowered {
    created: Option<OwnedHandle>,
    borrowed: HANDLE,
    ours: Option<OwnedHandle>,
}

impl Lowered {
    fn handle(&self) -> HANDLE {
        self.created
            .as_ref()
            .map_or(self.borrowed, AsRawHandle::as_raw_handle)
    }
}

/// Lowers the builder's description of one stream for one start. Nothing
/// is taken from the builder: the start duplicates a borrowed handle, and
/// pipes and the null device are created afresh.
fn lower(
    stdio: Option<&Stdio>,
    default: fn() -> Stdio,
    id: STD_HANDLE,
    inherited: Option<OwnedHandle>,
) -> io::Result<Lowered> {
    let readable = id == STD_INPUT_HANDLE;
    let mut lowered = Lowered {
        created: None,
        borrowed: std::ptr::null_mut(),
        ours: None,
    };
    let fallback = default();
    let kind = &stdio.unwrap_or(&fallback).0;
    match kind {
        // Already pinned before this start allocated any pipes or null handles.
        Kind::Inherit => lowered.created = inherited,
        Kind::Null => lowered.created = Some(open_null(readable)?),
        Kind::Piped => {
            let (ours, theirs) = pipe(!readable)?;
            lowered.ours = Some(ours);
            lowered.created = Some(theirs);
        }
        Kind::Handle(handle) => lowered.borrowed = handle.as_raw_handle(),
    }
    Ok(lowered)
}

/// A process builder with the API and semantics of
/// `std::process::Command`, which on Windows starts the child with only
/// its own standard handles.
pub struct Command {
    program: OsString,
    args: Vec<Arg>,
    env: BTreeMap<EnvKey, Option<OsString>>,
    clear_env: bool,
    saw_path: bool,
    cwd: Option<PathBuf>,
    flags: u32,
    stdin: Option<Stdio>,
    stdout: Option<Stdio>,
    stderr: Option<Stdio>,
}

impl fmt::Debug for Command {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", self.program)?;
        for arg in &self.args {
            write!(formatter, " {:?}", arg.as_os_str())?;
        }
        Ok(())
    }
}

impl Command {
    /// A command that runs `program`, resolved as `std` resolves it.
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        Self {
            program: program.as_ref().to_owned(),
            args: Vec::new(),
            env: BTreeMap::new(),
            clear_env: false,
            saw_path: false,
            cwd: None,
            flags: 0,
            stdin: None,
            stdout: None,
            stderr: None,
        }
    }

    /// Adds one argument, quoted for the child's parser.
    pub fn arg(&mut self, arg: impl AsRef<OsStr>) -> &mut Self {
        self.args.push(Arg::Regular(arg.as_ref().to_owned()));
        self
    }

    /// Adds arguments, each quoted for the child's parser.
    pub fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        for arg in args {
            self.arg(arg);
        }
        self
    }

    /// Appends `text` to the command line unchanged.
    pub fn raw_arg(&mut self, text: impl AsRef<OsStr>) -> &mut Self {
        self.args.push(Arg::Raw(text.as_ref().to_owned()));
        self
    }

    fn set_env(&mut self, key: &OsStr, value: Option<OsString>) {
        self.saw_path |= is_path(key);
        self.env.insert(EnvKey::new(key), value);
    }

    /// Sets one environment variable for the child.
    pub fn env(&mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> &mut Self {
        self.set_env(key.as_ref(), Some(value.as_ref().to_owned()));
        self
    }

    /// Sets environment variables for the child.
    pub fn envs<I, K, V>(&mut self, vars: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        for (key, value) in vars {
            self.env(key, value);
        }
        self
    }

    /// Removes one environment variable from the child's environment.
    pub fn env_remove(&mut self, key: impl AsRef<OsStr>) -> &mut Self {
        if self.clear_env {
            self.saw_path |= is_path(key.as_ref());
            self.env.remove(&EnvKey::new(key.as_ref()));
        } else {
            self.set_env(key.as_ref(), None);
        }
        self
    }

    /// Starts the child with only the variables set after this call.
    pub fn env_clear(&mut self) -> &mut Self {
        self.clear_env = true;
        self.env.clear();
        self
    }

    /// The child's working directory.
    pub fn current_dir(&mut self, dir: impl AsRef<Path>) -> &mut Self {
        self.cwd = Some(dir.as_ref().to_owned());
        self
    }

    /// The child's standard input.
    pub fn stdin(&mut self, cfg: impl Into<Stdio>) -> &mut Self {
        self.stdin = Some(cfg.into());
        self
    }

    /// The child's standard output.
    pub fn stdout(&mut self, cfg: impl Into<Stdio>) -> &mut Self {
        self.stdout = Some(cfg.into());
        self
    }

    /// The child's standard error.
    pub fn stderr(&mut self, cfg: impl Into<Stdio>) -> &mut Self {
        self.stderr = Some(cfg.into());
        self
    }

    /// Process creation flags, as `CreateProcessW` takes them.
    pub fn creation_flags(&mut self, flags: u32) -> &mut Self {
        self.flags = flags;
        self
    }

    /// The program as given.
    #[must_use]
    pub fn get_program(&self) -> &OsStr {
        &self.program
    }

    /// The arguments as given, raw ones included.
    pub fn get_args(&self) -> impl ExactSizeIterator<Item = &OsStr> {
        self.args.iter().map(Arg::as_os_str)
    }

    /// Variables set (`Some`) or removed (`None`) for the child, in order.
    pub fn get_envs(&self) -> impl ExactSizeIterator<Item = (&OsStr, Option<&OsStr>)> {
        self.env
            .iter()
            .map(|(key, value)| (key.name.as_os_str(), value.as_deref()))
    }

    /// The child's working directory, when one is set.
    #[must_use]
    pub fn get_current_dir(&self) -> Option<&Path> {
        self.cwd.as_deref()
    }

    /// The environment the child starts with, or `None` for this process's.
    fn captured_env(&self) -> Option<BTreeMap<EnvKey, OsString>> {
        if !self.clear_env && self.env.is_empty() {
            return None;
        }
        let mut captured = BTreeMap::new();
        if !self.clear_env {
            for (key, value) in std::env::vars_os() {
                captured.insert(EnvKey::new(&key), value);
            }
        }
        for (key, value) in &self.env {
            match value {
                Some(value) => {
                    captured.insert(key.clone(), value.clone());
                }
                None => {
                    captured.remove(key);
                }
            }
        }
        Some(captured)
    }

    fn start(&self, default: fn() -> Stdio, needs_stdin: bool) -> io::Result<Child> {
        let env = self.captured_env();
        let child_path = if self.saw_path || self.clear_env {
            env.as_ref()
                .and_then(|env| env.get(&EnvKey::new(OsStr::new("PATH"))))
                .map(OsString::as_os_str)
        } else {
            None
        };
        let program = line::resolve(&self.program, child_path)?;
        let (application, mut command_line) = if line::is_batch_file(&program)? {
            (
                line::command_prompt()?,
                line::batch_command_line(&program, &self.args)?,
            )
        } else {
            (program, line::command_line(&self.program, &self.args)?)
        };
        command_line.push(0);
        let environment = env.map(environment_block).transpose()?;
        let directory = self.cwd.as_deref().map(directory).transpose()?;
        let stdin_default = if needs_stdin { default } else { Stdio::null };
        let inherited_slot = |stdio: Option<&Stdio>, fallback: fn() -> Stdio, id| {
            let default = fallback();
            if matches!(stdio.unwrap_or(&default).0, Kind::Inherit) {
                // SAFETY: reads this process's standard handle slot.
                unsafe { GetStdHandle(id) }
            } else {
                std::ptr::null_mut()
            }
        };
        let [input, output, error] = start::pin_inherited([
            inherited_slot(self.stdin.as_ref(), stdin_default, STD_INPUT_HANDLE),
            inherited_slot(self.stdout.as_ref(), default, STD_OUTPUT_HANDLE),
            inherited_slot(self.stderr.as_ref(), default, STD_ERROR_HANDLE),
        ])?;
        let stdin = lower(self.stdin.as_ref(), stdin_default, STD_INPUT_HANDLE, input)?;
        let stdout = lower(self.stdout.as_ref(), default, STD_OUTPUT_HANDLE, output)?;
        let stderr = lower(self.stderr.as_ref(), default, STD_ERROR_HANDLE, error)?;
        let started = start::start(start::Start {
            application: &application,
            command_line: &mut command_line,
            flags: self.flags,
            environment: environment.as_deref(),
            directory: directory.as_deref(),
            stdio: [stdin.handle(), stdout.handle(), stderr.handle()],
        })?;
        Ok(Child {
            process: started.process,
            id: started.id,
            status: None,
            stdin: stdin.ours.map(ChildStdin::from),
            stdout: stdout.ours.map(ChildStdout::from),
            stderr: stderr.ours.map(ChildStderr::from),
        })
    }

    /// Starts the child; unset streams inherit this process's.
    ///
    /// # Errors
    /// Fails as `std::process::Command::spawn` does.
    pub fn spawn(&mut self) -> io::Result<Child> {
        self.start(Stdio::inherit, true)
    }

    /// Runs the child to exit; unset streams inherit this process's.
    ///
    /// # Errors
    /// Fails as `std::process::Command::status` does.
    pub fn status(&mut self) -> io::Result<ExitStatus> {
        self.start(Stdio::inherit, true)?.wait()
    }

    /// Runs the child to exit, collecting its output; unset output streams
    /// are piped and an unset input is the null device.
    ///
    /// # Errors
    /// Fails as `std::process::Command::output` does.
    pub fn output(&mut self) -> io::Result<Output> {
        self.start(Stdio::piped, false)?.wait_with_output()
    }
}

fn environment_block(env: BTreeMap<EnvKey, OsString>) -> io::Result<Vec<u16>> {
    let mut block = Vec::new();
    if env.is_empty() {
        block.push(0);
    }
    for (key, value) in env {
        if key.wide.contains(&0) || value.encode_wide().any(|unit| unit == 0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "strings passed to WinAPI cannot contain NULs",
            ));
        }
        block.extend(key.wide);
        block.push(u16::from(b'='));
        block.extend(value.encode_wide());
        block.push(0);
    }
    block.push(0);
    Ok(block)
}

/// The working directory without a verbatim prefix where removing it keeps
/// its meaning, since a process's current directory cannot be verbatim.
fn directory(dir: &Path) -> io::Result<Vec<u16>> {
    let full = line::to_wide(dir.as_os_str())?;
    let unc: Vec<u16> = r"\\?\UNC".encode_utf16().collect();
    let verbatim: Vec<u16> = r"\\?\".encode_utf16().collect();
    if full.starts_with(&unc) {
        let mut rest = full.get(r"\\?\UN".len()..).unwrap_or_default().to_vec();
        if let Some(first) = rest.first_mut() {
            *first = u16::from(b'\\');
        }
        if line::is_absolute_exact(&rest) {
            return Ok(rest);
        }
    } else if full.starts_with(&verbatim) {
        let rest = full.get(verbatim.len()..).unwrap_or_default();
        if line::is_absolute_exact(rest) {
            return Ok(rest.to_vec());
        }
    }
    Ok(full)
}

/// A started child: `std::process::Child`'s API over a process handle this
/// crate started.
pub struct Child {
    process: OwnedHandle,
    id: u32,
    status: Option<ExitStatus>,
    /// This process's end of the child's input pipe.
    pub stdin: Option<ChildStdin>,
    /// This process's end of the child's output pipe.
    pub stdout: Option<ChildStdout>,
    /// This process's end of the child's error pipe.
    pub stderr: Option<ChildStderr>,
}

impl fmt::Debug for Child {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Child")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl Child {
    /// The child's process identifier.
    #[must_use]
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Terminates the child; succeeds when it has already exited.
    ///
    /// # Errors
    /// Fails when the child is running and cannot be terminated.
    pub fn kill(&mut self) -> io::Result<()> {
        if self.status.is_some() {
            return Ok(());
        }
        // SAFETY: the owned process handle.
        if unsafe { TerminateProcess(self.process.as_raw_handle(), 1) } == 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(ERROR_ACCESS_DENIED.cast_signed())
                && self.try_wait()?.is_some()
            {
                return Ok(());
            }
            return Err(error);
        }
        Ok(())
    }

    fn observe(&mut self, timeout: u32) -> io::Result<Option<ExitStatus>> {
        if let Some(status) = self.status {
            return Ok(Some(status));
        }
        // SAFETY: the owned process handle.
        match unsafe { WaitForSingleObject(self.process.as_raw_handle(), timeout) } {
            WAIT_OBJECT_0 => {}
            WAIT_TIMEOUT => return Ok(None),
            WAIT_FAILED => return Err(io::Error::last_os_error()),
            _ => return Err(io::Error::other("unexpected process wait result")),
        }
        let mut code = 0;
        // SAFETY: the owned, exited process handle and a writable code.
        if unsafe { GetExitCodeProcess(self.process.as_raw_handle(), &raw mut code) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let status = ExitStatus::from_raw(code);
        self.status = Some(status);
        Ok(Some(status))
    }

    /// Waits for the child to exit, closing its input first.
    ///
    /// # Errors
    /// Fails when the process handle cannot be waited on.
    pub fn wait(&mut self) -> io::Result<ExitStatus> {
        drop(self.stdin.take());
        self.observe(INFINITE)?
            .ok_or_else(|| io::Error::other("an infinite wait timed out"))
    }

    /// The child's exit status, if it has exited.
    ///
    /// # Errors
    /// Fails when the process handle cannot be waited on.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.observe(0)
    }

    /// Waits for the child to exit and collects its piped output.
    ///
    /// # Errors
    /// Fails when reading output or waiting fails.
    pub fn wait_with_output(mut self) -> io::Result<Output> {
        drop(self.stdin.take());
        let (stdout, stderr) = (self.stdout.take(), self.stderr.take());
        let (stdout, stderr) = std::thread::scope(|scope| {
            let error = scope.spawn(move || read_all(stderr));
            let output = read_all(stdout);
            let error = error
                .join()
                .unwrap_or_else(|_| Err(io::Error::other("the stderr reader panicked")));
            (output, error)
        });
        let status = self.wait()?;
        Ok(Output {
            status,
            stdout: stdout?,
            stderr: stderr?,
        })
    }
}

fn read_all(pipe: Option<impl io::Read>) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    if let Some(mut pipe) = pipe {
        pipe.read_to_end(&mut bytes)?;
    }
    Ok(bytes)
}

impl AsRawHandle for Child {
    fn as_raw_handle(&self) -> RawHandle {
        self.process.as_raw_handle()
    }
}

impl AsHandle for Child {
    fn as_handle(&self) -> BorrowedHandle<'_> {
        self.process.as_handle()
    }
}

impl IntoRawHandle for Child {
    fn into_raw_handle(self) -> RawHandle {
        self.process.into_raw_handle()
    }
}

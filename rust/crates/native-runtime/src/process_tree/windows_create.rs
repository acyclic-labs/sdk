//! Windows process creation for process trees.
//!
//! One `CreateProcessW` call starts each tree's first member, suspended, with
//! exactly what the tree needs and nothing else:
//!
//! - **Program, command line, environment and directory** follow
//!   `std::process::Command::spawn` exactly (program search, `.exe` rules,
//!   argument quoting, the batch-file rules that answer `BatBadBut`, NUL
//!   refusal, the environment block and its ordering). A differential test
//!   holds this against the standard library's own spawns.
//! - **Handle inheritance** is limited by `PROC_THREAD_ATTRIBUTE_HANDLE_LIST`
//!   to the child's three standard handles. Those are inheritable only while
//!   the spawner thread creates the child, so no other process this process
//!   creates through this module can receive them. A concurrent
//!   `std::process::Command` spawn elsewhere in this process, which inherits
//!   every inheritable handle, can still receive them during that call: that
//!   is the Windows inheritance model, which no list on our side can change.
//! - **Containment** starts at creation: `PROC_THREAD_ATTRIBUTE_JOB_LIST`
//!   puts the process in the tree's Job before any of its code runs.
//! - **Console.** Every child is attached to one pseudoconsole that this
//!   owner process creates the first time it starts a tree. See [`console`].
mod console;

use super::command::{Arg, ProcessCommand, Stdio};
use std::ffi::{OsStr, OsString};
use std::io;
use std::os::windows::ffi::{OsStrExt as _, OsStringExt as _};
use std::os::windows::io::{
    AsHandle, AsRawHandle, BorrowedHandle, FromRawHandle as _, OwnedHandle,
};
use std::os::windows::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::{ChildStderr, ChildStdin, ChildStdout, ExitStatus};
use windows_sys::Win32::Foundation::{
    DUPLICATE_SAME_ACCESS, DuplicateHandle, ERROR_ACCESS_DENIED, GENERIC_READ, GENERIC_WRITE,
    HANDLE, HANDLE_FLAG_INHERIT, INVALID_HANDLE_VALUE, SetHandleInformation, SetLastError,
    WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED, FILE_SHARE_DELETE,
    FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_WRITE_ATTRIBUTES, GetFileAttributesW, GetFullPathNameW,
    INVALID_FILE_ATTRIBUTES, OPEN_EXISTING, PIPE_ACCESS_INBOUND, PIPE_ACCESS_OUTBOUND,
};
use windows_sys::Win32::System::Console::{
    GetStdHandle, STD_ERROR_HANDLE, STD_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
};
use windows_sys::Win32::System::Pipes::{
    CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE, PIPE_WAIT,
};
use windows_sys::Win32::System::SystemInformation::{GetSystemDirectoryW, GetWindowsDirectoryW};
use windows_sys::Win32::System::Threading::{
    CREATE_NEW_PROCESS_GROUP, CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, CreateProcessW,
    DeleteProcThreadAttributeList, EXTENDED_STARTUPINFO_PRESENT, GetCurrentProcess,
    GetExitCodeProcess, INFINITE, InitializeProcThreadAttributeList, LPPROC_THREAD_ATTRIBUTE_LIST,
    PROC_THREAD_ATTRIBUTE_HANDLE_LIST, PROC_THREAD_ATTRIBUTE_JOB_LIST,
    PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, PROCESS_INFORMATION, STARTF_USESTDHANDLES, STARTUPINFOEXW,
    TerminateProcess, UpdateProcThreadAttribute, WaitForSingleObject,
};

/// One ASCII byte as a UTF-16 unit.
const fn wide(byte: u8) -> u16 {
    u16::from_le_bytes([byte, 0])
}
const SEP: u16 = wide(b'\\');
const ALT_SEP: u16 = wide(b'/');
const QUOTE: u16 = wide(b'"');
const COLON: u16 = wide(b':');
const QUERY: u16 = wide(b'?');

/// The first member of a tree: its process handle and the tree's ends of
/// its piped standard streams.
pub(in crate::process_tree) struct Child {
    process: OwnedHandle,
    pub(in crate::process_tree) stdin: Option<ChildStdin>,
    pub(in crate::process_tree) stdout: Option<ChildStdout>,
    pub(in crate::process_tree) stderr: Option<ChildStderr>,
}

impl Child {
    /// The process ID, which only tests need.
    #[cfg(test)]
    pub(in crate::process_tree) fn id(&self) -> u32 {
        // SAFETY: the owned process handle.
        unsafe { windows_sys::Win32::System::Threading::GetProcessId(self.process.as_raw_handle()) }
    }

    fn exit_code(&self) -> io::Result<ExitStatus> {
        let mut code = 0;
        // SAFETY: the owned process handle and a live output.
        if unsafe { GetExitCodeProcess(self.process.as_raw_handle(), &raw mut code) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(ExitStatus::from_raw(code))
    }

    /// The exit status once the process has exited, without waiting.
    pub(in crate::process_tree) fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        // SAFETY: a zero wait on the owned process handle.
        match unsafe { WaitForSingleObject(self.process.as_raw_handle(), 0) } {
            WAIT_OBJECT_0 => self.exit_code().map(Some),
            WAIT_TIMEOUT => Ok(None),
            _ => Err(io::Error::last_os_error()),
        }
    }

    /// Closes the input pipe, then waits for the process to exit.
    pub(in crate::process_tree) fn wait(&mut self) -> io::Result<ExitStatus> {
        drop(self.stdin.take());
        // SAFETY: an unbounded wait on the owned process handle.
        if unsafe { WaitForSingleObject(self.process.as_raw_handle(), INFINITE) } != WAIT_OBJECT_0 {
            return Err(io::Error::last_os_error());
        }
        self.exit_code()
    }

    /// Terminates the process; one that has already exited is not an error.
    pub(in crate::process_tree) fn kill(&mut self) -> io::Result<()> {
        // SAFETY: the owned process handle.
        if unsafe { TerminateProcess(self.process.as_raw_handle(), 1) } == 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_ACCESS_DENIED.cast_signed())
                || self.try_wait()?.is_none()
            {
                return Err(error);
            }
        }
        Ok(())
    }
}

impl AsRawHandle for Child {
    fn as_raw_handle(&self) -> std::os::windows::io::RawHandle {
        self.process.as_raw_handle()
    }
}

impl AsHandle for Child {
    fn as_handle(&self) -> BorrowedHandle<'_> {
        self.process.as_handle()
    }
}

/// What `CreateProcessW` receives for a command: the application name, the
/// command line and the optional environment block and directory, each
/// NUL-terminated.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::process_tree) struct Plan {
    pub(in crate::process_tree) application: Vec<u16>,
    pub(in crate::process_tree) command_line: Vec<u16>,
    environment: Option<Vec<u16>>,
    directory: Option<Vec<u16>>,
}

/// Plans `command` exactly as `std::process::Command::spawn` would.
pub(in crate::process_tree) fn plan(command: &ProcessCommand) -> io::Result<Plan> {
    let environment = command.env.capture_if_changed();
    let child_paths = if command.env.changed_path() {
        environment.as_ref().and_then(|environment| {
            environment
                .get(&super::command::EnvKey::new(OsStr::new("PATH")))
                .cloned()
        })
    } else {
        None
    };
    let program = resolve_exe(
        &command.program,
        || std::env::var_os("PATH"),
        child_paths.as_deref(),
    )?;
    let is_batch_file = if is_verbatim(&program) {
        has_bat_extension(program.strip_suffix(&[0]).unwrap_or(&program))
    } else {
        has_bat_extension(&full_path_name(&program)?)
    };
    let (application, mut command_line) = if is_batch_file {
        (
            command_prompt()?,
            make_bat_command_line(&program, &command.args)?,
        )
    } else {
        (program, make_command_line(&command.program, &command.args)?)
    };
    command_line.push(0);
    Ok(Plan {
        application,
        command_line,
        environment: environment.map(make_environment).transpose()?,
        directory: command
            .cwd
            .as_deref()
            .map(|directory| make_directory(directory.as_os_str()))
            .transpose()?,
    })
}

fn ensure_no_nuls(text: &OsStr) -> io::Result<()> {
    if text.encode_wide().any(|unit| unit == 0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "nul byte found in provided data",
        ));
    }
    Ok(())
}

/// `text` as UTF-16 with a terminating NUL.
fn to_u16s(text: &OsStr) -> io::Result<Vec<u16>> {
    let mut wide: Vec<u16> = text.encode_wide().collect();
    if wide.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "strings passed to WinAPI cannot contain NULs",
        ));
    }
    wide.push(0);
    Ok(wide)
}

/// Calls a Windows string-returning function with a growing buffer and
/// returns the string it wrote, without its NUL.
fn fill_utf16(mut call: impl FnMut(*mut u16, u32) -> u32) -> io::Result<Vec<u16>> {
    let mut buffer = vec![0_u16; 512];
    loop {
        let capacity = u32::try_from(buffer.len()).unwrap_or(u32::MAX);
        // SAFETY: resetting the calling thread's last error has no preconditions.
        unsafe { SetLastError(0) };
        let written = call(buffer.as_mut_ptr(), capacity);
        if written == 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(0) {
                return Err(error);
            }
            return Ok(Vec::new());
        }
        let written = written as usize;
        if written < buffer.len() {
            buffer.truncate(written);
            return Ok(buffer);
        }
        let grown = written.max(buffer.len().saturating_mul(2));
        buffer.resize(grown, 0);
    }
}

/// `GetFullPathNameW` of a NUL-terminated path.
fn full_path_name(path: &[u16]) -> io::Result<Vec<u16>> {
    fill_utf16(|buffer, size| {
        // SAFETY: `path` is NUL-terminated and `buffer` holds `size` units.
        unsafe { GetFullPathNameW(path.as_ptr(), size, buffer, std::ptr::null_mut()) }
    })
}

fn is_verbatim(path: &[u16]) -> bool {
    path.starts_with(&[SEP, SEP, QUERY, SEP]) || path.starts_with(&[SEP, QUERY, QUERY, SEP])
}

const fn is_sep(unit: u16) -> bool {
    unit == SEP || unit == ALT_SEP
}

fn has_bat_extension(program: &[u16]) -> bool {
    matches!(
        program
            .len()
            .checked_sub(4)
            .and_then(|start| program.get(start..)),
        Some([46, 98 | 66, 97 | 65, 116 | 84] | [46, 99 | 67, 109 | 77, 100 | 68])
    )
}

/// A NUL-terminated path that can exceed the legacy path limit, without a
/// verbatim prefix unless it is long.
fn get_long_path(mut path: Vec<u16>) -> io::Result<Vec<u16>> {
    const LEGACY_MAX_PATH: usize = 248;
    const VERBATIM: [u16; 4] = [SEP, SEP, QUERY, SEP];
    const UNC: [u16; 8] = [
        SEP,
        SEP,
        QUERY,
        SEP,
        wide(b'U'),
        wide(b'N'),
        wide(b'C'),
        SEP,
    ];
    if is_verbatim(&path) || path == [0] {
        return Ok(path);
    }
    if path.len() < LEGACY_MAX_PATH {
        match path.as_slice() {
            [drive, COLON, 0] | [drive, COLON, SEP | ALT_SEP, ..] if !is_sep(*drive) => {
                return Ok(path);
            }
            [SEP | ALT_SEP, SEP | ALT_SEP, ..] => return Ok(path),
            _ => {}
        }
    }
    let absolute = full_path_name(&path)?;
    path.clear();
    let mut absolute = absolute.as_slice();
    if absolute.len() + 1 >= LEGACY_MAX_PATH {
        let prefix: &[u16] = match absolute {
            [_, COLON, SEP, ..] => &VERBATIM,
            [SEP, SEP, dot, SEP, ..] if *dot == wide(b'.') => {
                absolute = absolute.get(4..).unwrap_or_default();
                &VERBATIM
            }
            [SEP, SEP, QUERY, SEP, ..] | [SEP, QUERY, QUERY, SEP, ..] => &[],
            [SEP, SEP, ..] => {
                absolute = absolute.get(2..).unwrap_or_default();
                &UNC
            }
            _ => &[],
        };
        path.extend_from_slice(prefix);
    }
    path.extend_from_slice(absolute);
    path.push(0);
    Ok(path)
}

/// A NUL-terminated path without a verbatim prefix where removing it does
/// not change what the path names.
fn to_user_path(path: &Path) -> io::Result<Vec<u16>> {
    const LEGACY_MAX_PATH: usize = 260;
    let mut path = to_u16s(path.as_os_str())?;
    if path.len() > LEGACY_MAX_PATH {
        return Ok(path);
    }
    let strip = |path: Vec<u16>, start: usize| -> io::Result<Vec<u16>> {
        let rest = path.get(start..).unwrap_or_default();
        let full = full_path_name(rest)?;
        if full.as_slice() == rest.get(..rest.len().saturating_sub(1)).unwrap_or_default() {
            let mut full = full;
            full.push(0);
            Ok(full)
        } else {
            Ok(path)
        }
    };
    match path.as_slice() {
        [SEP, SEP, QUERY, SEP, _, COLON, SEP, ..] => strip(path, 4),
        [SEP, SEP, QUERY, SEP, u, n, c, SEP, ..]
            if [*u, *n, *c] == [wide(b'U'), wide(b'N'), wide(b'C')] =>
        {
            if let Some(unit) = path.get_mut(6) {
                *unit = SEP;
            }
            let stripped = strip(path, 6)?;
            if stripped.get(..2) == Some(&[SEP, SEP]) && stripped.get(2) != Some(&QUERY) {
                return Ok(stripped);
            }
            let mut restored = stripped;
            if let Some(unit) = restored.get_mut(6) {
                *unit = wide(b'C');
            }
            Ok(restored)
        }
        _ => get_long_path(path),
    }
}

/// The path, NUL-terminated, if something exists there (links not followed).
fn program_exists(path: &Path) -> Option<Vec<u16>> {
    let path = to_user_path(path).ok()?;
    // SAFETY: a NUL-terminated path.
    (unsafe { GetFileAttributesW(path.as_ptr()) } != INVALID_FILE_ATTRIBUTES).then_some(path)
}

fn resolve_exe(
    exe_path: &OsStr,
    parent_paths: impl FnOnce() -> Option<OsString>,
    child_paths: Option<&OsStr>,
) -> io::Result<Vec<u16>> {
    let bytes = exe_path.as_encoded_bytes();
    let has_trailing_slash = {
        let verbatim = bytes.starts_with(br"\\?\");
        bytes
            .last()
            .is_some_and(|last| *last == b'\\' || (!verbatim && *last == b'/'))
    };
    if exe_path.is_empty() || has_trailing_slash {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "program path has no file name",
        ));
    }
    let suffix = std::env::consts::EXE_SUFFIX.as_bytes();
    let has_exe_suffix = bytes.len() >= suffix.len()
        && bytes
            .get(bytes.len() - suffix.len()..)
            .is_some_and(|end| end.eq_ignore_ascii_case(suffix));
    let is_file_name = !bytes.iter().any(|byte| *byte == b'\\' || *byte == b'/');
    if !is_file_name {
        if has_exe_suffix {
            return to_user_path(Path::new(exe_path));
        }
        let mut with_suffix = exe_path.to_owned();
        with_suffix.push(std::env::consts::EXE_SUFFIX);
        let mut path = PathBuf::from(with_suffix);
        if let Some(path) = program_exists(&path) {
            return Ok(path);
        }
        path.set_extension("");
        return to_user_path(&path);
    }
    ensure_no_nuls(exe_path)?;
    let has_extension = bytes.contains(&b'.');
    let mut exists = |mut directory: PathBuf| {
        directory.push(exe_path);
        if !has_extension {
            directory.set_extension(std::env::consts::EXE_EXTENSION);
        }
        program_exists(&directory)
    };
    let search = |paths: &OsStr, exists: &mut dyn FnMut(PathBuf) -> Option<Vec<u16>>| {
        std::env::split_paths(paths)
            .filter(|path| !path.as_os_str().is_empty())
            .find_map(&mut *exists)
    };
    let found = child_paths
        .and_then(|paths| search(paths, &mut exists))
        .or_else(|| {
            let mut application = std::env::current_exe().ok()?;
            application.pop();
            exists(application)
        })
        .or_else(|| {
            let system = fill_utf16(|buffer, size| {
                // SAFETY: `buffer` holds `size` units.
                unsafe { GetSystemDirectoryW(buffer, size) }
            })
            .ok()?;
            exists(PathBuf::from(OsString::from_wide(&system)))
        })
        .or_else(|| {
            let windows = fill_utf16(|buffer, size| {
                // SAFETY: `buffer` holds `size` units.
                unsafe { GetWindowsDirectoryW(buffer, size) }
            })
            .ok()?;
            exists(PathBuf::from(OsString::from_wide(&windows)))
        })
        .or_else(|| search(&parent_paths()?, &mut exists));
    found.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "program not found"))
}

/// `cmd.exe` in the system directory, NUL-terminated.
fn command_prompt() -> io::Result<Vec<u16>> {
    let mut system = fill_utf16(|buffer, size| {
        // SAFETY: `buffer` holds `size` units.
        unsafe { GetSystemDirectoryW(buffer, size) }
    })?;
    system.extend("\\cmd.exe".encode_utf16().chain([0]));
    Ok(system)
}

/// The command line for an executable, without its NUL: the program as
/// given, always quoted, then each argument as `CommandLineToArgvW` and the
/// C runtime parse it back.
pub(in crate::process_tree) fn make_command_line(
    argv0: &OsStr,
    args: &[Arg],
) -> io::Result<Vec<u16>> {
    let mut line = vec![QUOTE];
    line.extend(argv0.encode_wide());
    line.push(QUOTE);
    for arg in args {
        line.push(wide(b' '));
        append_arg(&mut line, arg)?;
    }
    Ok(line)
}

fn append_arg(line: &mut Vec<u16>, arg: &Arg) -> io::Result<()> {
    let (text, escape) = match arg {
        Arg::Regular(text) => (text, true),
        Arg::Raw(text) => (text, false),
    };
    ensure_no_nuls(text)?;
    let bytes = text.as_encoded_bytes();
    let quote =
        escape && (bytes.is_empty() || bytes.iter().any(|byte| matches!(byte, b' ' | b'\t')));
    if quote {
        line.push(QUOTE);
    }
    let mut backslashes = 0_usize;
    for unit in text.encode_wide() {
        if escape {
            if unit == SEP {
                backslashes += 1;
            } else {
                if unit == QUOTE {
                    // n backslashes before a quote become 2n+1.
                    line.extend(std::iter::repeat_n(SEP, backslashes + 1));
                }
                backslashes = 0;
            }
        }
        line.push(unit);
    }
    if quote {
        // n backslashes before the closing quote become 2n.
        line.extend(std::iter::repeat_n(SEP, backslashes));
        line.push(QUOTE);
    }
    Ok(())
}

fn append_bat_arg(line: &mut Vec<u16>, text: &OsStr) -> io::Result<()> {
    ensure_no_nuls(text)?;
    let mut quote = text.is_empty() || text.encode_wide().last() == Some(SEP);
    const UNQUOTED: &str = r"#$*+-./:?@\_";
    for character in char::decode_utf16(text.encode_wide()).filter_map(Result::ok) {
        let ascii_needs_quotes = character.is_ascii()
            && !(character.is_ascii_alphanumeric() || UNQUOTED.contains(character));
        if ascii_needs_quotes || character.is_control() {
            quote = true;
        }
    }
    if quote {
        line.push(QUOTE);
    }
    let mut backslashes = 0_usize;
    for unit in text.encode_wide() {
        if unit == SEP {
            backslashes += 1;
        } else {
            if unit == QUOTE {
                // n backslashes before a quote become 2n, and the quote doubles.
                line.extend(std::iter::repeat_n(SEP, backslashes));
                line.push(QUOTE);
            } else if unit == wide(b'%') || unit == wide(b'\r') {
                // `%%cd:~,%` expands to nothing, so cmd never sees `%NAME%`.
                line.extend("%%cd:~,".encode_utf16());
            }
            backslashes = 0;
        }
        line.push(unit);
    }
    if quote {
        line.extend(std::iter::repeat_n(SEP, backslashes));
        line.push(QUOTE);
    }
    Ok(())
}

/// The command line that runs a batch script through `cmd.exe`, without its
/// NUL. `script` is the resolved script path, NUL-terminated or not.
pub(in crate::process_tree) fn make_bat_command_line(
    script: &[u16],
    args: &[Arg],
) -> io::Result<Vec<u16>> {
    let mut line: Vec<u16> = "cmd.exe /e:ON /v:OFF /d /c \"".encode_utf16().collect();
    line.push(QUOTE);
    if script.contains(&QUOTE) || script.last() == Some(&SEP) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Windows file names may not contain `\"` or end with `\\`",
        ));
    }
    line.extend_from_slice(script.strip_suffix(&[0]).unwrap_or(script));
    line.push(QUOTE);
    for arg in args {
        line.push(wide(b' '));
        match arg {
            Arg::Regular(text) => {
                if text
                    .as_encoded_bytes()
                    .iter()
                    .any(|byte| matches!(byte, b'\r' | b'\n'))
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "batch file arguments are invalid",
                    ));
                }
                append_bat_arg(&mut line, text)?;
            }
            Arg::Raw(_) => append_arg(&mut line, arg)?,
        }
    }
    line.push(QUOTE);
    Ok(line)
}

fn make_environment(
    environment: std::collections::BTreeMap<super::command::EnvKey, OsString>,
) -> io::Result<Vec<u16>> {
    let mut block = Vec::new();
    if environment.is_empty() {
        block.push(0);
    }
    for (key, value) in environment {
        ensure_no_nuls(&key.name)?;
        ensure_no_nuls(&value)?;
        block.extend(key.wide);
        block.push(wide(b'='));
        block.extend(value.encode_wide());
        block.push(0);
    }
    block.push(0);
    Ok(block)
}

/// Whether `GetFullPathNameW` leaves a NUL-terminated path unchanged.
fn is_absolute_exact(path: &[u16]) -> bool {
    path.last() == Some(&0)
        && full_path_name(path)
            .is_ok_and(|full| full.as_slice() == path.get(..path.len() - 1).unwrap_or_default())
}

/// The working directory, NUL-terminated, without a verbatim prefix that
/// the current directory cannot take where removing it changes nothing.
fn make_directory(directory: &OsStr) -> io::Result<Vec<u16>> {
    ensure_no_nuls(directory)?;
    let mut path: Vec<u16> = directory.encode_wide().chain([0]).collect();
    let unc: Vec<u16> = r"\\?\UNC".encode_utf16().collect();
    let verbatim: Vec<u16> = r"\\?\".encode_utf16().collect();
    if path.starts_with(&unc) {
        // `\\?\UNC\server` is tried as `\\server`: its `C` becomes a separator.
        let start = unc.len() - 1;
        if let Some(unit) = path.get_mut(start) {
            *unit = SEP;
        }
        if is_absolute_exact(path.get(start..).unwrap_or_default()) {
            return Ok(path.split_off(start));
        }
        if let Some(unit) = path.get_mut(start) {
            *unit = wide(b'C');
        }
    } else if path.starts_with(&verbatim) {
        let start = verbatim.len();
        if is_absolute_exact(path.get(start..).unwrap_or_default()) {
            return Ok(path.split_off(start));
        }
    }
    Ok(path)
}

/// A handle that is inheritable only while the spawner thread creates the
/// child it is meant for.
fn uninheritable_duplicate(handle: HANDLE) -> io::Result<OwnedHandle> {
    let mut duplicate = std::ptr::null_mut();
    // SAFETY: duplicating a live handle within this process.
    if unsafe {
        DuplicateHandle(
            GetCurrentProcess(),
            handle,
            GetCurrentProcess(),
            &raw mut duplicate,
            0,
            0,
            DUPLICATE_SAME_ACCESS,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful duplication returns one new owned handle.
    Ok(unsafe { OwnedHandle::from_raw_handle(duplicate) })
}

fn open_file(name: &[u16], access: u32, share: u32, flags: u32) -> io::Result<OwnedHandle> {
    // SAFETY: a NUL-terminated name and no borrowed security attributes.
    let handle = unsafe {
        CreateFileW(
            name.as_ptr(),
            access,
            share,
            std::ptr::null(),
            OPEN_EXISTING,
            flags,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful open returns one new owned handle.
    Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
}

/// A pipe whose end for this process is overlapped (the capture's I/O
/// completion port reads and writes it) and whose end for the child is
/// synchronous, as console programs expect.
fn pipe(ours_readable: bool) -> io::Result<(OwnedHandle, OwnedHandle)> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SERIAL: AtomicU64 = AtomicU64::new(0);
    static KEY: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    // 64 KiB, as the standard library's pipes and a typical Linux pipe.
    const CAPACITY: u32 = 64 * 1024;
    let key = *KEY.get_or_init(|| {
        let mut bytes = [0; 8];
        // A name other processes cannot predict; the serial keeps names
        // unique within this process even without it.
        let _ = getrandom::fill(&mut bytes);
        u64::from_ne_bytes(bytes)
    });
    let name: Vec<u16> = format!(
        r"\\.\pipe\acyclic-tree-{}-{key:016x}-{}",
        std::process::id(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    )
    .encode_utf16()
    .chain([0])
    .collect();
    let access = if ours_readable {
        PIPE_ACCESS_INBOUND
    } else {
        PIPE_ACCESS_OUTBOUND
    };
    // SAFETY: a NUL-terminated name and no borrowed security attributes.
    let ours = unsafe {
        CreateNamedPipeW(
            name.as_ptr(),
            access | FILE_FLAG_FIRST_PIPE_INSTANCE | FILE_FLAG_OVERLAPPED,
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
    // SAFETY: a successful creation returns one new owned handle.
    let ours = unsafe { OwnedHandle::from_raw_handle(ours) };
    let theirs = if ours_readable {
        open_file(&name, GENERIC_WRITE, 0, 0)?
    } else {
        open_file(&name, GENERIC_READ | FILE_WRITE_ATTRIBUTES, 0, 0)?
    };
    Ok((ours, theirs))
}

/// The child's handle for one standard stream, and the tree's end of a pipe.
fn stdio(
    stdio: Option<&Stdio>,
    id: STD_HANDLE,
) -> io::Result<(Option<OwnedHandle>, Option<OwnedHandle>)> {
    match stdio.unwrap_or(&Stdio::Inherit) {
        Stdio::Inherit => {
            // SAFETY: reading a standard handle has no preconditions.
            let handle = unsafe { GetStdHandle(id) };
            if handle.is_null() || handle == INVALID_HANDLE_VALUE {
                return Ok((None, None));
            }
            Ok((Some(uninheritable_duplicate(handle)?), None))
        }
        Stdio::Null => {
            let name: Vec<u16> = r"\\.\NUL".encode_utf16().chain([0]).collect();
            let access = if id == STD_INPUT_HANDLE {
                GENERIC_READ
            } else {
                GENERIC_WRITE
            };
            let share = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
            Ok((Some(open_file(&name, access, share, 0)?), None))
        }
        Stdio::Piped => {
            let (ours, theirs) = pipe(id != STD_INPUT_HANDLE)?;
            Ok((Some(theirs), Some(ours)))
        }
        Stdio::Owned(handle) => Ok((Some(uninheritable_duplicate(handle.as_raw_handle())?), None)),
    }
}

/// The tree's Job, borrowed by the spawner thread while its caller waits.
struct JobHandle(HANDLE);

// SAFETY: a kernel handle has no thread affinity; the caller keeps the Job
// open until the spawner thread has answered.
unsafe impl Send for JobHandle {}

/// Starts `command` suspended, in `job` when one is given, attached to this
/// owner's shared pseudoconsole. The caller resumes it.
pub(in crate::process_tree) fn spawn_suspended(
    command: &ProcessCommand,
    job: Option<HANDLE>,
) -> io::Result<Child> {
    let mut plan = plan(command)?;
    let (stdin, ours_stdin) = stdio(
        command.stdin.as_ref().map(|stdio| &stdio.0),
        STD_INPUT_HANDLE,
    )?;
    let (stdout, ours_stdout) = stdio(
        command.stdout.as_ref().map(|stdio| &stdio.0),
        STD_OUTPUT_HANDLE,
    )?;
    let (stderr, ours_stderr) = stdio(
        command.stderr.as_ref().map(|stdio| &stdio.0),
        STD_ERROR_HANDLE,
    )?;
    let job = job.map(JobHandle);
    let process = super::spawner::run(move || {
        let job = job;
        create(
            &mut plan,
            [stdin, stdout, stderr],
            job.as_ref().map(|job| job.0),
        )
    })?;
    Ok(Child {
        process,
        stdin: ours_stdin.map(ChildStdin::from),
        stdout: ours_stdout.map(ChildStdout::from),
        stderr: ours_stderr.map(ChildStderr::from),
    })
}

/// One initialized attribute list, deleted when dropped.
struct Attributes {
    buffer: Vec<usize>,
}

impl Attributes {
    fn new(count: u32) -> io::Result<Self> {
        let mut size = 0;
        // SAFETY: a null list with a size output queries the required size.
        unsafe { InitializeProcThreadAttributeList(std::ptr::null_mut(), count, 0, &raw mut size) };
        let mut buffer = vec![0_usize; size.div_ceil(size_of::<usize>())];
        // SAFETY: the buffer holds at least the size the probe requested.
        if unsafe {
            InitializeProcThreadAttributeList(buffer.as_mut_ptr().cast(), count, 0, &raw mut size)
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { buffer })
    }

    fn list(&mut self) -> LPPROC_THREAD_ATTRIBUTE_LIST {
        self.buffer.as_mut_ptr().cast()
    }

    /// Adds one attribute. `value` must outlive the process creation that
    /// uses this list.
    fn set<T>(&mut self, attribute: u32, value: &[T]) -> io::Result<()> {
        // SAFETY: the initialized list and a value the caller keeps alive
        // until creation returns.
        if unsafe {
            UpdateProcThreadAttribute(
                self.list(),
                0,
                attribute as usize,
                value.as_ptr().cast(),
                size_of_val(value),
                std::ptr::null_mut(),
                std::ptr::null(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Drop for Attributes {
    fn drop(&mut self) {
        // SAFETY: a constructed value always holds an initialized list.
        unsafe { DeleteProcThreadAttributeList(self.list()) };
    }
}

/// Creates the process on the spawner thread. The standard handles are
/// inheritable from here until they are closed when this returns.
fn create(
    plan: &mut Plan,
    standard: [Option<OwnedHandle>; 3],
    job: Option<HANDLE>,
) -> io::Result<OwnedHandle> {
    let raw = standard.each_ref().map(|handle| {
        handle
            .as_ref()
            .map_or(std::ptr::null_mut(), AsRawHandle::as_raw_handle)
    });
    let inherited: Vec<HANDLE> = raw.iter().copied().filter(|raw| !raw.is_null()).collect();
    for handle in &inherited {
        // SAFETY: each is a live handle owned by `standard`.
        if unsafe { SetHandleInformation(*handle, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT) } == 0 {
            return Err(io::Error::last_os_error());
        }
    }
    let console = console::shared()?;
    let jobs = job.map(|job| [job]);
    let mut attributes = Attributes::new(3)?;
    if !inherited.is_empty() {
        attributes.set(PROC_THREAD_ATTRIBUTE_HANDLE_LIST, &inherited)?;
    }
    if let Some(jobs) = &jobs {
        attributes.set(PROC_THREAD_ATTRIBUTE_JOB_LIST, jobs)?;
    }
    // The attribute's value is the console handle itself.
    // SAFETY: setting the pseudoconsole attribute passes the handle by value
    // in the pointer argument, as documented; the console lives for the
    // life of this process.
    if unsafe {
        UpdateProcThreadAttribute(
            attributes.list(),
            0,
            PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE as usize,
            console as *const std::ffi::c_void,
            size_of::<isize>(),
            std::ptr::null_mut(),
            std::ptr::null(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: zero is the documented initial state of the startup record.
    let mut startup: STARTUPINFOEXW = unsafe { std::mem::zeroed() };
    startup.StartupInfo.cb =
        u32::try_from(size_of::<STARTUPINFOEXW>()).map_err(|_| io::ErrorKind::InvalidInput)?;
    if !inherited.is_empty() {
        startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
        [
            startup.StartupInfo.hStdInput,
            startup.StartupInfo.hStdOutput,
            startup.StartupInfo.hStdError,
        ] = raw;
    }
    startup.lpAttributeList = attributes.list();
    // SAFETY: zero is the documented initial state of the output.
    let mut process: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };
    let environment = plan
        .environment
        .as_ref()
        .map_or(std::ptr::null(), |block| block.as_ptr().cast());
    let directory = plan
        .directory
        .as_ref()
        .map_or(std::ptr::null(), Vec::as_ptr);
    // SAFETY: every string is NUL-terminated and, with the attribute list
    // and the handles it names, outlives the call.
    let created = unsafe {
        CreateProcessW(
            plan.application.as_ptr(),
            plan.command_line.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            i32::from(!inherited.is_empty()),
            CREATE_SUSPENDED
                | CREATE_NEW_PROCESS_GROUP
                | EXTENDED_STARTUPINFO_PRESENT
                | CREATE_UNICODE_ENVIRONMENT,
            environment,
            directory,
            &raw const startup.StartupInfo,
            &raw mut process,
        )
    };
    if created == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful creation transfers both handles to this process;
    // the tree resumes the process through its process handle.
    let (process_handle, _thread) = unsafe {
        (
            OwnedHandle::from_raw_handle(process.hProcess),
            OwnedHandle::from_raw_handle(process.hThread),
        )
    };
    drop(standard);
    Ok(process_handle)
}

#[cfg(test)]
#[path = "windows_create/tests.rs"]
mod tests;

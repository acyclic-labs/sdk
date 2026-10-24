//! Program resolution and command-line encoding, ported from the Rust
//! standard library's Windows process implementation (1.98) so a
//! [`super::Command`] starts exactly what `std::process::Command` would:
//! the same `PATH` search and `.exe` rules, the same argument quoting, and
//! the same batch-file escaping through `cmd.exe` (the CVE-2024-24576 fix).

use std::ffi::{OsStr, OsString};
use std::io;
use std::os::windows::ffi::{OsStrExt as _, OsStringExt as _};
use std::path::{Path, PathBuf};
use windows_sys::Win32::Storage::FileSystem::{
    GetFileAttributesW, GetFullPathNameW, INVALID_FILE_ATTRIBUTES,
};
use windows_sys::Win32::System::SystemInformation::{GetSystemDirectoryW, GetWindowsDirectoryW};

/// One argument: quoted by the standard rules, or appended verbatim.
#[derive(Clone, Debug)]
pub(super) enum Arg {
    Regular(OsString),
    Raw(OsString),
}

impl Arg {
    pub(super) fn as_os_str(&self) -> &OsStr {
        match self {
            Self::Regular(arg) | Self::Raw(arg) => arg,
        }
    }
}

const SEP: u16 = b'\\' as u16;
const ALT_SEP: u16 = b'/' as u16;
const QUERY: u16 = b'?' as u16;
const COLON: u16 = b':' as u16;
const DOT: u16 = b'.' as u16;
const QUOTE: u16 = b'"' as u16;
const SPACE: u16 = b' ' as u16;
const PERCENT: u16 = b'%' as u16;
const CARRIAGE_RETURN: u16 = b'\r' as u16;
/// `\\?\UNC\`, the verbatim prefix of a network path.
const VERBATIM_UNC: [u16; 8] = [
    SEP,
    SEP,
    QUERY,
    SEP,
    b'U' as u16,
    b'N' as u16,
    b'C' as u16,
    SEP,
];
const EXE_SUFFIX: &str = ".exe";

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn ensure_no_nuls(value: &OsStr) -> io::Result<&OsStr> {
    if value.encode_wide().any(|unit| unit == 0) {
        Err(invalid("strings passed to WinAPI cannot contain NULs"))
    } else {
        Ok(value)
    }
}

/// `value` as a NUL-terminated wide string.
pub(super) fn to_wide(value: &OsStr) -> io::Result<Vec<u16>> {
    Ok(ensure_no_nuls(value)?.encode_wide().chain([0]).collect())
}

/// Calls a Win32 function that fills a wide buffer and returns the length
/// it wrote, or the size it needs when the buffer is too small.
fn fill(mut call: impl FnMut(*mut u16, u32) -> u32) -> io::Result<Vec<u16>> {
    let mut buffer = vec![0u16; 512];
    loop {
        let size = u32::try_from(buffer.len()).map_err(|_| invalid("path is too long"))?;
        let written = call(buffer.as_mut_ptr(), size);
        if written == 0 {
            return Err(io::Error::last_os_error());
        }
        let written = usize::try_from(written).map_err(|_| invalid("path is too long"))?;
        if written < buffer.len() {
            buffer.truncate(written);
            return Ok(buffer);
        }
        buffer.resize(written, 0);
    }
}

fn full_path(path: &[u16]) -> io::Result<Vec<u16>> {
    // SAFETY: `path` is NUL-terminated and the buffer holds `size` units.
    fill(|buffer, size| unsafe {
        GetFullPathNameW(path.as_ptr(), size, buffer, std::ptr::null_mut())
    })
}

/// Whether NUL-terminated `path` is absolute and unchanged by
/// normalization, so a verbatim prefix can be removed without changing it.
pub(super) fn is_absolute_exact(path: &[u16]) -> bool {
    match path.split_last() {
        Some((0, rest)) if !rest.is_empty() => full_path(path).is_ok_and(|full| full == rest),
        _ => false,
    }
}

fn is_verbatim(path: &[u16]) -> bool {
    path.starts_with(&[SEP, SEP, QUERY, SEP]) || path.starts_with(&[SEP, QUERY, QUERY, SEP])
}

/// A normalized path that can bypass `MAX_PATH`, prefixed only when long.
fn long_path(mut path: Vec<u16>) -> io::Result<Vec<u16>> {
    const LEGACY_MAX_PATH: usize = 248;
    if is_verbatim(&path) || path == [0] {
        return Ok(path);
    }
    if path.len() < LEGACY_MAX_PATH {
        match path.as_slice() {
            [drive, COLON, 0] | [drive, COLON, SEP | ALT_SEP, ..]
                if *drive != SEP && *drive != ALT_SEP =>
            {
                return Ok(path);
            }
            [SEP | ALT_SEP, SEP | ALT_SEP, ..] => return Ok(path),
            _ => {}
        }
    }
    let absolute = full_path(&path)?;
    let mut absolute = absolute.as_slice();
    path.clear();
    if absolute.len() + 1 >= LEGACY_MAX_PATH {
        let prefix: &[u16] = match absolute {
            [_, COLON, SEP, ..] => &[SEP, SEP, QUERY, SEP],
            [SEP, SEP, DOT, SEP, rest @ ..] => {
                absolute = rest;
                &[SEP, SEP, QUERY, SEP]
            }
            [SEP, SEP, QUERY, SEP, ..] | [SEP, QUERY, QUERY, SEP, ..] => &[],
            [SEP, SEP, rest @ ..] => {
                absolute = rest;
                &VERBATIM_UNC
            }
            _ => &[],
        };
        path.extend_from_slice(prefix);
    }
    path.extend_from_slice(absolute);
    path.push(0);
    Ok(path)
}

/// NUL-terminated `user` when normalization leaves it unchanged.
fn unprefixed(user: Vec<u16>) -> io::Result<Option<Vec<u16>>> {
    let full = full_path(&user)?;
    Ok((user.strip_suffix(&[0]) == Some(full.as_slice())).then_some(user))
}

/// `path` without a verbatim prefix where that leaves its meaning intact,
/// since `cmd.exe` and `CreateProcessW` do not take verbatim paths.
pub(super) fn to_user_path(path: &Path) -> io::Result<Vec<u16>> {
    let path = to_wide(path.as_os_str())?;
    const LEGACY_MAX_PATH: usize = 260;
    if path.len() > LEGACY_MAX_PATH {
        return Ok(path);
    }
    if matches!(path.as_slice(), [SEP, SEP, QUERY, SEP, _, COLON, SEP, ..]) {
        // `\\?\C:\x` is `C:\x` when normalization leaves that unchanged.
        let user = path.get(4..).unwrap_or_default().to_vec();
        Ok(unprefixed(user)?.unwrap_or(path))
    } else if path.starts_with(&VERBATIM_UNC) {
        // `\\?\UNC\server\share` is `\\server\share` likewise.
        let user = [SEP, SEP]
            .into_iter()
            .chain(
                path.get(VERBATIM_UNC.len()..)
                    .unwrap_or_default()
                    .iter()
                    .copied(),
            )
            .collect();
        Ok(unprefixed(user)?.unwrap_or(path))
    } else {
        long_path(path)
    }
}

fn is_separator(byte: u8) -> bool {
    byte == b'/' || byte == b'\\'
}

fn has_trailing_slash(path: &OsStr) -> bool {
    let bytes = path.as_encoded_bytes();
    let verbatim = bytes.starts_with(br"\\?\");
    bytes.last().is_some_and(|&last| {
        if verbatim {
            last == b'\\'
        } else {
            is_separator(last)
        }
    })
}

fn program_exists(path: &Path) -> Option<Vec<u16>> {
    let path = to_user_path(path).ok()?;
    // SAFETY: a NUL-terminated path; reading attributes follows no link.
    (unsafe { GetFileAttributesW(path.as_ptr()) } != INVALID_FILE_ATTRIBUTES).then_some(path)
}

fn system_directory(call: unsafe extern "system" fn(*mut u16, u32) -> u32) -> Option<PathBuf> {
    // SAFETY: the buffer holds `size` units.
    let directory = fill(|buffer, size| unsafe { call(buffer, size) }).ok()?;
    Some(PathBuf::from(OsString::from_wide(&directory)))
}

/// Resolves `program` as `std::process::Command` does: a bare file name is
/// searched for in the child's `PATH` (when the command changed it), the
/// application directory, the system directories, then this process's
/// `PATH`, with `.exe` appended when it has no extension; a path is used as
/// given, preferring an `.exe` beside it.
pub(super) fn resolve(program: &OsStr, child_path: Option<&OsStr>) -> io::Result<Vec<u16>> {
    if program.is_empty() || has_trailing_slash(program) {
        return Err(invalid("program path has no file name"));
    }
    let bytes = program.as_encoded_bytes();
    let has_exe_suffix = bytes
        .len()
        .checked_sub(EXE_SUFFIX.len())
        .and_then(|start| bytes.get(start..))
        .is_some_and(|suffix| suffix.eq_ignore_ascii_case(EXE_SUFFIX.as_bytes()));
    if bytes.iter().copied().any(is_separator) {
        if has_exe_suffix {
            return to_user_path(Path::new(program));
        }
        let mut with_suffix = program.to_os_string();
        with_suffix.push(EXE_SUFFIX);
        if let Some(path) = program_exists(Path::new(&with_suffix)) {
            return Ok(path);
        }
        return to_user_path(Path::new(program));
    }
    ensure_no_nuls(program)?;
    let has_extension = bytes.contains(&b'.');
    let exists = |mut directory: PathBuf| {
        directory.push(program);
        if !has_extension {
            directory.set_extension("exe");
        }
        program_exists(&directory)
    };
    let mut directories: Vec<PathBuf> = Vec::new();
    if let Some(paths) = child_path {
        directories
            .extend(std::env::split_paths(paths).filter(|path| !path.as_os_str().is_empty()));
    }
    if let Ok(mut application) = std::env::current_exe() {
        application.pop();
        directories.push(application);
    }
    directories.extend(system_directory(GetSystemDirectoryW));
    directories.extend(system_directory(GetWindowsDirectoryW));
    if let Some(paths) = std::env::var_os("PATH") {
        directories
            .extend(std::env::split_paths(&paths).filter(|path| !path.as_os_str().is_empty()));
    }
    directories
        .into_iter()
        .find_map(exists)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "program not found"))
}

/// Whether the resolved program is a batch file, which runs through
/// `cmd.exe` with its own escaping.
pub(super) fn is_batch_file(program: &[u16]) -> io::Result<bool> {
    let has_batch_extension = |name: &[u16]| {
        matches!(
            name.len()
                .checked_sub(4)
                .and_then(|start| name.get(start..)),
            Some([46, 98 | 66, 97 | 65, 116 | 84] | [46, 99 | 67, 109 | 77, 100 | 68])
        )
    };
    if is_verbatim(program) {
        Ok(has_batch_extension(
            program.strip_suffix(&[0]).unwrap_or(program),
        ))
    } else {
        Ok(has_batch_extension(&full_path(program)?))
    }
}

/// The system `cmd.exe`, NUL-terminated.
pub(super) fn command_prompt() -> io::Result<Vec<u16>> {
    // SAFETY: the buffer holds `size` units.
    let mut system = fill(|buffer, size| unsafe { GetSystemDirectoryW(buffer, size) })?;
    system.extend("\\cmd.exe".encode_utf16().chain([0]));
    Ok(system)
}

/// Appends `arg` so `CommandLineToArgvW` recovers it; a raw argument is
/// appended unchanged.
pub(super) fn append_arg(line: &mut Vec<u16>, arg: &Arg) -> io::Result<()> {
    let (arg, regular) = match arg {
        Arg::Regular(arg) => (arg, true),
        Arg::Raw(arg) => (arg, false),
    };
    let bytes = ensure_no_nuls(arg)?.as_encoded_bytes();
    let quote =
        regular && (bytes.is_empty() || bytes.iter().any(|&byte| byte == b' ' || byte == b'\t'));
    if quote {
        line.push(QUOTE);
    }
    let mut backslashes = 0usize;
    for unit in arg.encode_wide() {
        if regular {
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
        line.extend(std::iter::repeat_n(SEP, backslashes));
        line.push(QUOTE);
    }
    Ok(())
}

/// The command line for an ordinary program, without its terminating NUL.
pub(super) fn command_line(program: &OsStr, args: &[Arg]) -> io::Result<Vec<u16>> {
    let mut line = vec![QUOTE];
    line.extend(program.encode_wide());
    line.push(QUOTE);
    for arg in args {
        line.push(SPACE);
        append_arg(&mut line, arg)?;
    }
    Ok(line)
}

fn append_batch_arg(line: &mut Vec<u16>, arg: &OsStr) -> io::Result<()> {
    let bytes = ensure_no_nuls(arg)?.as_encoded_bytes();
    // An empty argument would vanish, and a trailing backslash would
    // escape the closing quote of `"%~1"`.
    let mut quote = bytes.is_empty() || bytes.last() == Some(&b'\\');
    for character in arg.to_string_lossy().chars() {
        // Every ASCII symbol needs quoting unless known safe; so do controls.
        const UNQUOTED: &str = r"#$*+-./:?@\_";
        let ascii_needs_quotes = character.is_ascii()
            && !(character.is_ascii_alphanumeric() || UNQUOTED.contains(character));
        if ascii_needs_quotes || character.is_control() {
            quote = true;
        }
    }
    if quote {
        line.push(QUOTE);
    }
    let mut backslashes = 0usize;
    for unit in arg.encode_wide() {
        if unit == SEP {
            backslashes += 1;
        } else {
            if unit == QUOTE {
                // n backslashes before a quote become 2n, and the quote is doubled.
                line.extend(std::iter::repeat_n(SEP, backslashes));
                line.push(QUOTE);
            } else if unit == PERCENT || unit == CARRIAGE_RETURN {
                // `%%cd:~,%` expands to nothing, so cmd never expands a
                // `%VARIABLE%` spelled by the argument.
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

/// The `cmd.exe` command line that runs batch file `script` with `args`,
/// without its terminating NUL.
pub(super) fn batch_command_line(script: &[u16], args: &[Arg]) -> io::Result<Vec<u16>> {
    let mut line: Vec<u16> = "cmd.exe /e:ON /v:OFF /d /c \"".encode_utf16().collect();
    line.push(QUOTE);
    if script.contains(&QUOTE) || script.last() == Some(&SEP) {
        return Err(invalid(
            "Windows file names may not contain `\"` or end with `\\`",
        ));
    }
    line.extend_from_slice(script.strip_suffix(&[0]).unwrap_or(script));
    line.push(QUOTE);
    for arg in args {
        line.push(SPACE);
        match arg {
            Arg::Regular(value) => {
                if value
                    .as_encoded_bytes()
                    .iter()
                    .any(|byte| b"\r\n".contains(byte))
                {
                    return Err(invalid("batch file arguments are invalid"));
                }
                append_batch_arg(&mut line, value)?;
            }
            Arg::Raw(_) => append_arg(&mut line, arg)?,
        }
    }
    line.push(QUOTE);
    Ok(line)
}

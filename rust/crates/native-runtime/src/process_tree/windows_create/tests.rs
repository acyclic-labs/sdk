//! Process creation is held against `std::process::Command`, and every
//! guarantee of the shared console is checked in real children, each with a
//! control that shows the check can fail.
use super::*;
use crate::process_tree::{ProcessStdio, ProcessTree};
use std::io::{BufRead, Write as _};
use std::os::windows::process::CommandExt as _;
use std::time::Duration;
use windows_sys::Wdk::System::Threading::NtQueryInformationProcess;
use windows_sys::Win32::Foundation::{
    CloseHandle, CompareObjectHandles, UNICODE_STRING, WAIT_OBJECT_0,
};
use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
use windows_sys::Win32::Storage::FileSystem::ReadFile;
use windows_sys::Win32::System::Console::{
    GetConsoleTitleW, GetConsoleWindow, ReadConsoleW, SetConsoleTitleW,
};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW, TH32CS_SNAPPROCESS,
};
use windows_sys::Win32::System::Threading::{
    CreateEventW, OpenEventW, OpenProcess, PROCESS_NAME_WIN32, PROCESS_SYNCHRONIZE,
    QueryFullProcessImageNameW,
};
use windows_sys::Win32::UI::WindowsAndMessaging::IsWindowVisible;

/// `ProcessCommandLineInformation`.
const COMMAND_LINE_INFORMATION: i32 = 60;
/// `SYNCHRONIZE`, the only right the probes need.
const SYNCHRONIZE: u32 = 0x0010_0000;
const PROBE: &str = "ACYCLIC_CONSOLE_PROBE";
const PROBE_TEST: &str = "process_tree::windows_create::tests::probe";
/// Long enough that only a child that never returns can exceed it.
const BOUND: Duration = Duration::from_secs(30);

/// The image and command line a process was created with.
fn created_as(process: HANDLE) -> io::Result<(OsString, Vec<u16>)> {
    let mut image = vec![0_u16; 32 * 1024];
    let mut size = u32::try_from(image.len()).map_err(io::Error::other)?;
    // SAFETY: a live process handle and a buffer of `size` units.
    if unsafe {
        QueryFullProcessImageNameW(
            process,
            PROCESS_NAME_WIN32,
            image.as_mut_ptr(),
            &raw mut size,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    image.truncate(size as usize);
    let mut buffer = vec![0_usize; 8 * 1024];
    let mut length = 0;
    // SAFETY: a live process handle and an aligned buffer of the stated size.
    let status = unsafe {
        NtQueryInformationProcess(
            process,
            COMMAND_LINE_INFORMATION,
            buffer.as_mut_ptr().cast(),
            u32::try_from(size_of_val(buffer.as_slice())).map_err(io::Error::other)?,
            &raw mut length,
        )
    };
    if status < 0 {
        return Err(io::Error::other(format!("command line query: {status:#x}")));
    }
    // SAFETY: on success the buffer starts with a counted string whose text
    // lies inside the same buffer.
    let line = unsafe {
        let text = &*buffer.as_ptr().cast::<UNICODE_STRING>();
        std::slice::from_raw_parts(text.Buffer, usize::from(text.Length) / 2).to_vec()
    };
    Ok((OsString::from_wide(&image), line))
}

/// What the standard library creates for `command`, or the kind of its
/// refusal.
fn standard(command: &ProcessCommand) -> Result<(OsString, Vec<u16>), io::ErrorKind> {
    let mut native = command.to_std().map_err(|error| error.kind())?;
    native
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .creation_flags(CREATE_SUSPENDED);
    let mut child = native.spawn().map_err(|error| error.kind())?;
    let created = created_as(child.as_raw_handle());
    child.kill().map_err(|error| error.kind())?;
    child.wait().map_err(|error| error.kind())?;
    created.map_err(|error| error.kind())
}

/// What this module creates for `command`, or the kind of its refusal.
fn ours(command: &mut ProcessCommand) -> Result<(OsString, Vec<u16>), io::ErrorKind> {
    command
        .stdin(ProcessStdio::null())
        .stdout(ProcessStdio::null())
        .stderr(ProcessStdio::null());
    let mut child = spawn_suspended(command, None).map_err(|error| error.kind())?;
    let created = created_as(child.as_raw_handle());
    child.kill().map_err(|error| error.kind())?;
    child.wait().map_err(|error| error.kind())?;
    created.map_err(|error| error.kind())
}

/// A small deterministic generator, so a failure names a reproducible case.
struct Cases(u64);

impl Cases {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: usize) -> usize {
        usize::try_from(self.next() % bound as u64).unwrap_or(0)
    }

    /// An argument built from the pieces that decide quoting: whitespace,
    /// quotes, backslash runs before quotes and ends, and every character
    /// `cmd.exe` treats specially (the `BatBadBut` inputs).
    fn argument(&mut self) -> String {
        const PIECES: &[&str] = &[
            "",
            " ",
            "\t",
            "\"",
            "\\",
            "\\\\",
            "\\\"",
            "a",
            "x y",
            "\u{e9}",
            "%",
            "%PATH%",
            "%cd%",
            "^",
            "&",
            "&calc",
            "|",
            "<",
            ">",
            "(",
            ")",
            "!",
            "!PATH!",
            "=",
            ",",
            ";",
            "\"&whoami",
            "\\\\\"",
            "\r",
            "\n",
            "#$*+-./:?@_",
        ];
        (0..self.below(5))
            .map(|_| {
                PIECES
                    .get(self.below(PIECES.len()))
                    .copied()
                    .unwrap_or_default()
            })
            .collect()
    }
}

/// A command line from a deliberately wrong escaper: no backslash doubling
/// before quotes, and no `%` neutralising in batch arguments.
fn wrong_command_line(argv0: &OsStr, args: &[Arg], batch: Option<&[u16]>) -> Vec<u16> {
    let mut line: Vec<u16> = match batch {
        Some(script) => format!(
            "cmd.exe /e:ON /v:OFF /d /c \"\"{}\"",
            String::from_utf16_lossy(script.strip_suffix(&[0]).unwrap_or(script))
        ),
        None => format!("\"{}\"", argv0.to_string_lossy()),
    }
    .encode_utf16()
    .collect();
    for arg in args {
        let text = arg.as_os_str().to_string_lossy();
        let quote = text.is_empty() || text.contains([' ', '\t', '%', '&', '"']);
        let text = if batch.is_some() {
            text.replace('"', "\"\"")
        } else {
            text.replace('"', "\\\"")
        };
        line.push(wide(b' '));
        if quote {
            line.extend(format!("\"{text}\"").encode_utf16());
        } else {
            line.extend(text.encode_utf16());
        }
    }
    if batch.is_some() {
        line.push(QUOTE);
    }
    line
}

/// Writes the scripts and the copied executable the resolution cases run.
fn fixtures(root: &Path) -> io::Result<()> {
    let system = std::env::var_os("SystemRoot").ok_or(io::ErrorKind::NotFound)?;
    std::fs::copy(
        Path::new(&system).join(r"System32\whoami.exe"),
        root.join("probe-tool.exe"),
    )?;
    for script in ["probe.bat", "probe two.cmd", "PROBE.BAT"] {
        std::fs::write(root.join(script), "@echo off\r\n")?;
    }
    Ok(())
}

/// The programs and environments whose resolution is compared.
fn programs(root: &Path) -> Vec<(OsString, Option<OsString>)> {
    let system = std::env::var_os("SystemRoot").unwrap_or_else(|| r"C:\Windows".into());
    let cmd = Path::new(&system).join(r"System32\cmd.exe");
    let child_path = Some(root.as_os_str().to_owned());
    vec![
        ("cmd".into(), None),
        ("cmd.exe".into(), None),
        ("CMD".into(), None),
        (cmd.clone().into_os_string(), None),
        (cmd.to_string_lossy().replace('\\', "/").into(), None),
        ("whoami".into(), None),
        ("no-such-program-anywhere".into(), None),
        ("probe-tool".into(), child_path.clone()),
        ("probe-tool.exe".into(), child_path.clone()),
        ("probe.bat".into(), child_path.clone()),
        ("probe two.cmd".into(), child_path),
        (root.join("probe.bat").into_os_string(), None),
        (root.join("PROBE.BAT").into_os_string(), None),
        (root.join("probe two.cmd").into_os_string(), None),
        (
            format!(r"\\?\{}", root.join("probe.bat").display()).into(),
            None,
        ),
        (
            std::env::current_exe().unwrap_or_default().into_os_string(),
            None,
        ),
    ]
}

#[test]
fn creation_matches_the_standard_library_and_a_wrong_escaper_is_caught() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    fixtures(root.path())?;
    let programs = programs(root.path());
    let mut cases = Cases(0x9E37_79B9_7F4A_7C15);
    let (mut compared, mut refused, mut caught_exe, mut caught_batch) = (0, 0, 0, 0);
    for round in 0..240 {
        let Some((program, path)) = programs.get(round % programs.len()) else {
            continue;
        };
        let mut command = ProcessCommand::new(program);
        if let Some(path) = path {
            command.env("PATH", path);
        }
        for _ in 0..cases.below(4) {
            if cases.below(8) == 0 {
                command.raw_arg(cases.argument());
            } else {
                command.arg(cases.argument());
            }
        }
        let expected = standard(&command);
        let actual = ours(&mut command);
        assert_eq!(
            actual, expected,
            "case {round}: {command:?}\n ours: {actual:?}\n std:  {expected:?}"
        );
        let Ok((image, line)) = expected else {
            refused += 1;
            continue;
        };
        compared += 1;
        let batch = image
            .to_string_lossy()
            .to_ascii_lowercase()
            .ends_with(r"\cmd.exe")
            && line.starts_with(&"cmd.exe /e:ON".encode_utf16().collect::<Vec<_>>());
        let script = plan(&command)?.command_line;
        let wrong = if batch {
            let resolved = resolve_exe(
                &command.program,
                || std::env::var_os("PATH"),
                path.as_deref(),
            )?;
            wrong_command_line(&command.program, &command.args, Some(&resolved))
        } else {
            wrong_command_line(&command.program, &command.args, None)
        };
        assert_eq!(script.strip_suffix(&[0]), Some(line.as_slice()));
        if wrong != line {
            if batch {
                caught_batch += 1;
            } else {
                caught_exe += 1;
            }
        }
    }
    eprintln!(
        "compared {compared}, both refused {refused}, wrong escaper caught {caught_exe} exe and {caught_batch} batch"
    );
    assert!(compared > 100 && refused > 0);
    assert!(
        caught_exe > 0 && caught_batch > 0,
        "the generator must reach inputs a wrong escaper gets wrong"
    );
    Ok(())
}

#[test]
fn nul_bytes_are_refused_like_the_standard_library() {
    for command in [
        ProcessCommand::new("cmd\0.exe"),
        {
            let mut command = ProcessCommand::new("cmd.exe");
            command.arg("a\0b");
            command
        },
        {
            let mut command = ProcessCommand::new("cmd.exe");
            command.env("A\0", "1");
            command
        },
        {
            let mut command = ProcessCommand::new("cmd.exe");
            command.current_dir("C:\\\0");
            command
        },
    ] {
        let mut command = command;
        assert_eq!(ours(&mut command), standard(&command), "{command:?}");
    }
}

/// `cmd /d /c` with `script`, run by the standard library and by a tree.
fn both_outputs(configure: impl Fn(&mut ProcessCommand)) -> io::Result<(Vec<u8>, Vec<u8>)> {
    let mut command = ProcessCommand::new("cmd.exe");
    configure(&mut command);
    let expected = command
        .to_std()?
        .stdin(std::process::Stdio::null())
        .output()?;
    let actual = crate::process_output(&mut command, BOUND, 1 << 20)?;
    assert_eq!(actual.status.code(), expected.status.code());
    Ok((actual.stdout, expected.stdout))
}

#[test]
fn the_environment_and_directory_match_the_standard_library() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let verbatim = format!(r"\\?\{}", root.path().display());
    type Case = Box<dyn Fn(&mut ProcessCommand)>;
    let cases: Vec<Case> = vec![
        Box::new(|command| {
            command.args(["/d", "/c", "set"]);
        }),
        Box::new(|command| {
            command
                .args(["/d", "/c", "set"])
                .env_clear()
                .env("b_probe", "2")
                .env("A_PROBE", "1")
                .env(
                    "SystemRoot",
                    std::env::var_os("SystemRoot").unwrap_or_default(),
                );
        }),
        Box::new(|command| {
            command
                .args(["/d", "/c", "set"])
                .env("ACYCLIC_PROBE", "upper")
                .env("acyclic_probe", "lower wins, keeping the first spelling")
                .env_remove("Path")
                .env("ZZ_PROBE", "=x=");
        }),
        Box::new(|command| {
            command.args(["/d", "/c", "set"]).env_clear();
        }),
        Box::new(move |command| {
            command.args(["/d", "/c", "cd"]).current_dir(&verbatim);
        }),
        Box::new(|command| {
            command
                .args(["/d", "/c", "cd"])
                .current_dir(std::env::temp_dir());
        }),
    ];
    let baseline = cases
        .first()
        .map(both_outputs)
        .transpose()?
        .unwrap_or_default()
        .1;
    let mut differ = 0;
    for (index, case) in cases.iter().enumerate() {
        let (actual, expected) = both_outputs(case)?;
        assert_eq!(
            String::from_utf8_lossy(&actual),
            String::from_utf8_lossy(&expected),
            "case {index}"
        );
        differ += usize::from(actual != baseline);
    }
    // Control: the cases change what a child sees, so equality is evidence.
    assert!(differ >= 4);
    Ok(())
}

/// The probe child: one console behaviour, reported as `PROBE=` lines.
#[test]
fn probe() -> io::Result<()> {
    let Some(mode) = std::env::var_os(PROBE) else {
        return Ok(());
    };
    let mut out = std::io::stdout().lock();
    match mode.to_string_lossy().as_ref() {
        "conin-file" | "conin-console" => {
            let name: Vec<u16> = "CONIN$".encode_utf16().chain([0]).collect();
            let conin = open_file(
                &name,
                GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                0,
            );
            let conin = match conin {
                Ok(conin) => conin,
                Err(error) => {
                    writeln!(out, "PROBE=open-failed {error}")?;
                    return Ok(());
                }
            };
            writeln!(out, "PROBE=reading")?;
            out.flush()?;
            let mut buffer = [0_u16; 16];
            let mut read = 0;
            // SAFETY: an owned console handle and a buffer of 16 units.
            let ok = unsafe {
                if mode == "conin-file" {
                    ReadFile(
                        conin.as_raw_handle(),
                        buffer.as_mut_ptr().cast(),
                        32,
                        &raw mut read,
                        std::ptr::null_mut(),
                    )
                } else {
                    ReadConsoleW(
                        conin.as_raw_handle(),
                        buffer.as_mut_ptr().cast(),
                        16,
                        &raw mut read,
                        std::ptr::null(),
                    )
                }
            };
            let error = io::Error::last_os_error();
            writeln!(out, "PROBE=returned ok={ok} read={read} error={error}")?;
        }
        "title-set" => {
            let title: Vec<u16> = "acyclic-probe-title".encode_utf16().chain([0]).collect();
            // SAFETY: a NUL-terminated title.
            let ok = unsafe { SetConsoleTitleW(title.as_ptr()) };
            writeln!(out, "PROBE=set ok={ok}")?;
        }
        "title-get" => writeln!(out, "PROBE=title {}", console_title())?,
        "window" | "window-grandchild" => {
            // SAFETY: no arguments; reads this process's console window.
            let window = unsafe { GetConsoleWindow() };
            // SAFETY: any window handle, including null, may be queried.
            let visible = unsafe { IsWindowVisible(window) };
            writeln!(
                out,
                "PROBE={mode} attached={} visible={visible}",
                !window.is_null(),
                mode = mode.to_string_lossy()
            )?;
            if mode == "window" {
                // A grandchild started the ordinary way inherits the console.
                let output = std::process::Command::new(std::env::current_exe()?)
                    .args(["--exact", PROBE_TEST, "--nocapture"])
                    .env(PROBE, "window-grandchild")
                    .output()?;
                out.write_all(&output.stdout)?;
            }
        }
        "handle" => {
            let value = std::env::var("ACYCLIC_PROBE_HANDLE").map_err(io::Error::other)?;
            let name = std::env::var_os("ACYCLIC_PROBE_NAME").ok_or(io::ErrorKind::NotFound)?;
            let name: Vec<u16> = name.encode_wide().chain([0]).collect();
            let inherited: usize = value.parse().map_err(io::Error::other)?;
            // SAFETY: a NUL-terminated name.
            let own = unsafe { OpenEventW(SYNCHRONIZE, 0, name.as_ptr()) };
            if own.is_null() {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: `own` is live; an invalid `inherited` only compares false.
            let seen = unsafe {
                CompareObjectHandles(own, std::ptr::with_exposed_provenance_mut(inherited))
            };
            // SAFETY: closing the handle opened above.
            unsafe { CloseHandle(own) };
            writeln!(out, "PROBE=seen {}", seen != 0)?;
        }
        "hold" => {
            writeln!(out, "PROBE=held {}", std::process::id())?;
            out.flush()?;
            let mut rest = Vec::new();
            std::io::Read::read_to_end(&mut std::io::stdin(), &mut rest)?;
        }
        "owner" => owner(&mut out)?,
        other => writeln!(out, "PROBE=unknown {other}")?,
    }
    out.flush()
}

fn console_title() -> String {
    let mut title = [0_u16; 256];
    // SAFETY: a buffer of 256 units.
    let length = unsafe { GetConsoleTitleW(title.as_mut_ptr(), 256) };
    String::from_utf16_lossy(title.get(..length as usize).unwrap_or_default())
}

/// The `PROBE=` lines of `output`.
fn probes(output: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(output)
        .lines()
        .filter_map(|line| line.strip_prefix("PROBE="))
        .map(str::to_owned)
        .collect()
}

fn probe_command(mode: &str) -> io::Result<ProcessCommand> {
    let mut command = ProcessCommand::new(std::env::current_exe()?);
    command
        .args(["--exact", PROBE_TEST, "--nocapture"])
        .env(PROBE, mode);
    Ok(command)
}

/// Runs one probe in a tree and returns its `PROBE=` lines.
fn run_probe(mode: &str, configure: impl FnOnce(&mut ProcessCommand)) -> io::Result<Vec<String>> {
    let mut command = probe_command(mode)?;
    configure(&mut command);
    let output = crate::process_output(&mut command, BOUND, 1 << 20)?;
    assert!(output.status.success(), "{mode}: {output:?}");
    Ok(probes(&output.stdout))
}

/// A probe started in a tree, once it has reported its first line.
fn started(mode: &str) -> io::Result<(ProcessTree, std::io::BufReader<std::process::ChildStdout>)> {
    let mut command = probe_command(mode)?;
    command
        .stdin(ProcessStdio::piped())
        .stdout(ProcessStdio::piped())
        .stderr(ProcessStdio::null());
    let mut tree = ProcessTree::spawn(&mut command)?;
    let mut output = std::io::BufReader::new(tree.take_stdout().ok_or(io::ErrorKind::BrokenPipe)?);
    first_probe(&mut output, mode)?;
    Ok((tree, output))
}

/// Reads up to and including the first `PROBE=` line.
fn first_probe(output: &mut impl BufRead, mode: &str) -> io::Result<String> {
    let mut line = String::new();
    loop {
        line.clear();
        if output.read_line(&mut line)? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("{mode} ended before reporting"),
            ));
        }
        if let Some(probe) = line.trim().strip_prefix("PROBE=") {
            return Ok(probe.to_owned());
        }
    }
}

#[test]
fn console_readers_wait_as_in_a_hidden_console_and_end_with_their_job() -> io::Result<()> {
    // Readers of the shared console wait: nothing is ever typed into it.
    let mut readers = ["conin-file", "conin-console"]
        .map(started)
        .into_iter()
        .collect::<io::Result<Vec<_>>>()?;
    // Control: a `CREATE_NO_WINDOW` child reading its own hidden console
    // waits the same way.
    let mut hidden = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", PROBE_TEST, "--nocapture"])
        .env(PROBE, "conin-file")
        .creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let mut hidden_output =
        std::io::BufReader::new(hidden.stdout.take().ok_or(io::ErrorKind::BrokenPipe)?);
    assert_eq!(first_probe(&mut hidden_output, "hidden")?, "reading");
    // Siblings start, finish and keep running beside the waiting readers.
    let (mut held, _held_output) = started("hold")?;
    // A sibling's captured output is exactly what it wrote.
    let sibling = run_probe("title-get", |_| {})?;
    assert!(
        sibling.len() == 1 && sibling.iter().all(|line| line.starts_with("title ")),
        "{sibling:?}"
    );
    for (reader, _) in &mut readers {
        assert!(reader.try_wait(None)?.is_none(), "a console read returned");
    }
    assert!(hidden.try_wait()?.is_none(), "the control's read returned");
    // The Job ends a waiting reader, and only that tree.
    for (mut reader, mut output) in readers {
        reader.terminate(None)?;
        let mut rest = String::new();
        std::io::Read::read_to_string(&mut output, &mut rest)?;
        assert!(!rest.contains("PROBE=returned"), "{rest}");
    }
    hidden.kill()?;
    hidden.wait()?;
    assert!(
        held.try_wait(None)?.is_none(),
        "a sibling's end ended this tree"
    );
    drop(held.take_stdin());
    assert!(held.wait(BOUND)?.success());
    // `set /p` reads its standard input, which the tree gave it.
    for stdin in [ProcessStdio::null(), ProcessStdio::piped()] {
        let mut command = ProcessCommand::new("cmd.exe");
        command
            .args(["/d", "/c", "set /p x=& echo returned"])
            .stdin(stdin)
            .stdout(ProcessStdio::piped())
            .stderr(ProcessStdio::null());
        let mut tree = ProcessTree::spawn(&mut command)?;
        drop(tree.take_stdin());
        let mut line = String::new();
        std::io::BufReader::new(tree.take_stdout().ok_or(io::ErrorKind::BrokenPipe)?)
            .read_line(&mut line)?;
        assert_eq!(line.trim(), "returned");
        assert!(tree.wait(BOUND)?.success());
    }
    Ok(())
}

#[test]
fn console_state_never_reaches_the_owner_or_captured_streams() -> io::Result<()> {
    let before = console_title();
    assert_eq!(run_probe("title-set", |_| {})?, ["set ok=1"]);
    // The owner's own console, if it has one, keeps its title.
    assert_eq!(console_title(), before);
    // Siblings share one console, so its title is shared state among them
    // (as among the processes of any one console); their streams carry only
    // what each wrote.
    assert_eq!(
        run_probe("title-get", |_| {})?,
        ["title acyclic-probe-title"]
    );
    Ok(())
}

#[test]
fn no_child_or_grandchild_has_a_visible_window() -> io::Result<()> {
    let lines = run_probe("window", |_| {})?;
    assert_eq!(
        lines,
        [
            "window attached=true visible=0",
            "window-grandchild attached=true visible=0"
        ]
    );
    Ok(())
}

/// An inheritable named event, and its name.
fn inheritable_event() -> io::Result<(OwnedHandle, OsString)> {
    let name = format!(
        r"Local\acyclic-handle-probe-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos()
    );
    let wide_name: Vec<u16> = name.encode_utf16().chain([0]).collect();
    let attributes = SECURITY_ATTRIBUTES {
        nLength: u32::try_from(size_of::<SECURITY_ATTRIBUTES>()).map_err(io::Error::other)?,
        lpSecurityDescriptor: std::ptr::null_mut(),
        bInheritHandle: 1,
    };
    // SAFETY: valid attributes and a NUL-terminated name.
    let event = unsafe { CreateEventW(&raw const attributes, 1, 0, wide_name.as_ptr()) };
    if event.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful creation returns one new owned handle.
    Ok((unsafe { OwnedHandle::from_raw_handle(event) }, name.into()))
}

#[test]
fn a_child_inherits_only_its_standard_handles() -> io::Result<()> {
    let (event, name) = inheritable_event()?;
    let mut command = probe_command("handle")?;
    command
        .env(
            "ACYCLIC_PROBE_HANDLE",
            event.as_raw_handle().expose_provenance().to_string(),
        )
        .env("ACYCLIC_PROBE_NAME", &name);
    // Control: the standard library passes every inheritable handle.
    let output = command
        .to_std()?
        .stdin(std::process::Stdio::null())
        .output()?;
    assert_eq!(probes(&output.stdout), ["seen true"], "{output:?}");
    assert_eq!(
        run_probe("handle", |probe| {
            probe
                .env(
                    "ACYCLIC_PROBE_HANDLE",
                    event.as_raw_handle().expose_provenance().to_string(),
                )
                .env("ACYCLIC_PROBE_NAME", &name);
        })?,
        ["seen false"]
    );
    Ok(())
}

/// The console host processes this process started.
fn hosts(owner: u32) -> io::Result<Vec<u32>> {
    // SAFETY: a process snapshot with no borrowed arguments.
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful snapshot returns one new owned handle.
    let snapshot = unsafe { OwnedHandle::from_raw_handle(snapshot) };
    // SAFETY: zero is a valid entry before its size is set.
    let mut entry: PROCESSENTRY32W = unsafe { std::mem::zeroed() };
    entry.dwSize = u32::try_from(size_of::<PROCESSENTRY32W>()).map_err(io::Error::other)?;
    let mut found = Vec::new();
    // SAFETY: the owned snapshot and a sized entry.
    let mut more = unsafe { Process32FirstW(snapshot.as_raw_handle(), &raw mut entry) } != 0;
    while more {
        let length = entry
            .szExeFile
            .iter()
            .position(|unit| *unit == 0)
            .unwrap_or(0);
        let image = String::from_utf16_lossy(entry.szExeFile.get(..length).unwrap_or_default());
        if entry.th32ParentProcessID == owner && image.eq_ignore_ascii_case("conhost.exe") {
            found.push(entry.th32ProcessID);
        }
        // SAFETY: as above.
        more = unsafe { Process32NextW(snapshot.as_raw_handle(), &raw mut entry) } != 0;
    }
    Ok(found)
}

/// An owner process: starts one held tree, reports it and its console host,
/// then exits, without ending the tree, once its own input closes.
fn owner(out: &mut impl io::Write) -> io::Result<()> {
    let before = hosts(std::process::id())?;
    let mut held = probe_command("hold")?;
    held.stdin(ProcessStdio::piped())
        .stdout(ProcessStdio::piped())
        .stderr(ProcessStdio::null());
    let mut tree = ProcessTree::spawn(&mut held)?;
    let mut output = std::io::BufReader::new(tree.take_stdout().ok_or(io::ErrorKind::BrokenPipe)?);
    let member = first_probe(&mut output, "hold")?
        .strip_prefix("held ")
        .ok_or(io::ErrorKind::InvalidData)?
        .to_owned();
    let host: Vec<u32> = hosts(std::process::id())?
        .into_iter()
        .filter(|host| !before.contains(host))
        .collect();
    writeln!(out, "PROBE=member {member}")?;
    writeln!(out, "PROBE=hosts {host:?}")?;
    out.flush()?;
    let mut rest = Vec::new();
    std::io::Read::read_to_end(&mut std::io::stdin(), &mut rest)?;
    // Exit with the tree still running: the owner's handles close with it.
    std::mem::forget(tree);
    std::process::exit(0)
}

fn open_waitable(pid: u32) -> io::Result<OwnedHandle> {
    // SAFETY: opening a process by id for waiting only.
    let process = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid) };
    if process.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful open returns one new owned handle.
    Ok(unsafe { OwnedHandle::from_raw_handle(process) })
}

fn exits_within(process: &OwnedHandle, bound: Duration) -> bool {
    let millis = u32::try_from(bound.as_millis()).unwrap_or(INFINITE - 1);
    // SAFETY: a live process handle with the synchronize right.
    unsafe { WaitForSingleObject(process.as_raw_handle(), millis) == WAIT_OBJECT_0 }
}

#[test]
fn an_owners_exit_ends_its_console_host_and_its_jobs_end_its_trees() -> io::Result<()> {
    let mut owner = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", PROBE_TEST, "--nocapture"])
        .env(PROBE, "owner")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let mut report = std::io::BufReader::new(owner.stdout.take().ok_or(io::ErrorKind::BrokenPipe)?);
    let (mut member, mut hosts) = (None, None);
    let mut line = String::new();
    while member.is_none() || hosts.is_none() {
        line.clear();
        if report.read_line(&mut line)? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        if let Some(pid) = line.trim().strip_prefix("PROBE=member ") {
            member = Some(open_waitable(pid.parse().map_err(io::Error::other)?)?);
        } else if let Some(list) = line.trim().strip_prefix("PROBE=hosts ") {
            hosts = Some(
                list.trim_matches(['[', ']'])
                    .split(", ")
                    .filter(|pid| !pid.is_empty())
                    .map(|pid| open_waitable(pid.parse().map_err(io::Error::other)?))
                    .collect::<io::Result<Vec<_>>>()?,
            );
        }
    }
    let (member, hosts) = (
        member.unwrap_or_else(|| unreachable!()),
        hosts.unwrap_or_default(),
    );
    assert_eq!(hosts.len(), 1, "one console host for the owner");
    // Control: while the owner lives, neither has exited.
    assert!(!exits_within(&member, Duration::ZERO));
    assert!(hosts.iter().all(|host| !exits_within(host, Duration::ZERO)));
    drop(owner.stdin.take());
    assert!(owner.wait()?.success());
    assert!(exits_within(&member, BOUND), "the Job ends the tree");
    assert!(
        hosts.iter().all(|host| exits_within(host, BOUND)),
        "the console host ends with its owner"
    );
    Ok(())
}

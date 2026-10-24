//! The Windows command starts what `std::process::Command` starts, with
//! the same command line, environment and directory, and its child
//! inherits only its own standard handles: no host handle changes, a host
//! whose own output is closed still starts children, and a host's reader
//! sees the host's output end while children it started still run.

use super::testing::{Tether, TetherEvent, Tethered};
use super::{Command, Stdio};
use crate::{ProcessCustody, ProcessTree};
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::{self, Read as _, Write as _};
use std::os::windows::io::AsHandle as _;
use std::os::windows::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_INVALID_HANDLE, GetHandleInformation, HANDLE, HANDLE_FLAG_INHERIT,
    SetHandleInformation, WAIT_TIMEOUT,
};
use windows_sys::Win32::System::Console::{
    GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
};
use windows_sys::Win32::System::Threading::{
    ExitProcess, OpenProcess, PROCESS_SYNCHRONIZE, PROCESS_TERMINATE, TerminateProcess,
    WaitForSingleObject,
};

/// Makes this executable report its command line, arguments, directory and
/// [`VALUE`], then exit.
const ECHO: &str = "--acyclic-command-echo";
/// Makes this executable write its next argument to its output and exit.
const PRINT: &str = "--acyclic-command-print";
/// When set, a service start of this executable reports its PID over this
/// tether and runs until the test releases it, instead of reaching the test
/// harness.
const PARK: &str = "ACYCLIC_COMMAND_TEST_PARK";
/// The tether a service owner reports over.
const REPORTS: &str = "ACYCLIC_COMMAND_TEST_REPORTS";
/// How long a service test may take before it fails, as a backstop: every
/// wait in it ends at once on the event it waits for or on a helper's end.
const BACKSTOP: Duration = Duration::from_secs(60);
const MODE: &str = "ACYCLIC_COMMAND_TEST_MODE";
const ROOT: &str = "ACYCLIC_COMMAND_TEST_ROOT";
const VALUE: &str = "ACYCLIC_COMMAND_TEST_VALUE";

#[used]
#[unsafe(link_section = ".CRT$XCU")]
static HOOK: extern "C" fn() = hook;

/// Runs before the test harness and serves this suite's helper modes.
extern "C" fn hook() {
    let mut args = std::env::args_os().skip(1);
    let first = args.next();
    let code = match first.as_deref().and_then(OsStr::to_str) {
        Some(ECHO) => echo(),
        Some(PRINT) => {
            let word = args.next().unwrap_or_default();
            let mut output = io::stdout().lock();
            let printed = output
                .write_all(word.as_encoded_bytes())
                .and_then(|()| output.write_all(b"\n"))
                .and_then(|()| output.flush());
            u32::from(printed.is_err())
        }
        Some("__service") => match std::env::var_os(PARK) {
            Some(tether) => park(&tether),
            None => return,
        },
        _ => return,
    };
    // SAFETY: ends this helper without returning to the harness's startup.
    unsafe { ExitProcess(code) }
}

fn echo() -> u32 {
    // SAFETY: the process's own NUL-terminated command line.
    let line = unsafe { windows_sys::Win32::System::Environment::GetCommandLineW() };
    let mut length = 0;
    // SAFETY: reads up to the terminating NUL of that line.
    while unsafe { *line.add(length) } != 0 {
        length += 1;
    }
    // SAFETY: the `length` units just read.
    let mut report = String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(line, length) });
    for arg in std::env::args_os().skip(2) {
        report.push('\u{1}');
        report.push_str(&arg.to_string_lossy());
    }
    report.push('\u{2}');
    report.push_str(
        &std::env::current_dir()
            .map(|dir| dir.display().to_string())
            .unwrap_or_default(),
    );
    report.push('\u{2}');
    report.push_str(&std::env::var_os(VALUE).map_or_else(
        || "<unset>".to_owned(),
        |value| value.to_string_lossy().into_owned(),
    ));
    let mut output = io::stdout().lock();
    // A child whose output is not writable still exits successfully.
    let _ = output
        .write_all(report.as_bytes())
        .and_then(|()| output.flush());
    0
}

/// Reports this process's PID over the tether named `name`, then runs
/// until the test releases it.
fn park(name: &OsStr) -> u32 {
    let Ok(tether) = Tethered::connect(name) else {
        return 1;
    };
    if tether.send(&std::process::id().to_string()).is_err() {
        return 1;
    }
    tether.released();
    0
}

fn exe() -> PathBuf {
    std::env::current_exe().expect("test executable")
}

/// One command, built the same way for this crate and for `std`.
#[derive(Clone, Default)]
struct Spec {
    program: OsString,
    args: Vec<(bool, OsString)>,
    clear: bool,
    env: Vec<(OsString, Option<OsString>)>,
    cwd: Option<PathBuf>,
}

impl Spec {
    fn new(program: impl AsRef<OsStr>) -> Self {
        Self {
            program: program.as_ref().to_owned(),
            ..Self::default()
        }
    }

    fn arg(mut self, arg: impl AsRef<OsStr>) -> Self {
        self.args.push((false, arg.as_ref().to_owned()));
        self
    }

    fn args<S: AsRef<OsStr>>(mut self, args: impl IntoIterator<Item = S>) -> Self {
        for arg in args {
            self = self.arg(arg);
        }
        self
    }

    fn raw(mut self, arg: impl AsRef<OsStr>) -> Self {
        self.args.push((true, arg.as_ref().to_owned()));
        self
    }

    fn env(mut self, key: impl AsRef<OsStr>, value: Option<&str>) -> Self {
        self.env
            .push((key.as_ref().to_owned(), value.map(OsString::from)));
        self
    }

    fn ours(&self) -> io::Result<Output> {
        let mut command = Command::new(&self.program);
        for (raw, arg) in &self.args {
            if *raw {
                command.raw_arg(arg);
            } else {
                command.arg(arg);
            }
        }
        if self.clear {
            command.env_clear();
        }
        for (key, value) in &self.env {
            match value {
                Some(value) => command.env(key, value),
                None => command.env_remove(key),
            };
        }
        if let Some(cwd) = &self.cwd {
            command.current_dir(cwd);
        }
        command.output()
    }

    fn std(&self) -> io::Result<Output> {
        let mut command = std::process::Command::new(&self.program);
        for (raw, arg) in &self.args {
            if *raw {
                command.raw_arg(arg);
            } else {
                command.arg(arg);
            }
        }
        if self.clear {
            command.env_clear();
        }
        for (key, value) in &self.env {
            match value {
                Some(value) => command.env(key, value),
                None => command.env_remove(key),
            };
        }
        if let Some(cwd) = &self.cwd {
            command.current_dir(cwd);
        }
        command.output()
    }

    /// Runs the command both ways and asserts the same outcome.
    fn same(&self) -> io::Result<Output> {
        match (self.ours(), self.std()) {
            (Ok(ours), Ok(theirs)) => {
                assert_eq!(
                    String::from_utf8_lossy(&ours.stdout),
                    String::from_utf8_lossy(&theirs.stdout),
                    "output differs from std"
                );
                assert_eq!(ours.status, theirs.status, "status differs from std");
                Ok(ours)
            }
            (Err(ours), Err(theirs)) => {
                assert_eq!(
                    ours.kind(),
                    theirs.kind(),
                    "{ours} differs from std's {theirs}"
                );
                assert_eq!(ours.raw_os_error(), theirs.raw_os_error());
                Err(ours)
            }
            (ours, theirs) => panic!("{ours:?} differs from std's {theirs:?}"),
        }
    }
}

/// The arguments the echo helper received, from its report.
fn received(output: &Output) -> Vec<String> {
    let report = String::from_utf8_lossy(&output.stdout);
    let line = report.split('\u{2}').next().expect("report");
    line.split('\u{1}').skip(1).map(str::to_owned).collect()
}

/// Arguments whose quoting `CommandLineToArgvW` and `cmd.exe` each make
/// hard: empties, spaces, quotes, backslash runs before quotes and at the
/// end, and every character `cmd.exe` treats specially.
const TRICKY: [&str; 30] = [
    "",
    " ",
    "plain",
    "two words",
    "tab\there",
    "\"",
    "a\"b",
    "\\",
    "a\\",
    "a\\\\",
    "a\\\"b",
    "\\\\\"",
    "trailing space ",
    "\"quoted\"",
    "%PATH%",
    "%%",
    "%",
    "^&|<>()",
    "!bang!",
    "unicode \u{e9} \u{2603}",
    "semi;colon",
    "comma,",
    "=equals",
    "-dash",
    "/slash",
    "a b\\",
    "\\\\server\\share",
    "c:\\path with space\\",
    "x\"y\\\"z",
    "@at#hash$dollar*star+plus.dot:colon?query_under",
];

#[test]
fn arguments_reach_the_child_as_std_quotes_them() {
    let output = Spec::new(exe())
        .arg(ECHO)
        .args(TRICKY)
        .same()
        .expect("echo");
    assert_eq!(received(&output), TRICKY, "an argument did not round-trip");
}

#[test]
fn raw_arguments_reach_the_child_unchanged() {
    let output = Spec::new(exe())
        .arg(ECHO)
        .raw("\"a b\"")
        .raw("x y")
        .raw("\\\"")
        .raw("")
        .arg("after")
        .same()
        .expect("echo");
    assert_eq!(received(&output), ["a b", "x", "y", "\"", "after"]);
}

/// A batch file directory whose script runs the echo helper with `%*`.
fn batch(root: &Path) -> PathBuf {
    let script = root.join("echo args.cmd");
    fs::write(&script, format!("@\"{}\" {ECHO} %*\r\n", exe().display())).expect("script");
    script
}

#[test]
fn batch_file_arguments_are_escaped_as_std_escapes_them() {
    let root = tempfile::tempdir().expect("root");
    let output = Spec::new(batch(root.path()))
        .args(TRICKY)
        .same()
        .expect("batch");
    let received = received(&output);
    assert!(
        received.iter().any(|arg| arg == "%PATH%"),
        "cmd expanded an argument: {received:?}"
    );
}

#[test]
fn batch_file_arguments_with_line_breaks_are_refused_as_std_refuses_them() {
    let root = tempfile::tempdir().expect("root");
    for broken in ["a\nb", "a\rb"] {
        let refused = Spec::new(batch(root.path())).arg(broken).same();
        assert_eq!(
            refused.expect_err("a line break reached cmd").kind(),
            io::ErrorKind::InvalidInput
        );
    }
}

#[test]
fn programs_resolve_as_std_resolves_them() {
    let root = tempfile::tempdir().expect("root");
    fs::write(root.path().join("probe.cmd"), "@echo probe\r\n").expect("probe");
    let path = root.path().as_os_str().to_str().expect("path");
    let found = Spec::new("probe.cmd")
        .env("PATH", Some(path))
        .same()
        .expect("child PATH");
    assert_eq!(String::from_utf8_lossy(&found.stdout).trim(), "probe");
    let system = Spec::new("cmd")
        .args(["/d", "/c", "echo", "system"])
        .same()
        .expect("system directory");
    assert_eq!(String::from_utf8_lossy(&system.stdout).trim(), "system");
    let without_suffix = PathBuf::from(std::env::var_os("SystemRoot").expect("SystemRoot"))
        .join("System32")
        .join("cmd");
    Spec::new(without_suffix)
        .args(["/d", "/c", "exit", "0"])
        .same()
        .expect(".exe appended to a path");
    for (program, kind) in [
        ("", io::ErrorKind::InvalidInput),
        ("trailing\\", io::ErrorKind::InvalidInput),
        ("acyclic-no-such-program", io::ErrorKind::NotFound),
    ] {
        assert_eq!(
            Spec::new(program).same().expect_err(program).kind(),
            kind,
            "{program:?}"
        );
    }
}

#[test]
fn environment_and_directory_reach_the_child_as_std_sets_them() {
    let root = tempfile::tempdir().expect("root");
    let verbatim = PathBuf::from(format!(r"\\?\{}", root.path().display()));
    let report = |output: &Output| String::from_utf8_lossy(&output.stdout).into_owned();
    let mut cleared = Spec::new(exe())
        .arg(ECHO)
        .env("acyclic_command_test_value", Some("lower"))
        .env(VALUE, Some("upper"));
    cleared.clear = true;
    cleared.cwd = Some(verbatim);
    let cleared = report(&cleared.same().expect("cleared"));
    assert!(cleared.ends_with("\u{2}upper"), "{cleared}");
    let removed = Spec::new(exe())
        .arg(ECHO)
        .env(VALUE, Some("set"))
        .env(VALUE, None)
        .same()
        .expect("removed");
    assert!(report(&removed).ends_with("\u{2}<unset>"));
}

/// Inheritance flags of this process's three standard handles.
fn standard_handle_flags() -> [Option<u32>; 3] {
    [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE].map(|id| {
        let mut flags = 0;
        // SAFETY: queries this process's standard handle slot.
        (unsafe { GetHandleInformation(GetStdHandle(id), &raw mut flags) } != 0).then_some(flags)
    })
}

fn echo_command(stdout: Stdio) -> Command {
    let mut command = Command::new(exe());
    command
        .arg(ECHO)
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(Stdio::null());
    command
}

/// Starts one plain, one bounded and one custodied tree child, as a host
/// does.
fn start_children(root: &Path) -> io::Result<()> {
    crate::process_output(
        &mut echo_command(Stdio::piped()),
        Duration::from_secs(30),
        1 << 16,
    )?;
    let status = echo_command(Stdio::null()).status()?;
    assert!(status.success(), "{status}");
    let custody = ProcessCustody::open(&root.join("processes"))?;
    let mut tree = ProcessTree::spawn_in_custody(&mut echo_command(Stdio::null()), &custody)?;
    tree.wait(Duration::from_secs(30))?;
    tree.terminate(None)?;
    Ok(())
}

/// Clears the inheritance flag of this process's output, as a start that
/// withheld the host's handles from its children would.
fn withhold_output() {
    // SAFETY: changes only the flag of this helper's own output handle.
    unsafe { SetHandleInformation(GetStdHandle(STD_OUTPUT_HANDLE), HANDLE_FLAG_INHERIT, 0) };
}

/// Starts this executable the way a C host does: inheriting handles,
/// with no standard handles named, so the child writes to whatever this
/// process's standard handles name.
fn legacy_print(word: &str) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt as _;
    use windows_sys::Win32::System::Threading::{
        CreateProcessW, INFINITE, PROCESS_INFORMATION, STARTUPINFOW,
    };
    let application: Vec<u16> = exe().as_os_str().encode_wide().chain([0]).collect();
    let mut line: Vec<u16> = format!("\"{}\" {PRINT} {word}", exe().display())
        .encode_utf16()
        .chain([0])
        .collect();
    // SAFETY: zero is the documented initial state.
    let mut startup: STARTUPINFOW = unsafe { std::mem::zeroed() };
    startup.cb =
        u32::try_from(size_of::<STARTUPINFOW>()).map_err(|_| io::ErrorKind::InvalidInput)?;
    // SAFETY: as above.
    let mut process: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: NUL-terminated buffers that outlive the call.
    let created = unsafe {
        CreateProcessW(
            application.as_ptr(),
            line.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            1,
            0,
            std::ptr::null(),
            std::ptr::null(),
            &raw const startup,
            &raw mut process,
        )
    };
    if created == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the handles CreateProcessW returned.
    unsafe {
        WaitForSingleObject(process.hProcess, INFINITE);
        CloseHandle(process.hThread);
        CloseHandle(process.hProcess);
    }
    Ok(())
}

/// The helper owner: a host process whose own output is an inheritable pipe
/// its starter reads, as a host started by another program has.
#[test]
fn command_owner_helper() {
    let Ok(mode) = std::env::var(MODE) else {
        return;
    };
    let root = PathBuf::from(std::env::var_os(ROOT).expect("owner root"));
    let outcome = match mode.as_str() {
        "inheritance" | "inheritance-control" => {
            let before = standard_handle_flags();
            start_children(&root).expect("children");
            if mode.ends_with("control") {
                withhold_output();
            }
            format!("{before:?}\n{:?}", standard_handle_flags())
        }
        "delivers" | "delivers-control" => {
            start_children(&root).expect("children");
            let status = std::process::Command::new(exe())
                .args([PRINT, "std-delivered"])
                .status()
                .expect("std child");
            assert!(status.success(), "{status}");
            if mode.ends_with("control") {
                withhold_output();
            }
            legacy_print("legacy-delivered").expect("legacy child");
            "printed".to_owned()
        }
        "closed" => closed_output(&root),
        "service" | "service-control" => serve_while_closed(&mode),
        other => panic!("unknown mode {other}"),
    };
    fs::write(root.join("outcome"), outcome).expect("outcome");
}

/// Closes this host's output and leaves its standard handle slot naming
/// no open handle, then starts children every way a host does. The slot
/// names a value no handle of this process takes, since the closed
/// handle's own value is reused by the next handle opened here.
fn closed_output(root: &Path) -> String {
    use windows_sys::Win32::System::Console::SetStdHandle;
    let stale: HANDLE = std::ptr::without_provenance_mut(0x00ff_fffc);
    // SAFETY: this helper never writes its output again; the slot then
    // names a value beyond any handle this process opens.
    unsafe {
        CloseHandle(GetStdHandle(STD_OUTPUT_HANDLE));
        SetStdHandle(STD_OUTPUT_HANDLE, stale);
    }
    // SAFETY: probes the stale value.
    let stale = unsafe { SetHandleInformation(stale, HANDLE_FLAG_INHERIT, 0) } == 0
        && io::Error::last_os_error().raw_os_error() == Some(ERROR_INVALID_HANDLE.cast_signed());
    let mut lines = vec![format!("stale={stale}")];
    for (name, stdio) in [
        ("null", Stdio::null as fn() -> Stdio),
        ("piped", Stdio::piped),
        ("inherit", Stdio::inherit),
    ] {
        let mut command = echo_command(stdio());
        let started = ProcessTree::spawn(&mut command).and_then(|mut tree| {
            let status = tree.wait(Duration::from_secs(30));
            tree.terminate(None)?;
            status
        });
        lines.push(format!(
            "{name}={}",
            started.is_ok_and(|status| status.success())
        ));
    }
    let custodied = ProcessCustody::open(&root.join("processes")).and_then(|custody| {
        let mut tree = ProcessTree::spawn_in_custody(&mut echo_command(Stdio::null()), &custody)?;
        let status = tree.wait(Duration::from_secs(30));
        tree.terminate(None)?;
        status
    });
    lines.push(format!(
        "custodied={}",
        custodied.is_ok_and(|status| status.success())
    ));
    lines.join("\n")
}

/// Starts a service (through this crate, or through `std` for the
/// control) and, once the test has seen it run, closes this host's output.
/// Reports the service's end, or its own failure, over its tether at once.
fn serve_while_closed(mode: &str) -> ! {
    let reports =
        Arc::new(Tethered::connect(&std::env::var_os(REPORTS).expect("reports")).expect("reports"));
    let reporter = Arc::clone(&reports);
    std::panic::set_hook(Box::new(move |panic| {
        let _ = reporter.send(&format!("panicked: {panic}"));
    }));
    let started = if mode == "service" {
        let readiness = crate::spawn_service_process(&exe()).expect("service");
        Box::new(move || format!("{:?}", readiness.wait())) as Box<dyn FnOnce() -> String + Send>
    } else {
        let mut child = std::process::Command::new(exe())
            .args(["__service", crate::SERVICE_READY_ARGUMENT])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("std service");
        Box::new(move || format!("{:?}", child.wait()))
    };
    let reporter = Arc::clone(&reports);
    thread::spawn(move || {
        let ended = started();
        let _ = reporter.send(&format!("the service ended: {ended}"));
        std::process::exit(1);
    });
    assert_eq!(reports.receive().expect("go"), Some("go".to_owned()));
    super::testing::close_standard_output().expect("close output");
    reports.send("ready").expect("ready");
    reports.released();
    std::process::exit(0)
}

fn owner(mode: &str, root: &Path, tethers: Option<(&Tether, &Tether)>) -> std::process::Child {
    let mut command = std::process::Command::new(exe());
    command
        .args([
            "--exact",
            "windows::command::tests::command_owner_helper",
            "--nocapture",
        ])
        .env(MODE, mode)
        .env(ROOT, root)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    if let Some((reports, service)) = tethers {
        command
            .env(REPORTS, reports.name())
            .env(PARK, service.name());
    }
    command.spawn().expect("owner")
}

/// Runs an owner to its end and returns its output and its outcome file.
fn finished(mode: &str) -> (String, String) {
    let root = tempfile::tempdir().expect("root");
    let output = owner(mode, root.path(), None)
        .wait_with_output()
        .expect("owner");
    assert!(
        output.status.success(),
        "the owner failed: {}",
        output.status
    );
    (
        String::from_utf8_lossy(&output.stdout).into_owned(),
        fs::read_to_string(root.path().join("outcome")).expect("outcome"),
    )
}

#[test]
fn spawning_a_tree_leaves_the_owners_handle_inheritance_unchanged() {
    let (_, outcome) = finished("inheritance");
    let (before, after) = outcome.split_once('\n').expect("two lines");
    assert!(
        before.contains(&format!("Some({HANDLE_FLAG_INHERIT})")),
        "the owner's output was not inheritable to begin with: {before}"
    );
    assert_eq!(
        before, after,
        "starting children changed the owner's handles"
    );
    // Negative control: withholding the owner's output is detected.
    let (_, control) = finished("inheritance-control");
    let (before, after) = control.split_once('\n').expect("two lines");
    assert_ne!(before, after, "the comparison misses a withheld handle");
}

#[test]
fn a_host_child_still_writes_to_the_owners_output_after_a_tree_spawn() {
    let (output, _) = finished("delivers");
    assert!(output.contains("std-delivered"), "{output}");
    assert!(output.contains("legacy-delivered"), "{output}");
    // Negative control: a host whose output was withheld loses the
    // implicitly inherited child's output.
    let (control, _) = finished("delivers-control");
    assert!(control.contains("std-delivered"), "{control}");
    assert!(!control.contains("legacy-delivered"), "{control}");
}

#[test]
fn a_closed_owner_output_never_blocks_a_spawn() {
    let (_, outcome) = finished("closed");
    assert_eq!(
        outcome,
        "stale=true\nnull=true\npiped=true\ninherit=true\ncustodied=true"
    );
}

/// An open process handle that kills its process when dropped.
struct Parked(HANDLE);

impl Parked {
    fn open(pid: u32) -> Self {
        // SAFETY: opens the published PID; owned below.
        let handle = unsafe { OpenProcess(PROCESS_SYNCHRONIZE | PROCESS_TERMINATE, 0, pid) };
        assert!(!handle.is_null(), "{}", io::Error::last_os_error());
        Self(handle)
    }

    fn running(&self) -> bool {
        // SAFETY: the owned handle; zero timeout.
        unsafe { WaitForSingleObject(self.0, 0) == WAIT_TIMEOUT }
    }
}

impl Drop for Parked {
    fn drop(&mut self) {
        // SAFETY: the owned handle.
        unsafe {
            TerminateProcess(self.0, 1);
            CloseHandle(self.0);
        }
    }
}

/// An owner that is killed and reaped however the test ends.
struct Reaped(std::process::Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Whether the owner's reader sees its output end once the owner closes
/// it, while the service the owner started still runs. Each wait ends on
/// the event it waits for, or at once on the owner's or service's failure.
fn output_ends_while_the_service_runs(mode: &str, bound: Duration) -> (bool, bool) {
    let deadline = std::time::Instant::now() + BACKSTOP;
    let root = tempfile::tempdir().expect("root");
    let mut reports = Tether::new().expect("reports");
    let mut service = Tether::new().expect("service");
    let mut owner = Reaped(owner(mode, root.path(), Some((&reports, &service))));
    let mut output = owner.0.stdout.take().expect("owner output");
    let (ended, end) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let _ = ended.send(output.read_to_end(&mut Vec::new()).map(|_| ()));
    });
    let started = Tether::wait(
        &mut [&mut service, &mut reports],
        &[owner.0.as_handle()],
        deadline,
    );
    let Ok(TetherEvent::Line(0, pid)) = started else {
        panic!("the service did not start: {started:?}");
    };
    // Killed when dropped, before the tether releases it.
    let service_process = Parked::open(pid.parse().expect("service PID"));
    reports.send("go").expect("go");
    let ready = Tether::wait(
        &mut [&mut reports, &mut service],
        &[owner.0.as_handle()],
        deadline,
    );
    assert_eq!(
        ready.ok(),
        Some(TetherEvent::Line(0, "ready".to_owned())),
        "the owner did not close its output"
    );
    let reached =
        end.recv_timeout(bound.min(deadline.saturating_duration_since(std::time::Instant::now())));
    let running = service_process.running();
    (matches!(reached, Ok(Ok(()))), running)
}

#[test]
fn a_service_start_releases_the_owners_output() {
    // The bound only detects an output the service still holds.
    let (ended, running) = output_ends_while_the_service_runs("service", Duration::from_secs(30));
    assert!(ended, "the service held the owner's output");
    assert!(running, "the service ended before the output did");
    // Negative control: a service started through `std` holds it.
    let (ended, running) =
        output_ends_while_the_service_runs("service-control", Duration::from_secs(2));
    assert!(!ended, "a std-started service released the owner's output");
    assert!(running, "the std-started service ended");
}

/// Production pinning classifies every slot before allocating duplicates. A real
/// closed value remains absent even when later pipe creation can reuse it.
#[test]
fn inherited_slots_are_pinned_before_pipe_creation() {
    use std::os::windows::io::AsRawHandle as _;
    let live = super::open_null(true).expect("live input");
    let closed = super::open_null(false).expect("output to close");
    let stale = closed.as_raw_handle();
    drop(closed);
    let [input, output, error] =
        super::start::pin_inherited([live.as_raw_handle(), stale, std::ptr::null_mut()])
            .expect("pin inherited slots");
    let _pipe = super::pipe(true).expect("allocate child pipe after pinning");
    assert!(input.is_some());
    assert!(
        output.is_none(),
        "a closed inherited output acquired a recycled handle"
    );
    assert!(error.is_none());
}

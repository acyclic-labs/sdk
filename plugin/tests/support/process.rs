use super::ScriptedProvider;
use acyclic_native_runtime::{ProcessTree, spawn_process_tree};
use std::{ffi::OsStr, fs, io::{Read as _, Seek as _, Write as _}, path::Path, process::{Command, Output, Stdio}, time::{Duration, Instant}};

const MAX_CAPTURED_OUTPUT_BYTES: u64 = 4 * 1024 * 1024;

pub const ACYCLIC: &str = env!("CARGO_BIN_EXE_acyclic");

pub fn command(program: impl AsRef<OsStr>) -> Command {
    let mut command = Command::new(program);
    command.env_remove("ACYCLIC_INSTALL_TEST_CRASH");
    command.env_remove("ACYCLIC_INSTALL_TEST_FAIL");
    command
}

pub fn isolated_state(command: &mut Command, root: &Path) {
    for directory in ["config", "state", "data", "local", "codex"] {
        fs::create_dir_all(root.join(directory)).expect("isolated host directory");
    }
    command.env("HOME", root);
    command.env("USERPROFILE", root);
    command.env("XDG_CONFIG_HOME", root.join("config"));
    command.env("XDG_STATE_HOME", root.join("state"));
    command.env("XDG_DATA_HOME", root.join("data"));
    command.env("LOCALAPPDATA", root.join("local"));
    command.env("CODEX_HOME", root.join("codex"));
}

pub fn output_with_stdin(command: &mut Command, input: &[u8]) -> Output {
    let bounded = output_with_stdin_timeout(command, input, Duration::from_secs(120));
    assert!(
        !bounded.expired,
        "child input operation exceeded its deadline"
    );
    bounded.output
}

pub struct BoundedOutput {
    pub output: Output,
    pub expired: bool,
    pub process_tree: ProcessTree,
}

pub fn output_with_timeout(command: &mut Command, timeout: Duration) -> BoundedOutput {
    try_output_with_timeout(command, timeout).expect("spawn bounded process tree")
}

pub fn output_with_stdin_timeout(
    command: &mut Command,
    input: &[u8],
    timeout: Duration,
) -> BoundedOutput {
    let mut stdin = tempfile::tempfile().expect("create bounded process input");
    stdin.write_all(input).expect("write bounded process input");
    stdin
        .seek(std::io::SeekFrom::Start(0))
        .expect("rewind bounded process input");
    try_output_with_timeout_and_stdin(command, timeout, Stdio::from(stdin))
        .expect("spawn bounded process tree with input")
}

pub fn output_after_provider_admission(
    command: &mut Command,
    provider: &ScriptedProvider,
    admission_timeout: Duration,
    stall_timeout: Duration,
) -> (BoundedOutput, bool) {
    let (mut process_tree, mut stdout, mut stderr) =
        spawn_captured_process_tree(command).expect("spawn provider-admission process tree");
    let admission_deadline = Instant::now() + admission_timeout;
    let admitted = loop {
        if !provider.wait_for_requests(1, Duration::ZERO).is_empty() {
            break true;
        }
        if process_tree
            .try_wait(None)
            .expect("poll provider-admission process tree")
            .is_some()
            || Instant::now() >= admission_deadline
        {
            break false;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let deadline = Instant::now() + stall_timeout;
    loop {
        if captured_output_exceeds_limit(&stdout, &stderr).expect("inspect provider output capture")
        {
            process_tree
                .terminate(None)
                .expect("terminate output-flooding provider descendants");
            panic!("provider output exceeded {MAX_CAPTURED_OUTPUT_BYTES} bytes");
        }
        match process_tree
            .try_wait(None)
            .expect("poll admitted provider process tree")
        {
            Some(status) => {
                let output = captured_output(status, &mut stdout, &mut stderr)
                    .expect("collect admitted provider process tree");
                return (
                    BoundedOutput {
                        output,
                        expired: false,
                        process_tree,
                    },
                    admitted,
                );
            }
            None if admitted && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            None => {
                process_tree
                    .terminate_descendants()
                    .expect("terminate admitted provider descendants");
                let status = process_tree
                    .wait(Duration::from_secs(5))
                    .expect("wait for timed-out provider process tree");
                let output = captured_output(status, &mut stdout, &mut stderr)
                    .expect("collect timed-out provider process tree");
                return (
                    BoundedOutput {
                        output,
                        expired: admitted,
                        process_tree,
                    },
                    admitted,
                );
            }
        }
    }
}

pub(super) fn try_output_with_timeout(
    command: &mut Command,
    timeout: Duration,
) -> std::io::Result<BoundedOutput> {
    try_output_with_timeout_and_stdin(command, timeout, Stdio::null())
}

fn try_output_with_timeout_and_stdin(
    command: &mut Command,
    timeout: Duration,
    stdin: Stdio,
) -> std::io::Result<BoundedOutput> {
    let (mut process_tree, mut stdout, mut stderr) =
        spawn_captured_process_tree_with_stdin(command, stdin)?;
    let deadline = Instant::now() + timeout;
    loop {
        if captured_output_exceeds_limit(&stdout, &stderr)? {
            process_tree.terminate(None)?;
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                format!("host output exceeded {MAX_CAPTURED_OUTPUT_BYTES} bytes"),
            ));
        }
        match process_tree.try_wait(None)? {
            Some(status) => {
                let output = captured_output(status, &mut stdout, &mut stderr)?;
                return Ok(BoundedOutput {
                    output,
                    expired: false,
                    process_tree,
                });
            }
            None if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            None => {
                process_tree.terminate_descendants()?;
                let status = process_tree.wait(Duration::from_secs(5))?;
                let output = captured_output(status, &mut stdout, &mut stderr)?;
                return Ok(BoundedOutput {
                    output,
                    expired: true,
                    process_tree,
                });
            }
        }
    }
}

fn spawn_captured_process_tree(
    command: &mut Command,
) -> std::io::Result<(ProcessTree, fs::File, fs::File)> {
    spawn_captured_process_tree_with_stdin(command, Stdio::null())
}

fn spawn_captured_process_tree_with_stdin(
    command: &mut Command,
    stdin: Stdio,
) -> std::io::Result<(ProcessTree, fs::File, fs::File)> {
    let stdout = tempfile::tempfile()?;
    let stderr = tempfile::tempfile()?;
    command
        .stdin(stdin)
        .stdout(Stdio::from(stdout.try_clone()?))
        .stderr(Stdio::from(stderr.try_clone()?));
    Ok((spawn_process_tree(command)?, stdout, stderr))
}

fn captured_output(
    status: std::process::ExitStatus,
    stdout: &mut fs::File,
    stderr: &mut fs::File,
) -> std::io::Result<Output> {
    if captured_output_exceeds_limit(stdout, stderr)? {
        return Err(std::io::Error::new(
            std::io::ErrorKind::FileTooLarge,
            format!("host output exceeded {MAX_CAPTURED_OUTPUT_BYTES} bytes"),
        ));
    }
    fn read(file: &mut fs::File) -> std::io::Result<Vec<u8>> {
        file.seek(std::io::SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        file.take(MAX_CAPTURED_OUTPUT_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_CAPTURED_OUTPUT_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                format!("host output exceeded {MAX_CAPTURED_OUTPUT_BYTES} bytes"),
            ));
        }
        Ok(bytes)
    }
    Ok(Output {
        status,
        stdout: read(stdout)?,
        stderr: read(stderr)?,
    })
}

fn captured_output_exceeds_limit(stdout: &fs::File, stderr: &fs::File) -> std::io::Result<bool> {
    Ok(stdout
        .metadata()?
        .len()
        .saturating_add(stderr.metadata()?.len())
        > MAX_CAPTURED_OUTPUT_BYTES)
}


use super::*;
use acyclic_native_runtime::{
    CaptureInput, ProcessInput, ProcessTree, run_blocking_io, shell_command,
};
use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

impl Hooks {
    /// Runs matching hooks concurrently, merging decisions in configuration order.
    /// `workspace` is the project root; `cwd` is the individual tool's directory.
    pub async fn run(
        &self,
        event: HookEvent,
        workspace: &Path,
        cwd: &Path,
        mut input: Value,
        registry: &ToolRegistry,
    ) -> Result<HookOutput> {
        if self.bindings.is_empty() {
            return Ok(HookOutput::default());
        }
        let fields = input
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("hook input must be an object".into()))?;
        fields.insert("hook_event_name".into(), Value::String(event.name().into()));
        fields.insert(
            "cwd".into(),
            Value::String(cwd.to_string_lossy().into_owned()),
        );
        let mut bytes =
            serde_json::to_vec(&input).map_err(|error| Error::Invalid(error.to_string()))?;
        bytes.push(b'\n');
        let request = Arc::new((workspace.to_path_buf(), cwd.to_path_buf(), bytes));
        self.run_commands(event, &input, registry, |hook| {
            run_hook(Arc::clone(hook), Arc::clone(&request))
        })
        .await
    }
}

pub(super) async fn run_hook(
    hook: Arc<HookSpec>,
    request: Arc<(PathBuf, PathBuf, Vec<u8>)>,
) -> Result<(Option<i32>, Vec<u8>, Vec<u8>)> {
    acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
    let cancellation = HookCancellation::new();
    let cancelled = Arc::clone(&cancellation.0);
    run_blocking_io(
        move || {
            let check = || check_cancelled(&cancelled);
            check().map_err(Error::process_io)?;
            let mut command = hook_command(&hook);
            command
                .current_dir(&request.1)
                .env("CLAUDE_PROJECT_DIR", &request.0)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            check().map_err(Error::process_io)?;
            let mut tree = ProcessTree::spawn(&mut command).map_err(Error::process_io)?;
            let output = tree
                .wait_with_output(
                    Duration::from_millis(hook.timeout_ms),
                    65_536,
                    Some(CaptureInput::Literal(&request.2)),
                    check,
                    |_| Ok(ProcessInput::default()),
                    None,
                    None,
                )
                .map_err(|failure| Error::process_io(failure.error))?;
            Ok((output.status.code(), output.stdout, output.stderr))
        },
        None,
    )
    .map_err(Error::process_io)?
    .await
    .map_err(Error::process_io)?
}

fn hook_command(hook: &HookSpec) -> Command {
    match &hook.run {
        HookRun::Executable { executable, argv } => {
            let mut command = Command::new(executable);
            command.args(argv);
            command
        }
        HookRun::Shell { command, shell } => shell_command(command, shell.as_deref()),
    }
}

struct HookCancellation(Arc<AtomicBool>);

impl HookCancellation {
    fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }
}

impl Drop for HookCancellation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn check_cancelled(cancelled: &AtomicBool) -> std::io::Result<()> {
    if cancelled.load(Ordering::Acquire) {
        Err(std::io::ErrorKind::Interrupted.into())
    } else {
        Ok(())
    }
}

use super::*;
use acyclic_native_runtime::{
    CaptureHandler, CaptureSchedule, ExecutableSearch, OwnedCaptureInput, ProcessInput,
    ProcessOutput, ProcessTree, run_blocking_io, shell_command,
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

#[doc(hidden)]
pub type HookCheck = Arc<dyn Fn() -> std::io::Result<()> + Send + Sync>;

impl Hooks {
    /// Runs matching hooks concurrently, merging decisions in configuration order.
    /// `workspace` is the project root; `cwd` is the individual tool's directory.
    pub async fn run<'a>(
        &self,
        event: HookEvent,
        workspace: &Path,
        cwd: &Path,
        input: impl Into<HookInput<'a>>,
        registry: &ToolRegistry,
    ) -> Result<HookOutput> {
        self.run_checked(
            event,
            (workspace, cwd),
            input.into(),
            registry,
            65_536,
            (Arc::new(|| Ok(())), None),
        )
        .await
    }

    #[doc(hidden)]
    pub async fn run_checked(
        &self,
        event: HookEvent,
        paths: (&Path, &Path),
        input: HookInput<'_>,
        registry: &ToolRegistry,
        maximum_bytes: u64,
        control: (HookCheck, Option<acyclic_native_runtime::ProcessWake>),
    ) -> Result<HookOutput> {
        let (workspace, cwd) = paths;
        let (check, wake) = control;
        if self.bindings.is_empty() {
            return Ok(HookOutput::default());
        }
        let bytes = input.encode(event, cwd)?;
        let request = Arc::new((workspace.to_path_buf(), cwd.to_path_buf(), bytes));
        self.run_commands(event, input, registry, maximum_bytes, |hook, quota| {
            run_hook(
                Arc::clone(hook),
                Arc::clone(&request),
                Arc::clone(&check),
                quota,
                wake.clone(),
            )
        })
        .await
    }
}

pub(super) async fn run_hook(
    hook: Arc<HookSpec>,
    request: Arc<(PathBuf, PathBuf, Vec<u8>)>,
    owner_check: HookCheck,
    maximum_bytes: usize,
    owner_wake: Option<acyclic_native_runtime::ProcessWake>,
) -> Result<(Option<i32>, Vec<u8>, Vec<u8>)> {
    acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
    let cancellation = HookCancellation::new().map_err(Error::process_io)?;
    let mut wakes = vec![cancellation.1.clone()];
    wakes.extend(owner_wake);
    let handler = HookHandler {
        cancelled: Arc::clone(&cancellation.0),
        owner_check,
        wakes,
    };
    let timeout = Duration::from_millis(hook.timeout_ms);
    let input = request.2.clone();
    // Spawning may block; the running hook then holds no worker.
    let (tree, handler) = run_blocking_io(
        move || {
            let mut handler = handler;
            if maximum_bytes == 0 {
                return Err(Error::Process(crate::ProcessError::OutputLimit {
                    exit_code: None,
                    message: "hook capture budget exhausted".into(),
                }));
            }
            handler.check().map_err(Error::process_io)?;
            let mut command = hook_command(
                &hook,
                &request.0,
                &request.1,
                crate::native_environment::variable,
            )?;
            command
                .current_dir(&request.1)
                .env("CLAUDE_PROJECT_DIR", &request.0)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            handler.check().map_err(Error::process_io)?;
            let tree = ProcessTree::spawn(&mut command).map_err(Error::process_io)?;
            Ok((tree, handler))
        },
        None,
    )
    .map_err(Error::process_io)?
    .await
    .map_err(Error::process_io)??;
    // The spawned capture keeps ownership of the tree until its cleanup
    // finishes, even if this hook's caller is cancelled.
    let outcome = acyclic_native_runtime::spawn(tree.capture(
        CaptureSchedule::new(timeout),
        maximum_bytes,
        Some(OwnedCaptureInput::Literal(input)),
        handler,
    ))
    .await
    .and_then(|outcome| outcome)
    .map_err(Error::process_io)?;
    let output = outcome.result.map_err(|failure| {
        Error::process_exit(
            failure.error,
            failure.status.and_then(|status| status.code()),
        )
    })?;
    drop(cancellation);
    Ok((output.status.code(), output.stdout, output.stderr))
}

/// A running hook's checks: its caller's cancellation, then its owner.
struct HookHandler {
    cancelled: Arc<AtomicBool>,
    owner_check: HookCheck,
    wakes: Vec<acyclic_native_runtime::ProcessWake>,
}

impl CaptureHandler for HookHandler {
    fn take_wakes(&mut self) -> std::io::Result<acyclic_native_runtime::CaptureWakes> {
        Ok(std::mem::take(&mut self.wakes).into())
    }

    fn check(&mut self) -> std::io::Result<()> {
        check_cancelled(&self.cancelled)?;
        (self.owner_check)()
    }

    fn observe(&mut self, _: ProcessOutput<'_>) -> std::io::Result<ProcessInput> {
        Ok(ProcessInput::default())
    }
}

fn hook_command(
    hook: &HookSpec,
    workspace: &Path,
    cwd: &Path,
    environment: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> Result<Command> {
    Ok(match &hook.run {
        HookRun::Executable { executable, argv } => {
            let executable = expand_exec_path(executable, workspace, &environment)?;
            let mut command = ExecutableSearch::from_environment(cwd, &environment)
                .command(&*executable)
                .map_err(Error::process_io)?;
            for argument in argv {
                command.arg(&*expand_exec_path(argument, workspace, &environment)?);
            }
            command
        }
        HookRun::Shell { command, shell } => {
            shell_command(command, shell.as_deref()).map_err(Error::process_io)?
        }
    })
}

pub(super) fn expand_exec_path<'a>(
    value: &'a str,
    workspace: &Path,
    environment: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> Result<std::borrow::Cow<'a, std::ffi::OsStr>> {
    acyclic_native_runtime::expand_environment(value, |reference| {
        let Some(reference) = reference else {
            return Ok(None);
        };
        if reference.fallback.is_some() {
            return Ok(None);
        }
        let name = reference.name;
        if name == "CLAUDE_PROJECT_DIR" {
            Ok(Some(workspace.as_os_str().to_owned()))
        } else if matches!(name, "CLAUDE_PLUGIN_ROOT" | "CLAUDE_PLUGIN_DATA") {
            environment(name).map(Some).ok_or_else(|| {
                Error::Invalid(format!("hook path placeholder {name} is not configured"))
            })
        } else {
            Ok(None)
        }
    })
}

struct HookCancellation(Arc<AtomicBool>, acyclic_native_runtime::ProcessWake);

impl HookCancellation {
    fn new() -> std::io::Result<Self> {
        Ok(Self(
            Arc::new(AtomicBool::new(false)),
            acyclic_native_runtime::ProcessWake::new()?,
        ))
    }
}

impl Drop for HookCancellation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
        let _ = self.1.stop();
    }
}

fn check_cancelled(cancelled: &AtomicBool) -> std::io::Result<()> {
    if cancelled.load(Ordering::Acquire) {
        Err(std::io::ErrorKind::Interrupted.into())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_hook_shell_uses_the_pinned_platform_contract() -> Result<()> {
        let hook = HookSpec {
            run: HookRun::Shell {
                command: "echo literal-source".into(),
                shell: None,
            },
            timeout_ms: 1_000,
            failure: HookFailure::Report,
        };
        let command = hook_command(
            &hook,
            Path::new("workspace"),
            Path::new("workspace"),
            |_| None,
        )?;
        assert_eq!(
            Path::new(command.get_program()),
            acyclic_native_runtime::platform_shell().map_err(Error::process_io)?
        );
        let args: Vec<_> = command.get_args().collect();
        let mut expected: Vec<_> = acyclic_native_runtime::shell_spec::PLATFORM
            .arguments
            .iter()
            .map(std::ffi::OsStr::new)
            .collect();
        expected.push(std::ffi::OsStr::new("echo literal-source"));
        assert_eq!(args, expected);
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn imported_batch_hooks_resolve_pathext_through_the_owned_process_core() -> Result<()> {
        let fixture = tempfile::tempdir().map_err(Error::process_io)?;
        let bin = fixture.path().join("hook & % launchers");
        std::fs::create_dir(&bin).map_err(Error::process_io)?;
        let registry = ToolRegistry::default();
        for extension in ["cmd", "bat"] {
            let name = format!("hook-{extension}");
            let path = bin.join(format!("{name}.{extension}"));
            std::fs::write(&path, "@echo off\r\n@echo batch-hook-ran\r\n")
                .map_err(Error::process_io)?;
            // JSON is the Claude/Codex adapter, TOML the Codex adapter;
            // normalized rules are also the native and plugin entry point.
            for adapter in ["json", "toml", "normalized"] {
                let mut hooks = Hooks::default();
                match adapter {
                    "json" => hooks.add_json(
                        serde_json::json!({"hooks":{"Stop":[{"hooks":[{
                            "type":"command", "command":name, "args":[]
                        }]}]}}).to_string().as_bytes(),
                    )?,
                    "toml" => hooks.add_toml(&format!(
                        "[[hooks.Stop]]\n[[hooks.Stop.hooks]]\ntype = 'command'\ncommand = '{name}'\nargs = []\n"
                    ))?,
                    _ => hooks.add_rules([HookRule {
                        on: HookEvent::Stop,
                        matcher: String::new(),
                        hooks: vec![HookSpec {
                            run: HookRun::Executable { executable: name.clone(), argv: vec![] },
                            timeout_ms: 1_000,
                            failure: HookFailure::Report,
                        }],
                    }])?,
                }
                let input = serde_json::json!({});
                let hook = hooks
                    .commands(HookEvent::Stop, &input, &registry)?
                    .next()
                    .ok_or_else(|| Error::Invalid("missing imported batch hook".into()))?;
                let mut command =
                    hook_command(hook, fixture.path(), fixture.path(), |key| match key {
                        "PATH" => Some(bin.as_os_str().to_owned()),
                        "PATHEXT" => Some(".EXE;.BAT;.CMD".into()),
                        _ => None,
                    })?;
                assert_eq!(Path::new(command.get_program()), path);
                command
                    .current_dir(fixture.path())
                    .stdin(Stdio::null())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped());
                let mut tree = ProcessTree::spawn(&mut command).map_err(Error::process_io)?;
                let output = tree
                    .wait_with_output(
                        Duration::from_secs(1),
                        1_024,
                        None,
                        || Ok(()),
                        |_| Ok(ProcessInput::default()),
                        None,
                        None,
                        None,
                    )
                    .map_err(|error| Error::process_io(error.error))?;
                assert!(output.status.success(), "{adapter}/{extension}: {output:?}");
                assert_eq!(
                    String::from_utf8_lossy(&output.stdout).trim(),
                    "batch-hook-ran"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn exec_path_placeholders_are_single_pass_literal_native_paths() -> Result<()> {
        use std::{borrow::Cow, ffi::OsString};
        let workspace = Path::new("project ${CLAUDE_PLUGIN_DATA} ' $ ` spaces");
        let environment = |name: &str| match name {
            "CLAUDE_PLUGIN_ROOT" => Some(OsString::from("plugin ' $ ` spaces")),
            "CLAUDE_PLUGIN_DATA" => Some(OsString::from("persistent plugin data")),
            _ => panic!("unexpected environment lookup"),
        };
        let argument = expand_exec_path(
            "${CLAUDE_PROJECT_DIR}/${CLAUDE_PLUGIN_ROOT}/${CLAUDE_PLUGIN_DATA}/${CLAUDE_PROJECT_DIR}",
            workspace,
            environment,
        )?;
        assert_eq!(
            &*argument,
            std::ffi::OsStr::new(
                "project ${CLAUDE_PLUGIN_DATA} ' $ ` spaces/plugin ' $ ` spaces/persistent plugin data/project ${CLAUDE_PLUGIN_DATA} ' $ ` spaces"
            )
        );
        for literal in [
            "literal $ ` ' spaces",
            "${HOME}",
            "${CLAUDE_PLUGIN_ROOT_OTHER}",
        ] {
            assert!(matches!(
                expand_exec_path(literal, workspace, |_| panic!("literal lookup"))?,
                Cow::Borrowed(_)
            ));
        }
        assert!(expand_exec_path("${CLAUDE_PLUGIN_ROOT}", workspace, |_| None).is_err());
        assert!(expand_exec_path("${CLAUDE_PLUGIN_DATA}", workspace, |_| None).is_err());
        Ok(())
    }
}

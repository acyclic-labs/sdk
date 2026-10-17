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

pub(crate) type HookCheck = Arc<dyn Fn() -> std::io::Result<()> + Send + Sync>;

impl Hooks {
    /// Runs matching hooks concurrently, merging decisions in configuration order.
    /// `workspace` is the project root; `cwd` is the individual tool's directory.
    pub async fn run(
        &self,
        event: HookEvent,
        workspace: &Path,
        cwd: &Path,
        input: Value,
        registry: &ToolRegistry,
    ) -> Result<HookOutput> {
        self.run_checked(
            event,
            (workspace, cwd),
            input,
            registry,
            65_536,
            Arc::new(|| Ok(())),
        )
        .await
    }

    pub(crate) async fn run_checked(
        &self,
        event: HookEvent,
        paths: (&Path, &Path),
        mut input: Value,
        registry: &ToolRegistry,
        maximum_bytes: u64,
        check: HookCheck,
    ) -> Result<HookOutput> {
        let (workspace, cwd) = paths;
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
        self.run_commands(event, &input, registry, maximum_bytes, |hook, quota| {
            run_hook(
                Arc::clone(hook),
                Arc::clone(&request),
                Arc::clone(&check),
                quota,
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
) -> Result<(Option<i32>, Vec<u8>, Vec<u8>)> {
    acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
    let cancellation = HookCancellation::new();
    let cancelled = Arc::clone(&cancellation.0);
    run_blocking_io(
        move || {
            if maximum_bytes == 0 {
                return Err(Error::Process(crate::ProcessError::OutputLimit {
                    exit_code: None,
                    message: "hook capture budget exhausted".into(),
                }));
            }
            let check = || {
                check_cancelled(&cancelled)?;
                owner_check()
            };
            check().map_err(Error::process_io)?;
            let mut command = hook_command(&hook, &request.0)?;
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
                    maximum_bytes,
                    Some(CaptureInput::Literal(&request.2)),
                    check,
                    |_| Ok(ProcessInput::default()),
                    None,
                    None,
                )
                .map_err(|failure| {
                    let mut error = Error::process_io(failure.error);
                    if let Error::Process(cause) = &mut error {
                        cause.set_exit_code(failure.status.and_then(|status| status.code()));
                    }
                    error
                })?;
            Ok((output.status.code(), output.stdout, output.stderr))
        },
        None,
    )
    .map_err(Error::process_io)?
    .await
    .map_err(Error::process_io)?
}

fn hook_command(hook: &HookSpec, workspace: &Path) -> Result<Command> {
    Ok(match &hook.run {
        HookRun::Executable { executable, argv } => {
            let environment = |name: &str| std::env::var_os(name);
            let executable = expand_exec_path(executable, workspace, environment)?;
            let mut command = Command::new(&*executable);
            for argument in argv {
                command.arg(&*expand_exec_path(argument, workspace, environment)?);
            }
            command
        }
        HookRun::Shell { command, shell } => shell_command(command, shell.as_deref()),
    })
}

pub(super) fn expand_exec_path<'a>(
    value: &'a str,
    workspace: &Path,
    environment: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> Result<std::borrow::Cow<'a, std::ffi::OsStr>> {
    let mut expanded = None;
    let mut consumed = 0;
    for (start, _) in value.match_indices("${") {
        let tail = &value[start..];
        let name = [
            "CLAUDE_PROJECT_DIR",
            "CLAUDE_PLUGIN_ROOT",
            "CLAUDE_PLUGIN_DATA",
        ]
        .into_iter()
        .find(|name| {
            tail.strip_prefix("${")
                .and_then(|tail| tail.strip_prefix(name))
                .is_some_and(|tail| tail.starts_with('}'))
        });
        let Some(name) = name else {
            continue;
        };
        let output = expanded.get_or_insert_with(std::ffi::OsString::new);
        output.push(&value[consumed..start]);
        if name == "CLAUDE_PROJECT_DIR" {
            output.push(workspace);
        } else {
            output.push(environment(name).ok_or_else(|| {
                Error::Invalid(format!("hook path placeholder {name} is not configured"))
            })?);
        }
        consumed = start + name.len() + 3;
    }
    match expanded {
        Some(mut expanded) => {
            expanded.push(&value[consumed..]);
            Ok(std::borrow::Cow::Owned(expanded))
        }
        None => Ok(std::borrow::Cow::Borrowed(std::ffi::OsStr::new(value))),
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

#[cfg(test)]
mod tests {
    use super::*;

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

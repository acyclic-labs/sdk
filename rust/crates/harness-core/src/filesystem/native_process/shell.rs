use super::Invocation;
#[cfg(test)]
#[cfg(unix)]
use super::{NativeProcessObserver, NativeProcessResult, capture};
use crate::{Error, Result, tools::ShellInput, workspace::MountTable};
use acyclic_fs::access::Action;
use std::path::Path;
#[cfg(test)]
#[cfg(unix)]
use std::sync::Arc;

/// Validates the already-composed task namespace and uses the native provider's
/// capture path. Its caller retains the view and publishes each writable mount.
#[cfg(test)]
#[cfg(unix)]
pub(crate) async fn run<T>(
    input: ShellInput,
    mounts: &MountTable<T>,
    invocation: Invocation,
    now: u64,
    observer: Option<Arc<NativeProcessObserver>>,
    check: impl Fn() -> std::io::Result<()> + Send + 'static,
) -> Result<Option<NativeProcessResult>> {
    let invocation = prepare(input, mounts, invocation, now)?;
    capture::capture_with_custody(
        invocation,
        move |_: &mut (), chunk| {
            if let Some(observer) = &observer {
                observer(chunk)?;
            }
            Ok(())
        },
        check,
        (),
        None,
        capture::OutputRetention::Collect,
    )
    .await
    .and_then(|(output, ())| output)
}

#[doc(hidden)]
pub fn prepare<T>(
    input: ShellInput,
    mounts: &MountTable<T>,
    mut invocation: Invocation,
    now: u64,
) -> Result<Invocation> {
    input.validate()?;
    let relative = Path::new(input.cwd.as_deref().unwrap_or(""));
    let (mount, path) = mounts.resolve(relative)?;
    mount.check(Action::List, &path, now)?;
    let io = |cause| Error::from(acyclic_fs::NativeTreeMaterializationError::Io(cause));
    // A volume's root is its binding, which may link to the view projecting
    // it, so the canonical cwd is checked against the canonical root.
    let native_root = mount
        .native_root
        .as_deref()
        .map(std::fs::canonicalize)
        .transpose()
        .map_err(io)?;
    let root = native_root.as_deref().unwrap_or(&mounts.root);
    let requested = if native_root.is_some() {
        root.join(&path)
    } else {
        mounts.root.join(relative)
    };
    let cwd = std::fs::canonicalize(requested).map_err(io)?;
    let selected = cwd
        .strip_prefix(root)
        .map_err(|_| Error::Invalid("shell cwd escapes the task view".into()))?;
    if mount.native_root.is_some() {
        let selected = selected
            .to_str()
            .ok_or_else(|| Error::Invalid("shell cwd is not UTF-8".into()))?;
        mount.check(Action::List, &selected.replace('\\', "/"), now)?;
    } else if selected != relative {
        let (mount, path) = mounts.resolve(selected)?;
        mount.check(Action::List, &path, now)?;
    }
    invocation.cwd = cwd;
    for (name, value) in mounts.environment()? {
        if invocation.environment.insert(name, value).is_some() {
            return Err(Error::Invalid(
                "volume discovery hint collides with the explicit process environment".into(),
            ));
        }
    }
    // Shell commands receive only the host's explicit ordinary variables and
    // discovery hints. Ambient credentials and startup hooks stay host-owned,
    // even when another native process caller requested inheritance.
    invocation.inherit_environment = false;
    invocation.schedule = input.timeout_ms()?.map_or(
        acyclic_native_runtime::CaptureSchedule::Unbounded,
        |timeout| {
            acyclic_native_runtime::CaptureSchedule::new(std::time::Duration::from_millis(
                u64::from(timeout),
            ))
        },
    );
    let shell = &crate::tools::shell::PLATFORM;
    let (program, arguments, standard_input) = input.command.prepare(shell)?;
    if let Some(program) = program {
        let search =
            acyclic_native_runtime::ExecutableSearch::from_environment(&invocation.cwd, |name| {
                invocation
                    .environment
                    .iter()
                    .find(|(key, _)| {
                        if cfg!(windows) {
                            key.eq_ignore_ascii_case(name)
                        } else {
                            key.as_str() == name
                        }
                    })
                    .map(|(_, value)| value.into())
            });
        invocation.executable = search.resolve(program).map_err(io)?;
    }
    invocation.argv = arguments;
    invocation.standard_input =
        standard_input.map(acyclic_native_runtime::OwnedCaptureInput::Literal);
    if invocation.execution_grant.is_none() {
        invocation.execution_grant = workspace_grant(mounts, &mut invocation.environment)?;
    }
    super::invocation::validate(
        &invocation.executable,
        &invocation.cwd,
        root,
        &invocation.environment,
        &invocation.argv,
    )?;
    Ok(invocation)
}

/// Host-owned workspace boundary shared by shell and lifecycle hooks. Explicit
/// execution grants retain their existing, separately authenticated authority.
/// Linux refuses missing kernel support at launch, never retries unconfined.
/// Other OS adapters remain pending and retain their inherited host boundary.
#[doc(hidden)]
pub fn workspace_grant<T>(
    mounts: &MountTable<T>,
    environment: &mut std::collections::BTreeMap<String, String>,
) -> Result<Option<acyclic_native_runtime::execution::ExecutionGrant>> {
    #[cfg(target_os = "linux")]
    {
        use acyclic_native_runtime::execution::{
            ExecutionAccess, ExecutionGrant, ExecutionNetwork,
        };
        // Workspace-write preserves host reads. Narrower explicit grants can
        // restrict reads; this default boundary confines filesystem mutation.
        let mut read = vec![std::path::PathBuf::from("/")];
        let mut write = Vec::new();
        let mut access = ExecutionAccess::ReadOnly;
        let io = Error::process_io;
        let mut primary = mounts.root.clone();
        for (relative, mount) in mounts.mounts() {
            let root = mount
                .native_root
                .clone()
                .unwrap_or_else(|| mounts.root.join(relative));
            if relative.is_empty() {
                primary = root;
                if !mount.mode.read_only() {
                    access = ExecutionAccess::WorkspaceWrite;
                }
            } else if mount.mode.read_only() {
                read.push(root);
            } else {
                write.push(root);
            }
        }
        // Standard temporary-file APIs stay inside the same writable view;
        // this adds no global /tmp allowance or per-agent mount/directory.
        environment.insert(
            "TMPDIR".into(),
            primary
                .to_str()
                .ok_or_else(|| Error::Invalid("sandbox temporary directory is not UTF-8".into()))?
                .into(),
        );
        ExecutionGrant::composed(&primary, access, write, read, ExecutionNetwork::On)
            .map(Some)
            .map_err(io)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (mounts, environment);
        Ok(None)
    }
}

#[cfg(test)]
#[cfg(unix)]
mod tests;

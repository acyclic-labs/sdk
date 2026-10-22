use super::Invocation;
#[cfg(all(test, unix))]
use super::{NativeProcessObserver, NativeProcessResult, capture};
use crate::{Error, Result, tools::ShellInput, workspace::MountTable};
use acyclic_fs::access::Action;
use std::path::Path;
#[cfg(all(test, unix))]
use std::sync::Arc;

/// Validates the already-composed task namespace and uses the native provider's
/// capture path. Its caller retains the view and publishes each writable mount.
#[cfg(all(test, unix))]
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
    )
    .await
    .map(|(output, ())| output)
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
    let root = mount.native_root.as_deref().unwrap_or(&mounts.root);
    let requested = if mount.native_root.is_some() {
        root.join(&path)
    } else {
        mounts.root.join(relative)
    };
    let cwd = std::fs::canonicalize(requested)
        .map_err(|cause| Error::from(acyclic_fs::NativeTreeMaterializationError::Io(cause)))?;
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
    invocation.environment = mounts.environment()?;
    invocation.inherit_environment = true;
    invocation.timeout_ms = input.timeout_ms()?;
    #[cfg(windows)]
    let command = format!(
        "{}& ([ScriptBlock]::Create('{}'))",
        crate::tools::shell::PLATFORM.command_prefix,
        input.command.replace('\'', "''"),
    );
    #[cfg(not(windows))]
    let command = input.command;
    invocation.argv = crate::tools::shell::PLATFORM
        .arguments
        .iter()
        .map(|argument| (*argument).into())
        .chain([command])
        .collect();
    invocation.standard_input = None;
    invocation.mcp_stdio = None;
    super::invocation::validate(
        &invocation.executable,
        &invocation.cwd,
        root,
        &invocation.environment,
        &invocation.argv,
    )?;
    Ok(invocation)
}

#[cfg(all(test, unix))]
mod tests;

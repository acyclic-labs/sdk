#[cfg(all(test, unix))]
use super::NativeProcessObserver;
use super::{Invocation, NativeProcessResult, capture};
use crate::{Error, Result, tools::BashInput, workspace::MountTable};
use acyclic_fs::access::Action;
use std::path::Path;
#[cfg(all(test, unix))]
use std::sync::Arc;

/// Validates the already-composed task namespace and uses the native provider's
/// capture path. Its caller retains the view and publishes each writable mount.
#[cfg(all(test, unix))]
pub(crate) async fn run<T>(
    input: BashInput,
    mounts: &MountTable<T>,
    invocation: Invocation,
    now: u64,
    observer: Option<Arc<NativeProcessObserver>>,
    check: impl Fn() -> std::io::Result<()> + Send + 'static,
) -> Result<Option<NativeProcessResult>> {
    run_with_custody(
        input,
        mounts,
        invocation,
        now,
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

pub(crate) async fn run_with_custody<T, C: Send + 'static>(
    input: BashInput,
    mounts: &MountTable<T>,
    mut invocation: Invocation,
    now: u64,
    observer: impl FnMut(&mut C, acyclic_native_runtime::ProcessOutput<'_>) -> std::io::Result<()>
    + Send
    + 'static,
    check: impl Fn() -> std::io::Result<()> + Send + 'static,
    custody: C,
) -> Result<(Option<NativeProcessResult>, C)> {
    input.validate()?;
    let relative = Path::new(input.cwd.as_deref().unwrap_or(""));
    let (mount, path) = mounts.resolve(relative)?;
    mount.check(Action::List, &path, now)?;
    let cwd = std::fs::canonicalize(mounts.root.join(relative))
        .map_err(|cause| Error::from(acyclic_fs::NativeTreeMaterializationError::Io(cause)))?;
    let selected = cwd
        .strip_prefix(&mounts.root)
        .map_err(|_| Error::Invalid("bash cwd escapes the task view".into()))?;
    if selected != relative {
        let (mount, path) = mounts.resolve(selected)?;
        mount.check(Action::List, &path, now)?;
    }
    invocation.cwd = cwd;
    invocation.environment = mounts.environment()?;
    invocation.timeout_ms = input.timeout_ms()?;
    invocation.argv = vec![
        "--noprofile".into(),
        "--norc".into(),
        "-c".into(),
        input.command,
    ];
    invocation.standard_input = None;
    invocation.mcp_stdio = None;
    super::invocation::validate(
        &invocation.executable,
        &invocation.cwd,
        &mounts.root,
        &invocation.environment,
        &invocation.argv,
    )?;
    capture::capture_with_custody(invocation, observer, check, custody).await
}

#[cfg(all(test, unix))]
mod tests;

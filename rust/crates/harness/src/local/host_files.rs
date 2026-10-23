//! A workspace-local file projection of ordinary harness command files.
//!
//! Only the trusted `GraphCoder` owner opens state. Sandboxed clients discover
//! this already-running endpoint and write within their admitted workspace.
use super::host_owner::{HostAction, HostWorkspaceOwner};
use super::*;
use crate::{
    client::{CommandResolution, CommandSession},
    task_volume::{Component, FileKey, FileValue, Query, TaskFile},
};
use notify::{RecursiveMode, Watcher as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{io::Read as _, time::Duration};

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

/// Reserved inside the host's existing writable workspace; never a state path.
pub const DIRECTORY: &str = ".acyclic-sdk/host-files";
const MAXIMUM_BYTES: u64 = 65_536;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Endpoint {
    version: u32,
    session: uuid::Uuid,
    workspace: PathBuf,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    session: uuid::Uuid,
    operation: OperationId,
    action: HostAction,
}

/// Contacts an existing harness. Does not select state, acquire a lifecycle
/// lock, publish an executable identity, or start any service or agent.
pub fn request(cwd: &Path, action: HostAction) -> Result<Value> {
    let cwd = std::fs::canonicalize(cwd).map_err(storage)?;
    let (directory, endpoint) = discover(&cwd)?;
    let operation = OperationId::new();
    let request = Request {
        session: endpoint.session,
        operation,
        action,
    };
    let result_path = directory.join("results").join(format!("{operation}.json"));
    let request_path = directory.join("requests").join(format!("{operation}.json"));
    let (wake, events) = std::sync::mpsc::sync_channel(1);
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        let _ = wake.try_send(event.map(|_| ()));
    })
    .map_err(storage)?;
    watcher
        .watch(&directory.join("results"), RecursiveMode::NonRecursive)
        .map_err(storage)?;
    let bytes = serde_json::to_vec(&request).map_err(storage)?;
    if bytes.len() as u64 > MAXIMUM_BYTES {
        return Err(Error::Invalid(
            "native host request exceeds file bound".into(),
        ));
    }
    immutable(&request_path, &bytes)?;
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    let result = loop {
        match regular_bytes(&result_path) {
            Ok(bytes) => break serde_json::from_slice::<Value>(&bytes).map_err(storage)?,
            Err(Error::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
        let remaining = deadline
            .checked_duration_since(std::time::Instant::now())
            .ok_or_else(|| Error::Indeterminate(operation))?;
        events
            .recv_timeout(remaining)
            .map_err(|_| Error::Indeterminate(operation))?
            .map_err(storage)?;
    };
    // These are disposable projections. Durable request/claim/result files
    // remain in TaskFiles, including uncertain outcomes after a killed owner.
    remove_projection(&request_path)?;
    remove_projection(&result_path)?;
    result.get("ok").cloned().ok_or_else(|| {
        Error::Conflict(
            result
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("harness returned no outcome")
                .into(),
        )
    })
}

fn discover(cwd: &Path) -> Result<(PathBuf, Endpoint)> {
    for ancestor in cwd.ancestors() {
        let directory = ancestor.join(DIRECTORY);
        match regular_bytes(&directory.join("owner.json")) {
            Ok(bytes) => {
                let endpoint: Endpoint = serde_json::from_slice(&bytes).map_err(storage)?;
                if endpoint.version != 1
                    || endpoint.workspace != ancestor
                    || std::fs::canonicalize(&directory).map_err(storage)? != directory
                {
                    return Err(Error::Unauthorized(
                        "native host endpoint is outside its admitted workspace".into(),
                    ));
                }
                return Ok((directory, endpoint));
            }
            Err(Error::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
    }
    Err(Error::NotFound("no running GraphCoder workspace owner; start graphcoder --host-workspace outside the host sandbox".into()))
}

/// Runs only in the trusted `GraphCoder` process, outside host sandboxes.
/// Every command uses the existing immutable request/claim/result consumer.
pub async fn serve(local: Arc<LocalHarness>) -> Result<()> {
    let workspace = local
        .workspace_path()
        .ok_or_else(|| Error::Invalid("native hosts require a checkout".into()))?
        .to_path_buf();
    let held = acyclic_fs::hold_root(&workspace).map_err(storage)?;
    let directory = held
        .create_directories(Path::new(DIRECTORY))
        .map_err(storage)?;
    let _ownership = acyclic_native_runtime::FileLock::try_acquire(
        directory
            .open_file(Path::new("owner.lock"), true)
            .map_err(storage)?,
        acyclic_native_runtime::FileLockMode::Exclusive,
    )
    .map_err(storage)?;
    directory
        .create_directories(Path::new("requests"))
        .map_err(storage)?;
    directory
        .create_directories(Path::new("results"))
        .map_err(storage)?;
    let session = uuid::Uuid::from_bytes(
        context::publication_operation(
            b"acyclic.native-files/v1\0",
            workspace.as_os_str().as_encoded_bytes(),
        )
        .into_bytes(),
    );
    let endpoint = Endpoint {
        version: 1,
        session,
        workspace,
    };
    let bytes = serde_json::to_vec(&endpoint).map_err(storage)?;
    let descriptor = directory.path().join("owner.json");
    immutable(&descriptor, &bytes)?;
    let mut owner = HostWorkspaceOwner::new(Arc::clone(&local))?;
    let mut commands = CommandSession::open_local(&local, session, MAXIMUM_BYTES, 4096).await?;
    let changed = Arc::new(tokio::sync::Notify::new());
    let wake = Arc::clone(&changed);
    let mut watcher =
        notify::recommended_watcher(move |_: notify::Result<notify::Event>| wake.notify_one())
            .map_err(storage)?;
    watcher
        .watch(
            &directory.path().join("requests"),
            RecursiveMode::NonRecursive,
        )
        .map_err(storage)?;
    loop {
        let waiting = changed.notified();
        for entry in std::fs::read_dir(directory.path().join("requests")).map_err(storage)? {
            let entry = entry.map_err(storage)?;
            if entry
                .path()
                .extension()
                .is_none_or(|extension| extension != "json")
            {
                continue;
            }
            // Request projections are writable by native tools. Invalid input
            // must not terminate the trusted owner or dispatch another action.
            let request = regular_bytes(&entry.path()).and_then(|bytes| {
                let request: Request = serde_json::from_slice(&bytes).map_err(storage)?;
                if request.session != session
                    || entry.file_name() != format!("{}.json", request.operation).as_str()
                {
                    return Err(Error::Unauthorized(
                        "native request identity changed".into(),
                    ));
                }
                Ok(request)
            });
            let Ok(request) = request else {
                // Held-root removal unlinks the projection itself, never a
                // symlink target. Clients keep their durable command records.
                let _ = remove_projection(&entry.path());
                continue;
            };
            process_request(
                &local,
                &mut owner,
                &mut commands,
                &Component::new(session.to_string()).map_err(storage)?,
                directory.path(),
                request,
            )
            .await?;
        }
        waiting.await;
    }
}

async fn process_request(
    local: &LocalHarness,
    owner: &mut HostWorkspaceOwner,
    commands: &mut CommandSession<dyn acyclic_stream::StreamProvider>,
    session: &Component,
    directory: &Path,
    request: Request,
) -> Result<()> {
    let component = Component::new(request.operation.to_string()).map_err(storage)?;
    let key = |file| FileKey::Session {
        session: session.clone(),
        file,
    };
    let resolution = CommandResolution::Sdk {
        command: "workspace".into(),
        arguments: serde_json::to_string(&request.action).map_err(storage)?,
    };
    let file = FileValue::new(
        key(TaskFile::Request(component.clone())),
        serde_json::to_vec(&resolution).map_err(storage)?,
    );
    let snapshot = local
        .task_files
        .snapshot(&Query::File(file.key.clone()), 1)
        .await
        .map_err(storage)?;
    crate::task_volume::publish(
        &local.task_files.inner,
        snapshot.generation,
        &file,
        || async { Ok::<_, crate::task_volume::Error>((Vec::new(), None)) },
    )
    .await
    .map_err(storage)?;
    let outcome = commands
        .consume(&file, |operation, _, resolution| {
            let owner = &mut *owner;
            // Keep the workspace action behind one future boundary. The shared
            // consumer's recovery state must not embed every filesystem action
            // in the native GraphCoder entrypoint's future layout.
            let action: acyclic_stream::BoxProviderFuture<'_, Result<Value>> =
                Box::pin(async move {
                    let CommandResolution::Sdk { command, arguments } = resolution else {
                        return Err(Error::Invalid(
                            "native request is not workspace coordination".into(),
                        ));
                    };
                    if command != "workspace" {
                        return Err(Error::Unsupported(command));
                    }
                    owner
                        .dispatch(
                            operation,
                            serde_json::from_str(&arguments).map_err(storage)?,
                        )
                        .await
                });
            action
        })
        .await;
    let result = local
        .task_files
        .snapshot(&Query::File(key(TaskFile::ExecutionResult(component))), 1)
        .await
        .map_err(storage)?;
    if let Some(result) = result.files.first() {
        immutable(
            &directory
                .join("results")
                .join(format!("{}.json", request.operation)),
            &result.bytes,
        )?;
    } else {
        outcome?;
    }
    Ok(())
}

fn remove_projection(path: &Path) -> Result<()> {
    let (parent, leaf) = projection_parent(path)?;
    parent.remove_file(&leaf).map_err(native_file_error)
}

fn regular_bytes(path: &Path) -> Result<Vec<u8>> {
    let (parent, leaf) = projection_parent(path)?;
    let file = parent.open_file(&leaf, false).map_err(native_file_error)?;
    let metadata = file.metadata().map_err(storage)?;
    if !metadata.is_file() || metadata.len() > MAXIMUM_BYTES {
        return Err(Error::Invalid(
            "native file projection must be a bounded regular file".into(),
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAXIMUM_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(storage)?;
    if bytes.len() as u64 > MAXIMUM_BYTES {
        return Err(Error::Invalid(
            "native file projection grew beyond its bound".into(),
        ));
    }
    Ok(bytes)
}

fn immutable(path: &Path, bytes: &[u8]) -> Result<()> {
    if bytes.len() as u64 > MAXIMUM_BYTES {
        return Err(Error::Invalid(
            "native file projection exceeds its bound".into(),
        ));
    }
    let (parent, leaf) = projection_parent(path)?;
    parent
        .publish_immutable(&leaf, bytes)
        .map_err(native_file_error)
}

fn native_file_error(error: acyclic_fs::demand::DemandError) -> Error {
    match error {
        acyclic_fs::demand::DemandError::Absent => {
            Error::NotFound("native file projection absent".into())
        }
        acyclic_fs::demand::DemandError::Io(ref cause)
            if cause.kind() == std::io::ErrorKind::NotFound =>
        {
            Error::NotFound("native file projection absent".into())
        }
        error => storage(error),
    }
}

fn projection_parent(path: &Path) -> Result<(acyclic_fs::HeldRoot, PathBuf)> {
    let endpoint = path
        .ancestors()
        .find(|ancestor| ancestor.ends_with(DIRECTORY))
        .ok_or_else(|| Error::Invalid("native projection is outside its workspace".into()))?;
    let workspace = endpoint
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| Error::Invalid("native projection workspace absent".into()))?;
    let parent = path
        .parent()
        .ok_or_else(|| Error::Invalid("native projection parent absent".into()))?;
    let leaf = PathBuf::from(
        path.file_name()
            .ok_or_else(|| Error::Invalid("native projection leaf absent".into()))?,
    );
    let root = acyclic_fs::hold_root(workspace).map_err(native_file_error)?;
    let parent = root
        .open_directory(parent.strip_prefix(workspace).map_err(storage)?)
        .map_err(native_file_error)?;
    Ok((parent, leaf))
}

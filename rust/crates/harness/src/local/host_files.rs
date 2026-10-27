//! A workspace-local file projection of ordinary harness command files.
//!
//! Only the trusted `GraphCoder` owner opens state. Sandboxed clients discover
//! this already-running endpoint and write within their admitted workspace.
use super::host_owner::{HostAction, HostWorkspaceOwner};
use super::*;
use notify::{RecursiveMode, Watcher as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{io::Read as _, time::Duration};

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

/// Reserved inside the host's existing writable workspace; never a state path.
pub const DIRECTORY: &str = ".acyclic-sdk/host-files";
const MAXIMUM_BYTES: u64 = 65_536;

// Both endpoints observe their own reads. Admit only events that can change
// the projection, so opening a file cannot schedule another scan of itself.
fn projection_changed(event: &notify::Result<notify::Event>) -> bool {
    !event
        .as_ref()
        .is_ok_and(|event| matches!(event.kind, notify::EventKind::Access(_)))
}

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

/// The existing owner's admitted workspace. This only reads its descriptor;
/// nested clients keep disposable custody files outside authored child views.
pub fn workspace_root(cwd: &Path) -> Result<PathBuf> {
    let cwd = std::fs::canonicalize(cwd).map_err(storage)?;
    discover(&cwd).map(|(_, endpoint)| endpoint.workspace)
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
        if !projection_changed(&event) {
            return;
        }
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
    // These are disposable projections. Every effect is an agent commit keyed
    // by this request's operation, so the reply is the only state they hold.
    remove_projection(&request_path)?;
    remove_projection(&result_path)?;
    if result.get("completion").and_then(Value::as_str) == Some("indeterminate") {
        return Err(Error::Indeterminate(operation));
    }
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
/// Each request dispatches once into the native agent runtime, whose commits
/// it keys by the request operation, then publishes its immutable reply.
pub async fn serve(local: Arc<LocalHarness>) -> Result<()> {
    serve_until(local, std::future::pending()).await
}

/// Stops accepting requests, joins view retirement and disconnects the shared
/// harness before reporting shutdown. Native callbacks keep their runtime.
pub async fn serve_until(
    local: Arc<LocalHarness>,
    shutdown: impl std::future::Future<Output = Result<()>>,
) -> Result<()> {
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
    let mut owner =
        HostWorkspaceOwner::new(Arc::clone(&local))?.with_requests(directory.path().to_path_buf());
    let changed = Arc::new(tokio::sync::Notify::new());
    let wake = Arc::clone(&changed);
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        // Scanning requests opens the watched directory. Access notifications
        // must not wake this same scan indefinitely and starve native mounts.
        if projection_changed(&event) {
            wake.notify_one();
        }
    })
    .map_err(storage)?;
    watcher
        .watch(
            &directory.path().join("requests"),
            RecursiveMode::NonRecursive,
        )
        .map_err(storage)?;
    let result = tokio::select! {
        result = serve_requests(&mut owner, directory.path(), session, &changed) => result,
        result = shutdown => result,
    };
    drop(watcher);
    drop(local);
    owner.retire().await?;
    result
}

async fn serve_requests(
    owner: &mut HostWorkspaceOwner,
    directory: &Path,
    session: uuid::Uuid,
    changed: &tokio::sync::Notify,
) -> Result<()> {
    loop {
        let waiting = changed.notified();
        for entry in std::fs::read_dir(directory.join("requests")).map_err(storage)? {
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
            process_request(owner, directory, request).await?;
        }
        waiting.await;
    }
}

async fn process_request(
    owner: &mut HostWorkspaceOwner,
    directory: &Path,
    request: Request,
) -> Result<()> {
    let result_path = directory
        .join("results")
        .join(format!("{}.json", request.operation));
    match regular_bytes(&result_path) {
        Ok(_) => return Ok(()),
        Err(Error::NotFound(_)) => {}
        Err(error) => return Err(error),
    }
    let outcome = owner.dispatch(request.operation, request.action).await;
    let bytes = crate::client::encode_command_result(0, &outcome, MAXIMUM_BYTES)?;
    immutable(&result_path, &bytes)
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

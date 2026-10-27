//! Drives a harness through files, for hosts whose sandbox admits only their
//! workspace (no network, no sockets).
//!
//! Only the trusted `GraphCoder` owner opens state. A sandboxed client drops
//! one request file under `requests/`: that file is the record, keyed by its
//! operation, and every effect it causes is an agent commit keyed by the same
//! operation. The owner keeps one latest-status file per agent under
//! `status/<agent>/`, holding the replies to that agent's requests whose files
//! are still present. A client awaits its reply on one process-wide watcher
//! and withdraws its request once read; the owner then drops the reply. There
//! is no per-request result file, watcher or deadline: a request waits until
//! it is answered or its owner exits.
use super::host_owner::{HostAction, HostWorkspaceOwner};
use super::*;
use notify::{RecursiveMode, Watcher as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read as _;
use std::sync::{Mutex, mpsc};

#[cfg(all(test, not(loom)))]
mod tests;

/// Reserved inside the host's existing writable workspace; never a state path.
pub const DIRECTORY: &str = ".acyclic-sdk/host-files";
const MAXIMUM_BYTES: u64 = 65_536;
/// Bound of one agent's status file: the replies of its in-flight requests.
const STATUS_BYTES: u64 = 16 * MAXIMUM_BYTES;
/// Status key of requests that name no agent yet.
const WORKSPACE: &str = "workspace";

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

/// The status file `action`'s reply appears in: its agent's, else the
/// workspace's.
fn status_key(action: &HostAction) -> String {
    let agent = match action {
        #[cfg(feature = "test-support")]
        HostAction::ResourceProbe { task, .. } => Some(task),
        HostAction::Sessions { parent } | HostAction::Session { parent, .. } => Some(parent),
        HostAction::Reopen { task }
        | HostAction::Capture { task }
        | HostAction::Fs { task, .. }
        | HostAction::Agents { task } => Some(task),
        HostAction::Hook { .. } | HostAction::Locate { .. } | HostAction::Open { .. } => None,
    };
    agent.map_or_else(|| WORKSPACE.into(), |agent| agent.id().to_string())
}

const NO_OWNER: &str = "no running GraphCoder workspace owner; start graphcoder --host-workspace outside the host sandbox";

enum Wake {
    Changed,
    OwnerGone,
}

/// This process's one wait for replies from one running owner: a watcher
/// over the status directories it awaits and a thread that wakes it when the
/// owner's lock is released. Requests from one process wait in turn; the
/// owner serves requests one at a time regardless.
struct Replies {
    directory: PathBuf,
    watcher: notify::RecommendedWatcher,
    watched: BTreeSet<PathBuf>,
    wakes: mpsc::Receiver<Wake>,
    gone: bool,
}

static REPLIES: Mutex<Option<Replies>> = Mutex::new(None);

impl Replies {
    fn open(directory: &Path) -> Result<Self> {
        let root = acyclic_fs::hold_root(directory).map_err(native_file_error)?;
        let open = || {
            root.open_file(Path::new("owner.lock"), false)
                .map_err(native_file_error)
        };
        let (lock, probe) = (open()?, open()?);
        // A shared lock is free exactly when no owner holds its exclusive one.
        if acyclic_native_runtime::FileLock::try_acquire(
            probe,
            acyclic_native_runtime::FileLockMode::Shared,
        )
        .is_ok()
        {
            return Err(Error::NotFound(NO_OWNER.into()));
        }
        let (wake, wakes) = mpsc::sync_channel(1);
        let changed = wake.clone();
        let watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
            if projection_changed(&event) {
                let _ = changed.try_send(Wake::Changed);
            }
        })
        .map_err(storage)?;
        std::thread::Builder::new()
            .name("acyclic-host-files-owner".into())
            .spawn(move || {
                // Blocks until the owner's exclusive lock is released.
                drop(acyclic_native_runtime::FileLock::acquire(
                    lock,
                    acyclic_native_runtime::FileLockMode::Shared,
                ));
                let _ = wake.send(Wake::OwnerGone);
            })
            .map_err(storage)?;
        Ok(Self {
            directory: directory.to_path_buf(),
            watcher,
            watched: BTreeSet::new(),
            wakes,
            gone: false,
        })
    }

    fn watch(&mut self, status: &Path) -> Result<()> {
        if self.watched.insert(status.to_path_buf()) {
            self.watcher
                .watch(status, RecursiveMode::NonRecursive)
                .map_err(storage)?;
        }
        Ok(())
    }

    /// Whether the owner has not been seen to exit; consumes pending wakes,
    /// after which the awaited status is read afresh.
    fn live(&mut self) -> bool {
        while let Ok(wake) = self.wakes.try_recv() {
            self.gone = self.gone || matches!(wake, Wake::OwnerGone);
        }
        !self.gone
    }

    /// Blocks until a status changes; false once the owner exited.
    fn changed(&mut self) -> bool {
        self.gone = self.gone || !matches!(self.wakes.recv(), Ok(Wake::Changed));
        !self.gone
    }
}

/// Contacts an existing harness. Does not select state, acquire a lifecycle
/// lock, publish an executable identity, or start any service or agent.
pub fn request(cwd: &Path, action: HostAction) -> Result<Value> {
    let cwd = std::fs::canonicalize(cwd).map_err(storage)?;
    let (directory, endpoint) = discover(&cwd)?;
    let operation = OperationId::new();
    let key = status_key(&action);
    let request = Request {
        session: endpoint.session,
        operation,
        action,
    };
    let bytes = serde_json::to_vec(&request).map_err(storage)?;
    if bytes.len() as u64 > MAXIMUM_BYTES {
        return Err(Error::Invalid(
            "native host request exceeds file bound".into(),
        ));
    }
    let mut replies = REPLIES
        .lock()
        .map_err(|_| Error::Conflict("native reply wait poisoned".into()))?;
    if replies
        .as_mut()
        .is_none_or(|replies| !replies.live() || replies.directory != directory)
    {
        *replies = Some(Replies::open(&directory)?);
    }
    let replies = replies
        .as_mut()
        .ok_or_else(|| Error::Conflict("native reply wait absent".into()))?;
    let status = directory.join("status").join(&key);
    acyclic_fs::hold_root(&directory)
        .map_err(native_file_error)?
        .create_directories(&Path::new("status").join(&key))
        .map_err(native_file_error)?;
    // Watch before publishing, so the reply cannot land unobserved.
    replies.watch(&status)?;
    let request_path = directory.join("requests").join(format!("{operation}.json"));
    immutable(&request_path, &bytes)?;
    let result = loop {
        if let Some(result) = reply(&status, operation)? {
            break result;
        }
        if !replies.changed() {
            // Withdrawn, so no later owner serves a request nobody awaits.
            // Whatever the exited owner committed stays keyed by `operation`.
            remove_projection(&request_path)?;
            return Err(Error::Indeterminate(operation));
        }
    };
    // Withdrawing the request retires its reply and any saved result. Every
    // effect is an agent commit keyed by this operation.
    remove_projection(&request_path)?;
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

/// The newest status file under `status`, by its sequence.
fn latest(status: &Path) -> Result<Option<(u64, PathBuf)>> {
    let mut newest: Option<(u64, PathBuf)> = None;
    for entry in std::fs::read_dir(status).map_err(storage)? {
        let path = entry.map_err(storage)?.path();
        let sequence = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .filter(|_| {
                path.extension()
                    .is_some_and(|extension| extension == "json")
            })
            .and_then(|stem| stem.parse::<u64>().ok());
        if let Some(sequence) = sequence
            && newest.as_ref().is_none_or(|(latest, _)| *latest < sequence)
        {
            newest = Some((sequence, path));
        }
    }
    Ok(newest)
}

/// `operation`'s reply in the agent's latest status, once published.
fn reply(status: &Path, operation: OperationId) -> Result<Option<Value>> {
    loop {
        let Some((_, path)) = latest(status)? else {
            return Ok(None);
        };
        let bytes = match bounded_bytes(&path, STATUS_BYTES) {
            Ok(bytes) => bytes,
            // Superseded between listing and reading: its successor exists.
            Err(Error::NotFound(_)) => continue,
            Err(error) => return Err(error),
        };
        let mut published: Status = serde_json::from_slice(&bytes).map_err(storage)?;
        return Ok(published.replies.remove(&operation));
    }
}

/// One agent's latest status: the replies of its requests still present.
#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Status {
    replies: BTreeMap<OperationId, Value>,
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
    Err(Error::NotFound(NO_OWNER.into()))
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
        .create_directories(Path::new("status"))
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

/// The owner's published statuses and which status holds each reply.
#[derive(Default)]
struct Published {
    statuses: BTreeMap<String, (u64, Status)>,
    answered: BTreeMap<OperationId, String>,
}

async fn serve_requests(
    owner: &mut HostWorkspaceOwner,
    directory: &Path,
    session: uuid::Uuid,
    changed: &tokio::sync::Notify,
) -> Result<()> {
    let mut published = Published::default();
    loop {
        let waiting = changed.notified();
        let mut present = BTreeSet::new();
        let mut changed_keys = BTreeSet::new();
        let mut pending = Vec::new();
        for entry in std::fs::read_dir(directory.join("requests")).map_err(storage)? {
            let path = entry.map_err(storage)?.path();
            if path.extension().is_none_or(|extension| extension != "json") {
                continue;
            }
            let Some(operation) =
                path.file_stem()
                    .and_then(|stem| stem.to_str())
                    .and_then(|stem| {
                        OperationId::parse(stem)
                            .ok()
                            .filter(|operation| operation.to_string() == stem)
                    })
            else {
                // Not a request record; see the invalid-request case below.
                let _ = remove_projection(&path);
                continue;
            };
            present.insert(operation);
            if published.answered.contains_key(&operation) {
                continue;
            }
            // Request projections are writable by native tools. Invalid input
            // must not terminate the trusted owner or dispatch another action.
            let request = regular_bytes(&path).and_then(|bytes| {
                let request: Request = serde_json::from_slice(&bytes).map_err(storage)?;
                if request.session != session || request.operation != operation {
                    return Err(Error::Unauthorized(
                        "native request identity changed".into(),
                    ));
                }
                Ok(request)
            });
            let Ok(request) = request else {
                // Held-root removal unlinks the projection itself, never a
                // symlink target. Clients keep their durable command records.
                let _ = remove_projection(&path);
                present.remove(&operation);
                continue;
            };
            pending.push(request);
        }
        // A withdrawn request's reply is read: drop it from its status before
        // any capacity check, so the space it frees serves this same scan.
        for (operation, key) in published
            .answered
            .extract_if(.., |operation, _| !present.contains(operation))
        {
            if let Some((_, status)) = published.statuses.get_mut(&key) {
                status.replies.remove(&operation);
            }
            changed_keys.insert(key);
        }
        for request in pending {
            let key = status_key(&request.action);
            let status = published.statuses.entry(key.clone()).or_default();
            let size = serde_json::to_vec(&status.1).map_err(storage)?.len() as u64;
            if size + MAXIMUM_BYTES > STATUS_BYTES {
                // Full of unread replies: dispatch once a client withdraws.
                continue;
            }
            let operation = request.operation;
            let outcome = owner.dispatch(request.operation, request.action).await;
            let bytes = crate::client::encode_command_result(0, &outcome, MAXIMUM_BYTES)?;
            let reply = serde_json::from_slice(&bytes).map_err(storage)?;
            status.1.replies.insert(operation, reply);
            published.answered.insert(operation, key.clone());
            changed_keys.insert(key);
        }
        for key in changed_keys {
            if let Some((sequence, status)) = published.statuses.get_mut(&key) {
                *sequence = publish_status(directory, &key, *sequence + 1, status)?;
            }
            if published
                .statuses
                .get(&key)
                .is_some_and(|(_, status)| status.replies.is_empty())
            {
                published.statuses.remove(&key);
            }
        }
        waiting.await;
    }
}

/// Publishes `status` as `key`'s latest and removes every earlier one,
/// returning the sequence it took.
fn publish_status(directory: &Path, key: &str, sequence: u64, status: &Status) -> Result<u64> {
    let root = acyclic_fs::hold_root(directory)
        .map_err(native_file_error)?
        .create_directories(&Path::new("status").join(key))
        .map_err(native_file_error)?;
    // A previous owner's statuses may remain: continue past them.
    let floor = latest(root.path())?.map_or(0, |(latest, _)| latest);
    let sequence = sequence.max(floor + 1);
    let bytes = serde_json::to_vec(status).map_err(storage)?;
    if bytes.len() as u64 > STATUS_BYTES {
        return Err(Error::Invalid("native status exceeds its bound".into()));
    }
    let leaf = format!("{sequence}.json");
    root.publish_immutable(Path::new(&leaf), &bytes)
        .map_err(native_file_error)?;
    for entry in std::fs::read_dir(root.path()).map_err(storage)? {
        let earlier = entry.map_err(storage)?.file_name();
        if earlier != leaf.as_str() {
            root.remove_file(Path::new(&earlier))
                .map_err(native_file_error)?;
        }
    }
    Ok(sequence)
}

fn remove_projection(path: &Path) -> Result<()> {
    let (parent, leaf) = projection_parent(path)?;
    parent.remove_file(&leaf).map_err(native_file_error)
}

fn regular_bytes(path: &Path) -> Result<Vec<u8>> {
    bounded_bytes(path, MAXIMUM_BYTES)
}

fn bounded_bytes(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    let (parent, leaf) = projection_parent(path)?;
    let file = parent.open_file(&leaf, false).map_err(native_file_error)?;
    let metadata = file.metadata().map_err(storage)?;
    if !metadata.is_file() || metadata.len() > maximum {
        return Err(Error::Invalid(
            "native file projection must be a bounded regular file".into(),
        ));
    }
    let mut bytes = Vec::new();
    file.take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(storage)?;
    if bytes.len() as u64 > maximum {
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

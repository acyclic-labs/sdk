//! Drives a harness through files, for hosts whose sandbox admits only their
//! workspace (no network, no sockets).
//!
//! Only the trusted `GraphCoder` owner opens state. A sandboxed client drops
//! one request file under `requests/`: that file is the record, keyed by its
//! operation, and every effect it causes is an agent commit keyed by the same
//! operation. The owner publishes each immutable reply under
//! `status/<agent>/<operation>.json` while its request is still present.
//! A client awaits its reply as one keyed waiter on its
//! process's watcher and withdraws its request once read; the owner then drops
//! the reply. There is no per-request watcher or deadline: a
//! request waits until it is answered, its notification source fails or its owner exits.
//!
//! Both endpoints follow notifications, never scans. The owner indexes the
//! request names each notification carries and dispatches every request
//! independently, publishing each reply as soon as it is ready; only a
//! notification overflow rescans the request directory. A client reads only
//! the exact reply some waiter awaits, watches its agent only while one does,
//! and holds no lock while it publishes or waits.
use super::host_owner::{HostAction, HostWorkspaceOwner};
use super::*;
use futures::StreamExt as _;
use notify::{RecursiveMode, Watcher as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::ffi::OsString;
use std::io::Read as _;
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError, Weak, mpsc};

#[cfg(all(test, not(loom)))]
mod tests;

/// Reserved inside the host's existing writable workspace; never a state path.
pub const DIRECTORY: &str = ".acyclic-sdk/host-files";
const MAXIMUM_BYTES: u64 = 65_536;
/// Status key of requests that name no agent yet.
const WORKSPACE: &str = "workspace";

// Both endpoints observe their own reads. Admit only events that can change
// the projection, so opening a file cannot schedule another read of itself.
fn projection_changed(event: &notify::Result<notify::Event>) -> bool {
    !event
        .as_ref()
        .is_ok_and(|event| matches!(event.kind, notify::EventKind::Access(_)))
}

fn notification_failure(error: &notify::Error) -> String {
    format!("step=observe native workspace files: {error}; recovery=acyclic agents")
}

fn owner_failure(step: &str, error: &std::io::Error) -> String {
    format!("step={step}: {error}; recovery=graphcoder --host-workspace")
}

/// The leaves under `directory` a notification names, or `None` when it
/// cannot name them all: an error, an overflow, or a path elsewhere. Only
/// then does an endpoint revisit everything it follows.
fn notified_leaves(
    event: &notify::Result<notify::Event>,
    directory: &Path,
) -> Option<Vec<OsString>> {
    let event = event.as_ref().ok()?;
    if event.need_rescan() || event.paths.is_empty() {
        return None;
    }
    event
        .paths
        .iter()
        .map(|path| {
            // Only a named entry: a `..` or root component names no leaf.
            match path.strip_prefix(directory).ok()?.components().next()? {
                std::path::Component::Normal(leaf) => Some(leaf.to_owned()),
                _ => None,
            }
        })
        .collect()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every critical section leaves its state consistent before it can fail.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
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
    if let Some(bridge) = Registry::live(&REGISTRY, &cwd) {
        return Ok(bridge.workspace.clone());
    }
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

/// What a waiter receives: its reply, observation failure or its owner's exit.
enum Delivery {
    Reply(Result<Value>),
    Gone,
}

/// One status key some request of this process awaits.
#[derive(Default)]
struct Key {
    /// Requests registered on it, delivered or not; it is watched while any.
    registered: usize,
    /// Each undelivered request's waiter.
    waiting: HashMap<OperationId, mpsc::SyncSender<Delivery>>,
}

#[derive(Default)]
struct Waiters {
    gone: bool,
    failure: Option<String>,
    keys: HashMap<String, Key>,
}

/// The waiters of one running owner, shared by its watcher callback and its
/// liveness thread. Each lock is held only to change or read the index.
struct Shared {
    status: PathBuf,
    waiters: Mutex<Waiters>,
}

impl Shared {
    fn note(&self, event: notify::Result<notify::Event>) {
        if let Err(error) = &event {
            self.failed(|| notification_failure(error));
            return;
        }
        if !projection_changed(&event) {
            return;
        }
        let replies = match notified_replies(&event, &self.status) {
            Some(replies) => replies,
            // Overflow: revisit only active replies, never a directory listing.
            None => lock(&self.waiters)
                .keys
                .iter()
                .flat_map(|(key, waiting)| {
                    waiting
                        .waiting
                        .keys()
                        .map(move |operation| (key.clone(), *operation))
                })
                .collect(),
        };
        for (key, operation) in replies {
            self.deliver(&key, operation);
        }
    }

    /// Reads only the immutable leaf named by this operation's notification.
    fn deliver(&self, key: &str, operation: OperationId) {
        if lock(&self.waiters)
            .keys
            .get(key)
            .is_none_or(|key| !key.waiting.contains_key(&operation))
        {
            return;
        }
        let reply = match regular_bytes(&reply_path(&self.status, key, operation)) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(storage),
            Err(Error::NotFound(_)) => return,
            Err(error) => Err(error),
        };
        let mut waiters = lock(&self.waiters);
        if let Some(waiter) = waiters
            .keys
            .get_mut(key)
            .and_then(|key| key.waiting.remove(&operation))
        {
            let _ = waiter.try_send(Delivery::Reply(reply));
        }
    }

    fn gone(&self) {
        let mut waiters = lock(&self.waiters);
        waiters.gone = true;
        for key in waiters.keys.values_mut() {
            for (_, waiter) in std::mem::take(&mut key.waiting) {
                let _ = waiter.try_send(Delivery::Gone);
            }
        }
    }

    fn failed(&self, failure: impl FnOnce() -> String) {
        let mut waiters = lock(&self.waiters);
        if waiters.gone || waiters.failure.is_some() {
            return;
        }
        let Waiters {
            failure: retained,
            keys,
            ..
        } = &mut *waiters;
        let failure = retained.insert(failure());
        for key in keys.values_mut() {
            for (operation, waiter) in std::mem::take(&mut key.waiting) {
                let _ = waiter.try_send(Delivery::Reply(Err(Error::Storage(format!(
                    "operation={operation}; {failure}"
                )))));
            }
        }
    }

    fn owner_finished(&self, outcome: std::io::Result<acyclic_native_runtime::FileLock>) {
        match outcome {
            Ok(lock) => {
                drop(lock);
                self.gone();
            }
            Err(error) => {
                self.failed(|| owner_failure("observe native workspace owner exit", &error));
            }
        }
    }
}

/// This process's one connection to one running owner: a watcher over only
/// the status directories some request awaits, each watched once however many
/// wait and retired when none does, and one thread that observes the owner's
/// lock release. Requests wait concurrently; none waits behind another.
struct Bridge {
    directory: PathBuf,
    session: uuid::Uuid,
    workspace: PathBuf,
    shared: Arc<Shared>,
    watching: Mutex<Watching>,
}

struct Watching {
    watcher: notify::RecommendedWatcher,
    watched: BTreeSet<String>,
}

impl Bridge {
    fn open(directory: &Path, endpoint: Endpoint) -> Result<Self> {
        let root = acyclic_fs::hold_root(directory).map_err(native_file_error)?;
        let open = || {
            root.open_file(Path::new("owner.lock"), false)
                .map_err(native_file_error)
        };
        let (lock_file, probe) = (open()?, open()?);
        // A shared lock is free exactly when no owner holds its exclusive one.
        match acyclic_native_runtime::FileLock::try_acquire(
            probe,
            acyclic_native_runtime::FileLockMode::Shared,
        ) {
            Ok(_) => return Err(Error::NotFound(NO_OWNER.into())),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => {
                return Err(Error::Storage(owner_failure(
                    "probe native workspace owner",
                    &error,
                )));
            }
        }
        let shared = Arc::new(Shared {
            status: directory.join("status"),
            waiters: Mutex::default(),
        });
        let notified = Arc::clone(&shared);
        let watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
            notified.note(event);
        })
        .map_err(storage)?;
        let exited = Arc::clone(&shared);
        std::thread::Builder::new()
            .name("acyclic-host-files-owner".into())
            .spawn(move || {
                // Blocks until the owner's exclusive lock is released.
                exited.owner_finished(acyclic_native_runtime::FileLock::acquire(
                    lock_file,
                    acyclic_native_runtime::FileLockMode::Shared,
                ));
            })
            .map_err(storage)?;
        Ok(Self {
            directory: directory.to_path_buf(),
            session: endpoint.session,
            workspace: endpoint.workspace,
            shared,
            watching: Mutex::new(Watching {
                watcher,
                watched: BTreeSet::new(),
            }),
        })
    }

    fn live(&self) -> bool {
        !lock(&self.shared.waiters).gone
    }

    /// Registers `operation`'s waiter on `key` and watches `key` before its
    /// request is published, so its reply cannot land unobserved.
    fn wait(
        self: &Arc<Self>,
        key: &str,
        operation: OperationId,
        waiter: mpsc::SyncSender<Delivery>,
    ) -> Result<Waiting> {
        {
            let mut waiters = lock(&self.shared.waiters);
            if let Some(failure) = &waiters.failure {
                return Err(Error::Storage(failure.clone()));
            }
            if waiters.gone {
                return Err(Error::NotFound(NO_OWNER.into()));
            }
            let entry = waiters.keys.entry(key.to_owned()).or_default();
            entry.registered += 1;
            entry.waiting.insert(operation, waiter);
        }
        let waiting = Waiting {
            bridge: Arc::clone(self),
            key: key.to_owned(),
            operation,
        };
        self.watch(key)?;
        Ok(waiting)
    }

    /// Brings `key`'s watch in line with its registrations. The index lock is
    /// never held while the watcher, whose callback takes it, changes.
    fn watch(&self, key: &str) -> Result<()> {
        let mut watching = lock(&self.watching);
        let wanted = lock(&self.shared.waiters).keys.contains_key(key);
        let path = self.shared.status.join(key);
        if wanted && !watching.watched.contains(key) {
            watching
                .watcher
                .watch(&path, RecursiveMode::NonRecursive)
                .map_err(storage)?;
            watching.watched.insert(key.to_owned());
        } else if !wanted && watching.watched.remove(key) {
            // A directory the watcher already lost needs no unwatch.
            let _ = watching.watcher.unwatch(&path);
        }
        Ok(())
    }
}

/// One registered request; dropping it retires its registration, and the
/// key's watch with the last one.
struct Waiting {
    bridge: Arc<Bridge>,
    key: String,
    operation: OperationId,
}

impl Drop for Waiting {
    fn drop(&mut self) {
        {
            let mut waiters = lock(&self.bridge.shared.waiters);
            if let Some(key) = waiters.keys.get_mut(&self.key) {
                key.waiting.remove(&self.operation);
                key.registered = key.registered.saturating_sub(1);
                if key.registered == 0 {
                    waiters.keys.remove(&self.key);
                }
            }
        }
        let _ = self.bridge.watch(&self.key);
    }
}

/// Each discovered owner, and which owner each client directory reached:
/// discovery and connection happen once per owner lifetime, not per request.
#[derive(Default)]
struct Registry {
    located: HashMap<PathBuf, Weak<Bridge>>,
    bridges: HashMap<PathBuf, Connection>,
}

type Connection = Arc<Mutex<Option<Arc<Bridge>>>>;

static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(Mutex::default);

impl Registry {
    fn live(registry: &Mutex<Self>, cwd: &Path) -> Option<Arc<Bridge>> {
        let bridge = lock(registry).located.get(cwd)?.upgrade()?;
        bridge.live().then_some(bridge)
    }

    fn bridge(
        registry: &Mutex<Self>,
        cwd: &Path,
        open: impl FnOnce(&Path, Endpoint) -> Result<Bridge>,
    ) -> Result<Arc<Bridge>> {
        if let Some(bridge) = Self::live(registry, cwd) {
            return Ok(bridge);
        }
        let (directory, endpoint) = discover(cwd)?;
        let connection = Arc::clone(lock(registry).bridges.entry(directory.clone()).or_default());
        // Only callers connecting to this owner wait behind its native opens.
        // Discovery, watcher registration and owner-exit observation never hold
        // the registry; the connection guard releases on either result.
        let mut connected = lock(&connection);
        let bridge = match connected.as_ref().filter(|bridge| bridge.live()) {
            Some(bridge) => Arc::clone(bridge),
            None => {
                let bridge = Arc::new(open(&directory, endpoint)?);
                *connected = Some(Arc::clone(&bridge));
                bridge
            }
        };
        lock(registry)
            .located
            .insert(cwd.to_path_buf(), Arc::downgrade(&bridge));
        Ok(bridge)
    }
}

/// Contacts an existing harness. Does not select state, acquire a lifecycle
/// lock, publish an executable identity, or start any service or agent.
pub fn request(cwd: &Path, action: HostAction) -> Result<Value> {
    request_with(&REGISTRY, cwd, action)
}

fn request_with(registry: &Mutex<Registry>, cwd: &Path, action: HostAction) -> Result<Value> {
    let cwd = std::fs::canonicalize(cwd).map_err(storage)?;
    let bridge = Registry::bridge(registry, &cwd, Bridge::open)?;
    let operation = OperationId::new();
    let key = status_key(&action);
    let request = Request {
        session: bridge.session,
        operation,
        action,
    };
    let bytes = serde_json::to_vec(&request).map_err(storage)?;
    if bytes.len() as u64 > MAXIMUM_BYTES {
        return Err(Error::Invalid(
            "native host request exceeds file bound".into(),
        ));
    }
    acyclic_fs::hold_root(&bridge.directory)
        .map_err(native_file_error)?
        .create_directories(&Path::new("status").join(&key))
        .map_err(native_file_error)?;
    let (waiter, delivery) = mpsc::sync_channel(1);
    let waiting = bridge.wait(&key, operation, waiter)?;
    let request_path = bridge
        .directory
        .join("requests")
        .join(format!("{operation}.json"));
    immutable(&request_path, &bytes)?;
    let result =
        match acyclic_native_runtime::obs::waiting_sync("native-workspace-reply", operation, || {
            delivery.recv()
        }) {
            Ok(Delivery::Reply(reply)) => reply,
            Ok(Delivery::Gone) | Err(_) => {
                drop(waiting);
                // Withdrawn, so no later owner serves a request nobody awaits.
                // Whatever the exited owner committed stays keyed by `operation`.
                remove_projection(&request_path)?;
                return Err(Error::Indeterminate(operation));
            }
        };
    drop(waiting);
    // Withdrawing the request retires its reply and any saved result. Every
    // effect is an agent commit keyed by this operation.
    remove_projection(&request_path)?;
    let result = result?;
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

fn reply_path(status: &Path, key: &str, operation: OperationId) -> PathBuf {
    status.join(key).join(format!("{operation}.json"))
}

fn operation_leaf(name: &std::ffi::OsStr) -> Option<OperationId> {
    let path = Path::new(name);
    if path.extension()? != "json" {
        return None;
    }
    let stem = path.file_stem()?.to_str()?;
    OperationId::parse(stem)
        .ok()
        .filter(|operation| operation.to_string() == stem)
}

/// Named reply leaves, or an overflow/directory notification requiring active
/// waiter checks. Unrelated files never participate in reply selection.
fn notified_replies(
    event: &notify::Result<notify::Event>,
    status: &Path,
) -> Option<BTreeSet<(String, OperationId)>> {
    use std::path::Component::Normal;
    let event = event.as_ref().ok()?;
    if event.need_rescan() || event.paths.is_empty() {
        return None;
    }
    let mut replies = BTreeSet::new();
    for path in &event.paths {
        let mut parts = path.strip_prefix(status).ok()?.components();
        let Normal(key) = parts.next()? else {
            return None;
        };
        let Normal(leaf) = parts.next()? else {
            return None;
        };
        if parts.next().is_some() {
            return None;
        }
        if let (Some(key), Some(operation)) = (key.to_str(), operation_leaf(leaf)) {
            replies.insert((key.to_owned(), operation));
        }
    }
    Some(replies)
}

fn discover(cwd: &Path) -> Result<(PathBuf, Endpoint)> {
    for ancestor in cwd.ancestors() {
        let directory = ancestor.join(DIRECTORY);
        match regular_bytes(&directory.join("owner.json")) {
            Ok(bytes) => {
                let endpoint: Endpoint = serde_json::from_slice(&bytes).map_err(storage)?;
                if endpoint.version != 2 {
                    return Err(Error::Unsupported(format!(
                        "native workspace protocol {}; requires 2; recovery=restart graphcoder --host-workspace and install its matching plugin",
                        endpoint.version
                    )));
                }
                if endpoint.workspace != ancestor
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
        version: 2,
        session,
        workspace,
    };
    let bytes = serde_json::to_vec(&endpoint).map_err(storage)?;
    let descriptor = directory.path().join("owner.json");
    immutable(&descriptor, &bytes)?;
    let owner =
        HostWorkspaceOwner::new(Arc::clone(&local))?.with_requests(directory.path().to_path_buf());
    // Notifications name the canonical paths; a non-canonical root would
    // turn every one into an overflow rescan.
    let requests = std::fs::canonicalize(directory.path().join("requests")).map_err(storage)?;
    let notices = Arc::new(Notices::new(requests.clone()));
    let notified = Arc::clone(&notices);
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        notified.note(&event);
    })
    .map_err(storage)?;
    watcher
        .watch(&requests, RecursiveMode::NonRecursive)
        .map_err(storage)?;
    let result = tokio::select! {
        result = serve_requests(&owner, directory.path(), session, &notices) => result,
        result = shutdown => result,
    };
    drop(watcher);
    drop(local);
    owner.retire().await?;
    result
}

/// The request names notifications reported since the owner last looked,
/// coalesced; `rescan` once one could not name them (an overflow).
struct Notices {
    requests: PathBuf,
    touched: Mutex<Touched>,
    changed: tokio::sync::Notify,
}

struct Touched {
    names: BTreeSet<OsString>,
    rescan: bool,
    failure: Option<String>,
}

impl Notices {
    /// Starts with one rescan: requests published before this owner ran.
    fn new(requests: PathBuf) -> Self {
        Self {
            requests,
            touched: Mutex::new(Touched {
                names: BTreeSet::new(),
                rescan: true,
                failure: None,
            }),
            changed: tokio::sync::Notify::new(),
        }
    }

    // Scanning requests opens the watched directory. Access notifications
    // must not wake the owner indefinitely and starve native mounts.
    fn note(&self, event: &notify::Result<notify::Event>) {
        if !projection_changed(event) {
            return;
        }
        {
            let mut touched = lock(&self.touched);
            if let Err(error) = event {
                touched
                    .failure
                    .get_or_insert_with(|| notification_failure(error));
            } else if touched.failure.is_none() {
                match notified_leaves(event, &self.requests) {
                    Some(leaves) => touched.names.extend(leaves),
                    None => touched.rescan = true,
                }
            }
        }
        self.changed.notify_one();
    }

    fn take(&self) -> Touched {
        let mut touched = lock(&self.touched);
        Touched {
            names: std::mem::take(&mut touched.names),
            rescan: std::mem::take(&mut touched.rescan),
            failure: touched.failure.clone(),
        }
    }
}

/// Where one request stands with this owner.
enum Tracked {
    /// Admitted by the current notification batch.
    Queued,
    /// Dispatched; `withdrawn` once its file disappeared.
    Running { withdrawn: bool },
    /// Its reply is published.
    Answered,
}

/// The owner's active requests. Each reply is published and retired by its
/// original operation, without reading or rewriting another operation.
#[derive(Default)]
struct Served {
    known: HashMap<OperationId, (String, Tracked)>,
    ready: Vec<Request>,
    /// Request-directory scans, all of them overflow or startup rescans.
    #[cfg(test)]
    rescans: u64,
}

impl Served {
    /// Applies what notifications reported: each named request is admitted
    /// or withdrawn on its own; only an overflow lists the whole directory.
    fn touch(&mut self, requests: &Path, session: uuid::Uuid, touched: Touched) -> Result<()> {
        if let Some(failure) = touched.failure {
            return Err(Error::Storage(failure));
        }
        let names = if touched.rescan {
            #[cfg(test)]
            {
                self.rescans += 1;
            }
            let mut names = touched.names;
            for entry in std::fs::read_dir(requests).map_err(storage)? {
                names.insert(entry.map_err(storage)?.file_name());
            }
            // A known request absent from the listing was withdrawn.
            names.extend(
                self.known
                    .keys()
                    .map(|operation| OsString::from(format!("{operation}.json"))),
            );
            names
        } else {
            touched.names
        };
        for name in names {
            self.observe(requests, session, &name)?;
        }
        Ok(())
    }

    fn observe(
        &mut self,
        requests: &Path,
        session: uuid::Uuid,
        name: &std::ffi::OsStr,
    ) -> Result<()> {
        let path = requests.join(name);
        if path.extension().is_none_or(|extension| extension != "json") {
            return Ok(());
        }
        let Some(operation) = operation_leaf(name) else {
            // Not a request record; see the invalid-request case below.
            let _ = remove_projection(&path);
            return Ok(());
        };
        if self.known.contains_key(&operation) {
            // A known request changes only by its withdrawal.
            if std::fs::symlink_metadata(&path).is_err() {
                self.withdraw(requests, operation)?;
            }
            return Ok(());
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
        match request {
            Ok(request) => {
                let key = status_key(&request.action);
                self.known.insert(operation, (key, Tracked::Queued));
                self.ready.push(request);
            }
            // Withdrawn before this owner read it.
            Err(Error::NotFound(_)) => {}
            // Held-root removal unlinks the projection itself, never a
            // symlink target. Clients keep their durable command records.
            Err(_) => {
                let _ = remove_projection(&path);
            }
        }
        Ok(())
    }

    /// Withdrawal retires only this request's reply, if already published.
    fn withdraw(&mut self, requests: &Path, operation: OperationId) -> Result<()> {
        let Some((key, tracked)) = self.known.remove(&operation) else {
            return Ok(());
        };
        match tracked {
            Tracked::Queued => {}
            Tracked::Running { .. } => {
                // Its dispatch still runs; its reply will not be published.
                self.known
                    .insert(operation, (key, Tracked::Running { withdrawn: true }));
            }
            Tracked::Answered => {
                let directory = requests.parent().ok_or_else(|| {
                    Error::Invalid("native request directory has no parent".into())
                })?;
                match remove_projection(&reply_path(&directory.join("status"), &key, operation)) {
                    Ok(()) | Err(Error::NotFound(_)) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }

    /// Starts this notification batch; withdrawal before dispatch cancels it.
    fn start(&mut self) -> Vec<Request> {
        let mut ready = std::mem::take(&mut self.ready);
        ready.retain(|request| {
            if let Some((_, tracked)) = self.known.get_mut(&request.operation) {
                *tracked = Tracked::Running { withdrawn: false };
                true
            } else {
                false
            }
        });
        ready
    }

    /// Publishes one dispatch's immutable reply unless it was withdrawn.
    fn answer(
        &mut self,
        directory: &Path,
        operation: OperationId,
        outcome: Result<Value>,
    ) -> Result<()> {
        let Some((key, Tracked::Running { withdrawn })) = self.known.remove(&operation) else {
            return Ok(());
        };
        if withdrawn {
            return Ok(());
        }
        let bytes = crate::client::encode_command_result(0, &outcome, MAXIMUM_BYTES)?;
        let root = acyclic_fs::hold_root(directory)
            .map_err(native_file_error)?
            .create_directories(&Path::new("status").join(&key))
            .map_err(native_file_error)?;
        root.publish_immutable(Path::new(&format!("{operation}.json")), &bytes)
            .map_err(native_file_error)?;
        self.known.insert(operation, (key, Tracked::Answered));
        Ok(())
    }
}

async fn serve_requests(
    owner: &HostWorkspaceOwner,
    directory: &Path,
    session: uuid::Uuid,
    notices: &Notices,
) -> Result<()> {
    let mut served = Served::default();
    // Every dispatch runs independently; a slow one delays no other reply.
    let mut running = futures::stream::FuturesUnordered::<
        acyclic_stream::BoxProviderFuture<'_, (OperationId, Result<Value>)>,
    >::new();
    loop {
        served.touch(&notices.requests, session, notices.take())?;
        for request in served.start() {
            let operation = request.operation;
            let dispatched = owner.dispatch(operation, request.action);
            running.push(Box::pin(async move { (operation, dispatched.await) }));
        }
        tokio::select! {
            () = notices.changed.notified() => {}
            Some((operation, outcome)) = running.next(), if !running.is_empty() => {
                served.answer(directory, operation, outcome)?;
            }
        }
    }
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

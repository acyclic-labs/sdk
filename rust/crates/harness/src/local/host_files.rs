//! Drives a harness through files, for hosts whose sandbox admits only their
//! workspace (no network, no sockets).
//!
//! Only the trusted `GraphCoder` owner opens state. A sandboxed client drops
//! one request file under `requests/`: that file is the record, keyed by its
//! operation, and every effect it causes is an agent commit keyed by the same
//! operation. The owner publishes each immutable reply under
//! `status/<operation>.json` while its request is still present.
//! A client awaits its reply as one keyed waiter on its
//! process's watcher and withdraws its request once read; the owner then drops
//! the reply. There is no per-request watcher or deadline: a
//! request waits until it is answered, withdrawn, its notification source fails or its owner exits.
//!
//! Both endpoints follow notifications, never scans. The owner indexes the
//! request names each notification carries and dispatches every request
//! independently, publishing each reply as soon as it is ready; only a
//! notification overflow rescans the request directory. A client reads only
//! the exact reply or request some waiter awaits, watches only while one does,
//! and holds no lock while it publishes or waits.
use super::host_owner::{HostAction, HostWorkspaceOwner};
use super::*;
use futures::StreamExt as _;
#[cfg(test)]
use notify::{RecursiveMode, Watcher as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ffi::OsString;
use std::io::Read as _;
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError, Weak, mpsc};

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod validation;

/// Reserved inside the host's existing writable workspace; never a state path.
pub const DIRECTORY: &str = ".acyclic-sdk/host-files";

// Allocation protection for writable input projections, far above the native
// prompt/event bounds. Published results have no independent envelope cap.
const MAXIMUM_REQUEST_BYTES: u64 = 64 * 1024 * 1024;

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
    format!("step={step}: {error}; recovery=acyclic run --host-workspace")
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
    requests: [u8; 16],
    status: [u8; 16],
}

impl Endpoint {
    fn decode(bytes: &[u8]) -> Result<Self> {
        #[derive(Deserialize)]
        struct Version {
            version: u32,
        }
        let version: Version = serde_json::from_slice(bytes).map_err(storage)?;
        if version.version != 4 {
            return Err(Error::Unsupported(format!(
                "step=admit native workspace protocol: version {}; requires 4; recovery=run acyclic run --host-workspace in a fresh workspace with its matching plugin",
                version.version
            )));
        }
        serde_json::from_slice(bytes).map_err(storage)
    }

    fn new(
        session: uuid::Uuid,
        workspace: PathBuf,
        requests: &acyclic_fs::HeldRoot,
        status: &acyclic_fs::HeldRoot,
    ) -> Self {
        Self {
            version: 4,
            session,
            workspace,
            requests: requests.identity().to_bytes(),
            status: status.identity().to_bytes(),
        }
    }

    fn open_roots(
        &self,
        directory: &acyclic_fs::HeldRoot,
    ) -> Result<(acyclic_fs::HeldRoot, acyclic_fs::HeldRoot)> {
        let requests = directory
            .open_directory(Path::new("requests"))
            .map_err(native_file_error)?;
        let status = directory
            .open_directory(Path::new("status"))
            .map_err(native_file_error)?;
        if requests.identity().to_bytes() != self.requests
            || status.identity().to_bytes() != self.status
        {
            return Err(Error::Unauthorized(
                "native host endpoint directories differ from the original owner's".into(),
            ));
        }
        Ok((requests, status))
    }
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

const NO_OWNER: &str = "no running Acyclic workspace owner; start acyclic run --host-workspace outside the host sandbox";

/// What a waiter receives: its reply, observation failure or its owner's exit.
enum Delivery {
    Reply(Result<Value>),
    Gone,
}

struct ReplyWaiter {
    /// Withdrawal is meaningful only after the original publication completed.
    published: bool,
    delivery: mpsc::SyncSender<Delivery>,
}

#[derive(Default)]
struct Waiters {
    gone: bool,
    failure: Option<String>,
    registered: usize,
    waiting: HashMap<OperationId, ReplyWaiter>,
    retirement: Option<Retirement>,
}

impl Waiters {
    fn gone(&mut self) {
        self.gone = true;
        for (_, waiter) in std::mem::take(&mut self.waiting) {
            let _ = waiter.delivery.try_send(Delivery::Gone);
        }
    }

    fn failed(&mut self, failure: impl FnOnce() -> String) {
        if self.gone || self.failure.is_some() {
            return;
        }
        let failure = self.failure.insert(failure());
        for (operation, waiter) in std::mem::take(&mut self.waiting) {
            let _ = waiter.delivery.try_send(Delivery::Reply(Err(
                Error::Storage(failure.clone()).for_request(operation)
            )));
        }
    }
}

/// The waiters of one running owner, shared by its watcher callback and its
/// liveness thread. Each lock is held only to change or read the index.
struct Shared {
    status: acyclic_fs::HeldRoot,
    requests: acyclic_fs::HeldRoot,
    waiters: Mutex<Waiters>,
}

impl Shared {
    #[cfg(test)]
    fn note(&self, event: notify::Result<notify::Event>) {
        self.note_at(event, self.status.path(), self.requests.path());
    }

    fn note_at(&self, event: notify::Result<notify::Event>, status: &Path, requests: &Path) {
        if let Err(error) = &event {
            lock(&self.waiters).failed(|| notification_failure(error));
            return;
        }
        if !projection_changed(&event) {
            return;
        }
        let replies = match notified_operations(&event, status, requests) {
            Some(replies) => replies,
            // Overflow: revisit only active replies, never a directory listing.
            None => lock(&self.waiters).waiting.keys().copied().collect(),
        };
        for operation in replies {
            self.deliver(operation);
        }
    }

    fn published(&self, operation: OperationId) {
        if let Some(waiter) = lock(&self.waiters).waiting.get_mut(&operation) {
            waiter.published = true;
        }
        self.deliver(operation);
    }

    /// A committed reply wins over withdrawal. Missing input cannot establish
    /// whether an accepted dispatch committed, so it settles as indeterminate.
    fn deliver(&self, operation: OperationId) {
        if lock(&self.waiters)
            .waiting
            .get(&operation)
            .is_none_or(|waiter| !waiter.published)
        {
            return;
        }
        let leaf = format!("{operation}.json");
        let leaf = Path::new(&leaf);
        let reply = match held_bytes(&self.status, leaf, u64::MAX) {
            Ok(bytes) => crate::client::decode_command_result(operation, 0, &bytes),
            Err(Error::NotFound(_)) => match self
                .requests
                .open_file(leaf, false)
                .map_err(native_file_error)
            {
                Ok(_) => return,
                Err(Error::NotFound(_)) => Err(Error::Indeterminate(operation)),
                Err(error) => Err(Error::reply_unavailable(operation, error)),
            },
            Err(error) => Err(Error::reply_unavailable(operation, error)),
        };
        let mut waiters = lock(&self.waiters);
        if let Some(waiter) = waiters.waiting.remove(&operation) {
            let _ = waiter.delivery.try_send(Delivery::Reply(reply));
        }
    }

    fn owner_finished(&self, outcome: std::io::Result<acyclic_native_runtime::FileLock>) {
        // Release any acquired native lock before taking the waiter lock.
        let outcome = outcome.map(drop);
        let mut waiters = lock(&self.waiters);
        match outcome {
            Ok(()) => waiters.gone(),
            Err(error) => {
                waiters.failed(|| owner_failure("observe native workspace owner exit", &error));
            }
        }
        // Both outcomes end the original observer. Retire outside its waiter
        // lock, on this observer rather than the watcher's callback thread.
        let retirement = waiters.retirement.take();
        drop(waiters);
        if let Some(retirement) = retirement {
            retirement.retire();
        }
    }
}

/// This process's one connection to one running owner: a watcher over only
/// the request and reply directories, retired when none waits, and one thread that observes the owner's
/// lock release. Requests wait concurrently; none waits behind another.
struct Bridge {
    session: uuid::Uuid,
    workspace: PathBuf,
    shared: Arc<Shared>,
    watching: Mutex<Watching>,
    // Tests join the original observer, rather than polling registry cleanup.
    #[cfg(test)]
    observer: Mutex<Option<std::thread::JoinHandle<()>>>,
}

struct Watching {
    watches: Option<[acyclic_fs::HeldRootWatch; 2]>,
}

impl Bridge {
    fn open(root: acyclic_fs::HeldRoot, endpoint: Endpoint) -> Result<Self> {
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
        let (requests, status) = endpoint.open_roots(&root)?;
        let shared = Arc::new(Shared {
            status,
            requests,
            waiters: Mutex::default(),
        });
        let exited = Arc::clone(&shared);
        let observer = std::thread::Builder::new()
            .name("acyclic-host-files-owner".into())
            .spawn(move || {
                // Blocks until the owner's exclusive lock is released.
                exited.owner_finished(acyclic_native_runtime::FileLock::acquire(
                    lock_file,
                    acyclic_native_runtime::FileLockMode::Shared,
                ));
            })
            .map_err(storage)?;
        #[cfg(not(test))]
        drop(observer);
        Ok(Self {
            session: endpoint.session,
            workspace: endpoint.workspace,
            shared,
            watching: Mutex::new(Watching { watches: None }),
            #[cfg(test)]
            observer: Mutex::new(Some(observer)),
        })
    }

    fn live(&self) -> bool {
        !lock(&self.shared.waiters).gone
    }

    /// Watches the original request and reply before publication. The waiter
    /// is armed only after publication so an old removal cannot reject it.
    fn wait(
        self: &Arc<Self>,
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
            if waiters.waiting.contains_key(&operation) {
                return Err(Error::Conflict(format!(
                    "step=register native operation {operation}: already waiting; recovery=acyclic agents"
                )));
            }
            waiters.registered += 1;
            waiters.waiting.insert(
                operation,
                ReplyWaiter {
                    published: false,
                    delivery: waiter,
                },
            );
        }
        let waiting = Waiting {
            bridge: Arc::clone(self),
            operation,
        };
        self.watch()?;
        Ok(waiting)
    }

    /// Brings the two directory watches in line with registrations. The index lock is
    /// never held while the watcher, whose callback takes it, changes.
    fn watch(&self) -> Result<()> {
        let mut watching = lock(&self.watching);
        let wanted = lock(&self.shared.waiters).registered != 0;
        if wanted && watching.watches.is_none() {
            let observe = |root: &acyclic_fs::HeldRoot| {
                let shared = Arc::clone(&self.shared);
                root.watch_changes(move |event| shared.note_at(event, Path::new(""), Path::new("")))
                    .map_err(native_file_error)
            };
            watching.watches = Some([
                observe(&self.shared.status)?,
                observe(&self.shared.requests)?,
            ]);
        } else if !wanted {
            watching.watches = None;
        }
        Ok(())
    }
}

/// One registered request; dropping it retires its registration, and the
/// directory watches with the last one.
struct Waiting {
    bridge: Arc<Bridge>,
    operation: OperationId,
}

impl Drop for Waiting {
    fn drop(&mut self) {
        {
            let mut waiters = lock(&self.bridge.shared.waiters);
            waiters.waiting.remove(&self.operation);
            waiters.registered -= 1;
        }
        let _ = self.bridge.watch();
    }
}

/// Each discovered owner, and which owner each client directory reached:
/// discovery and connection happen once per owner lifetime, not per request.
#[derive(Default)]
struct Registry {
    located: HashMap<PathBuf, Weak<Bridge>>,
    bridges: HashMap<PathBuf, Connection>,
}

type Connection = Arc<Mutex<Connected>>;

#[derive(Default)]
struct Connected {
    bridge: Option<Arc<Bridge>>,
    /// A connector that already borrowed this slot cannot reuse it after loss.
    retired: bool,
    aliases: HashSet<PathBuf>,
}

#[cfg_attr(test, derive(Clone))]
struct Retirement {
    registry: Weak<Mutex<Registry>>,
    connection: Weak<Mutex<Connected>>,
    bridge: Weak<Bridge>,
    directory: PathBuf,
}

impl Retirement {
    fn retire(self) {
        let (Some(registry), Some(connection)) =
            (self.registry.upgrade(), self.connection.upgrade())
        else {
            return;
        };
        let (bridge, aliases) = {
            let mut connected = lock(&connection);
            if connected
                .bridge
                .as_ref()
                .is_none_or(|bridge| !Weak::ptr_eq(&Arc::downgrade(bridge), &self.bridge))
            {
                return;
            }
            connected.retired = true;
            (
                connected.bridge.take(),
                std::mem::take(&mut connected.aliases),
            )
        };
        {
            let mut registry = lock(&registry);
            if registry
                .bridges
                .get(&self.directory)
                .is_some_and(|current| Arc::ptr_eq(current, &connection))
            {
                registry.bridges.remove(&self.directory);
            }
        }
        // Only this connection's indexed aliases; each short section checks
        // identity so a late original terminal cannot remove its replacement.
        for alias in aliases {
            let mut registry = lock(&registry);
            if registry
                .located
                .get(&alias)
                .is_some_and(|current| Weak::ptr_eq(current, &self.bridge))
            {
                registry.located.remove(&alias);
            }
        }
        // Dropping a watcher may join its callback. Never do that under an
        // index, connection or waiter lock needed by another producer.
        drop(bridge);
    }
}

static REGISTRY: LazyLock<Arc<Mutex<Registry>>> = LazyLock::new(Arc::default);

impl Registry {
    fn live(registry: &Mutex<Self>, cwd: &Path) -> Option<Arc<Bridge>> {
        let bridge = lock(registry).located.get(cwd)?.upgrade()?;
        bridge.live().then_some(bridge)
    }

    fn bridge(
        registry: &Arc<Mutex<Self>>,
        cwd: &Path,
        open: impl FnOnce(acyclic_fs::HeldRoot, Endpoint) -> Result<Bridge>,
    ) -> Result<Arc<Bridge>> {
        if let Some(bridge) = Self::live(registry, cwd) {
            return Ok(bridge);
        }
        let (root, endpoint) = discover(cwd)?;
        let directory = root.path().to_path_buf();
        let connection = Arc::clone(lock(registry).bridges.entry(directory.clone()).or_default());
        // Only callers connecting to this owner wait behind its native opens.
        // Discovery, watcher registration and owner-exit observation never hold
        // the registry; the connection guard releases on either result.
        let mut connected = lock(&connection);
        if connected.retired {
            return Err(Error::NotFound(NO_OWNER.into()));
        }
        let bridge = match connected.bridge.as_ref() {
            Some(bridge) if bridge.live() => Arc::clone(bridge),
            Some(_) => return Err(Error::NotFound(NO_OWNER.into())),
            None => {
                let bridge = match open(root, endpoint) {
                    Ok(bridge) => Arc::new(bridge),
                    Err(error) => {
                        connected.retired = true;
                        let mut registry = lock(registry);
                        if registry
                            .bridges
                            .get(&directory)
                            .is_some_and(|current| Arc::ptr_eq(current, &connection))
                        {
                            registry.bridges.remove(&directory);
                        }
                        return Err(error);
                    }
                };
                connected.bridge = Some(Arc::clone(&bridge));
                let retirement = Retirement {
                    registry: Arc::downgrade(registry),
                    connection: Arc::downgrade(&connection),
                    bridge: Arc::downgrade(&bridge),
                    directory,
                };
                let mut waiters = lock(&bridge.shared.waiters);
                let terminal = waiters
                    .failure
                    .as_ref()
                    .map(|failure| Error::Storage(failure.clone()))
                    .or_else(|| waiters.gone.then(|| Error::NotFound(NO_OWNER.into())));
                if let Some(error) = terminal {
                    drop(waiters);
                    drop(connected);
                    retirement.retire();
                    return Err(error);
                }
                waiters.retirement = Some(retirement);
                drop(waiters);
                bridge
            }
        };
        let mut registry = lock(registry);
        let alias = cwd.to_path_buf();
        registry
            .located
            .insert(alias.clone(), Arc::downgrade(&bridge));
        connected.aliases.insert(alias);
        Ok(bridge)
    }
}

/// Contacts an existing harness. Does not select state, acquire a lifecycle
/// lock, publish an executable identity, or start any service or agent.
pub fn request(cwd: &Path, action: HostAction) -> Result<Value> {
    request_with(&REGISTRY, cwd, action)
}

fn request_with(registry: &Arc<Mutex<Registry>>, cwd: &Path, action: HostAction) -> Result<Value> {
    request_operation(registry, cwd, OperationId::new(), action)
}

fn request_operation(
    registry: &Arc<Mutex<Registry>>,
    cwd: &Path,
    operation: OperationId,
    action: HostAction,
) -> Result<Value> {
    let result = (|| {
        let cwd = std::fs::canonicalize(cwd).map_err(storage)?;
        let bridge = Registry::bridge(registry, &cwd, Bridge::open)?;
        let request = Request {
            session: bridge.session,
            operation,
            action,
        };
        let bytes = serde_json::to_vec(&request).map_err(storage)?;
        let (waiter, delivery) = mpsc::sync_channel(1);
        let waiting = bridge.wait(operation, waiter)?;
        let leaf = format!("{operation}.json");
        let leaf = Path::new(&leaf);
        bridge
            .shared
            .requests
            .publish_immutable(leaf, &bytes, acyclic_fs::NativeDurability::Durable)
            .map_err(native_file_error)?;
        // A writable reply may predate registration. Watching before this read
        // covers both that case and publication concurrent with the read.
        bridge.shared.published(operation);
        let result = match acyclic_native_runtime::obs::waiting_sync(
            "native-workspace-reply",
            operation,
            || delivery.recv(),
        ) {
            Ok(Delivery::Reply(reply)) => reply,
            Ok(Delivery::Gone) | Err(_) => {
                drop(waiting);
                // Withdrawn, so no later owner serves a request nobody awaits.
                // Whatever the exited owner committed stays keyed by `operation`.
                return Error::after_cleanup(
                    Err(Error::Indeterminate(operation)),
                    remove_held_projection(&bridge.shared.requests, leaf),
                );
            }
        };
        drop(waiting);
        // Withdrawing the request retires its reply and any saved result. Every
        // effect is an agent commit keyed by this operation.
        Error::after_cleanup(
            result,
            remove_held_projection(&bridge.shared.requests, leaf),
        )
    })();
    result.map_err(|error: Error| error.for_request(operation))
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
fn notified_operations(
    event: &notify::Result<notify::Event>,
    status: &Path,
    requests: &Path,
) -> Option<BTreeSet<OperationId>> {
    use std::path::Component::Normal;
    let event = event.as_ref().ok()?;
    if event.need_rescan() || event.paths.is_empty() {
        return None;
    }
    let mut replies = BTreeSet::new();
    for path in &event.paths {
        let mut parts = path
            .strip_prefix(status)
            .or_else(|_| path.strip_prefix(requests))
            .ok()?
            .components();
        let Normal(leaf) = parts.next()? else {
            return None;
        };
        if parts.next().is_some() {
            return None;
        }
        if let Some(operation) = operation_leaf(leaf) {
            replies.insert(operation);
        }
    }
    Some(replies)
}

fn discover(cwd: &Path) -> Result<(acyclic_fs::HeldRoot, Endpoint)> {
    for ancestor in cwd.ancestors() {
        let candidate = (|| {
            let workspace = acyclic_fs::hold_root(ancestor).map_err(native_file_error)?;
            let directory = workspace
                .open_directory(Path::new(DIRECTORY))
                .map_err(native_file_error)?;
            let bytes = held_bytes(&directory, Path::new("owner.json"), MAXIMUM_REQUEST_BYTES)?;
            Ok((directory, bytes))
        })();
        match candidate {
            Ok((directory, bytes)) => {
                let endpoint = Endpoint::decode(&bytes)?;
                if endpoint.workspace != ancestor
                    || std::fs::canonicalize(directory.path()).map_err(storage)? != directory.path()
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
    serve_until(local, || Ok(()), std::future::pending()).await
}

/// Stops accepting requests, joins view retirement and disconnects the shared
/// harness before reporting shutdown. Native callbacks keep their runtime.
/// Calls `ready` once after acquiring ownership and installing request events.
/// A retained descriptor only discovers this endpoint; it cannot announce a
/// new serving lifetime. Readiness failure joins the same owner retirement.
pub async fn serve_until(
    local: Arc<LocalHarness>,
    ready: impl FnOnce() -> Result<()>,
    shutdown: impl std::future::Future<Output = Result<()>>,
) -> Result<()> {
    let workspace = local
        .workspace_path()
        .ok_or_else(|| Error::Invalid("native hosts require a checkout".into()))?
        .to_path_buf();
    let held = acyclic_fs::hold_root(&workspace).map_err(storage)?;
    let directory = held
        .create_directories(Path::new(DIRECTORY), acyclic_fs::NativeDurability::Durable)
        .map_err(storage)?;
    let ownership = Arc::new(
        acyclic_native_runtime::FileLock::try_acquire(
            directory
                .open_file(Path::new("owner.lock"), true)
                .map_err(storage)?,
            acyclic_native_runtime::FileLockMode::Exclusive,
        )
        .map_err(storage)?,
    );
    let request_root = Arc::new(
        directory
            .create_directories(Path::new("requests"), acyclic_fs::NativeDurability::Durable)
            .map_err(storage)?,
    );
    let status = directory
        .create_directories(Path::new("status"), acyclic_fs::NativeDurability::Durable)
        .map_err(storage)?;
    let session = uuid::Uuid::from_bytes(
        context::publication_operation(
            b"acyclic.native-files/v1\0",
            workspace.as_os_str().as_encoded_bytes(),
        )
        .into_bytes(),
    );
    let endpoint = Endpoint::new(session, workspace, &request_root, &status);
    let bytes = serde_json::to_vec(&endpoint).map_err(storage)?;
    if bytes.len() as u64 > MAXIMUM_REQUEST_BYTES {
        return Err(Error::Invalid(
            "native file projection exceeds its bound".into(),
        ));
    }
    directory
        .publish_immutable(
            Path::new("owner.json"),
            &bytes,
            acyclic_fs::NativeDurability::Durable,
        )
        .map_err(native_file_error)?;
    let owner =
        HostWorkspaceOwner::new(Arc::clone(&local))?.with_requests(Arc::clone(&request_root));
    let notices = Arc::new(Notices::new(PathBuf::new()));
    let notified = Arc::clone(&notices);
    let watcher = request_root
        .watch_changes(move |event| notified.note(&event))
        .map_err(native_file_error)?;
    let files = CarrierIo {
        served: Served::default(),
        requests: request_root,
        status,
        _ownership: Arc::clone(&ownership),
    };
    let result = match ready() {
        Ok(()) => serve_requests(&owner, files, session, &notices, shutdown).await,
        Err(error) => Err(error),
    };
    drop(watcher);
    drop(local);
    Error::after_cleanup(result, owner.retire().await)
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
    /// Its reply is published; original failure custody ends at withdrawal.
    Answered(Option<Box<Error>>),
}

/// The owner's active requests. Each reply is published and retired by its
/// original operation, without reading or rewriting another operation.
#[derive(Default)]
struct Served {
    known: HashMap<OperationId, Tracked>,
    ready: Vec<Request>,
    /// Request-directory scans, all of them overflow or startup rescans.
    #[cfg(test)]
    rescans: u64,
}

/// Only plain file state crosses the shared I/O worker boundary. The original
/// owner lock fences every accepted write even if its observer disappears.
struct CarrierIo {
    served: Served,
    requests: Arc<acyclic_fs::HeldRoot>,
    status: acyclic_fs::HeldRoot,
    _ownership: Arc<acyclic_native_runtime::FileLock>,
}

impl CarrierIo {
    async fn apply(
        mut self,
        key: impl std::fmt::Display,
        operation: impl FnOnce(&mut Self) -> Result<()> + Send + 'static,
    ) -> Result<Self> {
        let completion = acyclic_native_runtime::run_blocking_io(
            move || {
                operation(&mut self)?;
                Ok(self)
            },
            None,
        )
        .map_err(storage)?;
        acyclic_native_runtime::obs::waiting("native-workspace-files", key, completion.offloaded())
            .await
            .map_err(storage)?
    }
}

impl Served {
    /// Applies what notifications reported: each named request is admitted
    /// or withdrawn on its own; only an overflow lists the whole directory.
    fn touch(
        &mut self,
        requests: &acyclic_fs::HeldRoot,
        status: &acyclic_fs::HeldRoot,
        session: uuid::Uuid,
        touched: Touched,
    ) -> Result<()> {
        if let Some(failure) = touched.failure {
            return Err(Error::Storage(failure));
        }
        let names = if touched.rescan {
            #[cfg(test)]
            {
                self.rescans += 1;
            }
            let mut names = touched.names;
            for name in requests.read_names().map_err(native_file_error)? {
                names.insert(name.map_err(native_file_error)?);
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
            self.observe(requests, status, session, &name)?;
        }
        Ok(())
    }

    fn observe(
        &mut self,
        requests: &acyclic_fs::HeldRoot,
        status: &acyclic_fs::HeldRoot,
        session: uuid::Uuid,
        name: &std::ffi::OsStr,
    ) -> Result<()> {
        let path = Path::new(name);
        if path.extension().is_none_or(|extension| extension != "json") {
            return Ok(());
        }
        let Some(operation) = operation_leaf(name) else {
            // Not a request record; see the invalid-request case below.
            let _ = remove_held_projection(requests, path);
            return Ok(());
        };
        if self.known.contains_key(&operation) {
            // A known request changes only by its withdrawal.
            match requests.open_file(path, false).map_err(native_file_error) {
                Ok(_) => {}
                Err(Error::NotFound(_)) => self.withdraw(status, operation)?,
                Err(error) => return Err(error),
            }
            return Ok(());
        }
        // Request projections are writable by native tools. Invalid input
        // must not terminate the trusted owner or dispatch another action.
        let request = held_bytes(requests, path, MAXIMUM_REQUEST_BYTES).and_then(|bytes| {
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
                self.known.insert(operation, Tracked::Queued);
                self.ready.push(request);
            }
            // Withdrawn before this owner read it.
            Err(Error::NotFound(_)) => {}
            // Held-root removal unlinks the projection itself, never a
            // symlink target. Clients keep their durable command records.
            Err(admission) => {
                if let Err(cleanup) = remove_held_projection(requests, path) {
                    // No withdrawal event can be produced. End the original
                    // owner with both causes instead of abandoning its waiter.
                    return Error::after_cleanup(Err(admission), Err(cleanup));
                }
            }
        }
        Ok(())
    }

    /// Withdrawal retires only this request's reply, if already published.
    fn withdraw(&mut self, status: &acyclic_fs::HeldRoot, operation: OperationId) -> Result<()> {
        let std::collections::hash_map::Entry::Occupied(mut tracked) = self.known.entry(operation)
        else {
            return Ok(());
        };
        match tracked.get_mut() {
            Tracked::Queued => {}
            Tracked::Running { withdrawn } => {
                // Its dispatch still runs; its reply will not be published.
                *withdrawn = true;
                return Ok(());
            }
            Tracked::Answered(cause) => {
                let withdrawal =
                    remove_held_projection(status, Path::new(&format!("{operation}.json")));
                match withdrawal {
                    Ok(()) | Err(Error::NotFound(_)) => {}
                    Err(error) => {
                        // Original failure custody moves into the owner error.
                        return Error::after_cleanup(
                            cause.take().map_or(Ok(()), |cause| Err(*cause)),
                            Err(error),
                        );
                    }
                }
            }
        }
        drop(tracked.remove());
        Ok(())
    }

    /// Starts this notification batch; withdrawal before dispatch cancels it.
    fn start(&mut self) -> Vec<Request> {
        let mut ready = std::mem::take(&mut self.ready);
        ready.retain(|request| {
            if let Some(tracked) = self.known.get_mut(&request.operation) {
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
        status: &acyclic_fs::HeldRoot,
        operation: OperationId,
        outcome: Result<Value>,
    ) -> Result<()> {
        let std::collections::hash_map::Entry::Occupied(mut tracked) = self.known.entry(operation)
        else {
            return Ok(());
        };
        let Tracked::Running { withdrawn } = tracked.get() else {
            return Ok(());
        };
        if *withdrawn {
            drop(tracked.remove());
            return Ok(());
        }
        let bytes = match crate::client::encode_command_result(0, operation, &outcome, u64::MAX) {
            Ok(bytes) => bytes,
            Err(error) => return Error::after_cleanup(outcome.map(drop), Err(error)),
        };
        match status.publish_immutable(
            Path::new(&format!("{operation}.json")),
            &bytes,
            acyclic_fs::NativeDurability::Durable,
        ) {
            Ok(()) => {}
            // The immutable publisher observed different existing bytes.
            // This operation's reader observes that same writable projection;
            // a caller cannot terminate unrelated dispatches by occupying it.
            Err(acyclic_fs::demand::DemandError::Io(error))
                if error.kind() == std::io::ErrorKind::InvalidData => {}
            Err(error) => {
                return Error::after_cleanup(outcome.map(drop), Err(native_file_error(error)));
            }
        }
        tracked.insert(Tracked::Answered(outcome.err().map(Box::new)));
        Ok(())
    }
}

async fn serve_requests(
    owner: &HostWorkspaceOwner,
    mut files: CarrierIo,
    session: uuid::Uuid,
    notices: &Notices,
    shutdown: impl std::future::Future<Output = Result<()>>,
) -> Result<()> {
    tokio::pin!(shutdown);
    // Every dispatch runs independently; a slow one delays no other reply.
    let mut running = futures::stream::FuturesUnordered::<
        acyclic_stream::BoxProviderFuture<'_, (OperationId, Result<Value>)>,
    >::new();
    loop {
        let touched = notices.take();
        if touched.rescan || !touched.names.is_empty() || touched.failure.is_some() {
            files = files
                .apply(session, move |files| {
                    files
                        .served
                        .touch(&files.requests, &files.status, session, touched)
                })
                .await?;
        }
        for request in files.served.start() {
            let operation = request.operation;
            let dispatched = owner.dispatch(operation, request.action);
            running.push(Box::pin(async move { (operation, dispatched.await) }));
        }
        tokio::select! {
            result = &mut shutdown => return result,
            () = notices.changed.notified() => {}
            Some((operation, outcome)) = running.next(), if !running.is_empty() => {
                files = files.apply(operation, move |files| {
                    files.served.answer(&files.status, operation, outcome)
                }).await?;
            }
        }
    }
}

fn remove_held_projection(parent: &acyclic_fs::HeldRoot, leaf: &Path) -> Result<()> {
    match parent
        .remove_file(leaf, acyclic_fs::NativeDurability::Durable)
        .map_err(native_file_error)
    {
        Ok(()) | Err(Error::NotFound(_)) => Ok(()),
        Err(error) => Err(error),
    }
}

fn held_bytes(parent: &acyclic_fs::HeldRoot, leaf: &Path, maximum: u64) -> Result<Vec<u8>> {
    let file = parent.open_file(leaf, false).map_err(native_file_error)?;
    let metadata = file.metadata().map_err(storage)?;
    if !metadata.is_file() {
        return Err(Error::Invalid(
            "native file projection must be a regular file".into(),
        ));
    }
    // Publication is immutable. Bound the read by its opened length, with
    // allocation protection checked before reserving untrusted input bytes.
    let length = metadata.len();
    if length > maximum {
        return Err(Error::Invalid(
            "native input projection exceeds allocation protection".into(),
        ));
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(usize::try_from(length).map_err(storage)?)
        .map_err(storage)?;
    file.take(length.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(storage)?;
    if bytes.len() as u64 != length {
        return Err(Error::Invalid(
            "native file projection length changed while reading".into(),
        ));
    }
    Ok(bytes)
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

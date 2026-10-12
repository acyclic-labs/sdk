//! Service locking and the drain protocol.

use super::*;

pub(crate) struct ServiceLock {
    pub(crate) lifecycle_file: fs::File,
    pub(crate) data_file: Option<fs::File>,
}

impl ServiceLock {
    pub(crate) fn prepare_purge(&mut self) {
        if let Some(file) = self.data_file.take() {
            let _ = file.unlock();
        }
    }
}

impl Drop for ServiceLock {
    fn drop(&mut self) {
        self.prepare_purge();
        let _ = self.lifecycle_file.unlock();
    }
}

/// How any Acyclic binary identifies and stops the service of any other.
///
/// The control protocol changes between releases, and a service rejects a
/// protocol it does not speak, so a new binary could neither identify nor
/// drain an old service through it. This contract never changes, so every
/// binary from this one on can hand off to any other:
///
/// - The service holds an exclusive lock on `service.lock` for its whole
///   life, so a free lock proves that no service runs.
/// - While its endpoint accepts requests it publishes `service.identity`,
///   exactly `acyclic-service-v1\n{instance id}\n{binary identity}\n`.
/// - A file `service-stop/{instance id}` holding a drain ID of at most 128
///   ASCII letters, digits and hyphens asks that instance to drain every
///   session and exit. It then writes `service-drain/{instance id}.json`,
///   version 1, with its instance ID as `identity`, the `drainId` of the
///   request it served and whether teardown succeeded, and releases its
///   lock. Every requester of that instance is answered by that record:
///   an instance drains every session once, whichever request it read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ServiceMarker {
    pub(crate) instance_id: String,
    pub(crate) binary_identity: String,
}

pub(crate) const SERVICE_MARKER_HEADER: &str = "acyclic-service-v1";
pub(crate) const SERVICE_STOP_DIRECTORY: &str = "service-stop";

impl ServiceMarker {
    pub(crate) fn path(data: &Path) -> PathBuf {
        data.join("service.identity")
    }

    pub(crate) fn encode(&self) -> String {
        format!(
            "{SERVICE_MARKER_HEADER}\n{}\n{}\n",
            self.instance_id, self.binary_identity
        )
    }

    /// The running service's marker, if one is published.
    pub(crate) fn read(data: &Path) -> Option<Self> {
        let text = fs::read_to_string(Self::path(data)).ok()?;
        let mut lines = text.strip_suffix('\n')?.split('\n');
        let (Some(SERVICE_MARKER_HEADER), Some(instance_id), Some(binary_identity), None) =
            (lines.next(), lines.next(), lines.next(), lines.next())
        else {
            return None;
        };
        is_handoff_id(instance_id).then(|| Self {
            instance_id: instance_id.to_owned(),
            binary_identity: binary_identity.to_owned(),
        })
    }
}

/// Instance and drain IDs name files, so they are short and plain.
pub(crate) fn is_handoff_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

/// Asks the service instance `instance_id` to drain; see [`ServiceMarker`].
pub(crate) fn request_service_stop(
    data: &Path,
    instance_id: &str,
    drain_id: &str,
) -> Result<(), String> {
    let directory = data.join(SERVICE_STOP_DIRECTORY);
    fs::create_dir_all(&directory).map_err(display)?;
    // Written aside and renamed, so the service never reads a partial ID.
    let staged = directory.join(format!("{instance_id}.{drain_id}.next"));
    fs::write(&staged, drain_id).map_err(display)?;
    fs::rename(&staged, directory.join(instance_id)).map_err(display)
}

/// The service's side of stop requests; see [`ServiceMarker`].
pub(crate) struct StopRequests {
    pub(crate) request: PathBuf,
    pub(crate) arrived: Arc<tokio::sync::Notify>,
    pub(crate) _watcher: acyclic_fs::watch::NativeEventWatcher,
}

impl StopRequests {
    pub(crate) fn open(data: &Path, instance_id: &str) -> Result<Self, String> {
        let directory = data.join(SERVICE_STOP_DIRECTORY);
        fs::create_dir_all(&directory).map_err(display)?;
        // Requests addressed to earlier instances can never be served, and
        // their drain records were read by requesters that held this lock.
        let drains = data.join(SERVICE_DRAIN_DIRECTORY);
        match fs::remove_dir_all(&drains) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(display(error)),
        }
        for entry in fs::read_dir(&directory).map_err(display)? {
            match fs::remove_file(entry.map_err(display)?.path()) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(display(error)),
            }
        }
        let arrived = Arc::new(tokio::sync::Notify::new());
        let notification = Arc::clone(&arrived);
        // Any event, or a watcher error, only prompts another look.
        let mut watcher = <acyclic_fs::watch::NativeEventWatcher as notify::Watcher>::new(
            move |_| notification.notify_one(),
            notify::Config::default(),
        )
        .map_err(display)?;
        notify::Watcher::watch(
            &mut watcher,
            &directory,
            notify::RecursiveMode::NonRecursive,
        )
        .map_err(display)?;
        Ok(Self {
            request: directory.join(instance_id),
            arrived,
            _watcher: watcher,
        })
    }

    /// Waits for a stop request and returns its drain ID.
    pub(crate) async fn next(&self) -> String {
        loop {
            if let Some(drain_id) = fs::read_to_string(&self.request)
                .ok()
                .filter(|drain_id| is_handoff_id(drain_id))
            {
                return drain_id;
            }
            self.arrived.notified().await;
        }
    }
}

/// The published marker, withdrawn when the service stops answering.
pub(crate) struct PublishedServiceMarker {
    pub(crate) path: PathBuf,
}

impl PublishedServiceMarker {
    /// Publishes the marker whole: a reader sees no marker or all of it.
    pub(crate) fn create(data: &Path, marker: &ServiceMarker) -> Result<Self, String> {
        let path = ServiceMarker::path(data);
        let staged = data.join(format!("service.identity.{}.next", marker.instance_id));
        fs::write(&staged, marker.encode()).map_err(display)?;
        if let Err(error) = fs::rename(&staged, &path) {
            let _ = fs::remove_file(&staged);
            return Err(display(error));
        }
        Ok(Self { path })
    }
}

impl Drop for PublishedServiceMarker {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub(crate) const SERVICE_DRAIN_DIRECTORY: &str = "service-drain";

/// Where the instance `identity` records its drain; see [`ServiceMarker`].
pub(crate) fn service_drain_completion_path(data: &Path, identity: &str) -> PathBuf {
    data.join(SERVICE_DRAIN_DIRECTORY)
        .join(format!("{identity}.json"))
}

pub(crate) fn write_service_drain_completion(
    data: &Path,
    identity: &str,
    drain_id: &str,
    result: &acyclic_harness::Result<SessionCloseState>,
) -> Result<(), String> {
    let path = service_drain_completion_path(data, identity);
    let next = path.with_extension("next");
    fs::create_dir_all(data.join(SERVICE_DRAIN_DIRECTORY)).map_err(display)?;
    // This is the existing final drain-error wire rendering boundary. The
    // borrowed Root error retains its cause and custody through the write.
    let error = match result {
        Err(error) => Some(error.to_string()),
        Ok(SessionCloseState::Failed) => Some("Acyclic service retained a failed session shutdown".to_owned()),
        Ok(SessionCloseState::Closed) => None,
    };
    let value = json!({
        "version": 1,
        "identity": identity,
        "drainId": drain_id,
        "ok": matches!(result, Ok(SessionCloseState::Closed)),
        "error": error,
    });
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&next)
        .map_err(display)?;
    serde_json::to_writer_pretty(&mut file, &value).map_err(display)?;
    file.write_all(b"\n").map_err(display)?;
    file.sync_all().map_err(display)?;
    drop(file);
    durable_rename(&next, &path, RenameMode::Replace).map_err(display)
}

pub(crate) fn verify_service_drain_completion(data: &Path, identity: &str) -> Result<(), String> {
    let path = service_drain_completion_path(data, identity);
    let value: Value = serde_json::from_slice(&fs::read(&path).map_err(|error| {
        format!(
            "Acyclic service exited without durable drain confirmation at {}: {error}",
            path.display()
        )
    })?)
    .map_err(display)?;
    if value.get("version").and_then(Value::as_u64) != Some(1)
        || value.get("identity").and_then(Value::as_str) != Some(identity)
    {
        return Err("Acyclic service drain confirmation does not match this request".to_owned());
    }
    if value.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(format!(
            "Acyclic service teardown failed: {}",
            value
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unknown teardown error")
        ));
    }
    Ok(())
}

pub(crate) fn acquire_service_lock(data: &Path) -> Result<Option<ServiceLock>, String> {
    let parent = data
        .parent()
        .ok_or_else(|| "service data path has no parent".to_owned())?;
    fs::create_dir_all(parent).map_err(display)?;
    let lifecycle_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(parent.join(".acyclic-service-lifecycle.lock"))
        .map_err(display)?;
    match lifecycle_file.try_lock_exclusive() {
        Ok(()) => {}
        Err(error) if service_lock_is_contended(&error) => return Ok(None),
        Err(error) => return Err(display(error)),
    }
    fs::create_dir_all(data).map_err(display)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(data, fs::Permissions::from_mode(0o700)).map_err(display)?;
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(data.join("service.lock"))
        .map_err(display)?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(Some(ServiceLock {
            lifecycle_file,
            data_file: Some(file),
        })),
        Err(error) if service_lock_is_contended(&error) => Ok(None),
        Err(error) => Err(display(error)),
    }
}

pub(crate) fn service_lock_is_contended(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock || cfg!(windows) && error.raw_os_error() == Some(33)
}

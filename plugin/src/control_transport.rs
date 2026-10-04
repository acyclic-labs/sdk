//! Control endpoints and their per-platform transports.

use super::*;

pub(crate) const MAXIMUM_CONTROL_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAXIMUM_CONCURRENT_CONTROL_REQUESTS: usize = 64;
/// How long a response may take to reach a client that still holds its end.
/// A client that gave up closes it, which ends the write at once, so this
/// bounds only a client that stopped reading: the longest any client waits.
pub(crate) const CONTROL_RESPONSE_DELIVERY: std::time::Duration = CONTROL_COMMAND_WAIT;
/// How long a stopping service still delivers responses in flight.
pub(crate) const CONTROL_RESPONSE_DRAIN_GRACE: std::time::Duration =
    std::time::Duration::from_secs(1);
pub(crate) const CONTROL_PROBE_WAIT: std::time::Duration = std::time::Duration::from_secs(2);
pub(crate) const CONTROL_HOOK_WAIT: std::time::Duration = std::time::Duration::from_secs(15);
pub(crate) const CONTROL_COMMAND_WAIT: std::time::Duration = std::time::Duration::from_secs(120);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ControlCommand {
    Ping,
    Doctor,
    Hook,
    Git,
    Agents,
    Discard,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ControlRequest {
    pub(crate) version: u32,
    pub(crate) command: ControlCommand,
    pub(crate) cwd: PathBuf,
    pub(crate) argv: Vec<String>,
    pub(crate) name: String,
    pub(crate) arguments: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct CliRouting {
    pub(crate) selected_cwd: PathBuf,
    /// Explicit authorization to route a command into a direct child
    /// workspace while the invoking process remains in its parent workspace.
    /// The service verifies the lineage; this field is only a requested
    /// target, never a bearer grant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) authorized_agent: Option<String>,
}

pub(crate) fn cli_routing(selected_cwd: PathBuf) -> Value {
    json!(CliRouting {
        selected_cwd,
        authorized_agent: None,
    })
}

pub(crate) fn cli_routing_authorized(selected_cwd: PathBuf, agent: String) -> Value {
    json!(CliRouting {
        selected_cwd,
        authorized_agent: Some(agent),
    })
}

pub(crate) fn selected_cli_routing(request: &ControlRequest) -> Result<CliRouting, String> {
    if request.arguments.is_null() {
        return Ok(CliRouting {
            selected_cwd: request.cwd.canonicalize().map_err(display)?,
            authorized_agent: None,
        });
    }
    let mut routing: CliRouting =
        serde_json::from_value(request.arguments.clone()).map_err(display)?;
    routing.selected_cwd = routing.selected_cwd.canonicalize().map_err(display)?;
    Ok(routing)
}

pub(crate) struct ControlEndpoint {
    pub(crate) shutdown: watch::Sender<bool>,
    pub(crate) task: tokio::task::JoinHandle<Result<(), String>>,
    #[cfg(all(test, not(target_os = "linux")))]
    pub(crate) accepted: Arc<tokio::sync::Notify>,
    #[cfg(all(unix, not(target_os = "linux")))]
    pub(crate) socket_path: PathBuf,
    #[cfg(target_os = "linux")]
    pub(crate) mailbox_path: PathBuf,
    #[cfg(all(windows, test))]
    pub(crate) pipe_path: String,
}

impl ControlEndpoint {
    pub(crate) async fn shutdown(self) -> Result<(), String> {
        let _ = self.shutdown.send(true);
        let result = self.task.await.map_err(display)?;
        #[cfg(all(unix, not(target_os = "linux")))]
        match fs::remove_file(&self.socket_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(display(error)),
        }
        #[cfg(target_os = "linux")]
        match fs::remove_dir_all(&self.mailbox_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(display(error)),
        }
        result
    }
}

pub(crate) async fn start_control_endpoint(
    control: Arc<impl ConcurrentControlRequestDispatcher + 'static>,
    data: &Path,
) -> Result<ControlEndpoint, String> {
    fs::create_dir_all(data).map_err(display)?;
    let ledger = Arc::new(ControlLedger::open(data)?);
    let (shutdown, receiver) = watch::channel(false);
    #[cfg(all(test, not(target_os = "linux")))]
    let accepted = Arc::new(tokio::sync::Notify::new());

    #[cfg(target_os = "linux")]
    {
        let mailbox_path = prepare_linux_control_mailbox(data)?;
        let task = tokio::spawn(serve_linux_control_mailbox(
            mailbox_path.clone(),
            control,
            ledger,
            shutdown.clone(),
            receiver,
        ));
        Ok(ControlEndpoint {
            shutdown,
            task,
            mailbox_path,
        })
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let socket_path = prepare_unix_control_socket(data)?;
        match fs::remove_file(&socket_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(display(error)),
        }
        let listener = tokio::net::UnixListener::bind(&socket_path).map_err(display)?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).map_err(display)?;
        let task = tokio::spawn(serve_unix_control(
            listener,
            control,
            ledger,
            shutdown.clone(),
            receiver,
            #[cfg(test)]
            Arc::clone(&accepted),
        ));
        Ok(ControlEndpoint {
            shutdown,
            task,
            #[cfg(test)]
            accepted,
            socket_path,
        })
    }
    #[cfg(windows)]
    {
        let pipe_path = windows_control_pipe_path(data);
        #[cfg(test)]
        let endpoint_pipe_path = pipe_path.clone();
        // The first instance exists before this returns, so a started
        // endpoint accepts connections at once.
        let first = create_current_user_pipe(&pipe_path, true).map_err(display)?;
        let task = tokio::spawn(serve_windows_control(
            pipe_path,
            first,
            control,
            ledger,
            shutdown.clone(),
            receiver,
            #[cfg(test)]
            Arc::clone(&accepted),
        ));
        Ok(ControlEndpoint {
            shutdown,
            task,
            #[cfg(test)]
            accepted,
            #[cfg(test)]
            pipe_path: endpoint_pipe_path,
        })
    }
}

/// Suffix of an exchange that its client is still building.
#[cfg(target_os = "linux")]
pub(crate) const LINUX_EXCHANGE_UNPUBLISHED: &str = ".new";
#[cfg(target_os = "linux")]
pub(crate) const LINUX_EXCHANGE_REQUEST: &str = "request";
#[cfg(target_os = "linux")]
pub(crate) const LINUX_EXCHANGE_CLAIMED: &str = "processing";
#[cfg(target_os = "linux")]
pub(crate) const LINUX_EXCHANGE_RESPONSE: &str = "response";

/// The mailbox of the Linux control transport, for hosts whose sandbox denies
/// connecting to a Unix socket but allows the runtime directory. Each request
/// is an exchange directory in it. The client builds the exchange under a name
/// ending in [`LINUX_EXCHANGE_UNPUBLISHED`], holding the request and a FIFO for
/// the response, then publishes it with one rename into the mailbox, which the
/// service watches. The service claims a published exchange by renaming its
/// request, writes the newline-terminated response into the FIFO and removes
/// the exchange; a client removes only an exchange that it gives up on.
#[cfg(target_os = "linux")]
pub(crate) fn linux_control_mailbox_path(data: &Path) -> PathBuf {
    unix_control_runtime_directory().join(format!(
        "service-{}.inbox",
        short_hash(data.as_os_str().as_encoded_bytes())
    ))
}

#[cfg(target_os = "linux")]
pub(crate) fn prepare_linux_control_mailbox(data: &Path) -> Result<PathBuf, String> {
    use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};

    let runtime = prepare_unix_control_runtime_directory()?;
    let mailbox = linux_control_mailbox_path(data);
    match fs::remove_dir_all(&mailbox) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(display(error)),
    }
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(&mailbox).map_err(display)?;
    debug_assert_eq!(mailbox.parent(), Some(runtime.as_path()));
    fs::set_permissions(&mailbox, fs::Permissions::from_mode(0o700)).map_err(display)?;
    Ok(mailbox)
}

#[cfg(target_os = "linux")]
pub(crate) fn open_linux_directory(
    directory: impl rustix::fd::AsFd,
    name: impl rustix::path::Arg,
) -> io::Result<rustix::fd::OwnedFd> {
    rustix::fs::openat(
        directory,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(errno_to_io)
}

#[cfg(target_os = "linux")]
pub(crate) async fn serve_linux_control_mailbox(
    mailbox: PathBuf,
    control: Arc<impl ConcurrentControlRequestDispatcher + 'static>,
    ledger: Arc<ControlLedger>,
    shutdown_sender: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    let mailbox_directory =
        Arc::new(open_linux_directory(rustix::fs::CWD, &mailbox).map_err(display)?);
    // Watching before the first scan means that every exchange is either found
    // by that scan or published later, which wakes another scan.
    let published = rustix::fs::inotify::init(
        rustix::fs::inotify::CreateFlags::CLOEXEC | rustix::fs::inotify::CreateFlags::NONBLOCK,
    )
    .and_then(|published| {
        rustix::fs::inotify::add_watch(
            &published,
            &mailbox,
            rustix::fs::inotify::WatchFlags::MOVED_TO
                | rustix::fs::inotify::WatchFlags::ONLYDIR
                | rustix::fs::inotify::WatchFlags::DONT_FOLLOW,
        )?;
        Ok(published)
    })
    .map_err(errno_to_io)
    .and_then(tokio::io::unix::AsyncFd::new)
    .map_err(display)?;

    let mut requests = tokio::task::JoinSet::new();
    let mut result = loop {
        if let Err(error) = claim_linux_mailbox_requests(
            &mailbox_directory,
            &control,
            &ledger,
            &shutdown,
            &mut requests,
        ) {
            break Err(error);
        }
        tokio::select! {
            ready = published.readable() => {
                let mut ready = match ready {
                    Ok(ready) => ready,
                    Err(error) => break Err(display(error)),
                };
                // Events only wake the next scan, which finds every published
                // exchange, including any whose event an overflow dropped.
                let mut events = [0_u8; 4096];
                while let Ok(Ok(_)) = ready.try_io(|published| {
                    rustix::io::read(published.get_ref(), &mut events).map_err(errno_to_io)
                }) {}
            }
            completed = requests.join_next(), if !requests.is_empty() => {
                if let Some(Err(error)) = completed {
                    break Err(format!("Acyclic mailbox request task failed: {error}"));
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break Ok(());
                }
            }
        }
    };
    let _ = shutdown_sender.send(true);
    while let Some(completed) = requests.join_next().await {
        if let (Ok(()), Err(error)) = (&result, completed) {
            result = Err(format!("Acyclic mailbox request task failed: {error}"));
        }
    }
    result
}

/// Claims every published exchange, up to the concurrency bound. The mailbox
/// is private and holds only exchanges in flight, so it is scanned in place.
#[cfg(target_os = "linux")]
pub(crate) fn claim_linux_mailbox_requests(
    mailbox_directory: &Arc<rustix::fd::OwnedFd>,
    control: &Arc<impl ConcurrentControlRequestDispatcher + 'static>,
    ledger: &Arc<ControlLedger>,
    shutdown: &watch::Receiver<bool>,
    requests: &mut tokio::task::JoinSet<()>,
) -> Result<(), String> {
    use std::os::unix::ffi::OsStrExt as _;

    let entries = rustix::fs::Dir::read_from(&**mailbox_directory).map_err(display)?;
    for entry in entries {
        if requests.len() >= MAXIMUM_CONCURRENT_CONTROL_REQUESTS {
            break;
        }
        let entry = entry.map_err(display)?;
        let name = entry.file_name().to_bytes();
        if name.starts_with(b".")
            || name.ends_with(LINUX_EXCHANGE_UNPUBLISHED.as_bytes())
            || !entry.file_type().is_dir()
        {
            continue;
        }
        let name = std::ffi::OsStr::from_bytes(name).to_owned();
        let Ok(exchange) = open_linux_directory(&**mailbox_directory, &name) else {
            continue;
        };
        if rustix::fs::renameat(
            &exchange,
            LINUX_EXCHANGE_REQUEST,
            &exchange,
            LINUX_EXCHANGE_CLAIMED,
        )
        .is_err()
        {
            continue;
        }
        let exchange = LinuxMailboxExchange {
            mailbox: Arc::clone(mailbox_directory),
            exchange: Some(exchange),
            name,
        };
        let control = Arc::clone(control);
        let ledger = Arc::clone(ledger);
        requests.spawn(handle_linux_mailbox_request(
            exchange,
            control,
            ledger,
            shutdown.clone(),
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) async fn handle_linux_mailbox_request(
    exchange: LinuxMailboxExchange,
    control: Arc<impl ConcurrentControlRequestDispatcher>,
    ledger: Arc<ControlLedger>,
    shutdown: watch::Receiver<bool>,
) {
    if let Some(directory) = &exchange.exchange {
        let mut response = match read_linux_control_file_at(directory, LINUX_EXCHANGE_CLAIMED) {
            Ok(request) if request.len() <= MAXIMUM_CONTROL_MESSAGE_BYTES => {
                match serde_json::from_slice::<ControlEnvelope<ControlRequest>>(&request) {
                    Ok(envelope) => dispatch_control_envelope(&control, &ledger, envelope).await,
                    Err(error) => invalid_control_request_response(&request, &error),
                }
            }
            Ok(_) => {
                uncorrelated_control_response("Acyclic control request exceeds the 4 MiB bound")
            }
            Err(error) => uncorrelated_control_response(&display(error)),
        };
        response.push(b'\n');
        // A client that gave up holds no reader, so opening fails at once.
        let sender = rustix::fs::openat(
            directory,
            LINUX_EXCHANGE_RESPONSE,
            rustix::fs::OFlags::WRONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(errno_to_io)
        .and_then(tokio::net::unix::pipe::Sender::from_owned_fd);
        if let Ok(mut sender) = sender {
            let write = async { sender.write_all(&response).await.map_err(display) };
            let _ = deliver_control_response(write, shutdown).await;
        }
    }
    exchange.remove();
}

/// Exchange files are bounded and live in a private runtime directory, so
/// reading one directly is cheaper than a hop through the blocking pool.
#[cfg(target_os = "linux")]
pub(crate) fn read_linux_control_file_at(
    directory: &rustix::fd::OwnedFd,
    name: &str,
) -> io::Result<Vec<u8>> {
    let file = rustix::fs::openat(
        directory,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(errno_to_io)?;
    let metadata = rustix::fs::fstat(&file).map_err(errno_to_io)?;
    if !rustix::fs::FileType::from_raw_mode(metadata.st_mode).is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Acyclic control message is not a regular file",
        ));
    }
    let file = std::fs::File::from(file);
    let mut request = Vec::with_capacity(MAXIMUM_CONTROL_MESSAGE_BYTES.min(64 * 1024));
    file.take(
        u64::try_from(MAXIMUM_CONTROL_MESSAGE_BYTES)
            .unwrap_or(u64::MAX)
            .saturating_add(1),
    )
    .read_to_end(&mut request)?;
    Ok(request)
}

/// One exchange directory in a Linux mailbox; see
/// [`linux_control_mailbox_path`].
#[cfg(target_os = "linux")]
pub(crate) struct LinuxMailboxExchange {
    pub(crate) mailbox: Arc<rustix::fd::OwnedFd>,
    pub(crate) exchange: Option<rustix::fd::OwnedFd>,
    pub(crate) name: std::ffi::OsString,
}

#[cfg(target_os = "linux")]
impl LinuxMailboxExchange {
    pub(crate) fn remove(&self) {
        if let Some(directory) = &self.exchange {
            for name in [
                LINUX_EXCHANGE_REQUEST,
                LINUX_EXCHANGE_CLAIMED,
                LINUX_EXCHANGE_RESPONSE,
            ] {
                let _ = rustix::fs::unlinkat(directory, name, rustix::fs::AtFlags::empty());
            }
        }
        let _ = rustix::fs::unlinkat(&*self.mailbox, &self.name, rustix::fs::AtFlags::REMOVEDIR);
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn errno_to_io(error: rustix::io::Errno) -> io::Error {
    io::Error::from_raw_os_error(error.raw_os_error())
}

#[cfg(all(unix, not(target_os = "linux")))]
pub(crate) fn unix_control_socket_path(data: &Path) -> PathBuf {
    unix_control_runtime_directory().join(format!(
        "service-{}.sock",
        short_hash(data.as_os_str().as_encoded_bytes())
    ))
}

#[cfg(unix)]
#[allow(
    unsafe_code,
    reason = "geteuid has no preconditions and reads no memory"
)]
pub(crate) fn unix_control_runtime_directory() -> PathBuf {
    let uid = unsafe { libc::geteuid() };
    PathBuf::from("/tmp").join(format!("acyclic-{uid}"))
}

#[cfg(all(unix, not(target_os = "linux")))]
#[allow(
    unsafe_code,
    reason = "geteuid has no preconditions and reads no memory"
)]
pub(crate) fn prepare_unix_control_socket(data: &Path) -> Result<PathBuf, String> {
    let directory = prepare_unix_control_runtime_directory()?;

    // Unix-domain paths are short (typically 104-108 bytes), so an endpoint
    // cannot safely inherit the arbitrary length of the durable state path.
    // The installer allowlists this exact socket for Codex; peer credentials,
    // its private parent, and 0600 socket permissions authenticate clients.
    let socket = unix_control_socket_path(data);
    debug_assert_eq!(socket.parent(), Some(directory.as_path()));
    Ok(socket)
}

#[cfg(unix)]
#[allow(
    unsafe_code,
    reason = "geteuid has no preconditions and reads no memory"
)]
pub(crate) fn prepare_unix_control_runtime_directory() -> Result<PathBuf, String> {
    use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, PermissionsExt as _};

    let directory = unix_control_runtime_directory();
    match fs::symlink_metadata(&directory) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(&directory) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(display(error)),
            }
        }
        Err(error) => return Err(display(error)),
    }
    let metadata = fs::symlink_metadata(&directory).map_err(display)?;
    let uid = unsafe { libc::geteuid() };
    if !metadata.file_type().is_dir() || metadata.uid() != uid {
        return Err("Acyclic workspace directory is not owned by the current user".to_owned());
    }
    if metadata.permissions().mode() & 0o777 != 0o700 {
        return Err("Acyclic runtime directory permissions must be 0700".to_owned());
    }
    Ok(directory)
}

#[cfg(all(unix, not(target_os = "linux")))]
pub(crate) async fn serve_unix_control(
    listener: tokio::net::UnixListener,
    control: Arc<impl ConcurrentControlRequestDispatcher + 'static>,
    ledger: Arc<ControlLedger>,
    shutdown_sender: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
    #[cfg(test)] accepted: Arc<tokio::sync::Notify>,
) -> Result<(), String> {
    let mut connections = tokio::task::JoinSet::new();
    let result = loop {
        tokio::select! {
            incoming = listener.accept(), if connections.len() < MAXIMUM_CONCURRENT_CONTROL_REQUESTS => {
                let (stream, _) = match incoming {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(display(error)),
                };
                if !same_user_peer(&stream)? {
                    continue;
                }
                let control = Arc::clone(&control);
                let ledger = Arc::clone(&ledger);
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    let _ = handle_control_connection(stream, control, ledger, connection_shutdown).await;
                });
                #[cfg(test)]
                accepted.notify_one();
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                let _ = completed;
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break Ok(());
                }
            }
        }
    };
    let _ = shutdown_sender.send(true);
    while connections.join_next().await.is_some() {}
    result
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
#[allow(unsafe_code)]
pub(crate) fn same_user_peer(stream: &tokio::net::UnixStream) -> Result<bool, String> {
    use std::os::fd::AsRawFd as _;
    let mut uid = 0;
    let mut gid = 0;
    // SAFETY: the stream owns a valid descriptor and both output pointers are valid.
    let status = unsafe { libc::getpeereid(stream.as_raw_fd(), &mut uid, &mut gid) };
    if status != 0 {
        return Err(display(io::Error::last_os_error()));
    }
    // SAFETY: geteuid has no preconditions and does not dereference memory.
    Ok(uid == unsafe { libc::geteuid() })
}

#[cfg(windows)]
pub(crate) async fn serve_windows_control(
    pipe_path: String,
    first: tokio::net::windows::named_pipe::NamedPipeServer,
    control: Arc<impl ConcurrentControlRequestDispatcher + 'static>,
    ledger: Arc<ControlLedger>,
    shutdown_sender: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
    #[cfg(test)] accepted: Arc<tokio::sync::Notify>,
) -> Result<(), String> {
    // One instance listens at every moment: the next is created before a
    // connected one is handed to its task, since a client that finds no
    // listening instance gets `NotFound`, as if no service ran.
    let mut listening = first;
    let mut connections = tokio::task::JoinSet::new();
    let result = 'result: loop {
        let connected = loop {
            tokio::select! {
                connected = listening.connect(), if connections.len() < MAXIMUM_CONCURRENT_CONTROL_REQUESTS => break connected,
                completed = connections.join_next(), if !connections.is_empty() => {
                    let _ = completed;
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break 'result Ok(());
                    }
                }
            }
        };
        let next = match create_current_user_pipe(&pipe_path, false) {
            Ok(next) => next,
            Err(error) => break Err(display(error)),
        };
        let server = std::mem::replace(&mut listening, next);
        // A client that vanished before its connection completed costs only
        // its own instance.
        if connected.is_err() {
            continue;
        }
        let control = Arc::clone(&control);
        let ledger = Arc::clone(&ledger);
        let connection_shutdown = shutdown.clone();
        connections.spawn(async move {
            let _ = handle_control_connection(server, control, ledger, connection_shutdown).await;
        });
        #[cfg(test)]
        accepted.notify_one();
    };
    let _ = shutdown_sender.send(true);
    while connections.join_next().await.is_some() {}
    result
}

#[cfg(windows)]
#[allow(unsafe_code)]
pub(crate) fn create_current_user_pipe(
    pipe_path: &str,
    first: bool,
) -> Result<tokio::net::windows::named_pipe::NamedPipeServer, io::Error> {
    use std::ffi::c_void;
    use std::ptr;
    use windows_sys::Win32::Foundation::LocalFree;
    use windows_sys::Win32::Security::Authorization::ConvertStringSecurityDescriptorToSecurityDescriptorW;
    use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;

    // Protected DACL: the object owner and LocalSystem only. Remote clients
    // are rejected separately by the pipe mode below.
    let mut sddl = "D:P(A;;GA;;;OW)(A;;GA;;;SY)"
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let mut descriptor = ptr::null_mut::<c_void>();
    // SAFETY: `sddl` is a live NUL-terminated UTF-16 string and the output
    // pointer is valid for the duration of the call.
    let converted = unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_mut_ptr(),
            1,
            &mut descriptor,
            ptr::null_mut(),
        )
    };
    if converted == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut attributes = SECURITY_ATTRIBUTES {
        nLength: u32::try_from(std::mem::size_of::<SECURITY_ATTRIBUTES>()).unwrap_or(u32::MAX),
        lpSecurityDescriptor: descriptor,
        bInheritHandle: 0,
    };
    let mut options = tokio::net::windows::named_pipe::ServerOptions::new();
    options
        .first_pipe_instance(first)
        .reject_remote_clients(true);
    // SAFETY: `attributes` and its descriptor remain valid until `create`
    // returns; the kernel copies the descriptor into the new object.
    let result = unsafe {
        options.create_with_security_attributes_raw(
            pipe_path,
            (&mut attributes as *mut SECURITY_ATTRIBUTES).cast(),
        )
    };
    // SAFETY: the descriptor was allocated by the conversion API above and is
    // no longer referenced after pipe creation returns.
    unsafe {
        LocalFree(descriptor.cast());
    }
    result
}

#[cfg(any(test, not(target_os = "linux")))]
pub(crate) async fn handle_control_connection<S>(
    mut stream: S,
    control: Arc<impl ConcurrentControlRequestDispatcher + 'static>,
    ledger: Arc<ControlLedger>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut request = Vec::new();
    let read = async {
        let reader = BufReader::new(&mut stream);
        let mut bounded = reader.take((MAXIMUM_CONTROL_MESSAGE_BYTES + 1) as u64);
        bounded
            .read_until(b'\n', &mut request)
            .await
            .map_err(display)
    };
    tokio::select! {
        read = read => {
            read?;
        }
        changed = shutdown.changed() => {
            let _ = changed;
            return Ok(());
        }
    }
    let dispatch = async {
        if request.len() > MAXIMUM_CONTROL_MESSAGE_BYTES {
            uncorrelated_control_response("Acyclic control request exceeds the 4 MiB bound")
        } else if request.last() != Some(&b'\n') {
            uncorrelated_control_response("Acyclic control request must end with a newline")
        } else {
            request.pop();
            match serde_json::from_slice::<ControlEnvelope<ControlRequest>>(&request) {
                Ok(envelope) => dispatch_control_envelope(&control, &ledger, envelope).await,
                Err(error) => invalid_control_request_response(&request, &error),
            }
        }
    };
    tokio::pin!(dispatch);
    let mut peer_byte = [0_u8; 1];
    let response = tokio::select! {
        response = &mut dispatch => response,
        peer = stream.read(&mut peer_byte) => {
            match peer {
                Ok(0) => {
                    let _ = dispatch.await;
                    return Ok(());
                }
                Ok(_) => return Err("Acyclic control request included trailing bytes".to_owned()),
                Err(error) if matches!(
                    error.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::UnexpectedEof
                ) => {
                    let _ = dispatch.await;
                    return Ok(());
                }
                Err(error) => return Err(display(error)),
            }
        }
    };
    let write = async {
        stream.write_all(&response).await.map_err(display)?;
        stream.write_all(b"\n").await.map_err(display)?;
        stream.flush().await.map_err(display)
    };
    deliver_control_response(write, shutdown).await
}

/// Delivers one response while its client still reads it: up to
/// [`CONTROL_RESPONSE_DELIVERY`], or [`CONTROL_RESPONSE_DRAIN_GRACE`] once
/// the service stops.
pub(crate) async fn deliver_control_response(
    write: impl std::future::Future<Output = Result<(), String>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    let stopped = async {
        let _ = shutdown.wait_for(|stopping| *stopping).await;
        tokio::time::sleep(CONTROL_RESPONSE_DRAIN_GRACE).await;
    };
    tokio::select! {
        delivered = tokio::time::timeout(CONTROL_RESPONSE_DELIVERY, write) => delivered
            .map_err(|_| "Acyclic control response exceeded its delivery deadline".to_owned())?,
        () = stopped => Err("Acyclic control response exceeded its drain deadline".to_owned()),
    }
}

pub(crate) async fn dispatch_control_request(
    control: &Arc<impl ConcurrentControlRequestDispatcher>,
    request: ControlRequest,
) -> Result<Value, String> {
    if request.version != 1 {
        return Err("unsupported Acyclic control request".to_owned());
    }
    control.dispatch_request(request).await
}

pub(crate) async fn dispatch_control_envelope(
    control: &Arc<impl ConcurrentControlRequestDispatcher>,
    ledger: &Arc<ControlLedger>,
    envelope: ControlEnvelope<ControlRequest>,
) -> Vec<u8> {
    let request_id = envelope.request_id.clone();
    if let Err(error) = envelope.validate() {
        return control_response_for(&request_id, Err(error));
    }
    if matches!(
        envelope.request.command,
        ControlCommand::Ping | ControlCommand::Doctor | ControlCommand::Agents
    ) {
        return control_response_for(
            &request_id,
            dispatch_control_request(control, envelope.request).await,
        );
    }
    // Ledger transitions are single unflushed appends, cheaper than handing
    // them to a blocking worker.
    match ledger.begin(&envelope) {
        Err(error) => control_response_for(&request_id, Err(error)),
        Ok(LedgerDecision::Completed(response)) => response,
        Ok(LedgerDecision::Execute) => {
            let result = dispatch_control_request(control, envelope.request.clone()).await;
            // The ledger records exactly the bounded bytes that are sent.
            let response = control_response_for(&request_id, result);
            match ledger.complete(&envelope, &response) {
                Ok(()) => response,
                Err(error) => control_response_for(
                    &request_id,
                    Err(format!(
                        "Acyclic completed the operation but could not record its response: {error}"
                    )),
                ),
            }
        }
    }
}

pub(crate) trait ControlRequestDispatcher: Send {
    fn dispatch_request(
        &mut self,
        request: ControlRequest,
    ) -> impl std::future::Future<Output = Result<Value, String>> + Send;
}

pub(crate) trait ConcurrentControlRequestDispatcher: Send + Sync {
    fn dispatch_request(
        &self,
        request: ControlRequest,
    ) -> impl std::future::Future<Output = Result<Value, String>> + Send;
}

impl<T: ControlRequestDispatcher + Send> ConcurrentControlRequestDispatcher for AsyncMutex<T> {
    async fn dispatch_request(&self, request: ControlRequest) -> Result<Value, String> {
        self.lock().await.dispatch_request(request).await
    }
}

//! Model-visible identity tools, served by the shared harness stdio MCP
//! tool server. The server owns the protocol; this provider runs one identity
//! operation at a time on the command's runtime.

use super::*;
use acyclic_harness_core::mcp::stdio::{Settled, ToolCall, ToolProvider, ToolServer, Wake};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};
#[cfg(test)]
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};

const FRAME_LIMIT: usize = 1024 * 1024;

struct Server {
    identity: Identity,
    credentials: PathBuf,
    pending: Option<(DeviceCode, tokio::time::Instant)>,
    login_cancelled: Option<tokio::sync::oneshot::Receiver<()>>,
}

impl Server {
    fn new(options: Options) -> Result<Self, Error> {
        Ok(Self {
            identity: Identity::new(&options.endpoint)?,
            credentials: options.credentials,
            pending: None,
            login_cancelled: None,
        })
    }

    async fn call(&mut self, name: &str) -> Result<Value, Error> {
        if name == "identity_login" {
            let store = Store::open(&self.credentials)?;
            let device = self.identity.device_code("mcp", &store).await?;
            let result = json!({"user_code": device.user_code,
                "verification_uri": device.verification_uri,
                "verification_uri_complete": device.verification_uri_complete,
                "expires_in": device.expires_in});
            self.pending = Some((device, tokio::time::Instant::now()));
            return Ok(result);
        }
        let store = Store::open(&self.credentials)?;
        match name {
            "identity_login_complete" => {
                let cancellation = self.login_cancelled.take();
                let cancelled = async move {
                    match cancellation {
                        Some(receiver) => {
                            let _ = receiver.await;
                        }
                        None => std::future::pending().await,
                    }
                };
                let (device, started) = self
                    .pending
                    .as_mut()
                    .ok_or_else(|| Error::new("login_required", "call identity_login first"))?;
                let session = match async {
                    store.login_agent_for(&self.identity.endpoint, device)?;
                    self.identity.login(device, *started, cancelled).await
                }
                .await
                {
                    Ok(session) => session,
                    Err(error) => {
                        if !error.status.is_some_and(|status| status.is_server_error())
                            && !matches!(
                                error.code.as_str(),
                                "request_cancelled"
                                    | "transport_error"
                                    | "server_error"
                                    | "temporarily_unavailable"
                                    | "unavailable"
                            )
                        {
                            self.pending = None;
                        }
                        return Err(error);
                    }
                };
                let session = self.identity.complete_login(&store, session, device).await;
                self.pending = None;
                let session = session?;
                Ok(json!({"authenticated": true, "expires_at": session.expires_at}))
            }
            "identity_signup" if self.pending.is_some() => Err(Error::new(
                "login_pending",
                "complete the pending login before signup",
            )),
            "identity_signup" => self.identity.signup(&store).await,
            "identity_whoami" => self.identity.get(&store, "/v1/identity/session").await,
            "identity_orgs" => self.identity.get(&store, "/v1/identity/orgs").await,
            "identity_logout" => {
                let result = self.identity.logout(&store).await?;
                self.pending = None;
                Ok(result)
            }
            "identity_refresh" => {
                if store
                    .credentials(&self.identity.endpoint)?
                    .session
                    .is_none()
                {
                    return self.identity.renew_agent(&store).await;
                }
                let session = store.read(&self.identity.endpoint)?;
                let session = self.identity.refresh(&store, &session).await?;
                Ok(json!({"authenticated": true, "expires_at": session.expires_at}))
            }
            _ => Err(Error::new("unknown_tool", "unknown identity tool")),
        }
    }
}

fn tools() -> Vec<Value> {
    [
        ("identity_login", "Start device login. Show the returned URL and code to the user, then call identity_login_complete."),
        ("identity_login_complete", "Wait for device approval and save the session started by identity_login."),
        ("identity_signup", "Create a tiny-tier agent identity using Ed25519 and hashcash; accepts new or pending agent credentials."),
        ("identity_whoami", "Show the current signed-in identity; refresh expired access automatically."),
        ("identity_orgs", "List organizations visible to the signed-in identity."),
        ("identity_refresh", "Rotate the stored session refresh token or renew the agent certificate."),
        ("identity_logout", "Revoke the OAuth session and clear local session tokens; retain the agent key."),
    ].iter().map(|(name, description)| json!({"name": name, "description": description,
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": false},
        "annotations": {"readOnlyHint": false, "destructiveHint": false,
            "idempotentHint": false, "openWorldHint": true}})).collect()
}

/// The one identity operation in flight.
struct Active {
    key: String,
    task: tokio::task::JoinHandle<()>,
    result: Arc<Mutex<Option<Result<Value, Error>>>>,
    done: tokio::sync::watch::Receiver<()>,
    login_cancel: Option<tokio::sync::oneshot::Sender<()>>,
    /// A dispatched exchange that finishes and saves its credentials even
    /// after cancellation or the end of input.
    finishes: bool,
    cancelled: bool,
}

/// Identity tools on the shared tool server. Operations run on the command's
/// runtime while the server keeps answering pings and cancellations.
struct Identities {
    server: Arc<tokio::sync::Mutex<Server>>,
    runtime: tokio::runtime::Handle,
    active: Option<Active>,
    calls: u64,
}

impl Identities {
    /// Awaits the dispatched exchange still finishing once the server has
    /// stopped; [`ToolProvider::closed`] already ended every other operation.
    fn finish(&mut self) {
        if let Some(active) = self.active.take() {
            let _ = futures::executor::block_on(active.task);
        }
    }
}

impl ToolProvider for Identities {
    fn tools(&self) -> Value {
        json!({ "tools": tools() })
    }

    fn call(&mut self, name: &str, arguments: &Value) -> acyclic_harness_core::Result<ToolCall> {
        if !arguments.is_null()
            && arguments
                .as_object()
                .is_none_or(|object| !object.is_empty())
        {
            return Ok(ToolCall::Invalid("Identity tools take no arguments".into()));
        }
        if self.active.is_some() {
            return Ok(ToolCall::Busy("Identity operation in progress".into()));
        }
        let (login_cancel, login_cancelled) = if name == "identity_login_complete" {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
        let result = Arc::new(Mutex::new(None));
        let (done, completed) = tokio::sync::watch::channel(());
        let task = {
            let (server, name, result, done) = (
                Arc::clone(&self.server),
                name.to_owned(),
                Arc::clone(&result),
                done,
            );
            self.runtime.spawn(async move {
                let mut server = server.lock().await;
                server.login_cancelled = login_cancelled;
                let outcome = server.call(&name).await;
                *result.lock().unwrap_or_else(PoisonError::into_inner) = Some(outcome);
                done.send_replace(());
            })
        };
        self.calls += 1;
        let key = format!("identity-{}", self.calls);
        self.active = Some(Active {
            key: key.clone(),
            task,
            result,
            done: completed,
            login_cancel,
            finishes: matches!(
                name,
                "identity_login_complete"
                    | "identity_refresh"
                    | "identity_whoami"
                    | "identity_orgs"
            ),
            cancelled: false,
        });
        Ok(ToolCall::Park(key))
    }

    fn cancel(&mut self, key: &str) {
        let Some(active) = self.active.as_mut().filter(|active| active.key == key) else {
            return;
        };
        // Cancellation stops further login polls; a dispatched exchange
        // still finishes and keeps the operation until it does.
        if let Some(cancel) = active.login_cancel.take() {
            let _ = cancel.send(());
        }
        active.cancelled = true;
        if !active.finishes
            && let Some(active) = self.active.take()
        {
            active.task.abort();
        }
    }

    fn closed(&mut self) {
        // No answer is read any more: stop login polls and abort operations
        // that need not finish.
        let Some(active) = self.active.as_mut() else {
            return;
        };
        if let Some(cancel) = active.login_cancel.take() {
            let _ = cancel.send(());
        }
        if !active.finishes
            && let Some(active) = self.active.take()
        {
            active.task.abort();
        }
    }

    fn progress(
        &mut self,
        parked: &BTreeSet<String>,
        settled: &mut Vec<(String, Settled)>,
    ) -> acyclic_harness_core::Result<Option<Wake>> {
        let Some(active) = &self.active else {
            return Ok(None);
        };
        let finished = {
            // Inspect producer loss while holding the result lock: a producer
            // cannot publish between an empty read and a closed-event verdict.
            let mut result = active.result.lock().unwrap_or_else(PoisonError::into_inner);
            result.take().or_else(|| {
                active.done.has_changed().err().map(|_| {
                    Err(Error::new(
                        "operation_unavailable",
                        "identity operation ended before publishing its result; recovery: acyclic identity-mcp",
                    ))
                })
            })
        };
        let Some(outcome) = finished else {
            let mut done = active.done.clone();
            return Ok(Some(Box::pin(async move {
                let _ = done.changed().await;
            })));
        };
        if !active.cancelled && parked.contains(&active.key) {
            settled.push((active.key.clone(), outcome.map_err(|error| error.json())));
        }
        self.active = None;
        Ok(None)
    }
}

pub(super) async fn serve(arguments: &[String]) -> Result<(), io::Error> {
    let options = Options::parse(arguments).map_err(io::Error::other)?;
    let server = Server::new(options).map_err(io::Error::other)?;
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        serve_on(server, runtime, std::io::stdin(), std::io::stdout())
    })
    .await
    .map_err(io::Error::other)?
}

/// Serves `server` over `input` and `output`, then ends its operation.
fn serve_on(
    server: Server,
    runtime: tokio::runtime::Handle,
    input: impl io::Read + Send + 'static,
    output: impl io::Write + Send + 'static,
) -> io::Result<()> {
    let mut identities = Identities {
        server: Arc::new(tokio::sync::Mutex::new(server)),
        runtime,
        active: None,
        calls: 0,
    };
    let served = ToolServer {
        name: "acyclic-identity",
        version: env!("CARGO_PKG_VERSION"),
        parked: 1,
        // The one-MiB frame budget includes its delimiter.
        frame_bytes: u32::try_from(FRAME_LIMIT - 1).map_err(io::Error::other)?,
    }
    .serve(&mut identities, input, output);
    identities.finish();
    served.map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "transport tests fail immediately on invalid fixtures"
    )]

    use super::*;

    fn server(path: PathBuf) -> Server {
        Server::new(Options {
            command: "identity-mcp".to_owned(),
            json: true,
            endpoint: "https://example.test".to_owned(),
            credentials: path,
            agent: false,
            org: None,
            output: None,
        })
        .expect("server")
    }

    #[tokio::test]
    async fn operation_producer_loss_settles_the_parked_request() {
        let directory = tempfile::tempdir().expect("directory");
        let server = Arc::new(tokio::sync::Mutex::new(server(
            directory.path().join("identity.json"),
        )));
        // Hold admission before any credential access or HTTP operation. The
        // real task owns the completion producer while waiting for this lock.
        let held = server.lock().await;
        let mut identities = Identities {
            server: Arc::clone(&server),
            runtime: tokio::runtime::Handle::current(),
            active: None,
            calls: 0,
        };
        let ToolCall::Park(key) = identities
            .call("identity_whoami", &Value::Null)
            .expect("call")
        else {
            panic!("identity operation did not park");
        };
        let parked = BTreeSet::from([key.clone()]);
        let mut settled = Vec::new();
        let mut wake = identities
            .progress(&parked, &mut settled)
            .expect("progress")
            .expect("wake");
        assert!(
            futures::poll!(&mut wake).is_pending(),
            "a retained producer must remain pending"
        );
        assert!(settled.is_empty());
        identities.active.as_ref().expect("active").task.abort();
        wake.await;
        assert!(
            identities
                .progress(&parked, &mut settled)
                .expect("progress after task loss")
                .is_none()
        );
        assert_eq!(settled.len(), 1);
        let (settled_key, outcome) = settled.pop().expect("settled request");
        assert_eq!(settled_key, key);
        assert_eq!(
            outcome.expect_err("typed task loss")["error"],
            "operation_unavailable"
        );
        assert!(identities.active.is_none());
        drop(held);
    }

    #[tokio::test]
    async fn logout_forgets_pending_device_approval() {
        let directory = tempfile::tempdir().expect("directory");
        let mut server = server(directory.path().join("identity.json"));
        let device = serde_json::from_value(json!({"device_code": "device", "user_code": "code",
            "verification_uri": "https://example.test", "expires_in": 60}))
        .expect("device");
        server.pending = Some((device, tokio::time::Instant::now()));
        assert_eq!(
            server.call("identity_logout").await.expect("logout"),
            json!({"authenticated": false})
        );
        assert!(server.pending.is_none());
        assert_eq!(
            server
                .call("identity_login_complete")
                .await
                .expect_err("approval forgotten")
                .code,
            "login_required"
        );
    }

    fn pending(server: &mut Server) {
        let device = serde_json::from_value(json!({"device_code": "device", "user_code": "ABCD",
            "verification_uri": "https://example.test", "expires_in": 60, "interval": 1}))
        .expect("device");
        server.pending = Some((device, tokio::time::Instant::now()));
    }

    #[tokio::test]
    async fn unsigned_login_rejects_concurrent_agent_before_exchanging_device_code() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let router = axum::Router::new().fallback(move || {
            observed.fetch_add(1, Ordering::SeqCst);
            async {
                axum::Json(json!({"device_code": "device", "user_code": "ABCD",
                    "verification_uri": "https://example.test", "expires_in": 60, "interval": 1}))
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve");
        });
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("identity.json");
        let mut server = server(path.clone());
        server.identity = Identity::new(&endpoint).expect("identity");
        server
            .call("identity_login")
            .await
            .expect("unsigned login starts");
        let credentials: Credentials = serde_json::from_value(json!({
            "endpoint": endpoint, "session": null,
            "agent": {"status": "active", "private_key": "new-agent-key", "registration": {
                "user_id": "user", "org_id": "org", "key_id": "key", "account_id": "account",
                "birth": "birth", "certificate": "certificate", "tier": "agent", "expires_at": 900}}
        }))
        .expect("signup credentials");
        {
            let store =
                Store::open(&path).expect("concurrent CLI signup acquires the released lock");
            store.save(&credentials).expect("signup saves agent");
        }
        let before = std::fs::read(&path).expect("saved agent");
        let error = server
            .call("identity_login_complete")
            .await
            .expect_err("identity changed");
        assert_eq!(error.code, "credentials_changed");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "device token must not be consumed"
        );
        assert!(server.pending.is_none());
        assert_eq!(std::fs::read(&path).expect("retained agent"), before);
        task.abort();
    }

    #[tokio::test]
    async fn signup_is_rejected_while_device_approval_is_pending() {
        let directory = tempfile::tempdir().expect("directory");
        let mut server = server(directory.path().join("identity.json"));
        pending(&mut server);
        assert_eq!(
            server
                .call("identity_signup")
                .await
                .expect_err("pending login")
                .code,
            "login_pending"
        );
        assert!(server.pending.is_some());
        assert!(!server.credentials.exists());
    }

    #[tokio::test]
    async fn non_json_gateway_failures_retain_approval_until_success() {
        let _clock = crate::tests::poll_clock::Clock::start();
        use axum::response::IntoResponse as _;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let count = std::sync::Arc::new(AtomicUsize::new(0));
        let calls = count.clone();
        let router = axum::Router::new().fallback(move || {
            let calls = calls.clone();
            async move {
                match calls.fetch_add(1, Ordering::SeqCst) {
                    0 => (StatusCode::SERVICE_UNAVAILABLE, "<h1>Unavailable</h1>").into_response(),
                    1 => (StatusCode::SERVICE_UNAVAILABLE, "").into_response(),
                    _ => (
                        StatusCode::OK,
                        axum::Json(json!({"access_token": "access", "refresh_token": "refresh",
                        "token_type": "Bearer", "expires_in": 900})),
                    )
                        .into_response(),
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve");
        });
        let directory = tempfile::tempdir().expect("directory");
        let mut server = server(directory.path().join("identity.json"));
        server.identity = Identity::new(&endpoint).expect("identity");
        pending(&mut server);
        for _ in 0..2 {
            let error = server
                .call("identity_login_complete")
                .await
                .expect_err("transient");
            assert_eq!(error.code, "invalid_response");
            assert_eq!(error.status, Some(StatusCode::SERVICE_UNAVAILABLE));
            assert!(server.pending.is_some());
        }
        assert_eq!(
            server
                .call("identity_login_complete")
                .await
                .expect("retry")
                .get("authenticated"),
            Some(&json!(true))
        );
        assert!(server.pending.is_none());
        assert_eq!(count.load(Ordering::SeqCst), 3);
        task.abort();
    }

    #[tokio::test]
    async fn slowdown_survives_cancelled_completion_and_retry() {
        let _clock = crate::tests::poll_clock::Clock::start();
        use std::sync::atomic::{AtomicUsize, Ordering};
        let count = std::sync::Arc::new(AtomicUsize::new(0));
        let second_poll = std::sync::Arc::new(tokio::sync::Notify::new());
        let calls = count.clone();
        let notified = second_poll.clone();
        let router = axum::Router::new().fallback(move || {
            let calls = calls.clone();
            let notified = notified.clone();
            async move {
                match calls.fetch_add(1, Ordering::SeqCst) {
                    0 => (
                        StatusCode::BAD_REQUEST,
                        axum::Json(json!({"error": "slow_down"})),
                    ),
                    1 => {
                        notified.notify_one();
                        (
                            StatusCode::BAD_REQUEST,
                            axum::Json(json!({"error": "authorization_pending"})),
                        )
                    }
                    _ => (
                        StatusCode::OK,
                        axum::Json(json!({"access_token": "access",
                        "refresh_token": "refresh", "token_type": "Bearer", "expires_in": 900})),
                    ),
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve");
        });
        let directory = tempfile::tempdir().expect("directory");
        let mut server = server(directory.path().join("identity.json"));
        server.identity = Identity::new(&endpoint).expect("identity");
        pending(&mut server);
        {
            let completion = server.call("identity_login_complete");
            tokio::pin!(completion);
            tokio::select! {
                result = &mut completion => assert!(result.is_err(), "completion must still be polling"),
                () = second_poll.notified() => {}
            }
        }
        assert_eq!(server.pending.as_ref().expect("pending").0.interval, 6);
        let retry = tokio::time::Instant::now();
        assert_eq!(
            server
                .call("identity_login_complete")
                .await
                .expect("retry")
                .get("authenticated"),
            Some(&json!(true))
        );
        assert!(retry.elapsed() >= Duration::from_secs(6));
        assert_eq!(count.load(Ordering::SeqCst), 3);
        task.abort();
    }

    struct ConsumedExchange {
        server: Server,
        path: PathBuf,
        endpoint: String,
        http: tokio::task::JoinHandle<()>,
        consumed: std::sync::Arc<tokio::sync::Notify>,
        released: std::sync::Arc<tokio::sync::Notify>,
        _directory: tempfile::TempDir,
    }

    async fn consumed_exchange(login: bool) -> ConsumedExchange {
        use std::sync::Arc;
        let consumed = Arc::new(tokio::sync::Notify::new());
        let released = Arc::new(tokio::sync::Notify::new());
        let notify = consumed.clone();
        let release = released.clone();
        let router = axum::Router::new().fallback(move || {
            let notify = notify.clone();
            let release = release.clone();
            async move {
                notify.notify_one();
                release.notified().await;
                axum::Json(json!({"access_token": "issued", "refresh_token": "rotated",
                "token_type": "Bearer", "expires_in": 900}))
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        let http = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve");
        });
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("identity.json");
        let mut server = server(path.clone());
        server.identity = Identity::new(&endpoint).expect("identity");
        pending(&mut server);
        if !login {
            Store::open(&path)
                .expect("store")
                .write(
                    &endpoint,
                    Session {
                        access_token: "expired".to_owned(),
                        refresh_token: "original".to_owned(),
                        expires_at: 0,
                    },
                )
                .expect("seed");
        }

        ConsumedExchange {
            server,
            path,
            endpoint,
            http,
            consumed,
            released,
            _directory: directory,
        }
    }

    async fn wait_for_rotated_credentials(path: &Path) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if fs::read(path)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                    .and_then(|value| {
                        value
                            .pointer("/session/refresh_token")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    })
                    .as_deref()
                    == Some("rotated")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("credentials saved while stdout remains pending");
    }

    #[tokio::test]
    async fn interrupted_pending_response_keeps_approval_for_retry() {
        let _clock = crate::tests::poll_clock::Clock::start();
        use tokio::io::AsyncReadExt as _;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        let http = tokio::spawn(async move {
            for interrupted in [true, false] {
                let (socket, _) = listener.accept().await.expect("connection");
                let mut stream = tokio::io::BufReader::new(socket);
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    stream.read_line(&mut line).await.expect("request headers");
                    if line == "\r\n" {
                        break;
                    }
                    if let Some(value) = line.to_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().expect("length");
                    }
                }
                stream
                    .read_exact(&mut vec![0; length])
                    .await
                    .expect("request body");
                let response = if interrupted {
                    "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: 128\r\nConnection: close\r\n\r\n{\"error\":\"authorization_pending\"".to_owned()
                } else {
                    let body = json!({"access_token": "issued", "refresh_token": "rotated", "token_type": "Bearer", "expires_in": 900}).to_string();
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                };
                stream
                    .get_mut()
                    .write_all(response.as_bytes())
                    .await
                    .expect("response");
                stream
                    .get_mut()
                    .shutdown()
                    .await
                    .expect("close truncated body");
            }
        });
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("identity.json");
        let mut server = server(path.clone());
        server.identity = Identity::new(&endpoint).expect("identity");
        pending(&mut server);
        let error = server
            .call("identity_login_complete")
            .await
            .expect_err("download interrupted");
        assert_eq!(error.code, "transport_error");
        assert_eq!(error.status, Some(StatusCode::BAD_REQUEST));
        assert!(server.pending.is_some());
        server
            .call("identity_login_complete")
            .await
            .expect("same approval completes");
        assert!(server.pending.is_none());
        assert_eq!(
            Store::open(&path)
                .expect("store")
                .read(&endpoint)
                .expect("saved")
                .refresh_token,
            "rotated"
        );
        http.await.expect("fake server");
    }

    /// Client end of a served identity transport: requests in, replies out.
    struct Client {
        requests: Option<std::sync::mpsc::Sender<Vec<u8>>>,
        replies: tokio::sync::mpsc::UnboundedReceiver<Value>,
        served: tokio::task::JoinHandle<io::Result<()>>,
    }

    impl Client {
        fn send(&self, request: &Value) {
            if let Some(requests) = &self.requests {
                let _ = requests.send(format!("{request}\n").into_bytes());
            }
        }

        fn send_bytes(&self, bytes: Vec<u8>) {
            if let Some(requests) = &self.requests {
                let _ = requests.send(bytes);
            }
        }

        fn close(&mut self) {
            self.requests = None;
        }

        async fn reply(&mut self) -> Value {
            tokio::time::timeout(Duration::from_secs(3), self.replies.recv())
                .await
                .expect("reply in time")
                .expect("reply")
        }
    }

    /// Blocking input fed by the test, ending when the client closes.
    struct Input {
        requests: std::sync::mpsc::Receiver<Vec<u8>>,
        chunk: Vec<u8>,
        read: usize,
        counted: Option<(usize, Arc<tokio::sync::Notify>)>,
    }

    impl io::Read for Input {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            while self.read == self.chunk.len() {
                match self.requests.recv() {
                    Ok(chunk) => (self.chunk, self.read) = (chunk, 0),
                    Err(_) => return Ok(0),
                }
            }
            let count = buffer.len().min(self.chunk.len() - self.read);
            buffer
                .get_mut(..count)
                .expect("bounded read")
                .copy_from_slice(self.chunk.get(self.read..self.read + count).expect("chunk"));
            self.read += count;
            if let Some((total, failed)) = &mut self.counted {
                // The bounded codec rejects once the delimiter budget fills.
                *total += count;
                if *total >= FRAME_LIMIT {
                    failed.notify_one();
                }
            }
            Ok(count)
        }
    }

    /// How the served output behaves.
    #[derive(Clone, Default)]
    struct Output {
        /// While set, writes block, as a client that stopped reading.
        paused: Arc<(Mutex<bool>, std::sync::Condvar)>,
        stalled: Arc<tokio::sync::Notify>,
        /// While set, writes fail as a closed pipe.
        broken: Arc<std::sync::atomic::AtomicBool>,
        failed: Arc<tokio::sync::Notify>,
    }

    impl Output {
        fn pause(&self, paused: bool) {
            let (state, changed) = &*self.paused;
            *state.lock().expect("pause") = paused;
            changed.notify_all();
        }
    }

    struct Writer {
        output: Output,
        line: Vec<u8>,
        replies: tokio::sync::mpsc::UnboundedSender<Value>,
    }

    impl io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.output.broken.load(std::sync::atomic::Ordering::SeqCst) {
                self.output.failed.notify_one();
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            let (state, changed) = &*self.output.paused;
            let mut paused = state.lock().expect("pause");
            while *paused {
                self.output.stalled.notify_one();
                paused = changed.wait(paused).expect("pause");
            }
            drop(paused);
            self.line.extend_from_slice(bytes);
            while let Some(end) = self.line.iter().position(|byte| *byte == b'\n') {
                let line = self.line.drain(..=end).collect::<Vec<_>>();
                let _ = self
                    .replies
                    .send(serde_json::from_slice(&line).expect("JSON reply"));
            }
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn connect(
        server: Server,
        output: &Output,
        counted: Option<Arc<tokio::sync::Notify>>,
    ) -> Client {
        let (requests, received) = std::sync::mpsc::channel();
        let (sent, replies) = tokio::sync::mpsc::unbounded_channel();
        let input = Input {
            requests: received,
            chunk: Vec::new(),
            read: 0,
            counted: counted.map(|failed| (0, failed)),
        };
        let writer = Writer {
            output: output.clone(),
            line: Vec::new(),
            replies: sent,
        };
        let runtime = tokio::runtime::Handle::current();
        let served = tokio::task::spawn_blocking(move || serve_on(server, runtime, input, writer));
        let client = Client {
            requests: Some(requests),
            replies,
            served,
        };
        client.send(&json!({"jsonrpc": "2.0", "id": 0, "method": "initialize",
            "params": {"protocolVersion": "2025-11-25"}}));
        client
    }

    fn call(id: u64, name: &str) -> Value {
        json!({"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": {"name": name}})
    }

    #[tokio::test]
    async fn identity_tools_list_and_refuse_arguments() {
        let directory = tempfile::tempdir().expect("directory");
        let mut client = connect(
            server(directory.path().join("identity.json")),
            &Output::default(),
            None,
        );
        let initialized = client.reply().await;
        assert_eq!(
            initialized.pointer("/result/protocolVersion"),
            Some(&json!("2025-11-25"))
        );
        client.send(&json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}));
        let listed = client.reply().await;
        assert_eq!(
            listed
                .pointer("/result/tools")
                .and_then(Value::as_array)
                .expect("tools")
                .len(),
            7
        );
        client.send(&json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": "identity_whoami", "arguments": {"token": "untrusted"}}}));
        assert_eq!(
            client.reply().await.pointer("/error/code"),
            Some(&json!(-32602))
        );
        client.close();
        client.served.await.expect("served").expect("transport");
    }

    #[tokio::test]
    async fn login_transport_answers_ping_and_cancellation_and_retains_approval() {
        let directory = tempfile::tempdir().expect("directory");
        let mut server = server(directory.path().join("identity.json"));
        pending(&mut server);
        let mut client = connect(server, &Output::default(), None);
        client.reply().await;
        client.send(&call(1, "identity_login_complete"));
        client.send(&json!({"jsonrpc": "2.0", "id": 2, "method": "ping"}));
        assert_eq!(client.reply().await.get("id"), Some(&json!(2)));
        client.send(
            &json!({"jsonrpc": "2.0", "method": "notifications/cancelled",
            "params": {"requestId": 1}}),
        );
        assert_eq!(
            client.reply().await.pointer("/error/code"),
            Some(&json!(-32800))
        );
        // The cancelled completion holds the operation until it stops polling.
        let mut id = 3;
        loop {
            client.send(&call(id, "identity_login_complete"));
            client.send(&json!({"jsonrpc": "2.0", "id": id + 1, "method": "ping"}));
            let reply = client.reply().await;
            if reply.get("id") == Some(&json!(id + 1)) {
                break;
            }
            assert_eq!(reply.pointer("/error/code"), Some(&json!(-32000)));
            assert_eq!(client.reply().await.get("id"), Some(&json!(id + 1)));
            id += 2;
        }
        client.close();
        client.served.await.expect("served").expect("transport");
    }

    #[tokio::test]
    async fn blocked_output_still_reads_login_cancellation() {
        let _clock = crate::tests::poll_clock::Clock::start();
        let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = Arc::clone(&polls);
        let router = axum::Router::new().fallback(move || {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async {
                (
                    StatusCode::BAD_REQUEST,
                    axum::Json(json!({"error": "authorization_pending"})),
                )
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        let http = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve");
        });
        let directory = tempfile::tempdir().expect("directory");
        let mut server = server(directory.path().join("identity.json"));
        server.identity = Identity::new(&endpoint).expect("identity");
        pending(&mut server);
        let output = Output::default();
        let mut client = connect(server, &output, None);
        client.reply().await;
        output.pause(true);
        client.send(&call(1, "identity_login_complete"));
        client.send(&json!({"jsonrpc": "2.0", "id": 2, "method": "ping"}));
        tokio::time::timeout(Duration::from_secs(3), output.stalled.notified())
            .await
            .expect("writer pending");
        client.send(
            &json!({"jsonrpc": "2.0", "method": "notifications/cancelled",
            "params": {"requestId": 1}}),
        );
        // The poll stops although no reply can be written: the count settles.
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let mut last = (
            polls.load(std::sync::atomic::Ordering::SeqCst),
            std::time::Instant::now(),
        );
        loop {
            tokio::task::yield_now().await;
            let count = polls.load(std::sync::atomic::Ordering::SeqCst);
            if count != last.0 {
                last = (count, std::time::Instant::now());
            } else if last.1.elapsed() >= Duration::from_millis(300) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "login polling continued after cancellation behind blocked output"
            );
        }
        client.close();
        output.pause(false);
        let mut replies = Vec::new();
        while let Some(reply) = client.replies.recv().await {
            replies.push(reply);
        }
        client.served.await.expect("served").expect("transport");
        assert!(
            replies
                .iter()
                .any(|reply| reply.get("id") == Some(&json!(2)))
        );
        assert!(
            replies
                .iter()
                .any(|reply| reply.get("id") == Some(&json!(1))
                    && reply.pointer("/error/code") == Some(&json!(-32800)))
        );
        http.abort();
    }

    #[tokio::test]
    async fn blocked_output_keeps_consumed_device_and_refresh_exchanges_progressing() {
        let _clock = crate::tests::poll_clock::Clock::start();
        for (login, eof) in [(false, false), (true, false), (false, true)] {
            let ConsumedExchange {
                server,
                path,
                endpoint: _endpoint,
                http,
                consumed,
                released,
                _directory,
            } = consumed_exchange(login).await;
            let output = Output::default();
            let mut client = connect(server, &output, None);
            client.reply().await;
            output.pause(true);
            client.send(&call(
                1,
                if login {
                    "identity_login_complete"
                } else {
                    "identity_refresh"
                },
            ));
            tokio::time::timeout(Duration::from_secs(3), consumed.notified())
                .await
                .expect("consumed");
            client.send(&json!({"jsonrpc": "2.0", "id": 2, "method": "ping"}));
            tokio::time::timeout(Duration::from_secs(3), output.stalled.notified())
                .await
                .expect("writer pending");
            if eof {
                client.close();
            }
            released.notify_one();
            wait_for_rotated_credentials(&path).await;
            assert!(!client.served.is_finished());
            client.close();
            output.pause(false);
            let mut ids = Vec::new();
            while let Some(reply) = client.replies.recv().await {
                ids.extend(reply.get("id").cloned());
            }
            tokio::time::timeout(Duration::from_secs(3), client.served)
                .await
                .expect("drained after persistence")
                .expect("served")
                .expect("transport");
            assert!(ids.contains(&json!(2)));
            if !eof {
                assert!(ids.contains(&json!(1)));
            }
            http.abort();
        }
    }

    #[tokio::test]
    async fn transport_errors_drain_consumed_device_and_refresh_exchanges() {
        let _clock = crate::tests::poll_clock::Clock::start();
        for login in [false, true] {
            for fault in ["oversized", "ping", "cancel"] {
                let ConsumedExchange {
                    server,
                    path,
                    endpoint,
                    http,
                    consumed,
                    released,
                    _directory,
                } = consumed_exchange(login).await;
                let output = Output::default();
                let counted = Arc::new(tokio::sync::Notify::new());
                let mut client = connect(server, &output, Some(Arc::clone(&counted)));
                client.reply().await;
                let name = if login {
                    "identity_login_complete"
                } else {
                    "identity_refresh"
                };
                client.send(&call(1, name));
                tokio::time::timeout(Duration::from_secs(3), consumed.notified())
                    .await
                    .expect("consumed");
                let failed = if fault == "oversized" {
                    client.send_bytes(vec![b'x'; FRAME_LIMIT + 1]);
                    counted
                } else {
                    output
                        .broken
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    client.send(&if fault == "ping" {
                        json!({"jsonrpc": "2.0", "id": 2, "method": "ping"})
                    } else {
                        json!({"jsonrpc": "2.0", "method": "notifications/cancelled",
                            "params": {"requestId": 1}})
                    });
                    Arc::clone(&output.failed)
                };
                tokio::time::timeout(Duration::from_secs(3), failed.notified())
                    .await
                    .expect("transport fault reached");
                assert!(
                    !client.served.is_finished(),
                    "{name}/{fault} must drain before returning the transport error"
                );
                released.notify_one();
                tokio::time::timeout(Duration::from_secs(3), client.served)
                    .await
                    .expect("drained")
                    .expect("served")
                    .expect_err("transport error");
                assert_eq!(
                    Store::open(&path)
                        .expect("unlocked store")
                        .read(&endpoint)
                        .expect("saved")
                        .refresh_token,
                    "rotated"
                );
                http.abort();
            }
        }
    }
}

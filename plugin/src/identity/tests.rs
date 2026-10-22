#![allow(
    clippy::expect_used,
    reason = "fake-server setup must fail the test on invalid fixtures"
)]

use super::*;
use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::response::Response;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

struct Expected {
    method: &'static str,
    path: &'static str,
    bearer: Option<&'static str>,
    fields: Vec<(&'static str, &'static str)>,
    status: StatusCode,
    response: Value,
}

impl Expected {
    fn revoke(bearer: &'static str) -> Self {
        Self {
            method: "POST",
            path: "/v1/identity/revoke",
            bearer: Some(bearer),
            fields: vec![],
            status: StatusCode::NO_CONTENT,
            response: Value::Null,
        }
    }

    fn token(fields: Vec<(&'static str, &'static str)>, response: Value) -> Self {
        Self {
            method: "POST",
            path: "/v1/identity/token",
            bearer: None,
            fields,
            status: StatusCode::OK,
            response,
        }
    }

    fn get(path: &'static str, bearer: &'static str, response: Value) -> Self {
        Self {
            method: "GET",
            path,
            bearer: Some(bearer),
            fields: vec![],
            status: StatusCode::OK,
            response,
        }
    }
}

type ExpectedRequests = Arc<Mutex<VecDeque<Expected>>>;

struct Fake {
    identity: Identity,
    expected: ExpectedRequests,
    requests: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Fake {
    async fn start(expected: Vec<Expected>) -> Self {
        let expected = Arc::new(Mutex::new(VecDeque::from(expected)));
        let requests = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake");
        let endpoint = format!("http://{}", listener.local_addr().expect("fake address"));
        let router = Router::new()
            .fallback(handle)
            .with_state((expected.clone(), requests.clone()));
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve fake");
        });
        Self {
            identity: Identity::new(&endpoint).expect("identity"),
            expected,
            requests,
            task,
        }
    }

    fn done(&self) {
        assert!(self.expected.lock().expect("expected lock").is_empty());
    }
}

impl Drop for Fake {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle(
    State((expected, requests)): State<(ExpectedRequests, Arc<AtomicUsize>)>,
    request: Request,
) -> Response {
    requests.fetch_add(1, Ordering::Relaxed);
    let expected = expected
        .lock()
        .expect("expected lock")
        .pop_front()
        .expect("unexpected request");
    assert_eq!(request.method().as_str(), expected.method);
    assert_eq!(request.uri().path(), expected.path);
    let bearer = request
        .headers()
        .get("authorization")
        .map(|header| header.to_str().expect("header"));
    assert_eq!(
        bearer,
        expected
            .bearer
            .map(|bearer| format!("Bearer {bearer}"))
            .as_deref()
    );
    if expected.method == "POST" || !expected.fields.is_empty() {
        assert_eq!(
            request.headers().get("content-type").expect("content type"),
            "application/json"
        );
        let bytes = axum::body::to_bytes(request.into_body(), 4096)
            .await
            .expect("body");
        let mut fields: std::collections::BTreeMap<String, String> =
            serde_json::from_slice(&bytes).expect("JSON request");
        for (key, value) in &expected.fields {
            if *value == "*" {
                let actual = fields.get(*key).expect("PKCE field");
                assert_eq!(actual.len(), 43);
                assert!(
                    actual
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
                );
                fields.insert(key.to_string(), "*".to_owned());
            }
        }
        let wanted = expected
            .fields
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(fields, wanted);
    }
    Response::builder()
        .status(expected.status)
        .header("content-type", "application/json")
        .body(Body::from(expected.response.to_string()))
        .expect("response")
}

fn tokens(access: &str, refresh: &str) -> Value {
    json!({"access_token": access, "refresh_token": refresh, "token_type": "Bearer", "expires_in": 900})
}

fn refresh_fields(token: &'static str) -> Vec<(&'static str, &'static str)> {
    vec![("grant_type", "refresh_token"), ("refresh_token", token)]
}

fn session(access: &str, refresh: &str, expires_at: u64) -> Session {
    Session {
        access_token: access.to_owned(),
        refresh_token: refresh.to_owned(),
        expires_at,
    }
}

fn organization_bearer(response: Value) -> Expected {
    let mut expected = Expected::get("/v1/identity/orgs/org-1/bearer", "access", response);
    expected.method = "POST";
    expected
}

#[tokio::test]
async fn bearer_is_private_redacted_and_never_overwrites_an_existing_file() {
    let expires = now().expect("time") + 900;
    let fake = Fake::start(vec![
        organization_bearer(json!({"bearer": "private.token.signature", "account_id": "account-1", "expires_at": expires})),
    ]).await;
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("identity.json")).expect("store");
    store
        .write(
            &fake.identity.endpoint,
            session("access", "refresh", expires),
        )
        .expect("session");
    let before = fs::read(&store.path).expect("credentials");
    let output = directory.path().join("bearer.txt");
    let result = fake
        .identity
        .bearer(&store, "org-1", &output)
        .await
        .expect("bearer");
    assert_eq!(result.get("account_id"), Some(&json!("account-1")));
    assert!(!result.to_string().contains("private.token.signature"));
    assert_eq!(
        fs::read_to_string(&output).expect("output"),
        "private.token.signature"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(
            fs::metadata(&output)
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    assert!(
        fake.identity
            .bearer(&store, "org-1", &output)
            .await
            .is_err()
    );
    assert_eq!(
        fs::read_to_string(&output).expect("unchanged"),
        "private.token.signature"
    );
    assert_eq!(
        fs::read(&store.path).expect("unchanged credentials"),
        before
    );
    assert_eq!(fake.requests.load(Ordering::Relaxed), 1);
    fake.done();
}

#[tokio::test]
async fn bearer_cancellation_removes_the_reserved_output() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind unanswered request");
    let identity = Identity::new(&format!(
        "http://{}",
        listener.local_addr().expect("address")
    ))
    .expect("identity");
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("identity.json")).expect("store");
    store
        .write(
            &identity.endpoint,
            session("access", "refresh", now().expect("time") + 900),
        )
        .expect("session");
    let output = directory.path().join("bearer");
    {
        let operation = identity.bearer(&store, "org-1", &output);
        tokio::pin!(operation);
        let completed = tokio::select! {
            biased;
            _ = &mut operation => true,
            () = std::future::ready(()) => false,
        };
        assert!(!completed, "unanswered request completed");
        assert!(output.exists(), "destination reserved before the request");
    }
    assert!(!output.exists(), "cancellation releases the destination");
    let expires = now().expect("time") + 900;
    let router = Router::new().fallback(move || async move {
        axum::Json(json!({"bearer": "private.token.signature", "account_id": "account-1", "expires_at": expires}))
    });
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });
    identity
        .bearer(&store, "org-1", &output)
        .await
        .expect("retry");
    task.abort();
}

#[tokio::test]
async fn bearer_cleanup_failure_preserves_the_original_response_error() {
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("identity.json")).expect("store");
    let output = directory.path().join("bearer");
    let replaced = output.clone();
    let router = Router::new().fallback(move || {
        let replaced = replaced.clone();
        async move {
            fs::remove_file(&replaced).expect("remove reservation");
            fs::create_dir(&replaced).expect("make cleanup fail");
            (
                StatusCode::BAD_GATEWAY,
                axum::Json(json!({"error": "upstream_unavailable"})),
            )
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let identity = Identity::new(&format!(
        "http://{}",
        listener.local_addr().expect("address")
    ))
    .expect("identity");
    store
        .write(
            &identity.endpoint,
            session("access", "refresh", now().expect("time") + 900),
        )
        .expect("session");
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });
    let error = identity
        .bearer(&store, "org-1", &output)
        .await
        .expect_err("upstream error");
    task.abort();
    assert_eq!(error.code, "upstream_unavailable");
    assert_eq!(error.status, Some(StatusCode::BAD_GATEWAY));
    assert!(
        output.is_dir(),
        "failed cleanup leaves the replacement alone"
    );
}

#[tokio::test]
async fn bearer_refuses_unusable_output_before_consuming_an_expired_session() {
    let fake = Fake::start(vec![]).await;
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("identity.json")).expect("store");
    store
        .write(&fake.identity.endpoint, session("access", "refresh", 0))
        .expect("session");
    let before = fs::read(&store.path).expect("credentials");
    let occupied = directory.path().join("occupied");
    fs::write(&occupied, "existing output").expect("output");
    for output in [
        occupied.clone(),
        directory.path().to_path_buf(),
        directory.path().join("missing/bearer"),
    ] {
        assert!(
            fake.identity
                .bearer(&store, "org-1", &output)
                .await
                .is_err()
        );
        assert_eq!(
            fs::read(&store.path).expect("unchanged credentials"),
            before
        );
    }
    assert_eq!(
        fs::read_to_string(occupied).expect("unchanged output"),
        "existing output"
    );
    assert_eq!(fake.requests.load(Ordering::Relaxed), 0);
    fake.done();
}

#[tokio::test]
async fn bearer_refreshes_expiry_and_401_without_changing_the_post() {
    for expired in [true, false] {
        let time = now().expect("time");
        let mut expected = Vec::new();
        if !expired {
            let mut refused = organization_bearer(json!({"error": "invalid_token"}));
            refused.status = StatusCode::UNAUTHORIZED;
            expected.push(refused);
        }
        expected.push(Expected::token(
            refresh_fields("old-refresh"),
            tokens("new-access", "new-refresh"),
        ));
        let mut minted = organization_bearer(
            json!({"bearer": "private.token.signature", "account_id": "account-1", "expires_at": time + 900}),
        );
        minted.bearer = Some("new-access");
        expected.push(minted);
        let fake = Fake::start(expected).await;
        let directory = tempfile::tempdir().expect("directory");
        let store = Store::open(&directory.path().join("identity.json")).expect("store");
        store
            .write(
                &fake.identity.endpoint,
                session(
                    "access",
                    "old-refresh",
                    if expired { 0 } else { time + 900 },
                ),
            )
            .expect("session");
        let output = directory.path().join("bearer");
        let result = fake
            .identity
            .bearer(&store, "org-1", &output)
            .await
            .expect("mint after rotation");
        assert_eq!(result.get("account_id"), Some(&json!("account-1")));
        assert_eq!(
            fs::read_to_string(output).expect("bearer"),
            "private.token.signature"
        );
        let saved = store.read(&fake.identity.endpoint).expect("saved rotation");
        assert_eq!(saved.access_token, "new-access");
        assert_eq!(saved.refresh_token, "new-refresh");
        assert!(saved.expires_at > time);
        assert_eq!(
            fake.requests.load(Ordering::Relaxed),
            if expired { 2 } else { 3 }
        );
        fake.done();
    }
}

#[tokio::test]
async fn bearer_rejects_incomplete_or_unbounded_receipts_without_creating_output() {
    let time = now().expect("time");
    let fake = Fake::start(vec![
        organization_bearer(json!({"account_id": "account-1", "expires_at": time + 900})),
        organization_bearer(json!({"bearer": "private.token.signature", "account_id": "", "expires_at": time + 900})),
        organization_bearer(json!({"bearer": "private.token.signature", "account_id": "account-1", "expires_at": 0})),
        organization_bearer(json!({"bearer": "private.token.signature", "account_id": "account-1", "expires_at": time + 7200})),
    ]).await;
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("identity.json")).expect("store");
    store
        .write(
            &fake.identity.endpoint,
            session("access", "refresh", time + 900),
        )
        .expect("session");
    let output = directory.path().join("bearer.txt");
    for _ in 0..4 {
        let error = fake
            .identity
            .bearer(&store, "org-1", &output)
            .await
            .expect_err("invalid receipt");
        assert_eq!(error.code, "invalid_response");
        assert!(!output.exists());
    }
    fake.done();
}

#[tokio::test]
async fn bearer_refuses_sidecar_paths_before_contacting_global() {
    let fake = Fake::start(vec![]).await;
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("identity.json")).expect("store");
    for suffix in [".lock", ".next", ".LOCK", ".NEXT"] {
        let error = fake
            .identity
            .bearer(&store, "org-1", &sidecar(&store.path, suffix))
            .await
            .expect_err("reserved path");
        assert_eq!(error.code, "usage_error");
    }
    fake.done();
}

#[tokio::test]
async fn device_login_pending_slowdown_then_approval() {
    let _clock = poll_clock::Clock::start();
    let fields = vec![
        ("grant_type", DEVICE_GRANT),
        ("device_code", "device-secret"),
        ("code_verifier", "*"),
    ];
    let mut pending = Expected::token(fields.clone(), json!({"error": "authorization_pending"}));
    pending.status = StatusCode::BAD_REQUEST;
    let mut slowdown = Expected::token(fields.clone(), json!({"error": "slow_down"}));
    slowdown.status = StatusCode::BAD_REQUEST;
    let fake = Fake::start(vec![
        Expected { method: "POST", path: "/v1/identity/device/code", bearer: None,
            fields: vec![("client_kind", "cli"), ("code_challenge", "*")], status: StatusCode::OK,
            response: json!({"device_code": "device-secret", "user_code": "ABCD",
                "verification_uri": "https://example.test/device", "expires_in": 300, "interval": 1}) },
        pending, slowdown, Expected::token(fields, tokens("access-1", "refresh-1")),
    ]).await;
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("identity.json")).expect("store");
    let mut device = fake
        .identity
        .device_code("cli", &store)
        .await
        .expect("device code");
    let start = tokio::time::Instant::now();
    let session = fake
        .identity
        .login(&mut device, start, std::future::pending())
        .await
        .expect("login");
    assert!(start.elapsed() >= Duration::from_secs(8));
    assert_eq!(session.access_token, "access-1");
    fake.done();
}

#[tokio::test]
async fn device_denial_and_local_expiry_are_terminal() {
    let _clock = poll_clock::Clock::start();
    let mut denied = Expected::token(
        vec![
            ("grant_type", DEVICE_GRANT),
            ("device_code", "secret"),
            ("code_verifier", "test-verifier"),
        ],
        json!({"error": "access_denied"}),
    );
    denied.status = StatusCode::BAD_REQUEST;
    let fake = Fake::start(vec![denied]).await;
    let mut device = DeviceCode {
        device_code: "secret".to_owned(),
        user_code: "code".to_owned(),
        verification_uri: "https://example.test/device".to_owned(),
        verification_uri_complete: None,
        expires_in: 60,
        interval: 1,
        verifier: "test-verifier".to_owned(),
        upgrade: None,
        agent_identity: None,
        reconcile_upgrade: false,
        upgrade_tier: None,
    };
    assert_eq!(
        fake.identity
            .login(
                &mut device,
                tokio::time::Instant::now(),
                std::future::pending()
            )
            .await
            .err()
            .expect("denied")
            .code,
        "access_denied"
    );
    device.expires_in = 1;
    assert_eq!(
        fake.identity
            .login(
                &mut device,
                tokio::time::Instant::now(),
                std::future::pending()
            )
            .await
            .err()
            .expect("expired")
            .code,
        "expired_token"
    );
    fake.done();
}

#[tokio::test]
async fn expired_session_rotates_before_whoami_and_orgs_reuses_new_access() {
    let fake = Fake::start(vec![
        Expected::token(refresh_fields("refresh-1"), tokens("access-2", "refresh-2")),
        Expected::get("/v1/identity/session", "access-2", json!({"id": "user-1"})),
        Expected::get(
            "/v1/identity/orgs",
            "access-2",
            json!({"orgs": [{"id": "org-1"}]}),
        ),
    ])
    .await;
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("identity.json")).expect("store");
    store
        .write(&fake.identity.endpoint, session("access-1", "refresh-1", 0))
        .expect("write");
    assert_eq!(
        fake.identity
            .get(&store, "/v1/identity/session")
            .await
            .expect("whoami"),
        json!({"id": "user-1"})
    );
    assert_eq!(
        store
            .read(&fake.identity.endpoint)
            .expect("saved")
            .refresh_token,
        "refresh-2"
    );
    assert_eq!(
        fake.identity
            .get(&store, "/v1/identity/orgs")
            .await
            .expect("orgs"),
        json!({"orgs": [{"id": "org-1"}]})
    );
    fake.done();
}

#[tokio::test]
async fn unauthorized_access_refreshes_once() {
    let mut unauthorized = Expected::get(
        "/v1/identity/session",
        "access-1",
        json!({"error": "invalid_token"}),
    );
    unauthorized.status = StatusCode::UNAUTHORIZED;
    let fake = Fake::start(vec![
        unauthorized,
        Expected::token(refresh_fields("refresh-1"), tokens("access-2", "refresh-2")),
        Expected::get("/v1/identity/session", "access-2", json!({"id": "user-1"})),
    ])
    .await;
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("identity.json")).expect("store");
    store
        .write(
            &fake.identity.endpoint,
            session("access-1", "refresh-1", now().expect("time") + 900),
        )
        .expect("write");
    fake.identity
        .get(&store, "/v1/identity/session")
        .await
        .expect("whoami");
    fake.done();
}

#[tokio::test]
async fn refused_refresh_clears_family_and_redacts_server_description() {
    let mut refused = Expected::token(
        refresh_fields("secret"),
        json!({"error": "invalid_grant", "error_description": "secret"}),
    );
    refused.status = StatusCode::BAD_REQUEST;
    let fake = Fake::start(vec![refused]).await;
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("identity.json")).expect("store");
    let session = store
        .write(&fake.identity.endpoint, session("access", "secret", 0))
        .expect("write");
    let error = fake
        .identity
        .refresh(&store, &session)
        .await
        .map(|_| ())
        .expect_err("refused");
    assert!(!error.json().to_string().contains("secret"));
    assert_eq!(
        store
            .read(&fake.identity.endpoint)
            .err()
            .expect("cleared")
            .code,
        "login_required"
    );
    fake.done();
}

#[test]
fn credentials_are_exclusive_origin_bound_and_private() {
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("identity.json");
    let store = Store::open(&path).expect("store");
    assert_eq!(
        Store::open(&path).err().expect("busy").code,
        "credentials_busy"
    );
    store
        .write("https://example.test", session("access", "refresh", 900))
        .expect("write");
    assert_eq!(
        store.read("https://other.test").err().expect("bound").code,
        "endpoint_mismatch"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(
            fs::metadata(&path).expect("metadata").permissions().mode() & 0o777,
            0o600
        );
    }
    drop(store);
    Store::open(&path).expect("unlocked");
}

#[cfg(target_os = "linux")]
#[test]
#[allow(
    unsafe_code,
    reason = "the fork-inheritance control needs CLOEXEC pipes and an async-signal-safe pre_exec barrier"
)]
fn dropping_store_releases_lock_while_a_forked_child_waits_to_exec() {
    use std::io::Read as _;
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    use std::os::unix::process::CommandExt as _;
    use std::process::{Child, Command, ExitStatus};
    use std::time::Instant;

    fn pipe() -> io::Result<(File, File)> {
        let mut descriptors = [-1; 2];
        // SAFETY: descriptors provides two writable descriptor slots. CLOEXEC
        // keeps the fixture pipes out of the executed program.
        if unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful pipe2 returned two distinct owned descriptors;
        // neither is owned elsewhere before these Files take ownership.
        Ok(unsafe {
            (
                File::from_raw_fd(descriptors[0]),
                File::from_raw_fd(descriptors[1]),
            )
        })
    }

    fn await_child(reader: &mut File) -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "pre_exec barrier"));
            }
            let mut descriptor = libc::pollfd {
                fd: reader.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: descriptor is valid for one pollfd and reader owns its
            // descriptor throughout this bounded readiness wait.
            let ready = unsafe {
                libc::poll(
                    &mut descriptor,
                    1,
                    remaining.as_millis().max(1) as libc::c_int,
                )
            };
            if ready < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if ready == 0 {
                continue;
            }
            let mut byte = [0];
            reader.read_exact(&mut byte)?;
            return if byte == [1] {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "pre_exec signal",
                ))
            };
        }
    }

    struct ForkControl {
        ready: File,
        release: File,
        spawning: Option<std::thread::JoinHandle<io::Result<Child>>>,
    }

    impl ForkControl {
        fn finish(&mut self) -> Result<ExitStatus, String> {
            // Keep both parent endpoints alive until spawn has returned:
            // pre_exec closes their raw descriptor numbers in the child.
            // This also works if unwinding starts before the child has forked.
            let released = self.release.write_all(&[1]);
            let reaped = self.join_and_reap();
            let status = reaped?;
            released.map_err(|error| format!("release child to exec: {error}"))?;
            Ok(status)
        }

        fn join_and_reap(&mut self) -> Result<ExitStatus, String> {
            let spawning = self
                .spawning
                .take()
                .ok_or_else(|| "barrier controller already joined".to_owned())?;
            let mut child = spawning
                .join()
                .map_err(|_| "barrier controller panicked".to_owned())?
                .map_err(|error| format!("exec true: {error}"))?;
            match child.wait() {
                Ok(status) => Ok(status),
                Err(error) => {
                    let killed = child.kill();
                    let reaped = child.wait();
                    Err(format!(
                        "wait true: {error}; cleanup kill: {killed:?}; cleanup wait: {reaped:?}"
                    ))
                }
            }
        }
    }

    impl Drop for ForkControl {
        fn drop(&mut self) {
            if self.spawning.is_some() {
                if let Err(error) = self.finish() {
                    // Cleanup cannot panic during an existing unwind. Normal
                    // completion reports errors through finish's Result.
                    let _ = writeln!(io::stderr(), "fork control cleanup failed: {error}");
                }
            }
        }
    }

    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("identity.json");
    let store = Store::open(&path).expect("store");
    let (ready, child_ready) = pipe().expect("ready pipe");
    let (child_release, release) = pipe().expect("release pipe");
    let mut control = ForkControl {
        ready,
        release,
        spawning: None,
    };
    let unused_ready_fd = control.ready.as_raw_fd();
    let unused_release_fd = control.release.as_raw_fd();
    // Command::spawn waits for exec, so it cannot run on the barrier controller.
    control.spawning = Some(
        std::thread::Builder::new()
            .name("identity-store-fork-control".to_owned())
            .spawn(move || {
                let ready_fd = child_ready.as_raw_fd();
                let release_fd = child_release.as_raw_fd();
                let mut command = Command::new("/bin/true");
                // SAFETY: both pipe ends remain owned until spawn returns. The
                // child only uses stack bytes and async-signal-safe libc calls:
                // no allocation, Rust locks, destructors, or runtime interaction.
                unsafe {
                    command.pre_exec(move || {
                        // CLOEXEC does not act until exec. Close the unused ends
                        // now so parent death gives this reader EOF rather than
                        // leaving the child holding its own release writer.
                        if libc::close(unused_ready_fd) != 0 || libc::close(unused_release_fd) != 0
                        {
                            libc::_exit(124);
                        }
                        let mut byte = 1_u8;
                        loop {
                            let count = libc::write(ready_fd, (&raw const byte).cast(), 1);
                            if count == 1 {
                                break;
                            }
                            if count >= 0 || *libc::__errno_location() != libc::EINTR {
                                libc::_exit(125);
                            }
                        }
                        loop {
                            let count = libc::read(release_fd, (&raw mut byte).cast(), 1);
                            if count == 1 && byte == 1 {
                                return Ok(());
                            }
                            if count >= 0 || *libc::__errno_location() != libc::EINTR {
                                libc::_exit(126);
                            }
                        }
                    });
                }
                let child = command.spawn();
                drop(child_ready);
                drop(child_release);
                child
            })
            .expect("spawn barrier controller"),
    );

    // Record assertion failures while blocked; the guard also releases, joins,
    // and reaps if an unexpected panic unwinds through either Store operation.
    let reached_barrier = await_child(&mut control.ready);
    let live_owner_error = if reached_barrier.is_ok() {
        Store::open(&path).err()
    } else {
        None
    };
    drop(store);
    let dropped_owner_error = if reached_barrier.is_ok() {
        Store::open(&path).err()
    } else {
        None
    };
    let status = control.finish().expect("release, join, and reap child");
    reached_barrier.expect("child reached pre_exec without executing");
    drop(control);
    assert!(status.success(), "true status: {status}");
    assert_eq!(
        live_owner_error
            .expect("live Store must exclude another owner")
            .code,
        "credentials_busy"
    );
    Store::open(&path).expect("exec closed the child's inherited lock descriptor");
    assert!(
        dropped_owner_error.is_none(),
        "dropping Store must release its lock before the child execs: {dropped_owner_error:?}"
    );
}

#[tokio::test]
async fn identity_read_errors_preserve_code_and_redact_description() {
    let mut refused = Expected::get(
        "/v1/identity/orgs",
        "access",
        json!({
            "error": "permission_denied", "error_description": "refresh-secret"
        }),
    );
    refused.status = StatusCode::FORBIDDEN;
    let fake = Fake::start(vec![refused]).await;
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("identity.json")).expect("store");
    store
        .write(
            &fake.identity.endpoint,
            session("access", "refresh-secret", now().expect("time") + 900),
        )
        .expect("write");
    let error = fake
        .identity
        .get(&store, "/v1/identity/orgs")
        .await
        .expect_err("refused");
    assert_eq!(error.code, "permission_denied");
    assert!(!error.json().to_string().contains("refresh-secret"));
    fake.done();
}

#[test]
fn endpoint_rejects_credentials_paths_and_remote_cleartext() {
    for endpoint in [
        "http://example.test",
        "https://user:password@example.test",
        "https://example.test/path",
        "https://example.test?token=secret",
        "https://example.test#fragment",
    ] {
        assert!(Identity::new(endpoint).is_err(), "{endpoint}");
    }
    assert!(Identity::new("http://127.0.0.1:1234").is_ok());
    assert!(Identity::new("https://example.test/").is_ok());
    assert_eq!(
        Identity::new("https://EXAMPLE.test:443/")
            .expect("canonical origin")
            .endpoint,
        "https://example.test"
    );
}

#[test]
fn malformed_tokens_never_become_credentials() {
    for value in [
        tokens("", "refresh"),
        tokens("access", ""),
        json!({"access_token": "access", "refresh_token": "refresh", "token_type": "unknown", "expires_in": 1}),
    ] {
        assert!(
            serde_json::from_value::<Tokens>(value)
                .expect("tokens")
                .session()
                .is_err()
        );
    }
}

#[tokio::test]
async fn failed_rotation_persistence_clears_old_refresh_token() {
    let fake = Fake::start(vec![Expected::token(
        refresh_fields("refresh-1"),
        tokens("access-2", "refresh-2"),
    )])
    .await;
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("identity.json");
    let store = Store::open(&path).expect("store");
    let previous = store
        .write(&fake.identity.endpoint, session("access-1", "refresh-1", 0))
        .expect("write");
    fs::create_dir(sidecar(&path, ".next")).expect("block temporary file");
    assert!(fake.identity.refresh(&store, &previous).await.is_err());
    assert_eq!(
        store
            .read(&fake.identity.endpoint)
            .err()
            .expect("cleared")
            .code,
        "login_required"
    );
    fake.done();
}

#[tokio::test]
async fn newly_refreshed_access_is_not_refreshed_again_on_unauthorized() {
    let mut unauthorized = Expected::get(
        "/v1/identity/session",
        "access-2",
        json!({"error": "invalid_token"}),
    );
    unauthorized.status = StatusCode::UNAUTHORIZED;
    let fake = Fake::start(vec![
        Expected::token(refresh_fields("refresh-1"), tokens("access-2", "refresh-2")),
        unauthorized,
    ])
    .await;
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("identity.json")).expect("store");
    store
        .write(&fake.identity.endpoint, session("access-1", "refresh-1", 0))
        .expect("write");
    assert_eq!(
        fake.identity
            .get(&store, "/v1/identity/session")
            .await
            .expect_err("refused")
            .code,
        "invalid_token"
    );
    fake.done();
}

#[test]
fn sidecar_names_cannot_be_opened_as_independent_credential_stores() {
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("session");
    let store = Store::open(&path).expect("store");
    for name in [
        "session.lock",
        "session.next",
        "session.LOCK",
        "session.NeXt",
    ] {
        let sidecar = directory.path().join(name);
        assert_eq!(
            Store::open(&sidecar).err().expect("reserved sidecar").code,
            "configuration_error"
        );
        assert!(!super::sidecar(&sidecar, ".lock").exists());
    }
    store
        .write("https://example.test", session("access", "refresh", 900))
        .expect("write");
    assert_eq!(
        Store::open(&path).err().expect("same lock").code,
        "credentials_busy"
    );
    assert_eq!(
        store
            .read("https://example.test")
            .expect("saved")
            .refresh_token,
        "refresh"
    );
}

#[cfg(unix)]
#[test]
fn credential_aliases_cannot_bypass_the_shared_lock_or_consumption() {
    use std::os::unix::fs::symlink;
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("identity.json");
    let store = Store::open(&path).expect("store");
    store
        .write("https://example.test", session("access", "refresh", 900))
        .expect("write");
    let alias = directory.path().join("alias.json");
    symlink(&path, &alias).expect("symlink");
    assert_eq!(
        Store::open(&alias).err().expect("reject alias").code,
        "credential_error"
    );
    assert_eq!(
        store
            .read("https://example.test")
            .expect("unchanged")
            .refresh_token,
        "refresh"
    );
    let parent_alias = directory.path().join("parent-alias");
    symlink(directory.path(), &parent_alias).expect("parent alias");
    assert_eq!(
        Store::open(&parent_alias.join("identity.json"))
            .err()
            .expect("same lock")
            .code,
        "credentials_busy"
    );
    drop(store);
    let hardlink = directory.path().join("hardlink.json");
    fs::hard_link(&path, &hardlink).expect("hard link");
    assert_eq!(
        Store::open(&hardlink).err().expect("reject hard link").code,
        "credential_error"
    );
    assert_eq!(
        Store::open(&path)
            .err()
            .expect("reject linked original")
            .code,
        "credential_error"
    );
}

fn agent_credentials(endpoint: &str) -> Credentials {
    serde_json::from_value(json!({"endpoint": endpoint,
        "session": {"access_token": "access", "refresh_token": "old-refresh", "expires_at": 0},
        "agent": {"status": "active", "private_key": "retained-key", "registration": {
            "user_id": "user", "org_id": "org", "key_id": "key", "account_id": "account",
            "birth": "birth", "certificate": "certificate", "tier": "agent", "expires_at": 900}}
    }))
    .expect("agent credentials")
}

#[tokio::test]
async fn logout_revokes_the_session_and_preserves_agent_keys() {
    for retain_agent in [false, true] {
        let fake = Fake::start(vec![Expected::revoke("access")]).await;
        let directory = tempfile::tempdir().expect("directory");
        let store = Store::open(&directory.path().join("identity.json")).expect("store");
        let mut credentials = agent_credentials(&fake.identity.endpoint);
        credentials.session = Some(session("access", "refresh", now().expect("time") + 900));
        if !retain_agent {
            credentials.agent = None;
        }
        store.save(&credentials).expect("credentials");
        assert_eq!(
            fake.identity.logout(&store).await.expect("logout"),
            json!({"authenticated": false})
        );
        if retain_agent {
            let saved = store.credentials(&fake.identity.endpoint).expect("agent");
            assert!(saved.session.is_none());
            assert_eq!(
                serde_json::to_value(saved.agent).expect("agent key"),
                serde_json::to_value(credentials.agent).expect("original agent key")
            );
        } else {
            assert!(!store.path.exists());
        }
        fake.done();
    }
}

#[tokio::test]
async fn logout_refreshes_expired_or_refused_access_once() {
    for expired in [false, true] {
        let mut expected = vec![];
        if !expired {
            let mut refused = Expected::revoke("access");
            refused.status = StatusCode::UNAUTHORIZED;
            refused.response = json!({"error": "invalid session"});
            expected.push(refused);
        }
        expected.push(Expected::token(
            refresh_fields("refresh"),
            tokens("new-access", "new-refresh"),
        ));
        expected.push(Expected::revoke("new-access"));
        let fake = Fake::start(expected).await;
        let directory = tempfile::tempdir().expect("directory");
        let store = Store::open(&directory.path().join("identity.json")).expect("store");
        store
            .write(
                &fake.identity.endpoint,
                session(
                    "access",
                    "refresh",
                    if expired {
                        0
                    } else {
                        now().expect("time") + 900
                    },
                ),
            )
            .expect("session");
        fake.identity.logout(&store).await.expect("logout");
        assert!(!store.path.exists());
        fake.done();
    }
}

#[tokio::test]
async fn failed_revocation_keeps_credentials_for_retry() {
    for status in [StatusCode::SERVICE_UNAVAILABLE, StatusCode::OK] {
        let mut refused = Expected::revoke("access");
        refused.status = status;
        refused.response = json!({"error": "unavailable", "error_description": "refresh-secret"});
        let fake = Fake::start(vec![refused, Expected::revoke("access")]).await;
        let directory = tempfile::tempdir().expect("directory");
        let store = Store::open(&directory.path().join("identity.json")).expect("store");
        store
            .write(
                &fake.identity.endpoint,
                session("access", "refresh-secret", now().expect("time") + 900),
            )
            .expect("session");
        let before = fs::read(&store.path).expect("credentials");
        let error = fake.identity.logout(&store).await.expect_err("refused");
        assert!(!error.json().to_string().contains("refresh-secret"));
        assert_eq!(fs::read(&store.path).expect("retained credentials"), before);
        fake.identity.logout(&store).await.expect("retry");
        assert!(!store.path.exists());
        fake.done();
    }
}

#[tokio::test]
async fn logout_without_a_session_preserves_agent_registration() {
    let fake = Fake::start(vec![]).await;
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("identity.json")).expect("store");
    assert_eq!(
        fake.identity
            .logout(&store)
            .await
            .expect("already logged out"),
        json!({"authenticated": false})
    );
    assert!(!store.path.exists());
    let mut credentials = agent_credentials(&fake.identity.endpoint);
    credentials.session = None;
    store.save(&credentials).expect("agent");
    let before = fs::read(&store.path).expect("agent bytes");
    fake.identity
        .logout(&store)
        .await
        .expect("agent-only logout");
    assert_eq!(fs::read(&store.path).expect("retained agent"), before);
    fake.done();
}

#[tokio::test]
async fn failed_agent_rotation_save_preserves_key_without_spent_token() {
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("identity.json");
    let blocked = sidecar(&path, ".next");
    let handler_path = path.clone();
    let router = Router::new().fallback(move || {
        let blocked = blocked.clone();
        let path = handler_path.clone();
        async move {
            let disk: Credentials =
                serde_json::from_slice(&fs::read(path).expect("read")).expect("credentials");
            assert!(
                disk.session.is_none(),
                "token must be consumed before dispatch"
            );
            fs::create_dir(blocked).expect("block rotated save");
            axum::Json(tokens("new-access", "new-refresh"))
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });
    let identity = Identity::new(&endpoint).expect("identity");
    let store = Store::open(&path).expect("store");
    store
        .save(&agent_credentials(&endpoint))
        .expect("save agent");
    let previous = store.read(&endpoint).expect("session");
    assert!(identity.refresh(&store, &previous).await.is_err());
    assert_eq!(
        store.read(&endpoint).err().expect("consumed").code,
        "login_required"
    );
    let saved = fs::read_to_string(&path).expect("saved agent");
    assert!(saved.contains("retained-key"));
    assert!(!saved.contains("old-refresh"));
    task.abort();
}

#[test]
fn login_rejects_changed_agent_and_only_applies_the_signed_upgrade() {
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("identity.json")).expect("store");
    let endpoint = "https://example.test";
    store
        .save(&agent_credentials(endpoint))
        .expect("save agent added during approval");
    let mut device: DeviceCode =
        serde_json::from_value(json!({"device_code": "device", "user_code": "code",
        "verification_uri": endpoint, "expires_in": 60, "interval": 1}))
        .expect("device");
    let before = fs::read(&store.path).expect("saved agent");
    assert_eq!(
        store
            .complete_login(endpoint, session("access", "refresh", 900), &device)
            .err()
            .expect("agent added during approval")
            .code,
        "credentials_changed"
    );
    assert_eq!(fs::read(&store.path).expect("retained agent"), before);
    assert_eq!(
        store
            .credentials(endpoint)
            .expect("credentials")
            .agent
            .expect("agent")
            .registration()
            .expect("registration")
            .tier,
        "agent"
    );
    device.agent_identity = Some(("org".to_owned(), "key".to_owned()));
    device.upgrade = Some(("other-org".to_owned(), "key".to_owned()));
    assert_eq!(
        store
            .complete_login(endpoint, session("access", "refresh", 900), &device)
            .err()
            .expect("mismatched agent")
            .code,
        "credentials_changed"
    );
    device.upgrade = Some(("org".to_owned(), "key".to_owned()));
    store
        .complete_login(endpoint, session("access", "refresh", 900), &device)
        .expect("signed upgrade");
    assert_eq!(
        store
            .credentials(endpoint)
            .expect("credentials")
            .agent
            .expect("agent")
            .registration()
            .expect("registration")
            .tier,
        "oauth"
    );
}

#[test]
fn fresh_login_replaces_unusable_oauth_credentials_but_preserves_agent_keys() {
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("identity.json");
    let store = Store::open(&path).expect("store");
    let endpoint = "https://example.test";
    let device: DeviceCode =
        serde_json::from_value(json!({"device_code": "device", "user_code": "code",
        "verification_uri": endpoint, "expires_in": 60}))
        .expect("device");
    for value in [
        json!({"endpoint": "https://other.test", "session": {}}),
        json!({"endpoint": endpoint, "session": "broken", "agent": null}),
    ] {
        fs::write(&path, value.to_string()).expect("write unusable OAuth credentials");
        assert!(
            store
                .login_agent(endpoint)
                .expect("OAuth credentials")
                .is_none()
        );
        store
            .complete_login(endpoint, session("new-access", "new-refresh", 900), &device)
            .expect("fresh login");
        assert_eq!(
            store.read(endpoint).expect("new session").refresh_token,
            "new-refresh"
        );
    }
    store
        .save(&agent_credentials("https://other.test"))
        .expect("save agent");
    let original = fs::read(&path).expect("original");
    assert_eq!(
        store
            .login_agent(endpoint)
            .err()
            .expect("foreign agent")
            .code,
        "endpoint_mismatch"
    );
    assert!(
        store
            .complete_login(endpoint, session("access", "refresh", 900), &device)
            .is_err()
    );
    assert_eq!(fs::read(&path).expect("retained agent"), original);
    fs::write(&path, br#"{"agent":{"private_key":"retained""#).expect("truncated agent");
    let original = fs::read(&path).expect("truncated contents");
    assert!(store.login_agent(endpoint).is_err());
    assert_eq!(fs::read(&path).expect("retained truncated file"), original);
}

#[tokio::test]
async fn recovered_login_preserves_replacement_credentials() {
    let fake = Fake::start(vec![]).await;
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("identity.json")).expect("store");
    let mut device: DeviceCode =
        serde_json::from_value(json!({"device_code": "device", "user_code": "code",
        "verification_uri": "https://example.test", "expires_in": 60}))
        .expect("device");
    device.upgrade = Some(("org".to_owned(), "key".to_owned()));
    device.agent_identity = device.upgrade.clone();
    device.reconcile_upgrade = true;
    for replacement in [Some(("other-org", "key")), Some(("org", "other-key")), None] {
        let mut credentials = agent_credentials(&fake.identity.endpoint);
        if let Some((org_id, key_id)) = replacement {
            if let Some(agent::Agent::Active { registration, .. }) = &mut credentials.agent {
                registration.org_id = org_id.to_owned();
                registration.key_id = key_id.to_owned();
            }
        } else {
            credentials.agent = None;
        }
        store.save(&credentials).expect("replacement credentials");
        let before = fs::read(&store.path).expect("replacement bytes");
        assert_eq!(
            fake.identity
                .complete_login(
                    &store,
                    session("new-access", "new-refresh", 900),
                    &mut device
                )
                .await
                .err()
                .expect("changed agent")
                .code,
            "credentials_changed"
        );
        assert_eq!(fs::read(&store.path).expect("retained credentials"), before);
    }
    fake.done();
}

#[tokio::test]
async fn recovered_login_keeps_issued_tokens_when_the_tier_read_fails() {
    let mut unavailable = Expected::get(
        "/v1/identity/orgs",
        "new-access",
        json!({"error": "unavailable"}),
    );
    unavailable.status = StatusCode::SERVICE_UNAVAILABLE;
    let fake = Fake::start(vec![unavailable]).await;
    let directory = tempfile::tempdir().expect("directory");
    let store = Store::open(&directory.path().join("identity.json")).expect("store");
    store
        .save(&agent_credentials(&fake.identity.endpoint))
        .expect("agent");
    let mut device: DeviceCode =
        serde_json::from_value(json!({"device_code": "device", "user_code": "code",
        "verification_uri": "https://example.test", "expires_in": 60}))
        .expect("device");
    device.upgrade = Some(("org".to_owned(), "key".to_owned()));
    device.agent_identity = device.upgrade.clone();
    device.reconcile_upgrade = true;
    assert!(
        fake.identity
            .complete_login(
                &store,
                session("new-access", "new-refresh", 900),
                &mut device
            )
            .await
            .is_err()
    );
    let saved = store
        .credentials(&fake.identity.endpoint)
        .expect("saved credentials");
    assert_eq!(
        saved.session.expect("issued session").refresh_token,
        "new-refresh"
    );
    let agent = saved.agent.expect("retained agent");
    assert_eq!(agent.registration().expect("registration").tier, "agent");
    fake.done();
}

// Advances only production device-poll timers that actually become pending.
pub(super) mod poll_clock;

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

struct Fake {
    identity: Identity,
    expected: Arc<Mutex<VecDeque<Expected>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Fake {
    async fn start(expected: Vec<Expected>) -> Self {
        let expected = Arc::new(Mutex::new(VecDeque::from(expected)));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake");
        let endpoint = format!("http://{}", listener.local_addr().expect("fake address"));
        let router = Router::new().fallback(handle).with_state(expected.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve fake");
        });
        Self {
            identity: Identity::new(&endpoint).expect("identity"),
            expected,
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
    State(expected): State<Arc<Mutex<VecDeque<Expected>>>>,
    request: Request,
) -> Response {
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
    if !expected.fields.is_empty() {
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

#[tokio::test]
async fn device_login_pending_slowdown_then_approval() {
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
fn unsigned_login_never_upgrades_an_agent_added_after_login_started() {
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
    store
        .complete_login(endpoint, session("access", "refresh", 900), &device)
        .expect("unsigned login");
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

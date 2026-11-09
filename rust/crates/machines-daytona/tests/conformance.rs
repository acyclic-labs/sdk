use acyclic_harness_core::{Error, Result, http::*};
use acyclic_harness_providers::ReqwestTransport;
use acyclic_machines::{conformance, *};
use acyclic_machines_daytona::{DaytonaConfig, DaytonaProvider};
use acyclic_stream::BoxProviderFuture;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use url::Url;

const MACHINE: &str = "a1000000-0000-0000-0000-000000000001";
const ORIGINS: [&str; 2] = ["https://app.daytona.io", "https://proxy.app.daytona.io"];
// Fixture value belongs solely to the fake host binding, never provider config.
const HOST_VALUE: &str = "fixture-host-owned-daytona-credential";

#[derive(Default)]
struct State {
    sandbox: Option<Value>,
    files: BTreeMap<String, Vec<u8>>,
    requests: Vec<(String, bool)>,
    proxy: String,
    lose_create: bool,
    posts: usize,
}

struct Server {
    state: Arc<Mutex<State>>,
    task: tokio::task::JoinHandle<()>,
    address: String,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    async fn start(proxy: &str, lose_create: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(State {
            proxy: proxy.into(),
            lose_create,
            ..State::default()
        }));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut request = Vec::new();
                let head = loop {
                    let mut buffer = [0; 4096];
                    let count = stream.read(&mut buffer).await.unwrap();
                    if count == 0 {
                        return;
                    }
                    request.extend_from_slice(&buffer[..count]);
                    if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                        break end + 4;
                    }
                };
                let headers = String::from_utf8(request[..head].to_vec()).unwrap();
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|value| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                while request.len() - head < length {
                    let mut buffer = [0; 4096];
                    let count = stream.read(&mut buffer).await.unwrap();
                    assert_ne!(count, 0);
                    request.extend_from_slice(&buffer[..count]);
                }
                let first = headers.lines().next().unwrap();
                let mut parts = first.split_whitespace();
                let method = parts.next().unwrap();
                let target = parts.next().unwrap();
                let url = Url::parse(&format!("https://app.daytona.io{target}")).unwrap();
                let authorized = headers.lines().any(|line| {
                    line.eq_ignore_ascii_case(&format!("authorization: Bearer {HOST_VALUE}"))
                });
                let response = {
                    let mut state = shared.lock().unwrap();
                    state.requests.push((target.into(), authorized));
                    let mut status = 200;
                    let mut body = b"{}".to_vec();
                    if target == "/api/sandbox" && method == "POST" {
                        state.posts += 1;
                        let create: Value = serde_json::from_slice(&request[head..]).unwrap();
                        if state.sandbox.is_some() {
                            status = 409;
                        } else {
                            state.sandbox = Some(
                                json!({"id": MACHINE, "state": "started", "labels": create["labels"], "toolboxProxyUrl": state.proxy, "createdAt": "2026-11-09T00:00:00Z", "updatedAt": "2026-11-09T00:00:00Z"}),
                            );
                            body = serde_json::to_vec(state.sandbox.as_ref().unwrap()).unwrap();
                        }
                        if std::mem::take(&mut state.lose_create) {
                            None
                        } else {
                            Some((status, body))
                        }
                    } else {
                        if url.path() == format!("/api/sandbox/{MACHINE}") {
                            body = serde_json::to_vec(state.sandbox.as_ref().unwrap()).unwrap();
                        } else if url.path() == "/api/sandbox" {
                            body = serde_json::to_vec(
                                &json!({"items": [state.sandbox.as_ref().unwrap()]}),
                            )
                            .unwrap();
                        } else if url.path() == format!("/toolbox/{MACHINE}/files/download") {
                            assert_eq!(method, "GET");
                            let path = url
                                .query_pairs()
                                .find(|(key, _)| key == "path")
                                .unwrap()
                                .1
                                .into_owned();
                            if let Some(file) = state.files.get(&path) {
                                body = file.clone();
                            } else {
                                status = 404;
                            }
                        } else if url.path() == format!("/toolbox/{MACHINE}/files/upload-v2") {
                            assert_eq!(method, "POST");
                            let path = url
                                .query_pairs()
                                .find(|(key, _)| key == "path")
                                .unwrap()
                                .1
                                .into_owned();
                            let content_type = headers
                                .lines()
                                .find(|line| line.to_ascii_lowercase().starts_with("content-type:"))
                                .unwrap();
                            let boundary = content_type.split("boundary=").nth(1).unwrap();
                            let multipart = &request[head..];
                            let start = multipart
                                .windows(4)
                                .position(|bytes| bytes == b"\r\n\r\n")
                                .unwrap()
                                + 4;
                            let tail = format!("\r\n--{boundary}--\r\n");
                            assert!(multipart.ends_with(tail.as_bytes()));
                            state.files.insert(
                                path,
                                multipart[start..multipart.len() - tail.len()].to_vec(),
                            );
                        } else {
                            panic!("unadmitted fake Daytona route: {method} {}", url.path());
                        }
                        Some((status, body))
                    }
                };
                if let Some((status, body)) = response {
                    let header = format!(
                        "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    stream.write_all(header.as_bytes()).await.unwrap();
                    stream.write_all(&body).await.unwrap();
                }
            }
        });
        Self {
            state,
            task,
            address,
        }
    }
    fn provider(&self) -> DaytonaProvider {
        let config = DaytonaConfig {
            base_url: "https://app.daytona.io/api".into(),
            secret_reference: acyclic_auth::secrets::SecretRef::new(
                "sandboxes".into(),
                "daytona".into(),
                1,
            )
            .unwrap(),
            images: BTreeMap::from([("immutable-snapshot".into(), image())]),
            platform: Platform {
                os: OperatingSystem::Linux,
                arch: Architecture::X64,
                device_kind: DeviceKind::Container,
                shell: Shell::Bash,
                path_style: PathStyle::Posix,
            },
            block_all_network_policy: [19; 32],
        };
        assert!(!format!("{config:?}").contains(HOST_VALUE));
        DaytonaProvider::new(
            config,
            Arc::new(Binding(self.address.clone())),
            Arc::new(ReqwestTransport::new().unwrap()),
        )
        .unwrap()
    }
    fn all_authorized(&self) {
        let state = self.state.lock().unwrap();
        assert!(!state.requests.is_empty());
        assert!(
            state.requests.iter().all(|(_, authorized)| *authorized),
            "a request bypassed the protected host binding"
        );
    }
}

struct Binding(String);
struct Protected {
    address: String,
    origin: String,
    transport: Arc<dyn HttpTransport>,
}
impl CredentialResolver for Binding {
    fn bind(
        &self,
        source: CredentialSource<'_>,
        origin: &str,
        scheme: AuthorizationScheme,
        transport: Arc<dyn HttpTransport>,
    ) -> Result<CredentialBinding> {
        let CredentialSource::SecretReference(reference) = source else {
            return Err(Error::Invalid("raw fixture credential refused".into()));
        };
        assert_eq!(
            reference,
            &acyclic_auth::secrets::SecretRef::new("sandboxes".into(), "daytona".into(), 1)
                .unwrap()
        );
        assert_eq!(scheme, AuthorizationScheme::Bearer);
        if !ORIGINS.contains(&origin) {
            return Err(Error::Unauthorized("origin refused by fake binding".into()));
        }
        Ok(CredentialBinding::protected(
            [3; 32],
            Arc::new(Protected {
                address: self.0.clone(),
                origin: origin.into(),
                transport,
            }),
        ))
    }
}
impl HttpTransport for Protected {
    fn send(&self, mut request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            let url = Url::parse(&request.url).unwrap();
            assert_eq!(url.origin().ascii_serialization(), self.origin);
            assert!(
                !request
                    .headers
                    .keys()
                    .any(|name| name.eq_ignore_ascii_case("authorization"))
            );
            request
                .headers
                .insert("authorization".into(), format!("Bearer {HOST_VALUE}"));
            request.url = format!(
                "{}{}{}",
                self.address,
                url.path(),
                url.query()
                    .map_or(String::new(), |query| format!("?{query}"))
            );
            self.transport.send(request).await
        })
    }
    fn delay(&self, duration: Duration) -> BoxProviderFuture<'static, Result<()>> {
        self.transport.delay(duration)
    }
    fn now(&self) -> Duration {
        self.transport.now()
    }
}
fn image() -> Image {
    Image::custom([17; 32]).unwrap()
}
fn request() -> CreateMachine {
    let mut request = CreateMachine::new(
        IdempotencyKey::parse("b1000000-0000-0000-0000-000000000001").unwrap(),
        Some(image()),
        [19; 32],
    );
    request.suspension = SuspensionPolicy::Manual;
    request
}
fn machine() -> MachineId {
    MachineId::parse(MACHINE).unwrap()
}
fn access(deadline: u64) -> FileAccess {
    FileAccess::new("/tmp/binary file?#".into(), 0, deadline).unwrap()
}

#[tokio::test]
async fn protected_http_adapter_passes_shared_capability_and_file_conformance() {
    let server = Server::start("https://proxy.app.daytona.io/toolbox", false).await;
    let provider = server.provider();
    conformance::verify(&conformance::Fixture::new(provider, image(), [19; 32]))
        .await
        .unwrap();
    let provider = server.provider();
    conformance::verify_files(
        &provider,
        machine(),
        access(4_102_444_800_000).path(),
        4_102_444_800_000,
    )
    .await
    .unwrap();
    // Unavailable native attachment is declared and refuses before transport IO.
    assert_eq!(
        provider.capability(ProviderCapability::AttachVolume),
        Support::Unsupported
    );
    assert!(matches!(
        provider
            .attach_volume(machine(), Default::default(), IdempotencyKey::new())
            .await,
        Err(ProviderError::Unsupported(_))
    ));
    server.all_authorized();
}

#[tokio::test]
async fn sandbox_reported_origins_outside_the_binding_are_refused_before_toolbox_io() {
    for proxy in [
        "https://attacker.example/toolbox",
        "https://proxy.app.daytona.io.attacker.example/toolbox",
        "https://proxy.app.daytona.io:444/toolbox",
        "http://proxy.app.daytona.io/toolbox",
        "https://user@proxy.app.daytona.io/toolbox",
    ] {
        let server = Server::start(proxy, false).await;
        let provider = server.provider();
        provider.create(request()).await.unwrap();
        assert!(
            matches!(provider.download_file(machine(), access(4_102_444_800_000)).await, Err(ProviderError::Rejected(message)) if message.contains("binding") || message.contains("HTTPS"))
        );
        assert!(
            server
                .state
                .lock()
                .unwrap()
                .requests
                .iter()
                .all(|(path, _)| !path.starts_with("/toolbox"))
        );
        server.all_authorized();
    }
}

#[tokio::test]
async fn lost_create_reply_is_recovered_without_redispatch_and_expired_files_never_send() {
    let server = Server::start("https://app.daytona.io/toolbox", true).await;
    let provider = server.provider();
    let request = request();
    let key = request.idempotency_key;
    assert!(
        matches!(provider.create(request.clone()).await, Err(ProviderError::Indeterminate(actual)) if actual == key)
    );
    assert!(
        matches!(provider.create(request).await, Err(ProviderError::Indeterminate(actual)) if actual == key)
    );
    let recovered = provider.recover(key).await.unwrap();
    assert!(
        matches!(recovered, MutationOutcome::Created(observation) if observation.id == machine())
    );
    assert_eq!(server.state.lock().unwrap().posts, 1);
    write_file_at(
        &provider,
        machine(),
        access(4_102_444_800_000),
        vec![0, 255],
        true,
    )
    .await
    .unwrap();
    assert_eq!(
        read_file_at(&provider, machine(), access(4_102_444_800_000), 2)
            .await
            .unwrap(),
        FileRead::Data(vec![0, 255])
    );
    let count = server.state.lock().unwrap().requests.len();
    assert!(matches!(
        provider.download_file(machine(), access(1)).await,
        Err(ProviderError::Cancelled)
    ));
    assert_eq!(server.state.lock().unwrap().requests.len(), count);
    server.all_authorized();
}

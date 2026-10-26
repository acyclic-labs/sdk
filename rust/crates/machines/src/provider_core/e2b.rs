//! E2B REST control plane and envd Connect RPC transport.
//! Wire definitions: e2b npm 2.61.0, process.Process and /v2/sandboxes.
use super::{
    Allocation, ExecSession, NetworkPolicy, SandboxDriver, SandboxInstance, SandboxSpec,
    http::{Http, secret},
};
use crate::{ExecCommand, ExecOutput, MachineState, ProviderError};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use futures::{StreamExt, stream};
use reqwest::{Method, header::HeaderMap};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

/// E2B cloud or account-free Embed deployment. Authentication values are never
/// included in resource endpoints or diagnostics. Templates must provide the
/// configured workspace and POSIX shell utilities.
#[derive(Clone)]
pub struct E2b {
    api: Http,
    guest_domain: String,
    dev_proxy: Option<Http>,
    guests: Arc<Mutex<BTreeMap<String, Http>>>,
    maximum_lifetime: Duration,
}

impl E2b {
    /// Uses the public E2B API with the one-hour lifetime ceiling. Applications
    /// with another account limit can configure their deployment explicitly.
    pub fn cloud(api_key: &str) -> Result<Self, ProviderError> {
        Self::new(
            "https://api.e2b.app",
            api_key,
            "e2b.app",
            None,
            Duration::from_secs(3600),
        )
    }

    /// `dev_proxy` is Embed's envd proxy (normally loopback port 3002).
    /// Guest API credentials are sent only to this configured proxy or to
    /// `49983-{sandbox_id}.{guest_domain}`, never to a reply-supplied host.
    pub fn new(
        api_url: &str,
        api_key: &str,
        guest_domain: &str,
        dev_proxy: Option<&str>,
        maximum_lifetime: Duration,
    ) -> Result<Self, ProviderError> {
        if guest_domain.is_empty()
            || !guest_domain
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-')
            || maximum_lifetime.is_zero()
        {
            return Err(ProviderError::Invalid("E2B deployment admission failed; recovery: configure a DNS domain and nonzero provider lifetime ceiling".into()));
        }
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", secret(api_key)?);
        Ok(Self {
            api: Http::new(api_url, headers)?,
            guest_domain: guest_domain.into(),
            dev_proxy: dev_proxy
                .map(|url| Http::new(url, HeaderMap::new()))
                .transpose()?,
            guests: Arc::default(),
            maximum_lifetime,
        })
    }

    fn guest(&self, instance: &SandboxInstance) -> Result<Http, ProviderError> {
        self.guests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&instance.id)
            .cloned()
            .ok_or_else(|| ProviderError::NotFound(instance.id.clone()))
    }
    async fn raw_start(
        &self,
        instance: &SandboxInstance,
        argv: Vec<String>,
        timeout_ms: u32,
    ) -> Result<crate::ExecStream, ProviderError> {
        let guest = self.guest(instance)?;
        let tag = uuid::Uuid::new_v4().to_string();
        let mut argv = argv.iter();
        let executable = argv.next().ok_or_else(|| {
            ProviderError::Invalid("E2B argv missing; recovery: supply an executable".into())
        })?;
        let request = connect_message(
            &serde_json::json!({"process": {"cmd": executable, "args": argv.collect::<Vec<_>>()}, "tag": tag, "stdin": false}),
        )?;
        let response = Http::checked(
            guest
                .request(Method::POST, &["process.Process", "Start"])?
                .header("content-type", "application/connect+json")
                .header("connect-protocol-version", "1")
                .timeout(Duration::from_millis(u64::from(timeout_ms)) + Duration::from_secs(10))
                .body(request)
                .send()
                .await,
        )
        .await?;
        let output = stream::try_unfold((response.bytes_stream(), Vec::<u8>::new()), |(mut source, mut pending)| async move {
            loop {
                if let Some(header) = pending.get(..5) {
                    let flags = header.first().copied().unwrap_or(0);
                    let length = header.get(1..5).and_then(|value| <[u8; 4]>::try_from(value).ok())
                        .map(u32::from_be_bytes).ok_or(ProviderError::Unavailable)? as usize;
                    if length > 1024 * 1024 || flags & !2 != 0 { return Err(ProviderError::Unavailable); }
                    if pending.len() >= length + 5 {
                        let frame = pending.drain(..length + 5).skip(5).collect::<Vec<_>>();
                        if flags == 2 {
                            let trailer: serde_json::Value = serde_json::from_slice(&frame).map_err(|_| ProviderError::Unavailable)?;
                            if trailer.get("error").is_some() { return Err(ProviderError::Unavailable); }
                            return Ok(None);
                        }
                        let frame: StartResponse = serde_json::from_slice(&frame).map_err(|_| ProviderError::Unavailable)?;
                        let output = match frame.event {
                            Event::Start(_value) | Event::Keepalive(_value) => continue,
                            Event::Data(Data::Stdout(data)) => ExecOutput::Stdout(STANDARD.decode(data).map_err(|_| ProviderError::Unavailable)?),
                            Event::Data(Data::Stderr(data)) => ExecOutput::Stderr(STANDARD.decode(data).map_err(|_| ProviderError::Unavailable)?),
                            Event::End(end) if end.exited && end.error.is_none() => ExecOutput::ExitCode(end.code),
                            Event::End(_) => return Err(ProviderError::Unavailable),
                        };
                        if matches!(&output, ExecOutput::Stdout(data) | ExecOutput::Stderr(data) if data.is_empty()) { continue; }
                        return Ok(Some((output, (source, pending))));
                    }
                }
                match source.next().await {
                    Some(Ok(data)) if pending.len().saturating_add(data.len()) <= 2 * 1024 * 1024 => pending.extend_from_slice(&data),
                    Some(_) => return Err(ProviderError::Unavailable),
                    None if pending.is_empty() => return Ok(None),
                    None => return Err(ProviderError::Unavailable),
                }
            }
        }).boxed();
        Ok(output)
    }
}

#[derive(Deserialize)]
struct Created {
    #[serde(rename = "sandboxID")]
    id: String,
    #[serde(rename = "envdAccessToken")]
    access_token: String,
    #[serde(rename = "cpuCount", default)]
    cpu: u64,
    #[serde(rename = "memoryMB", default)]
    memory_mb: u64,
    #[serde(rename = "diskSizeMB", default)]
    disk_mb: u64,
}

#[derive(Deserialize)]
struct State {
    state: String,
}

#[derive(Deserialize)]
struct StartResponse {
    event: Event,
}
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum Event {
    Start(serde_json::Value),
    Data(Data),
    End(End),
    Keepalive(serde_json::Value),
}
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum Data {
    Stdout(String),
    Stderr(String),
}
#[derive(Deserialize)]
struct End {
    #[serde(rename = "exitCode", default)]
    code: i32,
    #[serde(default)]
    exited: bool,
    error: Option<String>,
}

fn connect_message(message: &serde_json::Value) -> Result<Vec<u8>, ProviderError> {
    let data = serde_json::to_vec(message).map_err(|_| {
        ProviderError::Invalid(
            "E2B process serialization failed; recovery: inspect the command without redispatching"
                .into(),
        )
    })?;
    let length = u32::try_from(data.len()).map_err(|_| {
        ProviderError::Invalid("E2B process request too large; recovery: reduce argv".into())
    })?;
    let mut framed = vec![0];
    framed.extend_from_slice(&length.to_be_bytes());
    framed.extend(data);
    Ok(framed)
}

#[async_trait]
impl SandboxDriver for E2b {
    fn name(&self) -> &'static str {
        "e2b"
    }
    fn supports_suspend(&self) -> bool {
        false
    }
    fn maximum_lifetime(&self) -> Option<Duration> {
        Some(self.maximum_lifetime)
    }
    fn validate(&self, spec: &SandboxSpec) -> Result<(), ProviderError> {
        if !matches!(spec.network, NetworkPolicy::BlockAll) {
            return Err(ProviderError::Unsupported("E2B network admission failed; recovery: register BlockAll until an egress selector policy is configured".into()));
        }
        spec.lifetime_seconds().map(|_| ())
    }

    async fn create(&self, spec: &SandboxSpec) -> Result<SandboxInstance, ProviderError> {
        let created: Created = Http::checked(self.api.request(Method::POST, &["v2", "sandboxes"])?
            .header("idempotency-key", &spec.name)
            .timeout(Duration::from_secs(30))
            .json(&serde_json::json!({"templateID": spec.image, "metadata": {"acyclic.machine": spec.name}, "envVars": spec.env,
                "timeout": spec.lifetime_seconds()?, "allow_internet_access": false, "autoPause": false, "secure": true}))
            .send().await).await?.json().await.map_err(|_| ProviderError::Unavailable)?;
        if created.id.is_empty()
            || !created
                .id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(ProviderError::Unavailable);
        }
        let mut headers = HeaderMap::new();
        headers.insert("x-access-token", secret(&created.access_token)?);
        headers.insert(
            reqwest::header::AUTHORIZATION,
            secret(&format!("Basic {}", STANDARD.encode("user:")))?,
        );
        let endpoint = if let Some(proxy) = &self.dev_proxy {
            headers.insert("e2b-sandbox-id", secret(&created.id)?);
            headers.insert(
                "e2b-sandbox-port",
                reqwest::header::HeaderValue::from_static("49983"),
            );
            proxy.endpoint.to_string()
        } else {
            format!("https://49983-{}.{}", created.id, self.guest_domain)
        };
        let guest = Http::new(&endpoint, headers)?;
        self.guests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(created.id.clone(), guest);
        Ok(SandboxInstance {
            endpoint,
            id: created.id,
            allocation: Allocation {
                cpu: created.cpu,
                memory_gib: created.memory_mb / 1024,
                disk_gib: created.disk_mb / 1024,
            },
        })
    }

    async fn state(&self, instance: &SandboxInstance) -> Result<MachineState, ProviderError> {
        let state: State = Http::checked(
            self.api
                .request(Method::GET, &["sandboxes", &instance.id])?
                .timeout(Duration::from_secs(5))
                .send()
                .await,
        )
        .await?
        .json()
        .await
        .map_err(|_| ProviderError::Unavailable)?;
        match state.state.as_str() {
            "running" => Ok(MachineState::Running),
            "paused" => Ok(MachineState::Suspended),
            _ => Ok(MachineState::Indeterminate),
        }
    }

    async fn destroy(&self, instance: &SandboxInstance) -> Result<(), ProviderError> {
        match Http::checked(
            self.api
                .request(Method::DELETE, &["sandboxes", &instance.id])?
                .timeout(Duration::from_secs(10))
                .send()
                .await,
        )
        .await
        {
            Ok(_) | Err(ProviderError::NotFound(_)) => {
                self.guests
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&instance.id);
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn start(
        &self,
        instance: &SandboxInstance,
        command: ExecCommand,
    ) -> Result<ExecSession, ProviderError> {
        let start_driver = self.clone();
        let start_instance = instance.clone();
        let maintenance_driver = self.clone();
        let maintenance_instance = instance.clone();
        let timeout = command.timeout_ms();
        Ok(super::process_group_session(
            &command,
            move |argv| {
                Box::pin(
                    async move { start_driver.raw_start(&start_instance, argv, timeout).await },
                )
            },
            move |argv| {
                let driver = maintenance_driver.clone();
                let instance = maintenance_instance.clone();
                Box::pin(async move { driver.raw_start(&instance, argv, 10_000).await })
            },
        ))
    }

    async fn read_file(
        &self,
        instance: &SandboxInstance,
        path: &str,
    ) -> Result<Vec<u8>, ProviderError> {
        Http::checked(
            self.guest(instance)?
                .request(Method::GET, &["files"])?
                .query(&[("path", path), ("username", "user")])
                .timeout(Duration::from_secs(30))
                .send()
                .await,
        )
        .await?
        .bytes()
        .await
        .map(|data| data.to_vec())
        .map_err(|_| ProviderError::Unavailable)
    }

    async fn write_file(
        &self,
        instance: &SandboxInstance,
        path: &str,
        data: &[u8],
    ) -> Result<(), ProviderError> {
        Http::checked(
            self.guest(instance)?
                .request(Method::POST, &["files"])?
                .query(&[("path", path), ("username", "user")])
                .header("content-type", "application/octet-stream")
                .timeout(Duration::from_secs(30))
                .body(data.to_vec())
                .send()
                .await,
        )
        .await?;
        Ok(())
    }
}

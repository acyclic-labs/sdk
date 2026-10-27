//! E2B REST control plane and envd Connect RPC transport.
//! Wire definitions: e2b npm 2.61.0, process.Process and /v2/sandboxes.
use super::instances::Instances;
use super::{
    Allocation, SandboxDriver, SandboxInstance, SandboxSpec,
    http::{Http, credential_headers},
};
use crate::{ExecOutput, MachineState, ProviderError};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use futures::{StreamExt, TryStreamExt};
use reqwest::{Method, header::HeaderMap};
use serde::Deserialize;
use std::time::Duration;

/// E2B cloud or account-free Embed deployment. Authentication values are never
/// included in resource endpoints or diagnostics. Templates must provide the
/// configured workspace and POSIX shell utilities.
#[derive(Clone)]
pub struct E2b {
    api: Http,
    guest_domain: String,
    dev_proxy: Option<Http>,
    guests: Instances<Http>,
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
        Ok(Self {
            api: Http::new(api_url, credential_headers([("x-api-key", api_key)])?)?,
            guest_domain: guest_domain.into(),
            dev_proxy: dev_proxy
                .map(|url| Http::new(url, HeaderMap::new()))
                .transpose()?,
            guests: Instances::default(),
            maximum_lifetime,
        })
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
impl Event {
    fn output(self) -> Result<Option<ExecOutput>, ProviderError> {
        Ok(match self {
            Self::Start(_value) | Self::Keepalive(_value) => None,
            Self::Data(data) => {
                let (data, channel): (_, fn(Vec<u8>) -> ExecOutput) = match data {
                    Data::Stdout(data) => (data, ExecOutput::Stdout),
                    Data::Stderr(data) => (data, ExecOutput::Stderr),
                };
                let bytes = STANDARD
                    .decode(data)
                    .map_err(|_| ProviderError::Unavailable)?;
                (!bytes.is_empty()).then(|| channel(bytes))
            }
            Self::End(end) if end.exited && end.error.is_none() => {
                Some(ExecOutput::ExitCode(end.code))
            }
            Self::End(_) => return Err(ProviderError::Unavailable),
        })
    }
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

#[async_trait]
impl SandboxDriver for E2b {
    fn name(&self) -> &'static str {
        "e2b"
    }
    fn image_selector(&self) -> super::ImageSelector {
        super::ImageSelector::TemplateBuild
    }
    fn supports_disk_snapshots(&self) -> bool {
        true
    }
    async fn snapshot(&self, instance: &SandboxInstance, _: &str) -> Result<String, ProviderError> {
        let response: serde_json::Value = Http::json(
            self.api
                .request(Method::POST, &["sandboxes", &instance.id, "snapshots"])?
                .timeout(Duration::from_secs(30))
                .json(&serde_json::json!({"memory": false})),
        )
        .await?;
        response
            .get("snapshotID")
            .and_then(serde_json::Value::as_str)
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
            .ok_or(ProviderError::Unavailable)
    }
    async fn release_snapshot(
        &self,
        snapshot: &super::ProviderSnapshot,
    ) -> Result<super::SnapshotRelease, ProviderError> {
        Http::empty(
            self.api
                .request(Method::DELETE, &["templates", &snapshot.handle])?
                .timeout(Duration::from_secs(10)),
        )
        .await?;
        Ok(super::SnapshotRelease::Deleted)
    }
    fn maximum_lifetime(&self) -> Option<Duration> {
        Some(self.maximum_lifetime)
    }

    async fn create(&self, spec: &SandboxSpec) -> Result<SandboxInstance, ProviderError> {
        let created: Created = Http::json(self.api.request(Method::POST, &["v2", "sandboxes"])?
            .header("idempotency-key", &spec.name)
            .timeout(Duration::from_secs(30))
            .json(&serde_json::json!({"templateID": spec.snapshot.as_ref().map_or(spec.image.as_str(), |snapshot| snapshot.handle.as_str()), "metadata": {"acyclic.machine": spec.name}, "envVars": spec.env,
                "timeout": spec.lifetime_seconds()?, "allow_internet_access": false, "autoPause": false, "secure": true}))).await?;
        if created.id.is_empty()
            || !created
                .id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(ProviderError::Unavailable);
        }
        let mut headers = credential_headers([
            ("x-access-token", created.access_token.as_str()),
            (
                "authorization",
                format!("Basic {}", STANDARD.encode("user:")).as_str(),
            ),
        ])?;
        let endpoint = if let Some(proxy) = &self.dev_proxy {
            headers.extend(credential_headers([
                ("e2b-sandbox-id", created.id.as_str()),
                ("e2b-sandbox-port", "49983"),
            ])?);
            proxy.endpoint.to_string()
        } else {
            format!("https://49983-{}.{}", created.id, self.guest_domain)
        };
        let guest = Http::new(&endpoint, headers)?;
        self.guests.insert(created.id.clone(), guest);
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
        let state: State = Http::json(
            self.api
                .request(Method::GET, &["sandboxes", &instance.id])?
                .timeout(Duration::from_secs(5)),
        )
        .await?;
        match state.state.as_str() {
            "running" => Ok(MachineState::Running),
            "paused" => Ok(MachineState::Suspended),
            _ => Ok(MachineState::Indeterminate),
        }
    }

    async fn destroy(&self, instance: &SandboxInstance) -> Result<(), ProviderError> {
        match Http::empty(
            self.api
                .request(Method::DELETE, &["sandboxes", &instance.id])?
                .timeout(Duration::from_secs(10)),
        )
        .await
        {
            Ok(_) | Err(ProviderError::NotFound(_)) => {
                self.guests.remove(instance);
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn exec(
        &self,
        instance: &SandboxInstance,
        argv: Vec<String>,
        timeout_ms: u32,
        _input: Option<Vec<u8>>,
    ) -> Result<crate::ExecStream, ProviderError> {
        let guest = self.guests.get(instance)?;
        let tag = uuid::Uuid::new_v4().to_string();
        let mut argv = argv.iter();
        let executable = argv.next().ok_or_else(|| {
            ProviderError::Invalid("E2B argv missing; recovery: supply an executable".into())
        })?;
        let response = Http::connect::<StartResponse>(
            guest
                .request(Method::POST, &["process.Process", "Start"])?
                .timeout(Duration::from_millis(u64::from(timeout_ms)) + Duration::from_secs(10)),
            &serde_json::json!({"process": {"cmd": executable, "args": argv.collect::<Vec<_>>()}, "tag": tag, "stdin": false}),
        )
        .await?;
        let output = response
            .try_filter_map(|frame| async move { frame.event.output() })
            .boxed();
        Ok(output)
    }

    async fn read_file(
        &self,
        instance: &SandboxInstance,
        path: &str,
    ) -> Result<Vec<u8>, ProviderError> {
        Http::bytes(
            self.guests
                .get(instance)?
                .request(Method::GET, &["files"])?
                .query(&[("path", path), ("username", "user")])
                .timeout(Duration::from_secs(30)),
        )
        .await
    }

    async fn write_file(
        &self,
        instance: &SandboxInstance,
        path: &str,
        data: &[u8],
    ) -> Result<(), ProviderError> {
        Http::empty(
            self.guests
                .get(instance)?
                .request(Method::POST, &["files"])?
                .query(&[("path", path), ("username", "user")])
                .header("content-type", "application/octet-stream")
                .timeout(Duration::from_secs(30))
                .body(data.to_vec()),
        )
        .await
    }
}

#[cfg(test)]
mod tests;

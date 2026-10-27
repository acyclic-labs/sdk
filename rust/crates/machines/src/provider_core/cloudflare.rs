//! Cloudflare native Containers transport for the companion Worker in
//! `deploy/machines-cloudflare`. Lifecycle policy is implemented by `RemoteMachines`.

use super::framing::{Framing, frames};
use super::http::{Http, secret};
use super::{Allocation, ExecSession, NetworkPolicy, SandboxDriver, SandboxInstance, SandboxSpec};
use crate::{ExecCommand, ExecOutput, MachineState, ProviderError};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use reqwest::{Method, header::HeaderMap};
use serde::Deserialize;
use std::time::Duration;

/// Connection to an application-owned Containers Worker. No account API token
/// is sent to the guest. Put access control in front of the Worker when hosted.
#[derive(Clone)]
pub struct Cloudflare {
    http: Http,
}

impl Cloudflare {
    /// Connects to a Worker implementing the shipped low-level transport.
    /// Plain HTTP is accepted only for loopback local development.
    pub fn new(endpoint: &str, bearer: Option<&str>) -> Result<Self, ProviderError> {
        let mut headers = HeaderMap::new();
        if let Some(bearer) = bearer {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                secret(&format!("Bearer {bearer}"))?,
            );
        }
        Ok(Self {
            http: Http::new(endpoint, headers)?,
        })
    }

    fn request(
        &self,
        method: Method,
        id: &str,
        suffix: &[&str],
    ) -> Result<reqwest::RequestBuilder, ProviderError> {
        let mut path = vec!["sandboxes", id];
        path.extend_from_slice(suffix);
        self.http.request(method, &path)
    }
    async fn raw_start(
        &self,
        instance: &SandboxInstance,
        argv: Vec<String>,
        timeout_ms: u32,
    ) -> Result<crate::ExecStream, ProviderError> {
        let id = uuid::Uuid::new_v4().to_string();
        let response = Http::checked(
            self.request(Method::POST, &instance.id, &["exec", &id])?
                .timeout(Duration::from_millis(u64::from(timeout_ms)) + Duration::from_secs(10))
                .json(&serde_json::json!({"argv": argv}))
                .send()
                .await,
        )
        .await?;
        let output = frames(response.bytes_stream(), Framing::Lines)
            .and_then(|frame| async move {
                let frame: Frame = serde_json::from_slice(&frame.data).map_err(|_| ProviderError::Rejected("Cloudflare exec frame decoding failed; recovery: inspect the Worker version without replaying the command".into()))?;
                match frame {
                    Frame::Stdout(data) => Ok(ExecOutput::Stdout(data)),
                    Frame::Stderr(data) => Ok(ExecOutput::Stderr(data)),
                    Frame::Exit(code) => Ok(ExecOutput::ExitCode(code)),
                    Frame::Error(_reason) => Err(ProviderError::Unavailable),
                }
            }).boxed();
        Ok(output)
    }
}

#[derive(Deserialize)]
struct State {
    running: bool,
}

#[derive(Deserialize)]
#[serde(tag = "stream", content = "value", rename_all = "lowercase")]
enum Frame {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Exit(i32),
    Error(String),
}

#[async_trait]
impl SandboxDriver for Cloudflare {
    fn name(&self) -> &'static str {
        "cloudflare"
    }
    fn supports_suspend(&self) -> bool {
        false
    }
    fn supports_disk_snapshots(&self) -> bool {
        true
    }
    fn snapshot_retention(&self) -> Option<Duration> {
        Some(Duration::from_secs(30 * 24 * 60 * 60))
    }
    async fn snapshot(&self, instance: &SandboxInstance, _: &str) -> Result<String, ProviderError> {
        let snapshot: serde_json::Value = Http::checked(
            self.request(Method::POST, &instance.id, &["snapshot"])?
                .timeout(Duration::from_secs(30))
                .send()
                .await,
        )
        .await?
        .json()
        .await
        .map_err(|_| ProviderError::Unavailable)?;
        if snapshot
            .get("id")
            .and_then(serde_json::Value::as_str)
            .is_none_or(str::is_empty)
        {
            return Err(ProviderError::Unavailable);
        }
        serde_json::to_string(&snapshot).map_err(|_| ProviderError::Unavailable)
    }
    async fn release_snapshot(
        &self,
        _: &super::ProviderSnapshot,
    ) -> Result<super::SnapshotRelease, ProviderError> {
        // Native Worker snapshots expose no deletion API. The core revokes the
        // SDK reference; data remains under Cloudflare's documented native TTL.
        Ok(super::SnapshotRelease::ProviderRetained)
    }
    fn maximum_lifetime(&self) -> Option<Duration> {
        Some(Duration::from_secs(6 * 60 * 60))
    }
    fn validate(&self, spec: &SandboxSpec) -> Result<(), ProviderError> {
        if !matches!(spec.network, NetworkPolicy::BlockAll) {
            return Err(ProviderError::Unsupported("Cloudflare network admission failed; recovery: register BlockAll until outbound Worker policy is configured".into()));
        }
        Ok(())
    }

    async fn create(&self, spec: &SandboxSpec) -> Result<SandboxInstance, ProviderError> {
        let mut body = serde_json::json!({"image": spec.image, "env": spec.env, "internet": false});
        if let Some(snapshot) = &spec.snapshot {
            body["snapshot"] =
                serde_json::from_str(&snapshot.handle).map_err(|_| ProviderError::Unavailable)?;
        }
        Http::checked(
            self.request(Method::POST, &spec.name, &[])?
                .timeout(Duration::from_secs(20))
                .json(&body)
                .send()
                .await,
        )
        .await?;
        Ok(SandboxInstance {
            id: spec.name.clone(),
            endpoint: format!(
                "{}/sandboxes/{}",
                self.http.endpoint.as_str().trim_end_matches('/'),
                spec.name
            ),
            allocation: Allocation::default(),
        })
    }

    async fn state(&self, instance: &SandboxInstance) -> Result<MachineState, ProviderError> {
        let state: State = Http::checked(
            self.request(Method::GET, &instance.id, &[])?
                .timeout(Duration::from_secs(5))
                .send()
                .await,
        )
        .await?
        .json()
        .await
        .map_err(|error| {
            tracing::warn!(%error, "Cloudflare state response decoding failed; recovery: inspect the deployed Worker version");
            ProviderError::Unavailable
        })?;
        Ok(if state.running {
            MachineState::Running
        } else {
            MachineState::Destroyed
        })
    }

    async fn destroy(&self, instance: &SandboxInstance) -> Result<(), ProviderError> {
        Http::checked(
            self.request(Method::DELETE, &instance.id, &[])?
                .timeout(Duration::from_secs(10))
                .send()
                .await,
        )
        .await?;
        Ok(())
    }

    fn start(
        &self,
        instance: &SandboxInstance,
        command: ExecCommand,
    ) -> Result<ExecSession, ProviderError> {
        let driver = self.clone();
        let instance = instance.clone();
        Ok(super::process_group_io(
            &command,
            None,
            move |argv, timeout, _| {
                let driver = driver.clone();
                let instance = instance.clone();
                Box::pin(async move { driver.raw_start(&instance, argv, timeout).await })
            },
        ))
    }

    async fn read_file(
        &self,
        instance: &SandboxInstance,
        path: &str,
    ) -> Result<Vec<u8>, ProviderError> {
        Http::checked(
            self.request(Method::GET, &instance.id, &["file"])?
                .timeout(Duration::from_secs(20))
                .query(&[("path", path)])
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
            self.request(Method::PUT, &instance.id, &["file"])?
                .timeout(Duration::from_secs(20))
                .query(&[("path", path)])
                .body(data.to_vec())
                .send()
                .await,
        )
        .await?;
        Ok(())
    }
}

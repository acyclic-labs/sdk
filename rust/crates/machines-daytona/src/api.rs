//! Thin typed HTTP client over the Daytona REST API.
//!
//! Every method names the endpoint it calls. Paths and bodies follow Daytona's published
//! `OpenAPI` document (paths under `/api`, default base [`crate::DEFAULT_API_URL`]). Methods marked
//! *live-verified* have round-tripped against a real organization with a container sandbox.
//! The VM-only calls (pause, memory snapshot, fork, auto-pause) follow the specification and
//! their routes answer on the live API, but their bodies have not yet run against a Linux VM.
//! Daytona has no `resume` (use `start`), no snapshot-fork, and no per-sandbox usage route.
//! Wire structs keep unknown fields in an `extra` map so shape drift shows up instead of being
//! silently dropped.

use std::collections::BTreeMap;

use acyclic_machines::ProviderError;
use reqwest::{Method, RequestBuilder, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::DaytonaConfig;

/// Header carrying the organization to act in when a credential spans several.
const ORGANIZATION_HEADER: &str = "X-Daytona-Organization-ID";
/// Largest page `GET /sandbox` accepts.
const LIST_PAGE_LIMIT: &str = "200";
/// Upper bound on list pages followed, so a cursor loop cannot run forever.
const MAX_LIST_PAGES: usize = 256;

/// One Daytona sandbox (`Sandbox` and `SandboxListItem` in the specification).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sandbox {
    /// Sandbox identity; a UUID for every sandbox Daytona creates.
    pub id: String,
    /// Organization-unique sandbox name; defaults to the id.
    #[serde(default)]
    pub name: Option<String>,
    /// Daytona lifecycle state (`creating`, `started`, `paused`, `forking`, ...).
    #[serde(default)]
    pub state: Option<String>,
    /// State Daytona is driving the sandbox towards.
    #[serde(default)]
    pub desired_state: Option<String>,
    /// Snapshot the sandbox was created from.
    #[serde(default)]
    pub snapshot: Option<String>,
    /// Free-form string labels.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Sandbox class (`linux-vm`, `container`, `windows`, `android`); inherited from the
    /// snapshot, never chosen at create time.
    #[serde(default)]
    pub sandbox_class: Option<String>,
    /// Region target.
    #[serde(default)]
    pub target: Option<String>,
    /// Allocated vCPUs.
    #[serde(default)]
    pub cpu: Option<u64>,
    /// Allocated memory in GiB.
    #[serde(default)]
    pub memory: Option<u64>,
    /// Allocated disk in GiB.
    #[serde(default)]
    pub disk: Option<u64>,
    /// Idle minutes before Daytona stops the sandbox; zero disables.
    #[serde(default)]
    pub auto_stop_interval: Option<i64>,
    /// Idle minutes before Daytona pauses a VM sandbox; zero disables.
    #[serde(default)]
    pub auto_pause_interval: Option<i64>,
    /// Stopped minutes before Daytona deletes the sandbox; negative disables.
    #[serde(default)]
    pub auto_delete_interval: Option<i64>,
    /// Failure detail when `state` is `error` or `build_failed`.
    #[serde(default)]
    pub error_reason: Option<String>,
    /// Base URL of the toolbox proxy serving this sandbox.
    #[serde(default)]
    pub toolbox_proxy_url: Option<String>,
    /// RFC 3339 creation timestamp.
    #[serde(default)]
    pub created_at: Option<String>,
    /// RFC 3339 last-change timestamp.
    #[serde(default)]
    pub updated_at: Option<String>,
    /// Fields this client does not model.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Request body of `POST /sandbox` (`CreateSandbox`).
///
/// There is deliberately no class field: the sandbox class is a property of the snapshot.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSandboxRequest {
    /// Organization-unique name. The provider derives it from the idempotency key, so a
    /// replayed create collides instead of duplicating.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Snapshot id or name to boot from.
    pub snapshot: String,
    /// Region target.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Environment variables set in the sandbox.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Labels attached at creation.
    pub labels: BTreeMap<String, String>,
    /// Idle minutes before automatic stop; zero disables.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_stop_interval: Option<u32>,
    /// Idle minutes before automatic pause (VM classes only); zero disables. At most one of
    /// the stop and pause intervals may be non-zero.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_pause_interval: Option<u32>,
    /// Stopped minutes before automatic deletion; negative disables, zero deletes on stop.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_delete_interval: Option<i64>,
    /// Wall-clock minutes after creation before Daytona destroys the sandbox; zero disables.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_minutes: Option<u64>,
    /// Comma-separated outbound domain allow list. Mutually exclusive with the CIDR allow list
    /// and block-all.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain_allow_list: Option<String>,
    /// Block all outbound traffic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network_block_all: Option<bool>,
}

/// Request body of `POST /sandbox/{id}/snapshot` (`CreateSandboxSnapshot`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSnapshotRequest {
    /// Snapshot name; unique within the organization.
    pub name: String,
    /// Capture VM memory ("hot snapshot"); the sandbox must be started. VM classes only.
    pub include_memory: bool,
}

/// Request body of `POST /sandbox/{id}/fork` (`ForkSandbox`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ForkRequest {
    /// Child name; Daytona generates one when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Request body of `PUT /sandbox/{id}/labels` (`SandboxLabels`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct LabelsBody {
    labels: BTreeMap<String, String>,
}

/// One Daytona snapshot (`SnapshotDto`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    /// Snapshot identity.
    pub id: String,
    /// Snapshot name.
    #[serde(default)]
    pub name: Option<String>,
    /// Snapshot state (`pending`, `snapshotting`, `active`, `error`, ...).
    #[serde(default)]
    pub state: Option<String>,
    /// Class of the sandboxes this snapshot boots.
    #[serde(default)]
    pub sandbox_class: Option<String>,
    /// Sandbox the snapshot was taken from, for snapshots created from a sandbox.
    #[serde(default)]
    pub source_sandbox_id: Option<String>,
    /// Failure detail when `state` is `error`.
    #[serde(default)]
    pub error_reason: Option<String>,
    /// RFC 3339 creation timestamp.
    #[serde(default)]
    pub created_at: Option<String>,
    /// Fields this client does not model.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// `GET /sandbox` page (`ListSandboxesResponse`).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SandboxPage {
    items: Vec<Sandbox>,
    #[serde(default)]
    next_cursor: Option<String>,
}

/// Request body of the toolbox `POST /process/execute`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ExecuteRequest {
    /// Shell command line.
    pub command: String,
    /// Working directory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Timeout in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u32>,
}

/// Response of the toolbox `POST /process/execute`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteResponse {
    /// Process exit code.
    #[serde(default)]
    pub exit_code: Option<i64>,
    /// Combined output.
    #[serde(default)]
    pub result: Option<String>,
    /// Fields this client does not model.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Authenticated client bound to one Daytona API base URL.
#[derive(Clone)]
pub struct DaytonaApi {
    http: reqwest::Client,
    base: String,
    api_key: String,
    organization_id: Option<String>,
    toolbox_proxy_hosts: Vec<String>,
}

impl std::fmt::Debug for DaytonaApi {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaytonaApi")
            .field("base", &self.base)
            .field("organization_id", &self.organization_id)
            .finish_non_exhaustive()
    }
}

impl DaytonaApi {
    /// Builds a client from the provider configuration.
    ///
    /// # Errors
    /// Returns [`ProviderError::Invalid`] when the API key is empty or the HTTP client cannot
    /// be built.
    pub fn new(config: &DaytonaConfig) -> Result<Self, ProviderError> {
        if config.api_key.is_empty() {
            return Err(ProviderError::Invalid("Daytona API key is empty".into()));
        }
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|error| ProviderError::Invalid(format!("HTTP client: {error}")))?;
        Ok(Self {
            http,
            base: config.api_url.trim_end_matches('/').to_owned(),
            api_key: config.api_key.clone(),
            organization_id: config.organization_id.clone(),
            toolbox_proxy_hosts: config.toolbox_proxy_hosts.clone(),
        })
    }

    /// Base URL the client was bound to, without a trailing slash.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base
    }

    fn authorized(&self, builder: RequestBuilder) -> RequestBuilder {
        let builder = builder.bearer_auth(&self.api_key);
        match &self.organization_id {
            Some(organization) => builder.header(ORGANIZATION_HEADER, organization),
            None => builder,
        }
    }

    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        self.authorized(self.http.request(method, format!("{}{path}", self.base)))
    }

    async fn send(&self, builder: RequestBuilder) -> Result<String, ProviderError> {
        let response = builder
            .send()
            .await
            .map_err(|error| transport_error(&error))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| transport_error(&error))?;
        if status.is_success() {
            Ok(body)
        } else {
            Err(status_error(status, &body))
        }
    }

    async fn json<T: for<'de> Deserialize<'de>>(
        &self,
        builder: RequestBuilder,
    ) -> Result<T, ProviderError> {
        let body = self.send(builder).await?;
        serde_json::from_str(&body).map_err(|error| {
            tracing::warn!(%error, body_len = body.len(), "Daytona response did not match the expected shape");
            ProviderError::Rejected(format!("Daytona response shape: {error}"))
        })
    }

    async fn empty(&self, builder: RequestBuilder) -> Result<(), ProviderError> {
        self.send(builder).await.map(|_| ())
    }

    /// Creates a sandbox: `POST /sandbox` with a [`CreateSandboxRequest`]. Live-verified.
    ///
    /// # Errors
    /// Maps transport failures and non-2xx statuses to [`ProviderError`].
    pub async fn create(&self, request: &CreateSandboxRequest) -> Result<Sandbox, ProviderError> {
        self.json(self.request(Method::POST, "/sandbox").json(request))
            .await
    }

    /// Reads one sandbox by id or name: `GET /sandbox/{idOrName}`. Live-verified.
    ///
    /// # Errors
    /// Maps transport failures and non-2xx statuses to [`ProviderError`]; 404 is
    /// [`ProviderError::NotFound`].
    pub async fn get(&self, id_or_name: &str) -> Result<Sandbox, ProviderError> {
        self.json(self.request(Method::GET, &format!("/sandbox/{id_or_name}")))
            .await
    }

    /// Lists sandboxes matching every given label: `GET /sandbox?labels={json}&limit=200`,
    /// following `nextCursor` to the end. Daytona documents this listing as eventually
    /// consistent. Live-verified.
    ///
    /// # Errors
    /// Maps transport failures and non-2xx statuses to [`ProviderError`].
    pub async fn list(
        &self,
        labels: Option<&BTreeMap<String, String>>,
    ) -> Result<Vec<Sandbox>, ProviderError> {
        let encoded = labels
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| ProviderError::Invalid(format!("label filter: {error}")))?;
        let mut sandboxes = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_LIST_PAGES {
            let mut query = vec![("limit", LIST_PAGE_LIMIT.to_owned())];
            if let Some(encoded) = &encoded {
                query.push(("labels", encoded.clone()));
            }
            if let Some(cursor) = cursor.take() {
                query.push(("cursor", cursor));
            }
            let page: SandboxPage = self
                .json(self.request(Method::GET, "/sandbox").query(&query))
                .await?;
            sandboxes.extend(page.items);
            match page.next_cursor.filter(|value| !value.is_empty()) {
                Some(next) => cursor = Some(next),
                None => return Ok(sandboxes),
            }
        }
        Err(ProviderError::Rejected(
            "Daytona sandbox listing did not terminate".into(),
        ))
    }

    /// Starts a stopped or archived sandbox, or resumes a paused one:
    /// `POST /sandbox/{id}/start`. Daytona has no separate resume endpoint.
    ///
    /// # Errors
    /// Maps transport failures and non-2xx statuses to [`ProviderError`].
    pub async fn start(&self, id: &str) -> Result<(), ProviderError> {
        self.empty(self.request(Method::POST, &format!("/sandbox/{id}/start")))
            .await
    }

    /// Stops a sandbox (filesystem kept, memory discarded): `POST /sandbox/{id}/stop`.
    ///
    /// # Errors
    /// Maps transport failures and non-2xx statuses to [`ProviderError`].
    pub async fn stop(&self, id: &str) -> Result<(), ProviderError> {
        self.empty(self.request(Method::POST, &format!("/sandbox/{id}/stop")))
            .await
    }

    /// Pauses a started VM sandbox, retaining memory: `POST /sandbox/{id}/pause`. Container
    /// sandboxes reject it.
    ///
    /// # Errors
    /// Maps transport failures and non-2xx statuses to [`ProviderError`].
    pub async fn pause(&self, id: &str) -> Result<(), ProviderError> {
        self.empty(self.request(Method::POST, &format!("/sandbox/{id}/pause")))
            .await
    }

    /// Starts a snapshot of a sandbox: `POST /sandbox/{id}/snapshot` with a
    /// [`CreateSnapshotRequest`]. Daytona answers with the *source sandbox* (now
    /// `snapshotting`), not the snapshot; read the snapshot back by name with
    /// [`Self::get_snapshot`].
    ///
    /// # Errors
    /// Maps transport failures and non-2xx statuses to [`ProviderError`].
    pub async fn snapshot(
        &self,
        id: &str,
        request: &CreateSnapshotRequest,
    ) -> Result<Sandbox, ProviderError> {
        self.json(
            self.request(Method::POST, &format!("/sandbox/{id}/snapshot"))
                .json(request),
        )
        .await
    }

    /// Reads one snapshot by id or name: `GET /snapshots/{idOrName}`. Live-verified.
    ///
    /// # Errors
    /// Maps transport failures and non-2xx statuses to [`ProviderError`]; 404 is
    /// [`ProviderError::NotFound`].
    pub async fn get_snapshot(&self, id_or_name: &str) -> Result<Snapshot, ProviderError> {
        self.json(self.request(Method::GET, &format!("/snapshots/{id_or_name}")))
            .await
    }

    /// Deletes one snapshot by id: `DELETE /snapshots/{id}`.
    ///
    /// # Errors
    /// Maps transport failures and non-2xx statuses to [`ProviderError`].
    pub async fn delete_snapshot(&self, id: &str) -> Result<(), ProviderError> {
        self.empty(self.request(Method::DELETE, &format!("/snapshots/{id}")))
            .await
    }

    /// Forks a started VM sandbox, memory and disk, into one new sandbox:
    /// `POST /sandbox/{idOrName}/fork` with a [`ForkRequest`]; answers with the child. The
    /// child records its parent in Daytona's fork tree, and the parent cannot be deleted while
    /// it has live fork children.
    ///
    /// # Errors
    /// Maps transport failures and non-2xx statuses to [`ProviderError`].
    pub async fn fork(&self, id: &str, request: &ForkRequest) -> Result<Sandbox, ProviderError> {
        self.json(
            self.request(Method::POST, &format!("/sandbox/{id}/fork"))
                .json(request),
        )
        .await
    }

    /// Lists the direct fork children of a sandbox: `GET /sandbox/{idOrName}/forks`.
    ///
    /// # Errors
    /// Maps transport failures and non-2xx statuses to [`ProviderError`].
    pub async fn forks(&self, id: &str) -> Result<Vec<Sandbox>, ProviderError> {
        self.json(self.request(Method::GET, &format!("/sandbox/{id}/forks")))
            .await
    }

    /// Replaces a sandbox's labels: `PUT /sandbox/{idOrName}/labels`. Live-verified.
    ///
    /// # Errors
    /// Maps transport failures and non-2xx statuses to [`ProviderError`].
    pub async fn replace_labels(
        &self,
        id: &str,
        labels: BTreeMap<String, String>,
    ) -> Result<(), ProviderError> {
        self.empty(
            self.request(Method::PUT, &format!("/sandbox/{id}/labels"))
                .json(&LabelsBody { labels }),
        )
        .await
    }

    /// Deletes one sandbox in any state: `DELETE /sandbox/{idOrName}`. Live-verified.
    ///
    /// # Errors
    /// Maps transport failures and non-2xx statuses to [`ProviderError`].
    pub async fn delete(&self, id: &str) -> Result<(), ProviderError> {
        self.empty(self.request(Method::DELETE, &format!("/sandbox/{id}")))
            .await
    }

    /// Sets the idle auto-stop interval in minutes, zero disabling it:
    /// `POST /sandbox/{idOrName}/autostop/{minutes}`.
    ///
    /// # Errors
    /// Maps transport failures and non-2xx statuses to [`ProviderError`].
    pub async fn set_autostop(&self, id: &str, minutes: u32) -> Result<(), ProviderError> {
        self.empty(self.request(Method::POST, &format!("/sandbox/{id}/autostop/{minutes}")))
            .await
    }

    /// Sets the idle auto-pause interval in minutes, zero disabling it (VM classes only):
    /// `POST /sandbox/{idOrName}/autopause/{minutes}`.
    ///
    /// # Errors
    /// Maps transport failures and non-2xx statuses to [`ProviderError`].
    pub async fn set_autopause(&self, id: &str, minutes: u32) -> Result<(), ProviderError> {
        self.empty(self.request(Method::POST, &format!("/sandbox/{id}/autopause/{minutes}")))
            .await
    }

    /// Runs one command through a sandbox's toolbox proxy:
    /// `POST {toolboxProxyUrl}/{sandboxId}/process/execute`. Live-verified.
    ///
    /// The proxy URL is never taken from the caller: the sandbox is read back by id from this
    /// client's own API, and its `toolboxProxyUrl` must pass [`Self::check_toolbox_proxy`]
    /// before the API key is attached to a request to it.
    ///
    /// # Errors
    /// Returns [`ProviderError::Rejected`] when the sandbox reports no toolbox proxy URL or one
    /// outside Daytona's own domains, and maps transport failures and non-2xx statuses to
    /// [`ProviderError`].
    pub async fn execute(
        &self,
        sandbox_id: &str,
        request: &ExecuteRequest,
    ) -> Result<ExecuteResponse, ProviderError> {
        let sandbox = self.get(sandbox_id).await?;
        let proxy = sandbox.toolbox_proxy_url.as_deref().ok_or_else(|| {
            ProviderError::Rejected(format!("sandbox {} has no toolbox proxy URL", sandbox.id))
        })?;
        let proxy = self.check_toolbox_proxy(proxy)?;
        let url = format!(
            "{}/{}/process/execute",
            proxy.as_str().trim_end_matches('/'),
            sandbox.id
        );
        self.json(self.authorized(self.http.post(url)).json(request))
            .await
    }

    /// Accepts a toolbox proxy URL only when it may receive this client's API key: `https`
    /// (or the API's own scheme), no userinfo, and a host that is the API host, a subdomain of
    /// it (Daytona serves `proxy.app.daytona.io` for `app.daytona.io`), or one of the
    /// configured [`crate::DaytonaConfig::toolbox_proxy_hosts`]. A host equal to the API host
    /// must also use its port.
    ///
    /// # Errors
    /// Returns [`ProviderError::Rejected`] for any other URL.
    pub fn check_toolbox_proxy(&self, proxy: &str) -> Result<reqwest::Url, ProviderError> {
        let rejected = || {
            ProviderError::Rejected(format!(
                "toolbox proxy URL {proxy:?} is not a Daytona proxy for {}",
                self.base
            ))
        };
        let base = reqwest::Url::parse(&self.base).map_err(|_| rejected())?;
        let url = reqwest::Url::parse(proxy).map_err(|_| rejected())?;
        let (Some(base_host), Some(host)) = (base.host_str(), url.host_str()) else {
            return Err(rejected());
        };
        let host = host.to_ascii_lowercase();
        let base_host = base_host.to_ascii_lowercase();
        let scheme_ok = url.scheme() == "https" || url.scheme() == base.scheme();
        let userinfo = !url.username().is_empty() || url.password().is_some();
        let host_ok = if host == base_host {
            url.port_or_known_default() == base.port_or_known_default()
        } else {
            host.ends_with(&format!(".{base_host}"))
                || self
                    .toolbox_proxy_hosts
                    .iter()
                    .any(|allowed| allowed.eq_ignore_ascii_case(&host))
        };
        if scheme_ok && !userinfo && host_ok {
            Ok(url)
        } else {
            Err(rejected())
        }
    }
}

fn transport_error(error: &reqwest::Error) -> ProviderError {
    tracing::warn!(%error, "Daytona request failed at the transport level");
    ProviderError::Unavailable
}

/// Maps an HTTP status to the closest provider-boundary failure.
fn status_error(status: StatusCode, body: &str) -> ProviderError {
    let detail = body.chars().take(512).collect::<String>();
    match status {
        StatusCode::NOT_FOUND => ProviderError::NotFound(detail),
        StatusCode::CONFLICT => ProviderError::Conflict(detail),
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
            ProviderError::Invalid(detail)
        }
        StatusCode::TOO_MANY_REQUESTS => ProviderError::Unavailable,
        _ if status.is_server_error() => ProviderError::Unavailable,
        _ => ProviderError::Rejected(format!("{status}: {detail}")),
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;

    #[test]
    fn list_page_follows_the_specified_shape() {
        let page: SandboxPage =
            serde_json::from_str(include_str!("../tests/fixtures/sandbox_list.json")).unwrap();
        assert_eq!(page.items.len(), 4);
        assert_eq!(page.next_cursor, None);
        assert_eq!(page.items[0].sandbox_class.as_deref(), Some("linux-vm"));
        let next: SandboxPage =
            serde_json::from_str(r#"{"items":[{"id":"a"}],"nextCursor":"c2"}"#).unwrap();
        assert_eq!(next.next_cursor.as_deref(), Some("c2"));
    }

    #[test]
    fn request_bodies_match_the_specification() {
        let fork = serde_json::to_value(ForkRequest::default()).unwrap();
        assert_eq!(fork, serde_json::json!({}));
        let fork = serde_json::to_value(ForkRequest {
            name: Some("c".into()),
        })
        .unwrap();
        assert_eq!(fork, serde_json::json!({ "name": "c" }));
        let snapshot = serde_json::to_value(CreateSnapshotRequest {
            name: "s".into(),
            include_memory: true,
        })
        .unwrap();
        assert_eq!(
            snapshot,
            serde_json::json!({ "name": "s", "includeMemory": true })
        );
        let bare = serde_json::to_value(CreateSandboxRequest {
            snapshot: "daytona-small".into(),
            ..CreateSandboxRequest::default()
        })
        .unwrap();
        assert_eq!(
            bare,
            serde_json::json!({ "snapshot": "daytona-small", "labels": {} })
        );
        let labels = serde_json::to_value(LabelsBody {
            labels: BTreeMap::from([("a".to_owned(), "b".to_owned())]),
        })
        .unwrap();
        assert_eq!(labels, serde_json::json!({ "labels": { "a": "b" } }));
    }

    fn client(base: &str, extra_hosts: &[&str]) -> DaytonaApi {
        let mut config = DaytonaConfig::new("k");
        config.api_url = base.to_owned();
        config.toolbox_proxy_hosts = extra_hosts.iter().map(|&h| h.to_owned()).collect();
        DaytonaApi::new(&config).unwrap()
    }

    #[test]
    fn toolbox_proxy_must_be_a_daytona_host() {
        let api = client(crate::DEFAULT_API_URL, &["toolbox.eu.example"]);
        for accepted in [
            "https://proxy.app.daytona.io/toolbox",
            "https://app.daytona.io/toolbox",
            "https://toolbox.eu.example/toolbox",
        ] {
            assert!(api.check_toolbox_proxy(accepted).is_ok(), "{accepted}");
        }
        for rejected in [
            "https://attacker.example/toolbox",
            "https://app.daytona.io.attacker.example/",
            "https://evilapp.daytona.io/",
            "http://proxy.app.daytona.io/toolbox",
            "https://user:pw@proxy.app.daytona.io/",
            "https://app.daytona.io:8443/",
            "not a url",
        ] {
            assert!(
                matches!(
                    api.check_toolbox_proxy(rejected),
                    Err(ProviderError::Rejected(_))
                ),
                "{rejected}"
            );
        }
        let local = client("http://127.0.0.1:9000/api", &[]);
        assert!(local.check_toolbox_proxy("http://127.0.0.1:9000/t").is_ok());
        assert!(
            local
                .check_toolbox_proxy("http://127.0.0.1:9001/t")
                .is_err()
        );
    }

    #[test]
    fn status_mapping_is_stable() {
        assert!(matches!(
            status_error(StatusCode::NOT_FOUND, ""),
            ProviderError::NotFound(_)
        ));
        assert!(matches!(
            status_error(StatusCode::CONFLICT, ""),
            ProviderError::Conflict(_)
        ));
        assert!(matches!(
            status_error(StatusCode::BAD_REQUEST, ""),
            ProviderError::Invalid(_)
        ));
        assert!(matches!(
            status_error(StatusCode::BAD_GATEWAY, ""),
            ProviderError::Unavailable
        ));
        assert!(matches!(
            status_error(StatusCode::UNAUTHORIZED, ""),
            ProviderError::Rejected(_)
        ));
    }
}

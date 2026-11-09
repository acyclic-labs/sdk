//! Daytona v0 REST adapter. Unsupported native guarantees are explicit.
use acyclic_auth::secrets::SecretRef;
use acyclic_harness_core::http::{
    AuthorizationScheme, CredentialResolver, CredentialSource, HttpRequest, HttpTransport,
};
use acyclic_machines::*;
use bytes::Bytes;
use futures::StreamExt as _;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use url::Url;

/// Protected provider configuration; no credential value or environment fallback.
#[derive(Clone, Debug)]
pub struct DaytonaConfig {
    /// REST API base, normally `https://app.daytona.io/api`.
    pub base_url: String,
    /// Canonical protected reference selected by the host.
    pub secret_reference: SecretRef,
    /// Host-admitted immutable image to Daytona snapshot mapping.
    pub images: BTreeMap<String, Image>,
    /// Actual host-admitted platform of those snapshots.
    pub platform: Platform,
    /// Host-admitted commitment to Daytona's block-all network policy.
    pub block_all_network_policy: [u8; 32],
}

/// One adapter over the same resolver and HTTP transport as model providers.
pub struct DaytonaProvider {
    config: DaytonaConfig,
    resolver: Arc<dyn CredentialResolver>,
    transport: Arc<dyn HttpTransport>,
    attempts: futures::lock::Mutex<BTreeMap<IdempotencyKey, ([u8; 32], Option<MutationOutcome>)>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Sandbox {
    id: String,
    state: String,
    labels: BTreeMap<String, String>,
    toolbox_proxy_url: Option<String>,
    created_at: String,
    updated_at: String,
}

fn timestamp(value: &str) -> Result<u64, ProviderError> {
    let time = time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .map_err(|_| invalid("invalid sandbox timestamp"))?;
    u64::try_from(time.unix_timestamp_nanos() / 1_000_000)
        .map_err(|_| invalid("sandbox timestamp precedes epoch"))
}

fn invalid(message: &str) -> ProviderError {
    ProviderError::Rejected(format!(
        "{message}; recovery: retain the admitted Daytona binding"
    ))
}

impl DaytonaConfig {
    /// Starts a protected selection with the standard API origin. Images and
    /// network policy must be enrolled explicitly before provisioning.
    pub fn new(secret_reference: SecretRef, platform: Platform) -> Self {
        Self {
            base_url: "https://app.daytona.io/api".into(),
            secret_reference,
            platform,
            images: BTreeMap::new(),
            block_all_network_policy: [0; 32],
        }
    }
}

impl DaytonaProvider {
    /// Selects existing protected authority without resolving its secret value.
    pub fn new(
        config: DaytonaConfig,
        resolver: Arc<dyn CredentialResolver>,
        transport: Arc<dyn HttpTransport>,
    ) -> Result<Self, ProviderError> {
        let provider = Self {
            config,
            resolver,
            transport,
            attempts: futures::lock::Mutex::new(BTreeMap::new()),
        };
        provider.bound(&provider.config.base_url)?;
        Ok(provider)
    }

    fn bound(&self, endpoint: &str) -> Result<Arc<dyn HttpTransport>, ProviderError> {
        let url = Url::parse(endpoint).map_err(|_| invalid("invalid Daytona URL"))?;
        if url.scheme() != "https"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid(
                "Daytona URL must be HTTPS without user information or fragment",
            ));
        }
        let origin = url.origin().ascii_serialization();
        self.resolver
            .bind(
                CredentialSource::SecretReference(&self.config.secret_reference),
                &origin,
                AuthorizationScheme::Bearer,
                self.transport.clone(),
            )
            .and_then(|binding| binding.protected_transport())
            .map_err(|_| invalid("Daytona origin is outside the protected binding's allowed set"))
    }

    async fn exchange(
        &self,
        method: http::Method,
        url: String,
        body: Bytes,
        content_type: &str,
    ) -> Result<Bytes, ProviderError> {
        let transport = self.bound(&url)?;
        let response = transport
            .send(HttpRequest {
                method,
                url,
                headers: BTreeMap::from([("content-type".into(), content_type.into())]),
                body,
            })
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        match response.status {
            200..=299 => (),
            404 => return Err(ProviderError::NotFound("Daytona resource".into())),
            409 => return Err(ProviderError::Conflict("Daytona resource".into())),
            300..=399 => return Err(invalid("Daytona redirect refused")),
            500..=599 => return Err(ProviderError::Unavailable),
            _ => return Err(invalid("Daytona exchange refused")),
        }
        let mut stream = response.body;
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk.map_err(|_| ProviderError::Unavailable)?);
        }
        Ok(bytes.into())
    }

    async fn api(
        &self,
        method: http::Method,
        path: &str,
        body: Value,
    ) -> Result<Bytes, ProviderError> {
        self.exchange(
            method.clone(),
            format!("{}{path}", self.config.base_url.trim_end_matches('/')),
            if method == http::Method::GET {
                Bytes::new()
            } else {
                Bytes::from(body.to_string())
            },
            "application/json",
        )
        .await
    }

    fn decode(bytes: &[u8]) -> Result<Sandbox, ProviderError> {
        serde_json::from_slice(bytes).map_err(|_| invalid("invalid Daytona sandbox response"))
    }

    fn observation(sandbox: &Sandbox) -> Result<MachineObservation, ProviderError> {
        let contract = sandbox
            .labels
            .get("acyclic.contract.v1")
            .ok_or_else(|| invalid("sandbox has no v1 admitted contract"))?;
        Ok(MachineObservation {
            id: MachineId::parse(&sandbox.id).map_err(|_| invalid("invalid sandbox identity"))?,
            state: match sandbox.state.as_str() {
                "started" => MachineState::Running,
                "creating" | "starting" => MachineState::Starting,
                "destroyed" => MachineState::Destroyed,
                "error" | "build_failed" => MachineState::Failed,
                _ => MachineState::Indeterminate,
            },
            contract: serde_json::from_str(contract)
                .map_err(|_| invalid("invalid v1 sandbox contract"))?,
            endpoints: Vec::new(),
            last_checkpoint: None,
            created_at_unix_ms: timestamp(&sandbox.created_at)?,
            changed_at_unix_ms: timestamp(&sandbox.updated_at)?,
        })
    }

    async fn sandbox(&self, machine: MachineId) -> Result<Sandbox, ProviderError> {
        let sandbox = Self::decode(
            &self
                .api(
                    http::Method::GET,
                    &format!("/sandbox/{machine}"),
                    Value::Null,
                )
                .await?,
        )?;
        if MachineId::parse(&sandbox.id).map_err(|_| invalid("invalid sandbox identity"))?
            != machine
        {
            return Err(invalid("Daytona substituted sandbox identity"));
        }
        Ok(sandbox)
    }

    async fn toolbox(
        &self,
        machine: MachineId,
        route: &str,
        access: &FileAccess,
    ) -> Result<String, ProviderError> {
        if self
            .transport
            .unix_ms()
            .map_err(|_| invalid("file deadline clock unavailable"))?
            >= access.deadline_unix_ms()
        {
            return Err(ProviderError::Cancelled);
        }
        let sandbox = self.sandbox(machine).await?;
        let proxy = sandbox
            .toolbox_proxy_url
            .ok_or_else(|| invalid("sandbox has no toolboxProxyUrl"))?;
        self.bound(&proxy)?;
        let mut url = Url::parse(&format!(
            "{}/{machine}/{route}",
            proxy.trim_end_matches('/')
        ))
        .map_err(|_| invalid("invalid toolboxProxyUrl"))?;
        url.query_pairs_mut().append_pair("path", access.path());
        Ok(url.into())
    }

    async fn file_until<T>(
        &self,
        machine: MachineId,
        access: &FileAccess,
        future: impl std::future::Future<Output = Result<T, ProviderError>>,
    ) -> Result<T, ProviderError> {
        let now = self
            .transport
            .unix_ms()
            .map_err(|_| invalid("file deadline clock unavailable"))?;
        let remaining = access
            .deadline_unix_ms()
            .checked_sub(now)
            .filter(|remaining| *remaining > 0)
            .ok_or(ProviderError::Cancelled)?;
        let timer = self
            .transport
            .delay(std::time::Duration::from_millis(remaining));
        match futures::future::select(Box::pin(future), timer).await {
            futures::future::Either::Left((result, _)) => result,
            futures::future::Either::Right((Err(_), _)) => Err(ProviderError::DeadlineSourceLost {
                machine: Some(machine),
                cause: DeadlineSourceLost(Arc::new(std::io::Error::other(
                    "HTTP alarm producer lost",
                ))),
            }),
            futures::future::Either::Right((Ok(()), _)) => {
                Err(ProviderError::FileIndeterminate(machine))
            }
        }
    }

    fn snapshot(&self, image: &Image) -> Result<&str, ProviderError> {
        self.config
            .images
            .iter()
            .find_map(|(name, candidate)| (candidate == image).then_some(name.as_str()))
            .ok_or_else(|| invalid("image has no admitted immutable Daytona snapshot"))
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl MachinesProvider for DaytonaProvider {
    fn assurance(&self) -> ProviderAssurance {
        ProviderAssurance::CustomerHosted
    }
    fn capability(&self, capability: ProviderCapability) -> Support {
        match capability {
            ProviderCapability::WholeFile | ProviderCapability::RequestResponseExec => {
                Support::Supported
            }
            _ => Support::Unsupported,
        }
    }
    async fn read_file(
        &self,
        _machine: MachineId,
        _access: FileAccess,
        _limit: u32,
    ) -> Result<FileRead, ProviderError> {
        Err(ProviderError::Unsupported("Daytona has no admitted native range-read/errno contract; recovery: use the shared whole-file workspace seam".into()))
    }
    async fn write_file(
        &self,
        _machine: MachineId,
        _access: FileAccess,
        _data: Vec<u8>,
        _truncate: bool,
    ) -> Result<FileWrite, ProviderError> {
        Err(ProviderError::Unsupported("Daytona has no native range-write; recovery: use the shared whole-file workspace seam under exclusive ownership".into()))
    }
    async fn download_file(
        &self,
        machine: MachineId,
        access: FileAccess,
    ) -> Result<FileRead, ProviderError> {
        if access.offset() != 0 {
            return Err(invalid("whole-file download requires offset zero"));
        }
        self.file_until(machine, &access, async {
            let url = self.toolbox(machine, "files/download", &access).await?;
            self.exchange(
                http::Method::GET,
                url,
                Bytes::new(),
                "application/octet-stream",
            )
            .await
            .map(|bytes| FileRead::Data(bytes.to_vec()))
        })
        .await
    }
    async fn upload_file(
        &self,
        machine: MachineId,
        access: FileAccess,
        data: Vec<u8>,
    ) -> Result<(), ProviderError> {
        if access.offset() != 0 {
            return Err(invalid("whole-file upload requires offset zero"));
        }
        self.file_until(machine, &access, async {
        let url = self.toolbox(machine, "files/upload-v2", &access).await?;
        // The multipart boundary cannot occur in an encoded binary payload: choose
        // the first absent candidate rather than imposing a content restriction.
        let mut boundary = "acyclic".to_owned();
        while data.windows(boundary.len()).any(|part| part == boundary.as_bytes()) { boundary.push('x'); }
        let mut body = format!("--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"upload\"\r\nContent-Type: application/octet-stream\r\n\r\n").into_bytes();
        body.extend(data);
        body.extend(format!("\r\n--{boundary}--\r\n").as_bytes());
        self.exchange(http::Method::POST, url, body.into(), &format!("multipart/form-data; boundary={boundary}")).await.map(|_| ()).map_err(|error| match error { ProviderError::Unavailable => ProviderError::FileIndeterminate(machine), error => error })
        }).await
    }
    async fn exec(
        &self,
        _machine: MachineId,
        _command: ExecCommand,
    ) -> Result<ExecStream, ProviderError> {
        Err(ProviderError::Unsupported("Daytona v0 text sessions do not supply binary duplex exec and terminal cleanup receipts; recovery: select an admitted duplex provider".into()))
    }
    async fn exec_request(
        &self,
        machine: MachineId,
        command: ExecCommand,
    ) -> Result<TextExecResult, ProviderError> {
        if !command.stdin().is_empty() {
            return Err(ProviderError::Unsupported(
                "Daytona request/response exec has no stdin; recovery: upload input as a file"
                    .into(),
            ));
        }
        let now = self
            .transport
            .unix_ms()
            .map_err(|_| invalid("exec deadline clock unavailable"))?;
        let remaining = command
            .expiry_unix_ms()
            .map(|expiry| expiry.saturating_sub(now));
        let timeout = match (remaining, command.timeout_ms()) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        if timeout == Some(0) {
            return Err(ProviderError::Cancelled);
        }
        let deadline = timeout.map(|ms| now.saturating_add(ms));
        let exchange = async {
            // Lookup consumes the same budget as dispatch. A delayed lookup
            // must never start a command after its original authority expires.
            let sandbox = self.sandbox(machine).await?;
            let proxy = sandbox
                .toolbox_proxy_url
                .ok_or_else(|| invalid("sandbox has no toolboxProxyUrl"))?;
            let url = format!("{}/{machine}/process/execute", proxy.trim_end_matches('/'));
            self.bound(&url)?;
            let remaining = match deadline {
                Some(deadline) => {
                    let now = self
                        .transport
                        .unix_ms()
                        .map_err(|_| invalid("exec deadline clock unavailable"))?;
                    if now >= deadline {
                        return Err(ProviderError::Cancelled);
                    }
                    Some(deadline - now)
                }
                None => None,
            };
            let quote = |word: &String| format!("'{}'", word.replace('\'', "'\\''"));
            let body = json!({"command": command.argv().iter().map(quote).collect::<Vec<_>>().join(" "), "cwd": command.cwd(), "timeout": remaining.map(|ms| ms.div_ceil(1000))});
            let bytes = self
                .exchange(
                    http::Method::POST,
                    url,
                    Bytes::from(body.to_string()),
                    "application/json",
                )
                .await
                .map_err(|_| ProviderError::CommandIndeterminate(machine))?;
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct Receipt {
                exit_code: i32,
                result: String,
            }
            let receipt: Receipt = serde_json::from_slice(&bytes)
                .map_err(|_| ProviderError::CommandIndeterminate(machine))?;
            Ok(TextExecResult {
                output: receipt.result,
                exit_code: receipt.exit_code,
            })
        };
        match timeout {
            None => exchange.await,
            Some(ms) => match futures::future::select(
                Box::pin(exchange),
                self.transport.delay(std::time::Duration::from_millis(ms)),
            )
            .await
            {
                futures::future::Either::Left((result, _)) => result,
                futures::future::Either::Right(_) => {
                    Err(ProviderError::CommandIndeterminate(machine))
                }
            },
        }
    }
    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError> {
        self.snapshot(&image)?;
        Ok(ImageQualification {
            image,
            capabilities: Default::default(),
            compatibility_revision: [1; 32],
        })
    }
    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError> {
        let key = request.idempotency_key;
        let digest = intent_digest(&request)?;
        let image = request
            .image
            .clone()
            .ok_or_else(|| invalid("Daytona requires an admitted immutable image"))?;
        let snapshot = self.snapshot(&image)?;
        if request.selected_volume_ref.is_some()
            || request.suspension != SuspensionPolicy::Manual
            || request.expiration != ExpirationPolicy::Never
            || request.budgets != Budgets::default()
            || matches!(&request.compatibility, CompatibilityPolicy::Require(required) if !required.is_empty())
        {
            return Err(ProviderError::Unsupported("Daytona create cannot honor requested native mechanisms; recovery: use manual lifetime, copied workspace and supported capabilities".into()));
        }
        if request.network_policy_digest == [0; 32]
            || request.network_policy_digest != self.config.block_all_network_policy
        {
            return Err(invalid("network policy is not admitted"));
        }
        let contract = MachineContract {
            image,
            selected_volume_ref: None,
            capabilities: Default::default(),
            compatibility: request.compatibility.clone(),
            compatibility_revision: [1; 32],
            suspension: request.suspension,
            expiration: request.expiration,
            network_policy_digest: request.network_policy_digest,
            budgets: request.budgets,
            platform: self.config.platform,
            lifecycle: LifecycleClass::Provisioned,
        };
        {
            let mut attempts = self.attempts.lock().await;
            if let Some((previous, outcome)) = attempts.get(&key) {
                if previous != &digest {
                    return Err(ProviderError::Conflict("idempotency key rebound".into()));
                }
                return outcome.clone().ok_or(ProviderError::Indeterminate(key));
            }
            attempts.insert(key, (digest, None));
        }
        let intent =
            serde_json::to_string(&request).map_err(|_| invalid("create intent cannot encode"))?;
        let encoded =
            serde_json::to_string(&contract).map_err(|_| invalid("contract cannot encode"))?;
        let body = json!({"name": format!("acyclic-{key}"), "snapshot": snapshot, "labels": {"acyclic.key": key.to_string(), "acyclic.contract.v1": encoded, "acyclic.request.v1": intent}, "networkBlockAll": true, "autoStopInterval": 0, "autoDeleteInterval": -1});
        let bytes = self
            .api(http::Method::POST, "/sandbox", body)
            .await
            .map_err(|_| ProviderError::Indeterminate(key))?;
        let sandbox = Self::decode(&bytes).map_err(|_| ProviderError::Indeterminate(key))?;
        let mut observation =
            Self::observation(&sandbox).map_err(|_| ProviderError::Indeterminate(key))?;
        if observation.state != MachineState::Running {
            observation = self
                .inspect_machine(observation.id)
                .await
                .map_err(|_| ProviderError::Indeterminate(key))?;
        }
        if observation.state != MachineState::Running || observation.contract != contract {
            return Err(ProviderError::Indeterminate(key));
        }
        let outcome = MutationOutcome::Created(observation);
        self.attempts
            .lock()
            .await
            .insert(key, (digest, Some(outcome.clone())));
        Ok(outcome)
    }
    async fn inspect_machine(
        &self,
        machine: MachineId,
    ) -> Result<MachineObservation, ProviderError> {
        Self::observation(&self.sandbox(machine).await?)
    }
    async fn list_machines(
        &self,
        _after: Option<MachineId>,
        _limit: u32,
    ) -> Result<MachinePage, ProviderError> {
        Err(ProviderError::Unsupported(
            "Daytona has no admitted stable machine cursor; recovery: inspect the selected machine"
                .into(),
        ))
    }
    async fn destroy_machine(
        &self,
        _machine: MachineId,
        _key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        Err(ProviderError::Unsupported("Daytona deletion has no retained terminal observation; recovery: use the owning Daytona control plane".into()))
    }
    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        if let Some((_, Some(outcome))) = self.attempts.lock().await.get(&key) {
            return Ok(outcome.clone());
        }
        let labels = json!({"acyclic.key": key.to_string()}).to_string();
        let query: String = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("labels", &labels)
            .finish();
        let bytes = self
            .api(http::Method::GET, &format!("/sandbox?{query}"), Value::Null)
            .await?;
        let page: Value =
            serde_json::from_slice(&bytes).map_err(|_| invalid("invalid Daytona recovery page"))?;
        let items = page
            .get("items")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("invalid Daytona recovery items"))?;
        if items.len() != 1 {
            return Err(ProviderError::Indeterminate(key));
        }
        let sandbox: Sandbox = serde_json::from_value(
            items
                .first()
                .cloned()
                .ok_or(ProviderError::Indeterminate(key))?,
        )
        .map_err(|_| invalid("invalid recovery sandbox"))?;
        if sandbox.labels.get("acyclic.key") != Some(&key.to_string()) {
            return Err(invalid("recovery key substituted"));
        }
        let request: CreateMachine = serde_json::from_str(
            sandbox
                .labels
                .get("acyclic.request.v1")
                .ok_or_else(|| invalid("recovery intent absent"))?,
        )
        .map_err(|_| invalid("invalid v1 recovery intent"))?;
        let digest = intent_digest(&request)?;
        if request.idempotency_key != key
            || self
                .attempts
                .lock()
                .await
                .get(&key)
                .is_some_and(|(expected, _)| expected != &digest)
        {
            return Err(ProviderError::Conflict("recovery intent differs".into()));
        }
        let observation = Self::observation(&sandbox)?;
        if observation.state != MachineState::Running {
            return Err(ProviderError::Indeterminate(key));
        }
        let outcome = MutationOutcome::Created(observation);
        self.attempts
            .lock()
            .await
            .insert(key, (digest, Some(outcome.clone())));
        Ok(outcome)
    }
}

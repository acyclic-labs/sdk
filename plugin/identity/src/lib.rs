#![deny(unsafe_code)]
//! Customer identity commands, independent of the local workspace service.

mod agent;
mod mcp;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::{Client, StatusCode, Url};
use ring::rand::{SecureRandom as _, SystemRandom};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEVICE_GRANT: &str = "urn:ietf:params:oauth:grant-type:device_code";

/// Whether a command is handled by the customer identity service.
#[must_use]
pub fn is_command(command: &str) -> bool {
    matches!(
        command,
        "login" | "logout" | "signup" | "whoami" | "orgs" | "refresh" | "bearer" | "identity-mcp"
    )
}

#[derive(Debug)]
struct Error {
    code: String,
    message: String,
    status: Option<StatusCode>,
}

impl Error {
    fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_owned(),
            message: message.into(),
            status: None,
        }
    }

    fn http(code: &str, message: impl Into<String>, status: StatusCode) -> Self {
        Self {
            status: Some(status),
            ..Self::new(code, message)
        }
    }

    fn json(&self) -> Value {
        json!({"error": self.code, "error_description": self.message})
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::new("credential_error", error.to_string())
    }
}

#[derive(Deserialize, Serialize)]
struct Session {
    access_token: String,
    refresh_token: String,
    expires_at: u64,
}

#[derive(Deserialize)]
struct Tokens {
    access_token: String,
    refresh_token: String,
    token_type: String,
    expires_in: u64,
}

impl Tokens {
    fn session(self) -> Result<Session, Error> {
        if !self.token_type.eq_ignore_ascii_case("bearer")
            || self.access_token.is_empty()
            || self.refresh_token.is_empty()
            || self.expires_in == 0
        {
            return Err(Error::new(
                "invalid_response",
                "invalid session token response",
            ));
        }
        Ok(Session {
            access_token: self.access_token,
            refresh_token: self.refresh_token,
            expires_at: now()?
                .checked_add(self.expires_in)
                .ok_or_else(|| Error::new("invalid_response", "token expiry overflow"))?,
        })
    }
}

#[derive(Deserialize, Serialize)]
struct Credentials {
    endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    session: Option<Session>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent: Option<agent::Agent>,
}

struct Store {
    path: PathBuf,
    /// Unlocked explicitly when dropped: a forked child can retain the open
    /// file description until exec.
    _lock: acyclic_native_runtime::FileLock,
}

impl Store {
    fn open(path: &Path) -> Result<Self, Error> {
        let path = std::path::absolute(path)?;
        let parent = path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .ok_or_else(|| {
                Error::new("configuration_error", "credentials need a parent directory")
            })?;
        fs::create_dir_all(parent)?;
        let path = fs::canonicalize(parent)?.join(
            path.file_name()
                .ok_or_else(|| Error::new("configuration_error", "credentials need a filename"))?,
        );
        reject_alias(&path)?;
        let path = match fs::canonicalize(&path) {
            Ok(path) => path,
            Err(error) if error.kind() == io::ErrorKind::NotFound => path,
            Err(error) => return Err(error.into()),
        };
        let name = path
            .file_name()
            .ok_or_else(|| Error::new("configuration_error", "credentials need a filename"))?
            .to_string_lossy();
        if [".lock", ".next"]
            .iter()
            .any(|suffix| name.to_ascii_lowercase().ends_with(suffix))
        {
            return Err(Error::new(
                "configuration_error",
                "credential filenames ending in .lock or .next are reserved for sidecars",
            ));
        }
        #[cfg(windows)]
        if name.ends_with([' ', '.']) {
            return Err(Error::new(
                "configuration_error",
                "credential filenames must not end in a space or dot",
            ));
        }
        let lock = acyclic_native_runtime::FileLock::try_acquire(
            private_file(&sidecar(&path, ".lock"), false)?,
            acyclic_native_runtime::FileLockMode::Exclusive,
        )
        .map_err(|_| {
            Error::new(
                "credentials_busy",
                "another identity operation holds the credential lock; retry after it completes",
            )
        })?;
        Ok(Self { path, _lock: lock })
    }

    fn credentials(&self, endpoint: &str) -> Result<Credentials, Error> {
        reject_alias(&self.path)?;
        let bytes = fs::read(&self.path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                Error::new("login_required", "run acyclic-plugin login first")
            } else {
                error.into()
            }
        })?;
        let credentials: Credentials = serde_json::from_slice(&bytes)
            .map_err(|_| Error::new("credential_error", "invalid credentials file"))?;
        if credentials.endpoint != endpoint {
            return Err(Error::new(
                "endpoint_mismatch",
                "credentials belong to another API endpoint",
            ));
        }
        Ok(credentials)
    }

    fn login_agent(&self, endpoint: &str) -> Result<Option<agent::Agent>, Error> {
        if !self.path.exists() {
            return Ok(None);
        }
        reject_alias(&self.path)?;
        let value: Value = serde_json::from_slice(&fs::read(&self.path)?).map_err(|_| {
            Error::new(
                "credential_error",
                "invalid credentials file; use a new --credentials path to preserve its keys",
            )
        })?;
        let object = value
            .as_object()
            .ok_or_else(|| Error::new("credential_error", "invalid credentials file"))?;
        if object.get("agent").is_none_or(Value::is_null) {
            return Ok(None);
        }
        // An unreadable or foreign agent identity must never be overwritten by login.
        Ok(self.credentials(endpoint)?.agent)
    }

    fn login_agent_for(
        &self,
        endpoint: &str,
        device: &DeviceCode,
    ) -> Result<Option<agent::Agent>, Error> {
        let agent = self.login_agent(endpoint)?;
        let identity = agent
            .as_ref()
            .map(|agent| {
                agent
                    .registration()
                    .map(|registration| (registration.org_id.clone(), registration.key_id.clone()))
            })
            .transpose();
        if identity.as_ref().ok() != Some(&device.agent_identity) {
            return Err(Error::new(
                "credentials_changed",
                "agent changed while device approval was pending",
            ));
        }
        Ok(agent)
    }

    fn read(&self, endpoint: &str) -> Result<Session, Error> {
        self.credentials(endpoint)?
            .session
            .ok_or_else(|| Error::new("login_required", "run acyclic-plugin login first"))
    }

    fn write(&self, endpoint: &str, session: Session) -> Result<Session, Error> {
        let agent = if self.path.exists() {
            self.credentials(endpoint)?.agent
        } else {
            None
        };
        let credentials = Credentials {
            endpoint: endpoint.to_owned(),
            session: Some(session),
            agent,
        };
        self.save(&credentials)?;
        credentials
            .session
            .ok_or_else(|| Error::new("credential_error", "missing saved session"))
    }

    fn complete_login(
        &self,
        endpoint: &str,
        session: Session,
        device: &DeviceCode,
    ) -> Result<Session, Error> {
        let mut credentials = Credentials {
            endpoint: endpoint.to_owned(),
            session: None,
            agent: self.login_agent_for(endpoint, device)?,
        };
        if let Some((org_id, key_id)) = &device.upgrade {
            match &mut credentials.agent {
                Some(agent::Agent::Active { registration, .. })
                    if &registration.org_id == org_id && &registration.key_id == key_id =>
                {
                    registration.tier = device
                        .upgrade_tier
                        .clone()
                        .unwrap_or_else(|| "oauth".to_owned());
                }
                _ => {
                    return Err(Error::new(
                        "credentials_changed",
                        "agent changed while device approval was pending",
                    ));
                }
            }
        }
        credentials.session = Some(session);
        self.save(&credentials)?;
        credentials
            .session
            .ok_or_else(|| Error::new("credential_error", "missing saved session"))
    }

    fn save(&self, credentials: &Credentials) -> Result<(), Error> {
        reject_alias(&self.path)?;
        let bytes = serde_json::to_vec(credentials)
            .map_err(|_| Error::new("credential_error", "cannot serialize credentials"))?;
        let temporary = sidecar(&self.path, ".next");
        let mut file = private_file(&temporary, true)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        acyclic_native_runtime::durable_rename(
            &temporary,
            &self.path,
            acyclic_native_runtime::RenameMode::Replace,
            None,
        )?;
        Ok(())
    }

    fn clear(&self) -> Result<(), Error> {
        reject_alias(&self.path)?;
        if self.path.exists() {
            let mut credentials: Credentials = serde_json::from_slice(&fs::read(&self.path)?)
                .map_err(|_| Error::new("credential_error", "invalid credentials file"))?;
            if credentials.agent.is_some() {
                credentials.session = None;
                return self.save(&credentials);
            }
        }
        match fs::remove_file(&self.path) {
            Ok(()) => acyclic_native_runtime::sync_parent(
                self.path
                    .parent()
                    .ok_or_else(|| Error::new("credential_error", "missing parent"))?,
                acyclic_native_runtime::Durability::Full,
            )
            .map_err(Into::into),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

fn private_file(path: &Path, truncate: bool) -> io::Result<File> {
    reject_alias(path)?;
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(true)
        .truncate(truncate);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

fn reject_alias(path: &Path) -> io::Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "credentials and sidecars must be regular files, not aliases",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if metadata.nlink() != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "credential hard links are not supported",
            ));
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        let file = fs::OpenOptions::new()
            .access_mode(0)
            .custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)?;
        let links = link_count(&file)?;
        if links != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "credential hard links are not supported",
            ));
        }
    }
    Ok(())
}

fn now() -> Result<u64, Error> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| Error::new("clock_error", "clock precedes Unix epoch"))
}

#[derive(Deserialize)]
struct DeviceCode {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: Option<String>,
    expires_in: u64,
    #[serde(default = "default_interval")]
    interval: u64,
    #[serde(skip)]
    verifier: String,
    #[serde(skip)]
    upgrade: Option<(String, String)>,
    #[serde(skip)]
    agent_identity: Option<(String, String)>,
    #[serde(skip)]
    reconcile_upgrade: bool,
    #[serde(skip)]
    upgrade_tier: Option<String>,
}

fn random_secret() -> Result<String, Error> {
    let mut bytes = [0; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| Error::new("key_error", "secure randomness unavailable"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

fn default_interval() -> u64 {
    5
}

struct Identity {
    client: Client,
    endpoint: String,
}

impl Identity {
    fn new(endpoint: &str) -> Result<Self, Error> {
        let url = Url::parse(endpoint)
            .map_err(|_| Error::new("configuration_error", "invalid API endpoint"))?;
        let loopback = matches!(url.host_str(), Some("127.0.0.1" | "[::1]" | "localhost"));
        if (url.scheme() != "https" && !(url.scheme() == "http" && loopback))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
        {
            return Err(Error::new(
                "configuration_error",
                "API endpoint must be an HTTPS origin (HTTP loopback is allowed for tests)",
            ));
        }
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|_| Error::new("configuration_error", "cannot create HTTP client"))?;
        Ok(Self {
            client,
            endpoint: url.origin().ascii_serialization(),
        })
    }

    async fn response(request: reqwest::RequestBuilder) -> Result<Value, Error> {
        let response = request
            .send()
            .await
            .map_err(|_| Error::new("transport_error", "identity request failed"))?;
        Self::decode(response).await
    }

    async fn decode(response: reqwest::Response) -> Result<Value, Error> {
        let status = response.status();
        let bytes = response.bytes().await.map_err(|_| {
            Error::http(
                "transport_error",
                "identity response body download failed",
                status,
            )
        })?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| {
            Error::http(
                "invalid_response",
                format!("identity returned non-JSON (HTTP {status})"),
                status,
            )
        })?;
        if !status.is_success() {
            // Server descriptions can contain submitted credentials; only expose the error code.
            let code = value
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("identity_error");
            return Err(Error::http(
                code,
                format!("identity request refused (HTTP {status})"),
                status,
            ));
        }
        Ok(value)
    }

    async fn device_code(&self, kind: &str, store: &Store) -> Result<DeviceCode, Error> {
        let verifier = random_secret()?;
        let mut upgrade = None;
        let agent = store.login_agent(&self.endpoint)?;
        let agent_identity = agent
            .as_ref()
            .map(|agent| {
                agent
                    .registration()
                    .map(|registration| (registration.org_id.clone(), registration.key_id.clone()))
            })
            .transpose()?;
        let mut body = serde_json::Map::from_iter([
            ("client_kind".to_owned(), json!(kind)),
            ("code_challenge".to_owned(), json!(challenge(&verifier))),
        ]);
        if let Some(agent) = agent
            && agent.registration()?.tier == "agent"
        {
            let registration = agent.registration()?;
            upgrade = Some((registration.org_id.clone(), registration.key_id.clone()));
            body.insert("agent_org_id".to_owned(), json!(registration.org_id));
            body.insert("agent_key_id".to_owned(), json!(registration.key_id));
            body.insert(
                "agent_signature".to_owned(),
                json!(agent.sign(&format!(
                    "acyclic-agent-upgrade-v1\n{}\n{}\n{}",
                    registration.org_id,
                    registration.key_id,
                    challenge(&verifier)
                ))?),
            );
        }
        let request = |body: &serde_json::Map<String, Value>| {
            Self::response(
                self.client
                    .post(format!("{}/v1/identity/device/code", self.endpoint))
                    .json(body),
            )
        };
        let mut reconcile_upgrade = false;
        let value = match request(&body).await {
            Err(error)
                if upgrade.is_some()
                    && error.status == Some(StatusCode::UNAUTHORIZED)
                    && error.code == "invalid agent upgrade proof" =>
            {
                // A committed upgrade can outlive its token response or local save.
                body.remove("agent_org_id");
                body.remove("agent_key_id");
                body.remove("agent_signature");
                reconcile_upgrade = true;
                request(&body).await?
            }
            result => result?,
        };
        let mut device: DeviceCode = serde_json::from_value(value)
            .map_err(|_| Error::new("invalid_response", "invalid device code response"))?;
        if device.device_code.is_empty() || device.user_code.is_empty() || device.expires_in == 0 {
            return Err(Error::new(
                "invalid_response",
                "invalid device code response",
            ));
        }
        device.verifier = verifier;
        device.upgrade = upgrade;
        device.agent_identity = agent_identity;
        device.reconcile_upgrade = reconcile_upgrade;
        Ok(device)
    }

    async fn complete_login(
        &self,
        store: &Store,
        session: Session,
        device: &mut DeviceCode,
    ) -> Result<Session, Error> {
        if !device.reconcile_upgrade {
            return store.complete_login(&self.endpoint, session, device);
        }
        store.login_agent_for(&self.endpoint, device)?;
        // Keep issued tokens even if the organization read fails after login.
        let session = store.write(&self.endpoint, session)?;
        let value = Self::response(
            self.client
                .get(format!("{}/v1/identity/orgs", self.endpoint))
                .bearer_auth(&session.access_token),
        )
        .await?;
        let (org_id, _) = device
            .upgrade
            .as_ref()
            .ok_or_else(|| Error::new("invalid_response", "missing upgrade identity"))?;
        let tier = value
            .get("orgs")
            .and_then(Value::as_array)
            .and_then(|orgs| {
                orgs.iter()
                    .find(|org| org.get("id").and_then(Value::as_str) == Some(org_id))
            })
            .and_then(|org| org.get("tier").and_then(Value::as_str))
            .filter(|tier| matches!(*tier, "agent" | "oauth" | "usage"))
            .ok_or_else(|| Error::new("invalid_response", "missing agent organization tier"))?;
        device.upgrade_tier = Some(tier.to_owned());
        store.complete_login(&self.endpoint, session, device)
    }

    async fn token(&self, fields: &[(&str, &str)]) -> Result<Session, Error> {
        let value = Self::response(
            self.client
                .post(format!("{}/v1/identity/token", self.endpoint))
                .json(
                    &fields
                        .iter()
                        .copied()
                        .collect::<std::collections::BTreeMap<_, _>>(),
                ),
        )
        .await?;
        serde_json::from_value::<Tokens>(value)
            .map_err(|_| Error::new("invalid_response", "invalid token response"))?
            .session()
    }

    async fn login(
        &self,
        device: &mut DeviceCode,
        started: tokio::time::Instant,
        cancelled: impl std::future::Future<Output = ()>,
    ) -> Result<Session, Error> {
        let deadline = started
            .checked_add(Duration::from_secs(device.expires_in))
            .ok_or_else(|| Error::new("invalid_response", "device expiry overflow"))?;
        tokio::pin!(cancelled);
        loop {
            let sleep = Duration::from_secs(device.interval.max(1));
            if tokio::time::Instant::now()
                .checked_add(sleep)
                .is_none_or(|next| next >= deadline)
            {
                return Err(Error::new("expired_token", "device approval expired"));
            }
            let delay = tokio::time::sleep(sleep);
            #[cfg(test)]
            let delay = tests::poll_clock::observe(delay, sleep);
            tokio::select! {
                biased;
                () = &mut cancelled => return Err(Error::new("request_cancelled", "device polling cancelled")),
                () = delay => {}
            }
            // Once dispatched, a successful exchange must reach the credential store.
            let result = self
                .token(&[
                    ("grant_type", DEVICE_GRANT),
                    ("device_code", &device.device_code),
                    ("code_verifier", &device.verifier),
                ])
                .await;
            match result {
                Ok(session) => return Ok(session),
                Err(error) if error.code == "authorization_pending" => {}
                Err(error) if error.code == "slow_down" => {
                    device.interval = device.interval.max(1).saturating_add(5);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn refresh(&self, store: &Store, session: &Session) -> Result<Session, Error> {
        // Consume the on-disk token before dispatch so interruption cannot replay it.
        store.clear()?;
        let next = self
            .token(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", &session.refresh_token),
            ])
            .await?;
        store.write(&self.endpoint, next)
    }

    async fn logout(&self, store: &Store) -> Result<Value, Error> {
        let credentials = match store.credentials(&self.endpoint) {
            Ok(credentials) => credentials,
            Err(error) if error.code == "login_required" => {
                return Ok(json!({"authenticated": false}));
            }
            Err(error) => return Err(error),
        };
        let Some(mut session) = credentials.session else {
            return Ok(json!({"authenticated": false}));
        };
        let refreshed = session.expires_at <= now()?.saturating_add(30);
        if refreshed {
            session = self.refresh(store, &session).await?;
        }
        let request = |session: &Session| {
            self.client
                .post(format!("{}/v1/identity/revoke", self.endpoint))
                .bearer_auth(&session.access_token)
                .json(&json!({}))
        };
        let mut response = request(&session)
            .send()
            .await
            .map_err(|_| Error::new("transport_error", "identity revocation failed"))?;
        if response.status() == StatusCode::UNAUTHORIZED && !refreshed {
            session = self.refresh(store, &session).await?;
            response = request(&session)
                .send()
                .await
                .map_err(|_| Error::new("transport_error", "identity revocation failed"))?;
        }
        if response.status() != StatusCode::NO_CONTENT {
            Self::decode(response).await?;
            return Err(Error::new(
                "invalid_response",
                "invalid revocation response",
            ));
        }
        store.clear()?;
        Ok(json!({"authenticated": false}))
    }

    async fn get(&self, store: &Store, path: &str) -> Result<Value, Error> {
        self.authenticated_json(store, reqwest::Method::GET, path)
            .await
    }

    async fn authenticated_json(
        &self,
        store: &Store,
        method: reqwest::Method,
        path: &str,
    ) -> Result<Value, Error> {
        let mut session = store.read(&self.endpoint)?;
        let refreshed = session.expires_at <= now()?.saturating_add(30);
        if refreshed {
            session = self.refresh(store, &session).await?;
        }
        let request = |session: &Session| {
            let request = self
                .client
                .request(method.clone(), format!("{}{path}", self.endpoint))
                .bearer_auth(&session.access_token);
            if method == reqwest::Method::POST {
                request.json(&json!({}))
            } else {
                request
            }
        };
        let response = request(&session)
            .send()
            .await
            .map_err(|_| Error::new("transport_error", "identity request failed"))?;
        if response.status() == StatusCode::UNAUTHORIZED {
            if refreshed {
                return Err(Error::new(
                    "invalid_token",
                    "refreshed access token was refused",
                ));
            }
            session = self.refresh(store, &session).await?;
            return Self::response(request(&session)).await;
        }
        Self::decode(response).await
    }

    async fn bearer(&self, store: &Store, org: &str, output: &Path) -> Result<Value, Error> {
        let name = output
            .file_name()
            .ok_or_else(|| Error::new("usage_error", "bearer output needs a filename"))?
            .to_string_lossy()
            .to_ascii_lowercase();
        if name.ends_with([' ', '.'])
            || [".lock", ".next"]
                .iter()
                .any(|suffix| name.ends_with(suffix))
        {
            return Err(Error::new(
                "usage_error",
                "bearer output must not use reserved credential sidecar names",
            ));
        }
        let mut url = Url::parse(&self.endpoint)
            .map_err(|_| Error::new("configuration_error", "invalid API endpoint"))?;
        url.path_segments_mut()
            .map_err(|()| Error::new("configuration_error", "invalid API endpoint"))?
            .extend(["v1", "identity", "orgs", org, "bearer"]);
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        // Reserve a usable destination before spending a rotating session token.
        let mut cleanup = BearerOutputCleanup(None);
        let mut file = options.open(output)?;
        cleanup.0 = Some(output);
        let result = async {
            let value = self
                .authenticated_json(store, reqwest::Method::POST, url.path())
                .await?;
            let bearer = value
                .get("bearer")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty() && !value.chars().any(char::is_whitespace));
            let account = value
                .get("account_id")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty());
            let time = now()?;
            let expires = value
                .get("expires_at")
                .and_then(Value::as_u64)
                .filter(|expires| *expires > time && *expires <= time.saturating_add(3600));
            let (Some(bearer), Some(account), Some(expires)) = (bearer, account, expires) else {
                return Err(Error::new(
                    "invalid_response",
                    "invalid organization bearer response",
                ));
            };
            file.write_all(bearer.as_bytes())?;
            file.sync_all()?;
            Ok(json!({"account_id": account, "expires_at": expires, "bearer_file": output}))
        }
        .await;
        drop(file);
        if result.is_ok() {
            cleanup.0 = None;
        }
        result
    }
}

struct BearerOutputCleanup<'a>(Option<&'a Path>);

impl Drop for BearerOutputCleanup<'_> {
    fn drop(&mut self) {
        if let Some(path) = self.0 {
            // Cleanup is best-effort so the original request or write error survives.
            let _ = fs::remove_file(path);
        }
    }
}

struct Options {
    command: String,
    json: bool,
    endpoint: String,
    credentials: PathBuf,
    agent: bool,
    org: Option<String>,
    output: Option<PathBuf>,
}

impl Options {
    fn parse(arguments: &[String]) -> Result<Self, Error> {
        let mut arguments = arguments.iter();
        let command = arguments
            .next()
            .ok_or_else(|| Error::new("usage_error", "missing command"))?
            .clone();
        let mut endpoint = std::env::var("ACYCLIC_API_URL").ok();
        let mut credentials = std::env::var_os("ACYCLIC_IDENTITY_FILE").map(PathBuf::from);
        let mut json = false;
        let mut agent = false;
        let mut org = None;
        let mut output = None;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--json" => json = true,
                "--agent" if command == "signup" => agent = true,
                "--org" if command == "bearer" => {
                    org = Some(
                        arguments
                            .next()
                            .ok_or_else(|| Error::new("usage_error", "--org needs a value"))?
                            .clone(),
                    );
                }
                "--output" if command == "bearer" => {
                    output =
                        Some(PathBuf::from(arguments.next().ok_or_else(|| {
                            Error::new("usage_error", "--output needs a value")
                        })?));
                }
                "--endpoint" => {
                    endpoint = Some(
                        arguments
                            .next()
                            .ok_or_else(|| Error::new("usage_error", "--endpoint needs a value"))?
                            .clone(),
                    );
                }
                "--credentials" => {
                    credentials = Some(PathBuf::from(arguments.next().ok_or_else(|| {
                        Error::new("usage_error", "--credentials needs a value")
                    })?));
                }
                _ => {
                    return Err(Error::new(
                        "usage_error",
                        format!("unexpected argument: {argument}"),
                    ));
                }
            }
        }
        if command == "signup" && !agent {
            return Err(Error::new(
                "usage_error",
                "signup requires --agent; use login for browser approval",
            ));
        }
        if command == "bearer" && (org.as_ref().is_none_or(String::is_empty) || output.is_none()) {
            return Err(Error::new(
                "usage_error",
                "bearer requires --org ID and --output NEW_FILE",
            ));
        }
        let credentials = credentials
            .or_else(|| {
                std::env::var_os("USERPROFILE")
                    .or_else(|| std::env::var_os("HOME"))
                    .map(|home| PathBuf::from(home).join(".acyclic/identity.json"))
            })
            .ok_or_else(|| {
                Error::new(
                    "configuration_error",
                    "set --credentials or ACYCLIC_IDENTITY_FILE",
                )
            })?;
        let endpoint = endpoint.ok_or_else(|| {
            Error::new(
                "configuration_error",
                "set --endpoint or ACYCLIC_API_URL to the public Edge origin",
            )
        })?;
        Ok(Self {
            command,
            json,
            endpoint,
            credentials,
            agent,
            org,
            output,
        })
    }
}

/// Execute an identity command using its explicit command-line arguments.
///
/// The returned future keeps the command state machine inside this crate, so
/// callers compile only the shared future interface.
///
/// # Errors
/// Returns command, credential-store, transport and output errors.
pub fn run(
    arguments: &[String],
) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + '_>> {
    Box::pin(async move {
        if arguments
            .first()
            .is_some_and(|command| command == "identity-mcp")
        {
            return mcp::serve(arguments).await;
        }
        let json_output = arguments.iter().any(|argument| argument == "--json");
        let result = execute(arguments).await;
        match result {
            Ok(value) => {
                println!("{}", serde_json::to_string_pretty(&value)?);
                Ok(())
            }
            Err(error) => {
                if json_output {
                    println!("{}", error.json());
                }
                Err(io::Error::other(error))
            }
        }
    })
}

async fn execute(arguments: &[String]) -> Result<Value, Error> {
    let options = Options::parse(arguments)?;
    let identity = Identity::new(&options.endpoint)?;
    let store = Store::open(&options.credentials)?;
    match options.command.as_str() {
        "login" => {
            let mut device = identity.device_code("cli", &store).await?;
            if options.json {
                // Progress goes to stderr so stdout remains one machine-readable result.
                eprintln!(
                    "{}",
                    json!({"user_code": device.user_code,
                    "verification_uri": device.verification_uri,
                    "verification_uri_complete": device.verification_uri_complete})
                );
            } else {
                eprintln!(
                    "Open {} and enter {}",
                    device
                        .verification_uri_complete
                        .as_ref()
                        .unwrap_or(&device.verification_uri),
                    device.user_code
                );
            }
            let session = identity
                .login(
                    &mut device,
                    tokio::time::Instant::now(),
                    std::future::pending(),
                )
                .await?;
            let session = identity
                .complete_login(&store, session, &mut device)
                .await?;
            Ok(json!({"authenticated": true, "expires_at": session.expires_at}))
        }
        "refresh" => {
            if store.credentials(&identity.endpoint)?.session.is_none() {
                return identity.renew_agent(&store).await;
            }
            let session = store.read(&identity.endpoint)?;
            let session = identity.refresh(&store, &session).await?;
            Ok(json!({"authenticated": true, "expires_at": session.expires_at}))
        }
        "signup" if options.agent => identity.signup(&store).await,
        "whoami" => identity.get(&store, "/v1/identity/session").await,
        "orgs" => identity.get(&store, "/v1/identity/orgs").await,
        "logout" => identity.logout(&store).await,
        "bearer" => {
            identity
                .bearer(
                    &store,
                    options
                        .org
                        .as_deref()
                        .ok_or_else(|| Error::new("usage_error", "missing organization"))?,
                    options
                        .output
                        .as_deref()
                        .ok_or_else(|| Error::new("usage_error", "missing bearer output"))?,
                )
                .await
        }
        _ => Err(Error::new("usage_error", "unsupported identity command")),
    }
}

#[cfg(test)]
mod tests;

/// Query the link count from the open handle, including alias rejection on Windows.
#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "GetFileInformationByHandleEx initializes its matching fixed-size structure for a live handle"
)]
fn link_count(file: &File) -> io::Result<u32> {
    use std::os::windows::io::AsRawHandle as _;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_STANDARD_INFO, FileStandardInfo, GetFileInformationByHandleEx,
    };
    let mut information = std::mem::MaybeUninit::<FILE_STANDARD_INFO>::zeroed();
    let size =
        u32::try_from(std::mem::size_of::<FILE_STANDARD_INFO>()).map_err(io::Error::other)?;
    // SAFETY: the live handle, class, writable buffer and buffer size match FILE_STANDARD_INFO.
    if unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            FileStandardInfo,
            information.as_mut_ptr().cast(),
            size,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful call initialized the structure.
    Ok(unsafe { information.assume_init() }.NumberOfLinks)
}

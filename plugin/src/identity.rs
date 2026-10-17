//! Customer identity commands, independent of the local workspace service.

mod agent;
mod mcp;
mod storage;
use storage::Store;
#[cfg(test)]
use storage::sidecar;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use fs2::FileExt as _;
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

pub(crate) fn is_command(command: &str) -> bool {
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
            tokio::select! {
                biased;
                () = &mut cancelled => return Err(Error::new("request_cancelled", "device polling cancelled")),
                () = tokio::time::sleep(sleep) => {}
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

pub(crate) async fn run(arguments: &[String]) -> Result<(), io::Error> {
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

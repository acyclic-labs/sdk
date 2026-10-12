//! Customer identity commands, independent of the local workspace service.

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
    matches!(command, "login" | "whoami" | "orgs" | "refresh")
}

#[derive(Debug)]
struct Error {
    code: String,
    message: String,
}

impl Error {
    fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_owned(),
            message: message.into(),
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
    session: Session,
}

struct Store {
    path: PathBuf,
    _lock: File,
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
        let lock = private_file(&sidecar(&path, ".lock"), false)?;
        lock.try_lock_exclusive().map_err(|_| {
            Error::new(
                "credentials_busy",
                "another identity operation holds the credential lock; retry after it completes",
            )
        })?;
        Ok(Self { path, _lock: lock })
    }

    fn read(&self, endpoint: &str) -> Result<Session, Error> {
        reject_alias(&self.path)?;
        let bytes = fs::read(&self.path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                Error::new("login_required", "run acyclic login first")
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
        Ok(credentials.session)
    }

    fn write(&self, endpoint: &str, session: Session) -> Result<Session, Error> {
        reject_alias(&self.path)?;
        let credentials = Credentials {
            endpoint: endpoint.to_owned(),
            session,
        };
        let bytes = serde_json::to_vec(&credentials)
            .map_err(|_| Error::new("credential_error", "cannot serialize credentials"))?;
        let temporary = sidecar(&self.path, ".next");
        let mut file = private_file(&temporary, true)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        crate::durable_rename(&temporary, &self.path, crate::RenameMode::Replace)?;
        Ok(credentials.session)
    }

    fn clear(&self) -> Result<(), Error> {
        reject_alias(&self.path)?;
        match fs::remove_file(&self.path) {
            Ok(()) => crate::sync_parent(
                self.path
                    .parent()
                    .ok_or_else(|| Error::new("credential_error", "missing parent"))?,
                crate::Durability::Full,
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
        let parent = path.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "credentials need a parent")
        })?;
        let name = path.file_name().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "credentials need a filename")
        })?;
        let root = acyclic_fs::native_host::HostRoot::open(parent)?;
        if root.stat(Path::new(name))?.number_of_links() != Some(1) {
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

#[derive(Clone, Deserialize)]
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
        let value: Value = response.json().await.map_err(|_| {
            Error::new(
                "invalid_response",
                format!("identity returned non-JSON (HTTP {status})"),
            )
        })?;
        if !status.is_success() {
            // Server descriptions can contain submitted credentials; only expose the error code.
            let code = value
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("identity_error");
            return Err(Error::new(
                code,
                format!("identity request refused (HTTP {status})"),
            ));
        }
        Ok(value)
    }

    async fn device_code(&self, kind: &str, store: &Store) -> Result<DeviceCode, Error> {
        reject_alias(&store.path)?;
        let verifier = random_secret()?;
        let value = Self::response(
            self.client
                .post(format!("{}/v1/identity/device/code", self.endpoint))
                .json(&json!({"client_kind": kind, "code_challenge": challenge(&verifier)})),
        )
        .await?;
        let mut device: DeviceCode = serde_json::from_value(value)
            .map_err(|_| Error::new("invalid_response", "invalid device code response"))?;
        if device.device_code.is_empty() || device.user_code.is_empty() || device.expires_in == 0 {
            return Err(Error::new(
                "invalid_response",
                "invalid device code response",
            ));
        }
        device.verifier = verifier;
        Ok(device)
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

    async fn login(&self, device: &DeviceCode) -> Result<Session, Error> {
        let deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_secs(device.expires_in))
            .ok_or_else(|| Error::new("invalid_response", "device expiry overflow"))?;
        let mut interval = device.interval.max(1);
        loop {
            let sleep = Duration::from_secs(interval);
            if tokio::time::Instant::now()
                .checked_add(sleep)
                .is_none_or(|next| next >= deadline)
            {
                return Err(Error::new("expired_token", "device approval expired"));
            }
            tokio::time::sleep(sleep).await;
            let result = tokio::time::timeout_at(
                deadline,
                self.token(&[
                    ("grant_type", DEVICE_GRANT),
                    ("device_code", &device.device_code),
                    ("code_verifier", &device.verifier),
                ]),
            )
            .await
            .map_err(|_| Error::new("expired_token", "device approval expired"))?;
            match result {
                Ok(session) => return Ok(session),
                Err(error) if error.code == "authorization_pending" => {}
                Err(error) if error.code == "slow_down" => interval = interval.saturating_add(5),
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

    async fn get(&self, store: &Store, path: &str) -> Result<Value, Error> {
        let mut session = store.read(&self.endpoint)?;
        let refreshed = session.expires_at <= now()?.saturating_add(30);
        if refreshed {
            session = self.refresh(store, &session).await?;
        }
        let request = || {
            self.client
                .get(format!("{}{path}", self.endpoint))
                .bearer_auth(&session.access_token)
        };
        let response = request()
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
            return Self::response(
                self.client
                    .get(format!("{}{path}", self.endpoint))
                    .bearer_auth(&session.access_token),
            )
            .await;
        }
        Self::decode(response).await
    }
}

struct Options {
    command: String,
    json: bool,
    endpoint: String,
    credentials: PathBuf,
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
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--json" => json = true,
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
        })
    }
}

pub(crate) async fn run(arguments: &[String]) -> Result<(), io::Error> {
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
            let device = identity.device_code("cli", &store).await?;
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
            let session = identity.login(&device).await?;
            let session = store.write(&identity.endpoint, session)?;
            Ok(json!({"authenticated": true, "expires_at": session.expires_at}))
        }
        "refresh" => {
            let session = store.read(&identity.endpoint)?;
            let session = identity.refresh(&store, &session).await?;
            Ok(json!({"authenticated": true, "expires_at": session.expires_at}))
        }
        "whoami" => identity.get(&store, "/v1/identity/session").await,
        "orgs" => identity.get(&store, "/v1/identity/orgs").await,
        _ => Err(Error::new("usage_error", "unsupported identity command")),
    }
}

#[cfg(test)]
mod tests;

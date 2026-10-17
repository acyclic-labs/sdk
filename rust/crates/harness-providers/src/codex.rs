use crate::{HttpRequest, HttpTransport, invalid};
use acyclic_harness::Result;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::StreamExt as _;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    sync::OnceLock,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
#[path = "credential_io.rs"]
mod credential_io;
use acyclic_fs::{NativeAllocation, WorkCounters};
use credential_io::CredentialIo;

const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const MAX_AUTH_BYTES: u64 = 1024 * 1024;

/// Native-only reuse of an existing Codex CLI ChatGPT login.
/// Refreshes serialize in memory and defer to concurrent CLI writes.
pub struct CodexLogin {
    path: PathBuf,
    account: OnceLock<Option<blake3::Hash>>,
    refresh: crate::sync::Arc<crate::refresh::Refresh<RefreshFailure>>,
    clock: std::sync::Arc<dyn Fn() -> OffsetDateTime + Send + Sync>,
}

#[derive(Clone, Copy)]
struct RefreshFailure {
    code: acyclic_harness::wire::ErrorCode,
    status: Option<u16>,
    message: [u8; 256],
    length: usize,
}
impl RefreshFailure {
    fn from_error(error: &acyclic_harness::Error) -> Option<Self> {
        use acyclic_harness::Error;
        let (message, status) = match error {
            Error::ModelUnavailable { status, excerpt } => (excerpt.as_str(), *status),
            error => (
                match error {
                    Error::Unauthorized(message)
                    | Error::Storage(message)
                    | Error::Invalid(message)
                    | Error::NativeDiagnostic { message, .. } => message.as_str(),
                    Error::NativeBoundary { reason, .. } => reason,
                    _ => return None,
                },
                None,
            ),
        };
        if !matches!(
            error.code(),
            acyclic_harness::wire::ErrorCode::Unauthorized
                | acyclic_harness::wire::ErrorCode::Storage
                | acyclic_harness::wire::ErrorCode::Unavailable
                | acyclic_harness::wire::ErrorCode::Invalid
        ) || message.len() > 256
        {
            return None;
        }
        let mut failure = Self {
            code: error.code(),
            status,
            message: [0; 256],
            length: message.len(),
        };
        failure.message[..message.len()].copy_from_slice(message.as_bytes());
        Some(failure)
    }
    // Only call for token-endpoint failures, never per-model credential checks.
    fn share<T>(result: Result<T>, failure: &mut Option<Self>) -> Result<T> {
        if let Err(error) = &result {
            *failure = Self::from_error(error);
        }
        result
    }
    fn error(self, io: &mut CredentialIo<'_>) -> Result<acyclic_harness::Error> {
        use acyclic_harness::{Error, wire::ErrorCode};
        let text = std::str::from_utf8(&self.message[..self.length])
            .map_err(|_| invalid("invalid shared refresh diagnostic"))?;
        let message = io.string(text)?;
        Ok(match self.code {
            ErrorCode::Unauthorized => Error::Unauthorized(message),
            ErrorCode::Storage => Error::Storage(message),
            ErrorCode::Unavailable => Error::ModelUnavailable {
                status: self.status,
                excerpt: message,
            },
            _ => Error::Invalid(message),
        })
    }
}

impl CodexLogin {
    /// Reuses this auth.json; never starts an interactive login flow.
    pub fn from_path(path: PathBuf) -> Self {
        Self {
            path,
            account: OnceLock::new(),
            refresh: crate::sync::Arc::new(crate::refresh::Refresh::new()),
            clock: std::sync::Arc::new(OffsetDateTime::now_utc),
        }
    }

    /// Supplies the clock for expiry decisions and publication timestamps.
    pub fn with_clock(
        mut self,
        clock: impl Fn() -> OffsetDateTime + Send + Sync + 'static,
    ) -> Self {
        self.clock = std::sync::Arc::new(clock);
        self
    }

    pub(crate) fn fresh(&self) -> Self {
        Self {
            path: self.path.clone(),
            account: OnceLock::new(),
            refresh: self.refresh.clone(),
            clock: self.clock.clone(),
        }
    }

    /// Finds `$CODEX_HOME/auth.json` or the current user's `~/.codex/auth.json`.
    pub fn discover() -> Option<Self> {
        let root = std::env::var_os("CODEX_HOME")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
                    .map(|home| PathBuf::from(home).join(".codex"))
            })?;
        let path = root.join("auth.json");
        path.is_file().then(|| Self::from_path(path))
    }

    /// The rejected bearer is supplied only following HTTP 401, before any SSE is read.
    pub(crate) async fn bearer(
        &self,
        transport: &dyn HttpTransport,
        rejected: Option<&str>,
        custody: Option<&mut NativeAllocation>,
    ) -> Result<(String, Option<String>)> {
        let mut io = CredentialIo::new(custody)?;
        let result = crate::refresh::serialized(&self.refresh, async {
            let mut failure = None;
            let result = self
                .refresh_bearer(transport, rejected, &mut io, &mut failure)
                .await;
            (result, failure)
        })
        .await;
        let result = match result {
            Ok(result) => result,
            Err(failure) => Err(failure.error(&mut io)?),
        };
        io.checked(result)
    }

    async fn refresh_bearer(
        &self,
        transport: &dyn HttpTransport,
        rejected: Option<&str>,
        io: &mut CredentialIo<'_>,
        failure: &mut Option<RefreshFailure>,
    ) -> Result<(String, Option<String>)> {
        let (mut original, mut auth) = load(&self.path, io)?;
        self.check_account(&auth)?;
        for attempt in 0..2 {
            let access = token(&auth, "access_token")?;
            let force = rejected.is_some_and(|rejected| rejected == access);
            if !force && !needs_refresh(&auth, (self.clock)()) {
                break;
            }
            let refresh = token(&auth, "refresh_token")?;
            io.graph(refresh.len() as u64 * 6 + 1024, 32)?;
            let request = HttpRequest {
                url: "https://auth.openai.com/oauth/token".into(),
                headers: BTreeMap::new(),
                body: io.bytes(serde_json::to_vec(&json!({"grant_type":"refresh_token", "client_id":CLIENT_ID,
                    "refresh_token":refresh,"refresh_reason":if force {"unauthorized"} else {"proactive_expiry"}}))
                    .map_err(|_| invalid("Codex refresh encoding failed"))?)?,
            };
            let response = RefreshFailure::share(
                async {
                    match io.work() {
                        Some(work) => transport.send_with_native_work(request, work)?.await,
                        None => transport.send(request).await,
                    }
                }
                .await,
                failure,
            )?;
            if response.status != 200 {
                if attempt == 0 && matches!(response.status, 400 | 401) {
                    let (bytes, current) = load(&self.path, io)?;
                    self.check_account(&current)?;
                    if token(&current, "refresh_token").ok() != Some(refresh)
                        || token(&current, "access_token").ok() != Some(access)
                    {
                        original = bytes;
                        auth = current;
                        continue;
                    }
                }
                return RefreshFailure::share(
                    Err(if matches!(response.status, 400 | 401 | 403) {
                        acyclic_harness::Error::Unauthorized(format!(
                            "Codex refresh HTTP status {}; sign in with the Codex CLI again",
                            response.status
                        ))
                    } else {
                        crate::http_error(response.status)
                    }),
                    failure,
                );
            }
            let updated =
                RefreshFailure::share(refresh_document(response.body, io).await, failure)?;
            for field in ["access_token", "refresh_token", "id_token"] {
                if let Some(value) = updated.get(field) {
                    auth["tokens"][field] = value.clone();
                }
            }
            auth["last_refresh"] = json!(
                (self.clock)()
                    .format(&Rfc3339)
                    .map_err(|_| invalid("Codex refresh timestamp failed"))?
            );
            // Keep publication inside the refresh guard. A detached blocking task
            // could outlive cancellation and overwrite a subsequent refresh.
            auth = persist_if_unchanged(&self.path, &original, auth, io)?;
            self.check_account(&auth)?;
            break;
        }
        let access = io.string(token(&auth, "access_token")?)?;
        let account = auth["tokens"]["account_id"]
            .as_str()
            .map(|account| io.string(account))
            .transpose()?;
        Ok((access, account))
    }

    fn check_account(&self, auth: &Value) -> Result<()> {
        let current = auth["tokens"]["account_id"]
            .as_str()
            .map(|account| blake3::hash(account.as_bytes()));
        if self.account.get_or_init(|| current) != &current {
            return Err(acyclic_harness::Error::Unauthorized(
                "Codex login account changed; resolve the model again".into(),
            ));
        }
        Ok(())
    }
}

async fn refresh_document(
    mut body: acyclic_stream::BoxProviderStream<'static, Result<bytes::Bytes>>,
    io: &mut CredentialIo<'_>,
) -> Result<Value> {
    let mut bytes = Vec::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        if chunk.len() as u64 > MAX_AUTH_BYTES.saturating_sub(bytes.len() as u64) {
            return Err(invalid("Codex refresh response exceeds limit"));
        }
        if bytes.len() + chunk.len() > bytes.capacity() {
            let capacity = (bytes.len() + chunk.len())
                .max(bytes.capacity() * 2)
                .min(MAX_AUTH_BYTES as usize);
            io.claim(
                capacity as u64,
                1,
                WorkCounters {
                    bytes_copied: bytes.len() as u64,
                    ..WorkCounters::UNCHARGED
                },
            )?;
            bytes
                .try_reserve_exact(capacity - bytes.len())
                .map_err(|_| invalid("cannot allocate Codex refresh response"))?;
        }
        io.charge(WorkCounters {
            bytes_copied: chunk.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        bytes.extend_from_slice(&chunk);
    }
    let updated = io.json(&bytes)?;
    if !["access_token", "refresh_token", "id_token"]
        .iter()
        .any(|field| updated.get(field).is_some())
    {
        return Err(invalid("Codex refresh did not return any tokens"));
    }
    for field in ["access_token", "refresh_token", "id_token"] {
        if let Some(value) = updated.get(field) {
            if value.as_str().is_none_or(|token| token.trim().is_empty()) {
                return Err(invalid("Codex refresh returned an invalid token"));
            }
        }
    }
    Ok(updated)
}

fn token<'a>(auth: &'a Value, field: &str) -> Result<&'a str> {
    auth["tokens"][field]
        .as_str()
        .filter(|token| !token.trim().is_empty())
        .ok_or_else(|| invalid("Codex CLI ChatGPT login is incomplete; sign in again"))
}

pub(crate) fn needs_refresh(auth: &Value, now: OffsetDateTime) -> bool {
    #[derive(serde::Deserialize)]
    struct Claims {
        exp: Option<i64>,
    }
    let expiry = auth["tokens"]["access_token"]
        .as_str()
        .and_then(|token| token.split('.').nth(1))
        .and_then(|payload| URL_SAFE_NO_PAD.decode(payload).ok())
        .and_then(|payload| serde_json::from_slice::<Claims>(&payload).ok())
        .and_then(|claims| claims.exp);
    if let Some(expiry) = expiry {
        return expiry <= now.unix_timestamp().saturating_add(300);
    }
    auth["last_refresh"]
        .as_str()
        .and_then(|date| OffsetDateTime::parse(date, &Rfc3339).ok())
        .is_none_or(|last| now - last >= time::Duration::days(8))
}

fn load(path: &Path, io: &mut CredentialIo<'_>) -> Result<(Vec<u8>, Value)> {
    let bytes = read(path, io)?;
    let auth = document(&bytes, io)?;
    Ok((bytes, auth))
}

fn read(path: &Path, io: &mut CredentialIo<'_>) -> Result<Vec<u8>> {
    io.charge(WorkCounters {
        backend_read_operations: 2,
        object_probes: 1,
        ..WorkCounters::UNCHARGED
    })?;
    let mut file = File::open(path).map_err(|_| invalid("cannot read Codex auth.json"))?;
    let length = file
        .metadata()
        .map_err(|_| invalid("cannot stat Codex auth.json"))?
        .len();
    if length > MAX_AUTH_BYTES {
        return Err(invalid("Codex auth.json exceeds limit"));
    }
    let capacity = length as usize + 1;
    io.claim(capacity as u64, 1, WorkCounters::UNCHARGED)?;
    let mut bytes = vec![0; capacity];
    let read_allowance = io
        .work()
        .map(|work| {
            work.reserve_work(WorkCounters {
                source_bytes_read: capacity as u64,
                ..WorkCounters::UNCHARGED
            })
        })
        .transpose()?;
    let mut read = 0;
    while read < capacity {
        io.charge(WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        let count = file
            .read(&mut bytes[read..])
            .map_err(|_| invalid("cannot read Codex auth.json"))?;
        if count == 0 {
            break;
        }
        if let Some(reserved) = &read_allowance {
            reserved.record_completed(WorkCounters {
                source_bytes_read: count as u64,
                ..WorkCounters::UNCHARGED
            })?;
        }
        read += count;
    }
    if read > length as usize {
        return Err(invalid("Codex auth.json changed while reading"));
    }
    bytes.truncate(read);
    Ok(bytes)
}

fn document(bytes: &[u8], io: &mut CredentialIo<'_>) -> Result<Value> {
    let auth = io.json(bytes)?;
    validate_auth(&auth)?;
    Ok(auth)
}

#[cfg(any(test, feature = "fuzzing"))]
pub(crate) fn decode_auth(bytes: &[u8]) -> Result<Value> {
    if bytes.len() as u64 > MAX_AUTH_BYTES {
        return Err(invalid("Codex auth.json exceeds limit"));
    }
    document(bytes, &mut CredentialIo::new(None)?)
}

fn validate_auth(auth: &Value) -> Result<()> {
    if auth["auth_mode"]
        .as_str()
        .is_some_and(|mode| mode != "chatgpt")
    {
        return Err(invalid("Codex auth.json is not a ChatGPT CLI login"));
    }
    token(&auth, "access_token")?;
    Ok(())
}

fn persist_if_unchanged(
    path: &Path,
    original: &[u8],
    auth: Value,
    io: &mut CredentialIo<'_>,
) -> Result<Value> {
    persist_with(path, original, auth, || Ok(()), io)
}

fn persist_with(
    path: &Path,
    original: &[u8],
    auth: Value,
    before_replace: impl FnOnce() -> Result<()>,
    io: &mut CredentialIo<'_>,
) -> Result<Value> {
    io.charge(WorkCounters {
        backend_write_operations: 1,
        ..WorkCounters::UNCHARGED
    })?;
    io.claim(
        path.as_os_str().len() as u64 * 3 + 1024,
        16,
        WorkCounters::UNCHARGED,
    )?;
    let absolute = if path.is_absolute() {
        None
    } else {
        Some(std::path::absolute(path).map_err(|_| invalid("cannot resolve Codex auth path"))?)
    };
    let path = absolute.as_deref().unwrap_or(path);
    let parent = path
        .parent()
        .ok_or_else(|| invalid("Codex auth path needs a parent"))?;
    // Stage on the same filesystem. A metadata edit does not take credential
    // ownership: preserve it while publishing the rotated token fields.
    let baseline = document(original, io)?;
    let (mut observed, current) = load(path, io)?;
    let Some(mut auth) = merge_refresh(&baseline, &observed, &current, &auth, io)? else {
        return Ok(current);
    };
    let mut file = crate::credential_file::create(parent)
        .map_err(|_| invalid("cannot create Codex credential update"))?;
    stage(&mut file, &auth, io)?;
    before_replace()?;
    let bytes = read(path, io)?;
    if bytes != observed {
        let current = document(&bytes, io)?;
        let Some(merged) = merge_refresh(&baseline, &bytes, &current, &auth, io)? else {
            return Ok(current);
        };
        auth = merged;
        stage(&mut file, &auth, io)?;
        observed = bytes;
        if read(path, io)? != observed {
            return Err(acyclic_harness::Error::Storage(
                "Codex credentials changed during publication; retry".into(),
            ));
        }
    }
    // The CLI does not participate in our lock. This final read + replacement
    // is still best-effort, not an atomic compare-and-swap against CLI writes.
    io.charge(WorkCounters {
        backend_write_operations: 1,
        ..WorkCounters::UNCHARGED
    })?;
    file.persist(path)
        .map_err(|_| invalid("cannot replace Codex auth.json"))?;
    Ok(auth)
}

fn stage(
    file: &mut tempfile::NamedTempFile,
    auth: &Value,
    io: &mut CredentialIo<'_>,
) -> Result<()> {
    let encoded = (crate::projection::encoded_len(auth)? as u64)
        .checked_add(1)
        .filter(|&bytes| bytes <= MAX_AUTH_BYTES)
        .ok_or_else(|| invalid("Codex merged credentials exceed limit"))?;
    write_staged(file, auth, encoded, io)
}

fn write_staged(
    file: &mut tempfile::NamedTempFile,
    auth: &Value,
    encoded: u64,
    io: &mut CredentialIo<'_>,
) -> Result<()> {
    use std::io::{Seek as _, SeekFrom};
    io.charge(WorkCounters {
        bytes_encoded: encoded,
        output_bytes: encoded,
        backend_write_operations: 3,
        durability_operations: 1,
        ..WorkCounters::UNCHARGED
    })?;
    file.as_file_mut()
        .seek(SeekFrom::Start(0))
        .and_then(|_| file.as_file_mut().set_len(0))
        .map_err(|_| invalid("cannot prepare Codex credential update"))?;
    serde_json::to_writer(file.as_file_mut(), auth)
        .map_err(|_| invalid("cannot encode Codex credential update"))?;
    file.write_all(b"\n")
        .and_then(|()| file.as_file().sync_all())
        .map_err(|_| invalid("cannot flush Codex credential update"))
}

fn merge_refresh(
    original: &Value,
    current_wire: &[u8],
    current: &Value,
    refreshed: &Value,
    io: &mut CredentialIo<'_>,
) -> Result<Option<Value>> {
    const FIELDS: [&str; 4] = ["account_id", "access_token", "refresh_token", "id_token"];
    if original["auth_mode"].as_str().unwrap_or("chatgpt")
        != current["auth_mode"].as_str().unwrap_or("chatgpt")
        || original["OPENAI_API_KEY"] != current["OPENAI_API_KEY"]
        || FIELDS
            .iter()
            .any(|field| original["tokens"][field] != current["tokens"][field])
    {
        return Ok(None);
    }
    // The current wire supplies the graph's structural bound without encoding
    // another copy. Only OAuth's changed string fields need additional storage.
    let (bytes, nodes) = crate::native::shape(current_wire);
    let fields = ["access_token", "refresh_token", "id_token"];
    let mut replacement_bytes = 0u64;
    for value in fields
        .iter()
        .map(|field| &refreshed["tokens"][field])
        .chain(std::iter::once(&refreshed["last_refresh"]))
    {
        // Unchanged omitted fields may be null; refresh responses themselves
        // have already admitted nonempty token strings.
        if let Some(text) = value.as_str() {
            replacement_bytes = replacement_bytes
                .checked_add(text.len() as u64)
                .ok_or_else(|| invalid("Codex credential merge size overflow"))?;
        }
    }
    io.graph(
        bytes
            .checked_add(replacement_bytes)
            .ok_or_else(|| invalid("Codex credential merge size overflow"))?,
        nodes + 8,
    )?;
    let mut merged = current.clone();
    for field in fields {
        if merged["tokens"][field] != refreshed["tokens"][field] {
            merged["tokens"][field] = refreshed["tokens"][field].clone();
        }
    }
    merged["last_refresh"] = refreshed["last_refresh"].clone();
    Ok(Some(merged))
}

#[cfg(test)]
#[path = "codex_tests.rs"]
mod tests;

#[cfg(test)]
pub(crate) fn fixed_now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_792_150_400).expect("fixed fixture timestamp")
}

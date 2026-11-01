use crate::{HttpRequest, HttpTransport, invalid};
use acyclic_harness_core::Result;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
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

/// Native-only reuse of an existing Codex CLI ChatGPT login.
/// Refreshes serialize in memory and defer to concurrent CLI writes.
/// File reads, parsing and publication use the shared native I/O service.
/// Cancelling an admitted publication does not undo it: its original lock and
/// accounting remain owned until physical completion. Metered callers initialize
/// the shared native I/O services before admitting their original work.
pub struct CodexLogin {
    path: std::sync::Arc<Path>,
    account: OnceLock<Option<blake3::Hash>>,
    refresh: crate::sync::Arc<crate::refresh::Refresh<RefreshFailure>>,
    clock: std::sync::Arc<dyn Fn() -> OffsetDateTime + Send + Sync>,
}

#[derive(Clone, Copy)]
enum RefreshFailure {
    /// Preserve static native causes as the same error, without rendering or allocating them.
    Boundary {
        code: acyclic_harness_core::wire::ErrorCode,
        reason: &'static str,
    },
    Diagnostic(RefreshDiagnostic),
}

#[derive(Clone, Copy)]
struct RefreshDiagnostic {
    kind: RefreshDiagnosticKind,
    message: [u8; 256],
    length: usize,
}

#[derive(Clone, Copy)]
enum RefreshDiagnosticKind {
    Invalid,
    Unauthorized,
    Storage,
    Transport(acyclic_harness_core::ModelTransportFailure),
}

impl RefreshFailure {
    fn from_error(error: &acyclic_harness_core::Error) -> Option<Self> {
        use acyclic_harness_core::Error;
        if !matches!(
            error.code(),
            acyclic_harness_core::wire::ErrorCode::Unauthorized
                | acyclic_harness_core::wire::ErrorCode::Storage
                | acyclic_harness_core::wire::ErrorCode::Unavailable
                | acyclic_harness_core::wire::ErrorCode::Invalid
        ) {
            return None;
        }
        if let Error::NativeBoundary { code, reason } = error {
            return Some(Self::Boundary {
                code: *code,
                reason,
            });
        }
        use RefreshDiagnosticKind as Kind;
        use acyclic_harness_core::wire::ErrorCode as Code;
        let (message, kind) = match error {
            Error::ModelUnavailable { failure, excerpt } => {
                (excerpt.as_str(), Kind::Transport(*failure))
            }
            Error::Invalid(message)
            | Error::ModelDiagnostic {
                code: Code::Invalid,
                message,
                ..
            }
            | Error::NativeDiagnostic {
                code: Code::Invalid,
                message,
                ..
            } => (message.as_str(), Kind::Invalid),
            Error::Unauthorized(message)
            | Error::ModelDiagnostic {
                code: Code::Unauthorized,
                message,
                ..
            }
            | Error::NativeDiagnostic {
                code: Code::Unauthorized,
                message,
                ..
            } => (message.as_str(), Kind::Unauthorized),
            Error::Storage(message)
            | Error::NativeDiagnostic {
                code: Code::Storage,
                message,
                ..
            } => (message.as_str(), Kind::Storage),
            // An invocation-owned diagnostic with no reconstructible transport class stays
            // local, like an oversized diagnostic; never cache it as another failure class.
            _ => return None,
        };
        if message.len() > 256 {
            return None;
        }
        let mut failure = RefreshDiagnostic {
            kind,
            message: [0; 256],
            length: message.len(),
        };
        failure.message[..message.len()].copy_from_slice(message.as_bytes());
        Some(Self::Diagnostic(failure))
    }
    // Only call for token-endpoint failures, never per-model credential checks.
    fn share<T>(result: Result<T>, failure: &mut Option<Self>) -> Result<T> {
        if let Err(error) = &result {
            *failure = Self::from_error(error);
        }
        result
    }
    fn error(self, io: &mut CredentialIo) -> Result<acyclic_harness_core::Error> {
        use acyclic_harness_core::Error;
        let diagnostic = match self {
            Self::Boundary { code, reason } => return Ok(Error::NativeBoundary { code, reason }),
            Self::Diagnostic(diagnostic) => diagnostic,
        };
        let text = std::str::from_utf8(&diagnostic.message[..diagnostic.length])
            .map_err(|_| invalid("invalid shared refresh diagnostic"))?;
        let message = io.string(text)?;
        Ok(match diagnostic.kind {
            RefreshDiagnosticKind::Transport(failure) => Error::ModelUnavailable {
                failure,
                excerpt: message,
            },
            RefreshDiagnosticKind::Unauthorized => Error::Unauthorized(message),
            RefreshDiagnosticKind::Storage => Error::Storage(message),
            RefreshDiagnosticKind::Invalid => Error::Invalid(message),
        })
    }
}

impl CodexLogin {
    /// Reuses this auth.json; never starts an interactive login flow.
    pub fn from_path(path: PathBuf) -> Self {
        Self {
            path: path.into(),
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
        Self::in_home(&root)
    }

    /// `root/auth.json` when it exists; the file is read only on first use.
    pub(crate) fn in_home(root: &Path) -> Option<Self> {
        let path = root.join("auth.json");
        path.is_file().then(|| Self::from_path(path))
    }

    /// The rejected bearer is supplied only following HTTP 401, before any SSE is read.
    pub(crate) async fn bearer(
        &self,
        transport: &dyn HttpTransport,
        rejected: Option<&str>,
        mut custody: Option<&mut NativeAllocation>,
    ) -> Result<(String, Option<String>)> {
        let mut io = CredentialIo::new(custody.as_deref_mut())?;
        let result = match self.refresh.acquire().await {
            Ok(permit) => {
                let mut permit = Some(permit);
                let mut failure = None;
                let result = self
                    .refresh_bearer(transport, rejected, &mut io, &mut permit, &mut failure)
                    .await;
                if let Some(permit) = permit {
                    self.refresh.complete(permit, failure);
                }
                result
            }
            Err(failure) => Err(failure.error(&mut io)?),
        };
        let result = io.checked(result);
        io.finish(custody)?;
        result
    }

    async fn refresh_bearer(
        &self,
        transport: &dyn HttpTransport,
        rejected: Option<&str>,
        io: &mut CredentialIo,
        permit: &mut Option<crate::refresh::Permit<RefreshFailure>>,
        failure: &mut Option<RefreshFailure>,
    ) -> Result<(String, Option<String>)> {
        let path = self.path.clone();
        let (_, mut auth) = io.blocking(permit, move |io| load(&path, io)).await?;
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
                    let path = self.path.clone();
                    let (_, current) = io.blocking(permit, move |io| load(&path, io)).await?;
                    self.check_account(&current)?;
                    if token(&current, "refresh_token").ok() != Some(refresh)
                        || token(&current, "access_token").ok() != Some(access)
                    {
                        auth = current;
                        continue;
                    }
                }
                return RefreshFailure::share(
                    Err(if matches!(response.status, 400 | 401 | 403) {
                        acyclic_harness_core::Error::Unauthorized(format!(
                            "Codex refresh HTTP status {}; sign in with the Codex CLI again",
                            response.status
                        ))
                    } else {
                        crate::http_error(response.status)
                    }),
                    failure,
                );
            }
            let tokens = RefreshFailure::share(refresh_document(response.body, io).await, failure)?;
            let last_refresh = json!(
                (self.clock)()
                    .format(&Rfc3339)
                    .map_err(|_| invalid("Codex refresh timestamp failed"))?
            );
            let updated = RefreshUpdate {
                tokens,
                last_refresh,
            };
            let path = self.path.clone();
            auth = io
                .blocking(permit, move |io| {
                    persist_if_unchanged(&path, &auth, updated, io)
                })
                .await?
                .into_document();
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
            return Err(acyclic_harness_core::Error::Unauthorized(
                "Codex login account changed; resolve the model again".into(),
            ));
        }
        Ok(())
    }
}

async fn refresh_document(
    body: acyclic_stream::BoxProviderStream<'static, Result<bytes::Bytes>>,
    io: &mut CredentialIo,
) -> Result<Value> {
    let body = crate::http::bounded::read(body, None, io.work()).await?;
    // Encoded body custody retires with that buffer. Only the decoded document
    // joins the original credential operation and its publication owner.
    let updated = io.json(&body.bytes)?;

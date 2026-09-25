//! Endpoint, credential, and attribution configuration.

use std::{fmt, time::Duration};

/// `OpenRouter`'s OpenAI-compatible base URL.
pub const OPENROUTER_BASE_URL: &str = "https://openrouter.ai/api/v1";

/// Wire dialect spoken by the endpoint.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Dialect {
    /// Plain OpenAI-compatible chat completions.
    #[default]
    OpenAi,
    /// `OpenRouter`: requests inline usage accounting (including `cost`) and
    /// recognises its per-key limit responses.
    OpenRouter,
}

/// Application identity sent as `HTTP-Referer` and `X-Title`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppAttribution {
    /// Application URL (`HTTP-Referer`).
    pub referer: String,
    /// Human-readable application name (`X-Title`).
    pub title: String,
}

/// Bounded retry of rate-limited or transiently failed requests.
///
/// Retries happen only before any response byte is decoded, so no model event
/// has been emitted and the Harness still observes exactly one attempt. A
/// request is retried only when the provider provably did not start a
/// generation: it answered with a rate-limit or server-error status, or the
/// connection could not be established. A transport failure after the request
/// may have been sent (a reset or a missing response) is never retried, because
/// chat completions carry no idempotency key and the provider may already be
/// generating (and billing) the first attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts including the first; `1` disables retries.
    pub max_attempts: u32,
    /// First backoff delay, doubled per attempt when no `Retry-After` is given.
    pub initial_delay: Duration,
    /// Upper bound on any single delay, including a provider `Retry-After`.
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(20),
        }
    }
}

/// Default [`ProviderConfig::idle_timeout`].
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

impl RetryPolicy {
    /// A policy that never retries.
    #[must_use]
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            ..Self::default()
        }
    }

    pub(crate) fn delay(&self, attempt: u32, retry_after: Option<Duration>) -> Duration {
        let backoff = self
            .initial_delay
            .saturating_mul(2_u32.saturating_pow(attempt));
        retry_after.unwrap_or(backoff).min(self.max_delay)
    }
}

/// Configuration of one OpenAI-compatible endpoint.
///
/// The model name comes from each Harness [`Model`](acyclic_harness::model::Model),
/// so the journaled request is exactly what was sent.
#[derive(Clone, PartialEq, Eq)]
pub struct ProviderConfig {
    /// Bearer credential; only ever placed in the `Authorization` header.
    pub api_key: String,
    /// Base URL without the `/chat/completions` suffix.
    pub base_url: String,
    /// Wire dialect.
    pub dialect: Dialect,
    /// Per-request end-user id (`user`), used for spend attribution.
    pub user: Option<String>,
    /// Application attribution headers.
    pub app: Option<AppAttribution>,
    /// Retry of rate-limited or transient failures before streaming begins.
    pub retry: RetryPolicy,
    /// Longest silence tolerated while waiting for response headers or for the
    /// next body chunk (SSE keep-alive comments count as traffic). A stalled
    /// stream fails with [`ProviderError::Unavailable`](super::ProviderError::Unavailable)
    /// instead of hanging the turn.
    pub idle_timeout: Duration,
}

impl fmt::Debug for ProviderConfig {
    /// Renders the key as `<redacted>` and the base URL as its origin only.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderConfig")
            .field("api_key", &"<redacted>")
            .field("base_origin", &self.base_origin())
            .field("dialect", &self.dialect)
            .field("user", &self.user)
            .field("app", &self.app)
            .field("retry", &self.retry)
            .field("idle_timeout", &self.idle_timeout)
            .finish()
    }
}

impl ProviderConfig {
    /// Plain OpenAI-compatible endpoint.
    #[must_use]
    pub fn new(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: base_url.into(),
            dialect: Dialect::OpenAi,
            user: None,
            app: None,
            retry: RetryPolicy::default(),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
        }
    }

    /// `OpenRouter` at [`OPENROUTER_BASE_URL`].
    #[must_use]
    pub fn openrouter(api_key: impl Into<String>) -> Self {
        Self {
            dialect: Dialect::OpenRouter,
            ..Self::new(api_key, OPENROUTER_BASE_URL)
        }
    }

    /// Replaces the base URL (for example a regional or test endpoint).
    #[must_use]
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    /// Attributes every request to `user` (for example the agent id).
    #[must_use]
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Sends `HTTP-Referer` and `X-Title` on every request.
    #[must_use]
    pub fn with_app(mut self, referer: impl Into<String>, title: impl Into<String>) -> Self {
        self.app = Some(AppAttribution {
            referer: referer.into(),
            title: title.into(),
        });
        self
    }

    /// Replaces the retry policy.
    #[must_use]
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Replaces the idle timeout on response headers and body chunks.
    #[must_use]
    pub fn with_idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// Full chat completions endpoint.
    ///
    /// `/chat/completions` is appended to the base URL's path, so a query
    /// string or fragment on the base URL (for example a gateway token) is
    /// preserved rather than swallowing the suffix. A base URL that does not
    /// parse is suffixed textually and rejected by the HTTP client on send.
    #[must_use]
    pub fn completions_url(&self) -> String {
        match reqwest::Url::parse(&self.base_url) {
            Ok(mut url) if !url.cannot_be_a_base() => {
                let path = format!("{}/chat/completions", url.path().trim_end_matches('/'));
                url.set_path(&path);
                url.into()
            }
            _ => format!("{}/chat/completions", self.base_url.trim_end_matches('/')),
        }
    }

    /// Scheme, host, and non-default port of the base URL, safe to log.
    ///
    /// Userinfo, path, and query are dropped because a custom base URL may embed
    /// a credential. An unparseable base URL renders as `<invalid>`.
    #[must_use]
    pub fn base_origin(&self) -> String {
        reqwest::Url::parse(&self.base_url)
            .ok()
            .filter(reqwest::Url::has_host)
            .map_or_else(
                || "<invalid>".into(),
                |url| url.origin().ascii_serialization(),
            )
    }
}

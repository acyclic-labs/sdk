//! Typed model-provider failures and their stable mapping onto Harness errors.

use crate::config::Dialect;
use std::{fmt, time::Duration};

/// Longest provider error body echoed into an error message.
const ERROR_BODY_CHARS: usize = 2048;

/// Prefix of every Harness error message produced from a [`ProviderError`].
///
/// [`ProviderErrorCode::from_harness`] parses it back, so a host that only sees
/// the Harness error (for example from the stock executor) can still act on the
/// typed classification.
pub const HARNESS_MESSAGE_PREFIX: &str = "model_provider.";

/// Why the provider refused to spend more on this credential.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BudgetReason {
    /// The account behind the key has insufficient credits (HTTP 402).
    InsufficientCredits,
    /// The key's own spending limit has been reached.
    KeyLimitReached,
}

impl fmt::Display for BudgetReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InsufficientCredits => "insufficient credits",
            Self::KeyLimitReached => "key limit reached",
        })
    }
}

/// Stable, transport-independent classification of a [`ProviderError`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderErrorCode {
    /// Credits or the key limit are exhausted; every agent sharing the credential
    /// will fail the same way, so the work should stop rather than retry.
    BudgetExhausted,
    /// The provider throttled the request; retrying later may succeed.
    RateLimited,
    /// The credential was rejected, revoked, or expired.
    Unauthorized,
    /// The request itself is invalid and will fail again unchanged.
    Invalid,
    /// The provider or network failed transiently.
    Unavailable,
    /// The response violated the streaming protocol.
    Protocol,
}

impl ProviderErrorCode {
    /// Stable snake-case name used in Harness error messages.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BudgetExhausted => "budget_exhausted",
            Self::RateLimited => "rate_limited",
            Self::Unauthorized => "unauthorized",
            Self::Invalid => "invalid",
            Self::Unavailable => "unavailable",
            Self::Protocol => "protocol",
        }
    }

    /// Whether retrying the identical request later may succeed.
    #[must_use]
    pub fn is_retryable(self) -> bool {
        matches!(self, Self::RateLimited | Self::Unavailable)
    }

    /// Recovers the classification from a Harness error produced by this crate.
    ///
    /// Returns `None` for errors that did not originate in a [`ProviderError`].
    #[must_use]
    pub fn from_harness(error: &acyclic_harness::Error) -> Option<Self> {
        use acyclic_harness::Error;
        let (Error::Unauthorized(message) | Error::Invalid(message) | Error::Storage(message)) =
            error
        else {
            return None;
        };
        let code = message
            .strip_prefix(HARNESS_MESSAGE_PREFIX)?
            .split(':')
            .next()?;
        [
            Self::BudgetExhausted,
            Self::RateLimited,
            Self::Unauthorized,
            Self::Invalid,
            Self::Unavailable,
            Self::Protocol,
        ]
        .into_iter()
        .find(|candidate| candidate.as_str() == code)
    }
}

/// Typed failure of one model request.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum ProviderError {
    /// The credential cannot spend any more.
    #[error("model budget exhausted ({reason}): {message}")]
    BudgetExhausted {
        /// Which budget ran out.
        reason: BudgetReason,
        /// Provider-supplied detail.
        message: String,
    },
    /// The provider throttled the request.
    #[error("model provider rate limited the request: {message}")]
    RateLimited {
        /// Provider-requested delay from `Retry-After`, when present.
        retry_after: Option<Duration>,
        /// Provider-supplied detail.
        message: String,
    },
    /// The credential was rejected.
    #[error("model provider rejected the credential: {message}")]
    Unauthorized {
        /// Provider-supplied detail.
        message: String,
    },
    /// The request is malformed or unsupported.
    #[error("model request is invalid: {message}")]
    Invalid {
        /// Provider-supplied or local detail.
        message: String,
    },
    /// The provider or network failed transiently.
    #[error("model provider is unavailable: {message}")]
    Unavailable {
        /// HTTP status when the provider answered.
        status: Option<u16>,
        /// Provider-supplied or transport detail.
        message: String,
    },
    /// The response stream was malformed or ended early.
    #[error("model provider stream is malformed: {message}")]
    Protocol {
        /// Decoder detail.
        message: String,
    },
}

impl ProviderError {
    /// Stable classification.
    #[must_use]
    pub fn code(&self) -> ProviderErrorCode {
        match self {
            Self::BudgetExhausted { .. } => ProviderErrorCode::BudgetExhausted,
            Self::RateLimited { .. } => ProviderErrorCode::RateLimited,
            Self::Unauthorized { .. } => ProviderErrorCode::Unauthorized,
            Self::Invalid { .. } => ProviderErrorCode::Invalid,
            Self::Unavailable { .. } => ProviderErrorCode::Unavailable,
            Self::Protocol { .. } => ProviderErrorCode::Protocol,
        }
    }

    /// Whether retrying the identical request later may succeed.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        self.code().is_retryable()
    }

    /// Classifies a non-success HTTP status or an in-stream `error.code`.
    ///
    /// 402 is always an exhausted budget. The `OpenRouter` dialect also reports a
    /// reached per-key limit as 403 with a "limit" message; any other 403 is an
    /// authorization failure.
    #[must_use]
    pub fn from_status(
        dialect: Dialect,
        status: u16,
        retry_after: Option<Duration>,
        body: &str,
    ) -> Self {
        let message = format!("HTTP {status}: {}", truncate(body, ERROR_BODY_CHARS));
        match status {
            402 => Self::BudgetExhausted {
                reason: if mentions_key_limit(body) {
                    BudgetReason::KeyLimitReached
                } else {
                    BudgetReason::InsufficientCredits
                },
                message,
            },
            403 if dialect == Dialect::OpenRouter && mentions_key_limit(body) => {
                Self::BudgetExhausted {
                    reason: BudgetReason::KeyLimitReached,
                    message,
                }
            }
            429 => Self::RateLimited {
                retry_after,
                message,
            },
            401 | 403 => Self::Unauthorized { message },
            400 | 404 | 409 | 413 | 415 | 422 => Self::Invalid { message },
            _ => Self::Unavailable {
                status: Some(status),
                message,
            },
        }
    }

    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol {
            message: message.into(),
        }
    }

    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid {
            message: message.into(),
        }
    }
}

impl From<ProviderError> for acyclic_harness::Error {
    /// Maps onto the closest Harness error while keeping the typed code as a
    /// parseable message prefix (see [`ProviderErrorCode::from_harness`]).
    ///
    /// An exhausted budget is `Unauthorized`: the credential lost its authority
    /// to spend, and no Harness retry can restore it. Rate limits and transient
    /// failures are `Storage`, the Harness class for retryable provider failures.
    fn from(error: ProviderError) -> Self {
        let code = error.code();
        let message = format!("{HARNESS_MESSAGE_PREFIX}{}: {error}", code.as_str());
        match code {
            ProviderErrorCode::BudgetExhausted | ProviderErrorCode::Unauthorized => {
                Self::Unauthorized(message)
            }
            ProviderErrorCode::Invalid => Self::Invalid(message),
            ProviderErrorCode::RateLimited
            | ProviderErrorCode::Unavailable
            | ProviderErrorCode::Protocol => Self::Storage(message),
        }
    }
}

fn mentions_key_limit(body: &str) -> bool {
    let lower = body.to_ascii_lowercase();
    lower.contains("key limit")
        || lower.contains("limit exceeded")
        || lower.contains("limit reached")
}

fn truncate(text: &str, max_chars: usize) -> String {
    match text.char_indices().nth(max_chars) {
        Some((index, _)) => format!("{}…", text.get(..index).unwrap_or(text)),
        None => text.to_owned(),
    }
}

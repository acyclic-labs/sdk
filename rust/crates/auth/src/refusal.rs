//! What a server said about a failure beyond its category, shared by every
//! primitive's status conversion.
//!
//! A typed failure category names what happened; a refusal is everything else
//! the server said about it: the [`Cause`] it attributes the failure to, and
//! the member's original status code, message, details and product response
//! metadata (the `acyclic-` keys). Decoding keeps that status verbatim and
//! encoding reproduces it, so a category plus its refusal round-trips the
//! original status exactly. Transport framing (`content-type`, `grpc-*`) and
//! keys a primitive's own protocol consumes are not part of a refusal.
//!
//! A refusal is a diagnostic. It is not authorization, proof of
//! non-execution or retry permission; recovery follows the category alone.

use std::sync::Arc;

use bytes::Bytes;
use http::HeaderMap;

/// Namespace of product response metadata a refusal retains.
pub const METADATA_PREFIX: &str = "acyclic-";

/// A server's attribution of one remote failure, shared by every service error.
///
/// `component` and `reason` are closed snake_case tokens the server chooses,
/// such as `objects.medium` and `failed`: never prose, paths or topology.
/// `request_id` names the server's request. An empty part was not reported.
/// A service error exposes its cause through [`std::error::Error::source`],
/// so a consumer needs no service crate.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "auth/Cause.ts", rename_all = "camelCase")]
pub struct Cause {
    /// Server component that failed.
    pub component: String,
    /// Why that component failed.
    pub reason: String,
    /// The server's identity for the failed request.
    pub request_id: String,
}

impl Cause {
    /// A server-side cause from two closed tokens.
    #[must_use]
    pub fn new(component: &str, reason: &str) -> Self {
        Self {
            component: component.to_owned(),
            reason: reason.to_owned(),
            request_id: String::new(),
        }
    }

    /// The cause a peer reported, or `None` when it reported no part.
    #[must_use]
    pub fn reported(component: String, reason: String, request_id: String) -> Option<Self> {
        (!(component.is_empty() && reason.is_empty() && request_id.is_empty())).then_some(Self {
            component,
            reason,
            request_id,
        })
    }
}

impl std::fmt::Display for Cause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let attributed = !(self.component.is_empty() && self.reason.is_empty());
        match (self.component.as_str(), self.reason.as_str()) {
            ("", "") => {}
            (component, "") => formatter.write_str(component)?,
            ("", reason) => formatter.write_str(reason)?,
            (component, reason) => write!(formatter, "{component}: {reason}")?,
        }
        match self.request_id.as_str() {
            "" => Ok(()),
            id if attributed => write!(formatter, ", request {id}"),
            id => write!(formatter, "request {id}"),
        }
    }
}

impl std::error::Error for Cause {}

/// The status one category encodes to when it carries no original status.
/// A primitive whose status carries a [`Cause`] builds it with that cause.
#[derive(Clone, Copy, Debug)]
pub struct Canonical<'a> {
    /// gRPC status code number.
    pub code: i32,
    /// Status message.
    pub message: &'a str,
    /// Status details.
    pub details: &'a [u8],
    /// Product metadata keys the primitive's protocol owns and re-emits itself.
    pub owned: &'a [&'a str],
}

/// The parts of one status, as decoded from or encoded to the wire.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Parts {
    /// gRPC status code number.
    pub code: i32,
    /// Status message.
    pub message: String,
    /// Status details.
    pub details: Bytes,
    /// Product response metadata.
    pub metadata: HeaderMap,
}

/// What a server said beyond a category: its [`Cause`], the member's original
/// status, or both, and never neither. The original status is kept only when
/// it said something its category's canonical status, cause included, does
/// not, so it never encodes to that canonical status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Refusal(Arc<Said>);

#[derive(Clone, Debug, Eq, PartialEq)]
struct Said {
    cause: Option<Cause>,
    original: Option<Parts>,
}

impl Refusal {
    /// The server's attribution alone; it encodes to its category's canonical
    /// status carrying `cause`.
    #[must_use]
    pub fn caused(cause: Cause) -> Self {
        Self(Arc::new(Said {
            cause: Some(cause),
            original: None,
        }))
    }

    /// Retains the `cause` a primitive decoded from one status and whatever
    /// that status says beyond `canonical`, which already carries `cause`;
    /// `None` when there is neither. Work and allocation are linear in this
    /// status's own diagnostic bytes; a canonical status without a cause
    /// allocates nothing.
    #[must_use]
    pub fn capture(
        code: i32,
        message: &str,
        details: &[u8],
        metadata: &HeaderMap,
        canonical: Canonical<'_>,
        cause: Option<Cause>,
    ) -> Option<Self> {
        let product = |name: &http::HeaderName| {
            name.as_str().starts_with(METADATA_PREFIX) && !canonical.owned.contains(&name.as_str())
        };
        let original = (code != canonical.code
            || message != canonical.message
            || details != canonical.details
            || metadata.keys().any(product))
        .then(|| {
            let mut retained = HeaderMap::new();
            for (name, value) in metadata.iter().filter(|(name, _)| product(name)) {
                retained.append(name, value.clone());
            }
            Parts {
                code,
                message: message.to_owned(),
                details: Bytes::copy_from_slice(details),
                metadata: retained,
            }
        });
        (cause.is_some() || original.is_some()).then(|| Self(Arc::new(Said { cause, original })))
    }

    /// The server's attribution, when it reported one.
    #[must_use]
    pub fn cause(&self) -> Option<&Cause> {
        self.0.cause.as_ref()
    }

    /// Consumes this refusal's attribution, moving its strings when uniquely
    /// owned. If shared, clones only the attribution; other owners retain the
    /// original status.
    #[must_use]
    pub fn into_cause(self) -> Option<Cause> {
        match Arc::try_unwrap(self.0) {
            Ok(said) => said.cause,
            Err(said) => said.cause.clone(),
        }
    }

    /// The member's original status parts, when they said more than the
    /// canonical status.
    #[must_use]
    pub fn original(&self) -> Option<&Parts> {
        self.0.original.as_ref()
    }

    /// The status to emit: a refusal's original parts verbatim, else
    /// `canonical`, which carries the refusal's cause.
    #[must_use]
    pub fn encode(refusal: Option<&Self>, canonical: Canonical<'_>) -> Parts {
        refusal.and_then(Self::original).map_or_else(
            || Parts {
                code: canonical.code,
                message: canonical.message.to_owned(),
                details: Bytes::copy_from_slice(canonical.details),
                metadata: HeaderMap::new(),
            },
            Parts::clone,
        )
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut separator = ": ";
        if let Some(cause) = self.cause() {
            write!(f, "{separator}{cause}")?;
            separator = ", ";
        }
        if let Some(parts) = self.original() {
            write!(f, "{separator}status {} {:?}", parts.code, parts.message)?;
            for (name, value) in &parts.metadata {
                write!(f, " {name}={value:?}")?;
            }
        }
        Ok(())
    }
}

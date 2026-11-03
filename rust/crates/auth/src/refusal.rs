//! A member's original refusal diagnostics, shared by every primitive's
//! status conversion.
//!
//! A typed failure category names what happened; a refusal is everything else
//! the member said about it: its status code, message, details and product
//! response metadata (the `acyclic-` keys). Decoding keeps those verbatim and
//! encoding reproduces them, so a category plus its refusal round-trips the
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

/// The status one category encodes to when it carries no refusal.
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

/// Original diagnostics beyond a category. Only [`Refusal::capture`] makes
/// one, and only when the status said something its category's canonical
/// status does not, so a refusal never encodes to that canonical status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Refusal(Arc<Parts>);

impl Refusal {
    /// Retains what a status says beyond `canonical`, or `None` when it says
    /// nothing more. Work and allocation are linear in this status's own
    /// diagnostic bytes; a canonical status allocates nothing.
    #[must_use]
    pub fn capture(
        code: i32,
        message: &str,
        details: &[u8],
        metadata: &HeaderMap,
        canonical: Canonical<'_>,
    ) -> Option<Self> {
        let product = |name: &http::HeaderName| {
            name.as_str().starts_with(METADATA_PREFIX) && !canonical.owned.contains(&name.as_str())
        };
        if code == canonical.code
            && message == canonical.message
            && details == canonical.details
            && !metadata.keys().any(product)
        {
            return None;
        }
        let mut retained = HeaderMap::new();
        for (name, value) in metadata.iter().filter(|(name, _)| product(name)) {
            retained.append(name, value.clone());
        }
        Some(Self(Arc::new(Parts {
            code,
            message: message.to_owned(),
            details: Bytes::copy_from_slice(details),
            metadata: retained,
        })))
    }

    /// The member's original status parts.
    #[must_use]
    pub fn original(&self) -> &Parts {
        &self.0
    }

    /// The status to emit: a refusal's original parts verbatim, else `canonical`.
    #[must_use]
    pub fn encode(refusal: Option<&Self>, canonical: Canonical<'_>) -> Parts {
        refusal.map_or_else(
            || Parts {
                code: canonical.code,
                message: canonical.message.to_owned(),
                details: Bytes::copy_from_slice(canonical.details),
                metadata: HeaderMap::new(),
            },
            |refusal| Parts::clone(&refusal.0),
        )
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts = self.original();
        write!(f, ": status {} {:?}", parts.code, parts.message)?;
        for (name, value) in &parts.metadata {
            write!(f, " {name}={value:?}")?;
        }
        Ok(())
    }
}

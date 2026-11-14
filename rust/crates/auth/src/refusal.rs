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

#[cfg(all(feature = "javascript", target_arch = "wasm32"))]
pub mod javascript;

#[cfg(all(feature = "native-javascript", not(target_arch = "wasm32")))]
pub mod native;

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
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, ts_rs::TS)]
#[serde(deny_unknown_fields)]
#[ts(export_to = "auth/RefusalStatus.ts", rename = "RefusalStatus")]
pub struct Parts {
    /// gRPC status code number.
    #[serde(rename = "status")]
    #[ts(rename = "status")]
    pub code: i32,
    /// Status message.
    pub message: String,
    /// Status details.
    #[serde(
        serialize_with = "serialize_bytes",
        deserialize_with = "deserialize_bytes"
    )]
    #[ts(type = "Uint8Array")]
    pub details: Bytes,
    /// Product response metadata.
    #[serde(
        serialize_with = "serialize_metadata",
        deserialize_with = "deserialize_metadata"
    )]
    #[ts(type = "Array<[string, Uint8Array]>")]
    pub metadata: HeaderMap,
}

fn serialize_bytes<S: serde::Serializer>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_bytes(bytes)
}

fn deserialize_bytes<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Bytes, D::Error> {
    <Vec<u8> as serde::Deserialize>::deserialize(deserializer).map(Bytes::from)
}

fn deserialize_metadata<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<HeaderMap, D::Error> {
    use serde::de::Error as _;
    let fields = <Vec<(String, Vec<u8>)> as serde::Deserialize>::deserialize(deserializer)?;
    let mut metadata = HeaderMap::new();
    for (name, value) in fields {
        let name = http::HeaderName::from_bytes(name.as_bytes()).map_err(D::Error::custom)?;
        if !name.as_str().starts_with(METADATA_PREFIX) {
            return Err(D::Error::custom(
                "failure metadata is outside the product namespace",
            ));
        }
        let value = http::HeaderValue::from_bytes(&value).map_err(D::Error::custom)?;
        metadata.append(name, value);
    }
    Ok(metadata)
}

fn serialize_metadata<S: serde::Serializer>(
    metadata: &HeaderMap,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    struct Value<'a>(&'a http::HeaderValue);
    impl serde::Serialize for Value<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.serialize_bytes(self.0.as_bytes())
        }
    }
    serializer.collect_seq(
        metadata
            .iter()
            .map(|(name, value)| (name.as_str(), Value(value))),
    )
}

/// What a server said beyond a category: its [`Cause`], the member's original
/// status, or both, and never neither. The original status is kept only when
/// it said something its category's canonical status, cause included, does
/// not, so it never encodes to that canonical status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Refusal(Arc<Said>);

#[derive(Clone, Debug, Eq, PartialEq)]
enum Said {
    Cause {
        cause: Cause,
        original: Option<Parts>,
    },
    Status(Parts),
}

impl Refusal {
    /// The server's attribution alone; it encodes to its category's canonical
    /// status carrying `cause`.
    #[must_use]
    pub fn caused(cause: Cause) -> Self {
        Self(Arc::new(Said::Cause {
            cause,
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
        let original = differs(code, message, details, metadata, canonical).then(|| {
            let mut retained = HeaderMap::new();
            for (name, value) in metadata.iter().filter(|(name, _)| product(name, canonical)) {
                retained.append(name, value.clone());
            }
            Parts {
                code,
                message: message.to_owned(),
                details: Bytes::copy_from_slice(details),
                metadata: retained,
            }
        });
        Self::retain(cause, original)
    }

    /// Captures already owned status parts with the same admission as
    /// [`Self::capture`], moving the message and detail buffer without copying
    /// them. Only unowned product metadata is retained.
    #[must_use]
    pub fn capture_owned(
        mut parts: Parts,
        canonical: Canonical<'_>,
        cause: Option<Cause>,
    ) -> Option<Self> {
        let original = differs(
            parts.code,
            &parts.message,
            &parts.details,
            &parts.metadata,
            canonical,
        )
        .then(|| {
            if !parts.metadata.keys().all(|name| product(name, canonical)) {
                let mut retained = HeaderMap::new();
                let mut key = None;
                for (name, value) in parts.metadata {
                    if let Some(name) = name {
                        key = Some(name);
                    }
                    if let Some(name) = key.as_ref().filter(|name| product(name, canonical)) {
                        retained.append(name, value);
                    }
                }
                parts.metadata = retained;
            }
            parts
        });
        Self::retain(cause, original)
    }

    fn retain(cause: Option<Cause>, original: Option<Parts>) -> Option<Self> {
        match (cause, original) {
            (Some(cause), original) => Some(Self(Arc::new(Said::Cause { cause, original }))),
            (None, Some(parts)) => Some(Self(Arc::new(Said::Status(parts)))),
            (None, None) => None,
        }
    }

    /// The server's attribution, when it reported one.
    #[must_use]
    pub fn cause(&self) -> Option<&Cause> {
        match self.0.as_ref() {
            Said::Cause { cause, .. } => Some(cause),
            Said::Status(_) => None,
        }
    }

    /// Consumes this refusal's attribution, moving its strings when uniquely
    /// owned. If shared, clones only the attribution; other owners retain the
    /// original status.
    #[must_use]
    pub fn into_cause(self) -> Option<Cause> {
        match Arc::try_unwrap(self.0) {
            Ok(Said::Cause { cause, .. }) => Some(cause),
            Ok(Said::Status(_)) => None,
            Err(said) => Self(said).cause().cloned(),
        }
    }

    /// The member's original status parts, when they said more than the
    /// canonical status.
    #[must_use]
    pub fn original(&self) -> Option<&Parts> {
        match self.0.as_ref() {
            Said::Cause { original, .. } => original.as_ref(),
            Said::Status(parts) => Some(parts),
        }
    }

    /// The status whose body already carries its attribution. Original parts
    /// remain verbatim; otherwise no metadata encoding is necessary.
    #[must_use]
    pub fn encode_embedded(refusal: Option<&Self>, canonical: Canonical<'_>) -> Parts {
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

    /// Emit original parts verbatim, or encode attribution in metadata when
    /// the canonical body does not carry it. Encoding failures retain both
    /// the original refusal and the exact serialization/header cause.
    pub fn encode(
        refusal: Option<&Self>,
        canonical: Canonical<'_>,
    ) -> Result<Parts, EncodeFailure> {
        let mut parts = Self::encode_embedded(refusal, canonical);
        if let Some(refusal) = refusal.filter(|refusal| refusal.original().is_none())
            && let Some(cause) = refusal.cause().filter(|_| !canonical.cause_is_embedded)
        {
            let encode = || -> Result<http::HeaderValue, EncodeError> {
                let bytes = serde_json::to_vec(cause)?;
                let value = base64::engine::general_purpose::STANDARD.encode(bytes);
                Ok(value.parse()?)
            };
            let value = encode().map_err(|source| EncodeFailure {
                refusal: refusal.clone(),
                source,
            })?;
            parts.metadata.insert(CAUSE_METADATA, value);
        }
        Ok(parts)
    }
}

/// A refused status could not encode its diagnostic attribution.
#[derive(Debug, thiserror::Error)]
#[error("refusal attribution encoding failed: {source}")]
pub struct EncodeFailure {
    /// Original refusal, retained without formatting or replacing it.
    pub refusal: Refusal,
    /// Original encoding failure.
    #[source]
    pub source: EncodeError,
}

/// The original fallible operation in refusal encoding.
#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    /// Original JSON encoder failure.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Original HTTP metadata validation failure.
    #[error(transparent)]
    Header(#[from] http::header::InvalidHeaderValue),
}

fn product(name: &http::HeaderName, canonical: Canonical<'_>) -> bool {
    name.as_str().starts_with(METADATA_PREFIX) && !canonical.owned.contains(&name.as_str())
}

fn differs(
    code: i32,
    message: &str,
    details: &[u8],
    metadata: &HeaderMap,
    canonical: Canonical<'_>,
) -> bool {
    code != canonical.code
        || message != canonical.message
        || details != canonical.details
        || metadata.keys().any(|name| product(name, canonical))
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

impl std::fmt::Display for Parts {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "status {} {:?}", self.code, self.message)
    }
}

impl std::error::Error for Parts {}

impl std::error::Error for Refusal {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.0.as_ref() {
            Said::Cause { cause, .. } => Some(cause),
            Said::Status(parts) => Some(parts),
        }
    }
}

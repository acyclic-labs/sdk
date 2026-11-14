//! Credential callbacks and validity-aware snapshots shared by product transports.
mod action;
mod cache;
mod custodian;
#[cfg(all(target_arch = "wasm32", feature = "js-credential"))]
pub mod javascript;
mod watch;

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
#[path = "../tests/common/credential.rs"]
pub mod testing;
pub use action::{CredentialAction, CredentialPending};
pub use cache::{
    CredentialCache, CredentialCacheError, CredentialLease, CredentialRevision, CredentialSnapshot,
    CredentialToken, CredentialValidity,
};
pub use custodian::{CredentialRequest, CredentialResolution, CredentialSource, CustodianPlatform};
use std::{future::Future, pin::Pin};
pub use watch::CredentialWatch;
use zeroize::Zeroizing;

#[cfg(target_arch = "wasm32")]
use std::rc::Rc as Shared;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc as Shared;

/// Non-secret source partition and absolute lifetime of an admitted lease.
/// This is freshness metadata, never an authenticated permission or policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialBinding {
    /// Custodian-supplied account/audience/auth-context revision.
    pub revision: CredentialRevision,
    /// Half-open absolute expiry; reading this does not extend the lease.
    pub expires_at_unix_seconds: u64,
}

/// A failed callback or a value that cannot be sent as a credential.
#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    /// Provider failure, with its original diagnostic available as the source.
    #[error("credential provider failed")]
    Provider(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Original cache failure and its verdict, captured once at the boundary.
    /// Reading the verdict never walks a provider's diagnostic source chain.
    #[error("credential cache failed: {source}")]
    Cache {
        /// Original validity or refresh failure, without boxing or copying it.
        #[source]
        source: CredentialCacheError,
        /// Final verdict, or None when source freshness cannot be established.
        cause: Option<crate::DenialCause>,
    },
    /// Empty or header-delimited credentials fail before transport.
    #[error("invalid credential")]
    Invalid,
    /// The credential cannot be represented as an HTTP header.
    #[error("invalid credential header")]
    Header(#[source] http::header::InvalidHeaderValue),
    /// The credential cannot be represented as native gRPC metadata.
    #[cfg(all(not(target_arch = "wasm32"), feature = "grpc-metadata"))]
    #[error("invalid credential metadata")]
    Metadata(#[source] tonic::metadata::errors::InvalidMetadataValueBytes),
    /// The provider names the channel's own transport identity and has no
    /// bearer to resolve; only transports that honor it may dispatch.
    #[error("credential is the channel's transport identity")]
    TransportIdentity,
}

impl CredentialError {
    /// The final credential verdict. An arbitrary provider diagnostic and a
    /// missing bearer transport are availability failures, never inferred denials.
    #[must_use]
    pub const fn denial_cause(&self) -> Option<crate::DenialCause> {
        match self {
            Self::Invalid | Self::Header(_) => Some(crate::DenialCause::Malformed),
            #[cfg(all(not(target_arch = "wasm32"), feature = "grpc-metadata"))]
            Self::Metadata(_) => Some(crate::DenialCause::Malformed),
            Self::Cache { cause, .. } => *cause,
            Self::Provider(_) | Self::TransportIdentity => None,
        }
    }
}

impl From<CredentialCacheError> for CredentialError {
    fn from(source: CredentialCacheError) -> Self {
        let cause = source.denial_cause();
        Self::Cache { source, cause }
    }
}

/// One native renewal invocation, cancelled by dropping its future.
#[cfg(not(target_arch = "wasm32"))]
pub type CredentialFuture = Pin<Box<dyn Future<Output = Result<String, CredentialError>> + Send>>;
/// One browser renewal invocation, cancelled by dropping its future.
#[cfg(target_arch = "wasm32")]
pub type CredentialFuture = Pin<Box<dyn Future<Output = Result<String, CredentialError>>>>;

#[cfg(not(target_arch = "wasm32"))]
type Callback = dyn Fn() -> CredentialFuture + Send + Sync;
#[cfg(target_arch = "wasm32")]
type Callback = dyn Fn() -> CredentialFuture;

/// Immutable callback configuration. Raw callbacks resolve afresh. Convert a
/// validity-aware `CredentialCache` into this type to share trusted snapshots.
#[derive(Clone)]
pub struct CredentialProvider(Source);

#[derive(Clone)]
enum Source {
    Raw(Shared<Callback>),
    Cached(CredentialCache),
    Custodian(Shared<dyn CredentialSource>),
    TransportIdentity,
}

/// Exact credential custody for one transport attempt. Retain through the reply
/// and reject only on authenticated credential rejection (401/UNAUTHENTICATED),
/// never on resource policy denial, transport failure or caller cancellation.
#[derive(Clone)]
pub struct ResolvedCredential(Resolved);

#[derive(Clone)]
enum Resolved {
    Raw(Shared<Zeroizing<String>>),
    Cached {
        cache: CredentialCache,
        lease: CredentialLease,
    },
    Custodian(Shared<dyn CredentialRequest>),
}

impl std::fmt::Debug for ResolvedCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ResolvedCredential(..)")
    }
}

impl ResolvedCredential {
    /// Retain an existing owner's exact request, without creating a cache.
    /// Invalid bytes and already-refused requests fail before transport.
    pub async fn from_custodian(request: impl CredentialRequest) -> Result<Self, CredentialError> {
        if !valid_credential(request.token()) {
            return Err(CredentialError::Invalid);
        }
        request.check().await?;
        Ok(Self(Resolved::Custodian(Shared::new(request))))
    }

    /// Project trusted source metadata without exposing or decoding bearer bytes.
    /// Raw callbacks have no trusted validity and return None. Retain this exact
    /// request and call check before dispatch or accepting its result.
    pub fn binding(&self) -> Option<CredentialBinding> {
        match &self.0 {
            Resolved::Raw(_) => None,
            Resolved::Cached { lease, .. } => Some(CredentialBinding {
                revision: lease.revision(),
                expires_at_unix_seconds: lease.expires_at(),
            }),
            Resolved::Custodian(request) => request.binding(),
        }
    }

    /// Borrow only for the admitted request.
    pub fn as_str(&self) -> &str {
        match &self.0 {
            Resolved::Raw(token) => token,
            Resolved::Cached { lease, .. } => lease.token(),
            Resolved::Custodian(request) => request.token(),
        }
    }

    /// Revalidate after transport acquisition, immediately before dispatch.
    pub fn check(&self) -> CredentialAction<()> {
        match &self.0 {
            Resolved::Raw(_) => Ok(()).into(),
            Resolved::Cached { cache, lease } => {
                cache.check(lease).map_err(CredentialError::from).into()
            }
            Resolved::Custodian(request) => request.check(),
        }
    }

    /// Non-dispatching observation for synchronous getters and retained views.
    /// None means the source has no trusted synchronous guard; it must not be
    /// treated as permission to expose retained data. Some(Err) refuses access
    /// immediately, independently of watch delivery. No expiry is extended and
    /// no resolution, callback dispatch or asynchronous check is started here.
    /// Async `check` remains required around transport dispatch and admission.
    pub fn retained_access(&self) -> Option<Result<(), CredentialError>> {
        match &self.0 {
            Resolved::Raw(_) => None,
            Resolved::Cached { cache, lease } => {
                Some(cache.check(lease).map_err(CredentialError::from))
            }
            Resolved::Custodian(request) => request.retained_access(),
        }
    }

    /// A late rejection cannot discard a newer request's snapshot.
    pub fn reject(&self) -> CredentialAction<bool> {
        match &self.0 {
            Resolved::Raw(_) => Ok(false).into(),
            Resolved::Cached { cache, lease } => {
                cache.reject(lease).map_err(CredentialError::from).into()
            }
            Resolved::Custodian(request) => request.reject(),
        }
    }
}

impl std::ops::Deref for ResolvedCredential {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Debug for CredentialProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CredentialProvider(..)")
    }
}

impl From<CredentialCache> for CredentialProvider {
    fn from(cache: CredentialCache) -> Self {
        Self(Source::Cached(cache))
    }
}

impl CredentialProvider {
    /// Forward an existing source's resolution/watch and exact request custody.
    /// The source remains the only refresh and cache owner.
    pub fn from_custodian(source: impl CredentialSource) -> Self {
        Self(Source::Custodian(Shared::new(source)))
    }

    /// Subscribe before resolving or accepting cached reader state, then check
    /// the admitted request's binding and validity. Custodians must publish a
    /// changed/unavailable revision and call CredentialCache::invalidate on
    /// source events. Raw callbacks return None, never a fabricated identity.
    pub fn watch(&self) -> CredentialAction<Option<CredentialWatch>> {
        match &self.0 {
            Source::Raw(_) => Ok(None).into(),
            Source::Cached(cache) => cache
                .watch()
                .map(Some)
                .map_err(CredentialError::from)
                .into(),
            Source::Custodian(source) => source.watch(),
            Source::TransportIdentity => Ok(None).into(),
        }
    }

    /// The caller is the channel's own transport identity (mutual TLS): no
    /// authorization is attached. The server admits the peer by its verified
    /// certificate, which breaks the bearer-before-trust cycle on trust
    /// Streams. Transports that cannot omit authorization refuse it via
    /// [`CredentialError::TransportIdentity`], never sending an empty bearer.
    #[must_use]
    pub const fn transport_identity() -> Self {
        Self(Source::TransportIdentity)
    }

    /// Whether requests carry no authorization and rely on the channel's
    /// transport identity.
    #[must_use]
    pub const fn is_transport_identity(&self) -> bool {
        matches!(self.0, Source::TransportIdentity)
    }

    /// Capture a provider once, invoking it afresh for every request.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new<F, Fut>(callback: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, CredentialError>> + Send + 'static,
    {
        Self(Source::Raw(Shared::new(move || -> CredentialFuture {
            Box::pin(callback())
        })))
    }

    /// Capture a browser provider without requiring JavaScript futures to be Send.
    #[cfg(target_arch = "wasm32")]
    pub fn new<F, Fut>(callback: F) -> Self
    where
        F: Fn() -> Fut + 'static,
        Fut: Future<Output = Result<String, CredentialError>> + 'static,
    {
        Self(Source::Raw(Shared::new(move || -> CredentialFuture {
            Box::pin(callback())
        })))
    }

    /// Resolve and validate before sending. Errors propagate without fallback;
    /// clones retain the same buffer, scrubbed when its final request retires.
    pub async fn resolve(&self) -> Result<ResolvedCredential, CredentialError> {
        match &self.0 {
            Source::Raw(callback) => {
                let token = Zeroizing::new(callback().await?);
                if !valid_credential(&token) {
                    return Err(CredentialError::Invalid);
                }
                Ok(ResolvedCredential(Resolved::Raw(Shared::new(token))))
            }
            Source::Cached(cache) => {
                let lease = cache.resolve().await.map_err(CredentialError::from)?;
                Ok(ResolvedCredential(Resolved::Cached {
                    cache: cache.clone(),
                    lease,
                }))
            }
            Source::Custodian(source) => source.resolve().await,
            Source::TransportIdentity => Err(CredentialError::TransportIdentity),
        }
    }
}

/// Opaque credential admission, shared with static-token constructors.
pub fn valid_credential(value: &str) -> bool {
    !value.trim().is_empty() && !value.contains(['\r', '\n', '\0'])
}

/// Encodes admitted bearer text as the shared WebSocket subprotocol.
/// The original credential and encoded backing remain caller-owned secrets.
/// Returns `Invalid` if the encoded length is not representable, or the
/// original allocation failure as `Provider`, before writing any secret.
#[must_use]
pub fn websocket_protocol(credential: &str) -> Result<Zeroizing<String>, CredentialError> {
    use base64::Engine as _;
    const PREFIX: &str = "acyclic.bearer.";
    let capacity = base64::encoded_len(credential.len(), false)
        .and_then(|length| length.checked_add(PREFIX.len()))
        .ok_or(CredentialError::Invalid)?;
    // Reserve before writing any secret so growth cannot leave an old backing.
    let mut protocol = Zeroizing::new(String::new());
    protocol
        .try_reserve_exact(capacity)
        .map_err(|error| CredentialError::Provider(Box::new(error)))?;
    protocol.push_str(PREFIX);
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode_string(credential.as_bytes(), &mut protocol);
    Ok(protocol)
}

/// The one text form of an encoded capability: unpadded base64url, as a
/// bearer credential, an environment value or a string wire field carries it.
#[must_use]
pub fn capability_text(capability: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(capability)
}

/// The encoded capability a [`capability_text`] names.
///
/// # Errors
/// [`CredentialError::Invalid`] for anything else.
pub fn capability_from_text(text: &str) -> Result<Vec<u8>, CredentialError> {
    use base64::Engine as _;
    let mut decoded = Zeroizing::new(Vec::new());
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode_vec(text, &mut decoded)
        .map_err(|_| CredentialError::Invalid)?;
    Ok(std::mem::take(&mut *decoded))
}

/// Serde adapter for capability bytes using the same text form as
/// [`capability_text`]. Use `#[serde(with = "acyclic_auth::credential::capability_serde")]`
/// on an owned byte field. The protobuf carrier remains bytes.
///
/// Serialization writes directly into the original serializer. Deserialization
/// protects temporary text and partially decoded bytes on failure; the resulting
/// bytes retain the field owner's custody. Decoding authenticates nothing: the
/// original verifier must still admit the capability before use.
pub mod capability_serde {
    use serde::{Deserialize, Deserializer, Serializer};

    /// Serialize capability bytes without a temporary encoded string.
    ///
    /// # Errors
    /// The serializer's typed error if its original sink refuses the text.
    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&base64::display::Base64Display::new(
            bytes,
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        ))
    }

    /// Decode only canonical unpadded base64url, preserving failure cleanup.
    ///
    /// # Errors
    /// The deserializer's typed error for a non-string or invalid capability text.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let encoded = super::Zeroizing::new(String::deserialize(deserializer)?);
        super::capability_from_text(&encoded).map_err(serde::de::Error::custom)
    }
}

/// Encode bearer metadata in one exactly sized buffer. This does not admit a
/// request: validate static credentials first, or retain resolved custody and
/// check it at dispatch. A shared-buffer header constructor retains the same
/// protected allocation through its final clone, without copying the payload.
pub fn bearer_bytes(value: &str) -> bytes::Bytes {
    const PREFIX: &[u8] = b"Bearer ";
    let mut bytes = Zeroizing::new(Vec::with_capacity(PREFIX.len() + value.len()));
    bytes.extend_from_slice(PREFIX);
    bytes.extend_from_slice(value.as_bytes());
    crate::operation::ProtectedBytes::new(bytes).into_bytes()
}

fn validated_bearer(value: &str) -> Result<bytes::Bytes, CredentialError> {
    if !valid_credential(value) {
        return Err(CredentialError::Invalid);
    }
    Ok(bearer_bytes(value))
}

/// Construct sensitive HTTP authorization, retaining protected bytes through clones.
/// This checks representation, not the authority or freshness of a credential.
///
/// # Errors
/// Malformed credentials retain their original header parser failure when present.
pub fn bearer_header(value: &str) -> Result<http::HeaderValue, CredentialError> {
    let mut header = http::HeaderValue::from_maybe_shared(validated_bearer(value)?)
        .map_err(CredentialError::Header)?;
    header.set_sensitive(true);
    Ok(header)
}

/// Construct sensitive native gRPC authorization over the same protected bytes.
/// This checks representation, not the authority or freshness of a credential.
///
/// # Errors
/// Malformed credentials retain their original metadata parser failure when present.
#[cfg(all(not(target_arch = "wasm32"), feature = "grpc-metadata"))]
pub fn bearer_metadata(
    value: &str,
) -> Result<tonic::metadata::MetadataValue<tonic::metadata::Ascii>, CredentialError> {
    let mut metadata = tonic::metadata::MetadataValue::try_from(validated_bearer(value)?)
        .map_err(CredentialError::Metadata)?;
    metadata.set_sensitive(true);
    Ok(metadata)
}

/// Admit an origin only: no embedded credential, query, fragment or path.
/// HTTPS is required except for the loopback hosts used by local tools.
pub fn endpoint(value: &str) -> Result<url::Url, CredentialError> {
    let url = admit_endpoint(value).map_err(|_| CredentialError::Invalid)?;
    if url.path() == "/" {
        Ok(url)
    } else {
        Err(CredentialError::Invalid)
    }
}

/// Transport endpoint refusal without retaining URL user information.
#[derive(Debug, thiserror::Error)]
pub enum EndpointError {
    /// Original URL parser failure.
    #[error(transparent)]
    Parse(#[from] url::ParseError),
    /// HTTP is permitted only for literal loopback hosts or localhost.
    #[error("insecure endpoint")]
    Insecure,
    /// Authority must not carry credentials.
    #[error("endpoint contains credentials")]
    Credentials,
    /// A query is not a route base.
    #[error("endpoint contains a query")]
    Query,
    /// A fragment is not a route base.
    #[error("endpoint contains a fragment")]
    Fragment,
}

/// Whether a parsed URL/URI host is a loopback IP literal or localhost.
fn is_loopback_host(host: &str) -> bool {
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn admit_endpoint(value: &str) -> Result<url::Url, EndpointError> {
    let url = url::Url::parse(value)?;
    if url.host().is_none()
        || !(url.scheme() == "https"
            || url.scheme() == "http" && url.host_str().is_some_and(is_loopback_host))
    {
        return Err(EndpointError::Insecure);
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(EndpointError::Credentials);
    }
    if url.query().is_some() {
        return Err(EndpointError::Query);
    }
    if url.fragment().is_some() {
        return Err(EndpointError::Fragment);
    }
    Ok(url)
}

/// Admit an HTTPS or loopback HTTP route base, retaining its path prefix.
/// The final slash makes relative route joins stay below that prefix.
pub fn base_endpoint(value: &str) -> Result<url::Url, EndpointError> {
    let mut url = admit_endpoint(value)?;
    if !url.path().ends_with('/') {
        url.path_segments_mut()
            .map_err(|()| EndpointError::Insecure)?
            .push("");
    }
    Ok(url)
}

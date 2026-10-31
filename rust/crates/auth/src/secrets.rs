//! Shared secrets client, authorization targets and the canonical [`SecretRef`].
//! Transports carry bytes; they never interpret responses or expose response
//! bodies as diagnostics. Authorization is the one typed resource grant path:
//! [`put_target`], [`list_target`] and [`delete_target`] name the catalog
//! action and the canonical `secrets:/<scope>[/<name>]` resource.
use crate::contract::*;
use crate::credential::CredentialProvider;
use serde::{Serialize, de::DeserializeOwned};
use std::{future::Future, pin::Pin};
use zeroize::Zeroizing;
pub mod conformance;

/// Catalog action for create or replace.
pub const PUT: &str = "secrets/put";
/// Catalog action for listing one scope.
pub const LIST: &str = "secrets/list";
/// Catalog action for deletion.
pub const DELETE: &str = "secrets/delete";

/// Canonical resource of a whole scope: `secrets:/<scope>`. A prefix grant on
/// it covers every name in the scope.
pub fn scope_resource(scope: &str) -> String {
    format!("secrets:/{}", crate::encode_component(scope.as_bytes()))
}

/// Canonical resource of one variable: `secrets:/<scope>/<name>`.
pub fn variable_resource(scope: &str, name: &str) -> String {
    format!(
        "{}/{}",
        scope_resource(scope),
        crate::encode_component(name.as_bytes())
    )
}

/// Action and resource a provider checks for a put, through the shared authority.
pub fn put_target(request: &SecretPutRequest) -> (&'static str, String) {
    (PUT, variable_resource(&request.scope, &request.name))
}

/// Action and resource a provider checks for a list page.
pub fn list_target(request: &SecretListRequest) -> (&'static str, String) {
    (LIST, scope_resource(&request.scope))
}

/// Action and resource a provider checks for a deletion.
pub fn delete_target(request: &SecretDeleteRequest) -> (&'static str, String) {
    (DELETE, variable_resource(&request.scope, &request.name))
}

/// Decode an exact protobuf request, rejecting discarded fields (including an
/// account selector), duplicate fields and noncanonical encodings. All browser
/// request entry points use this admission before resolving credentials.
pub fn decode_request<P, R>(input: &[u8]) -> Result<R, Error>
where
    P: prost::Message + Default,
    R: TryFrom<P>,
{
    let wire = P::decode(input).map_err(|_| Error::InvalidRequest)?;
    let canonical = Zeroizing::new(wire.encode_to_vec());
    if canonical.as_slice() != input {
        return Err(Error::InvalidRequest);
    }
    R::try_from(wire).map_err(|_| Error::InvalidRequest)
}

/// Redacted failures. Neither provider errors nor their source chains survive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// Invalid or unsupported request shape.
    #[error("secrets: invalid request")]
    InvalidRequest,
    /// Provider reply violates the contract, including protected value leakage
    /// or a page that does not continue its cursor's listing.
    #[error("secrets: invalid response")]
    InvalidResponse,
    /// No secrets grant covers this operation.
    #[error("secrets: permission denied")]
    PermissionDenied,
    /// Scope generation changed before the mutation.
    #[error("secrets: generation conflict; list the scope before submitting a new mutation")]
    Conflict,
    /// The scope changed after the cursor's generation. The listing cannot be
    /// continued; start a new one with a null cursor.
    #[error("secrets: listing changed; restart the listing with a null cursor")]
    ListingChanged,
    /// Credential resolution failed; never retain the callback's diagnostic.
    #[error("secrets: credential unavailable")]
    Credential,
    /// Transport failure, without URL, headers, body or source diagnostic.
    #[error("secrets: transport failed")]
    Transport,
    /// Non-success status; response body is never included.
    #[error("secrets: HTTP status {0}")]
    Http(u16),
}

impl Error {
    /// Stable diagnostic code, without provider-controlled text.
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::InvalidResponse => "invalid_response",
            Self::PermissionDenied => "permission_denied",
            Self::Conflict => "generation_conflict",
            Self::ListingChanged => "listing_changed",
            Self::Credential => "credential_unavailable",
            Self::Transport => "transport_failed",
            Self::Http(_) => "http_error",
        }
    }
}

fn label(value: &str) -> bool {
    !value.is_empty() && !value.chars().any(char::is_control)
}
fn require(valid: bool) -> Result<(), crate::Error> {
    if valid {
        Ok(())
    } else {
        Err(crate::Error::Invalid)
    }
}
pub(crate) fn validate_put(value: &SecretPutRequest) -> Result<(), crate::Error> {
    require(
        label(&value.scope)
            && label(&value.name)
            && matches!(value.kind, SecretKind::Plain | SecretKind::Protected),
    )
}
pub(crate) fn validate_cursor(value: &SecretListCursor) -> Result<(), crate::Error> {
    require(label(&value.after))
}
pub(crate) fn validate_list(value: &SecretListRequest) -> Result<(), crate::Error> {
    require(label(&value.scope))
}
pub(crate) fn validate_delete(value: &SecretDeleteRequest) -> Result<(), crate::Error> {
    require(label(&value.scope) && label(&value.name))
}

/// The reference that selects a secret for its host credential adapter.
/// Its version is the scope generation that
/// wrote the value, which is never reused, so a reference resolves to the same
/// bytes or fails typed. Only this reference is ever recorded; the value is
/// resolved by the host without exposing it to guest execution.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SecretRef {
    scope: String,
    name: String,
    version: u64,
}

impl SecretRef {
    /// Admits a nonempty control-free scope and name and a written version.
    pub fn new(scope: String, name: String, version: u64) -> Result<Self, Error> {
        if label(&scope) && label(&name) && version != 0 {
            Ok(Self {
                scope,
                name,
                version,
            })
        } else {
            Err(Error::InvalidRequest)
        }
    }
    /// Owner-local scope.
    pub fn scope(&self) -> &str {
        &self.scope
    }
    /// Variable name.
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Immutable version identity.
    pub fn version(&self) -> u64 {
        self.version
    }
    /// Canonical `secrets:/<scope>/<name>` resource the reference names.
    pub fn resource(&self) -> String {
        variable_resource(&self.scope, &self.name)
    }
}

/// Wire form of [`SecretRef`]: the shared `acyclic.harness.v2.SecretRef`
/// (`harness/v2/resources.proto`) that every contract embeds.
#[derive(Clone, PartialEq, prost::Message)]
pub struct SecretRefProto {
    /// Owner-local scope.
    #[prost(string, tag = "1")]
    pub scope: String,
    /// Variable name.
    #[prost(string, tag = "2")]
    pub name: String,
    /// Immutable version identity.
    #[prost(uint64, tag = "3")]
    pub version: u64,
}

impl prost::Name for SecretRefProto {
    const NAME: &'static str = "SecretRef";
    const PACKAGE: &'static str = "acyclic.harness.v2";
}

impl TryFrom<SecretRefProto> for SecretRef {
    type Error = Error;
    fn try_from(value: SecretRefProto) -> Result<Self, Error> {
        Self::new(value.scope, value.name, value.version)
    }
}

impl From<SecretRef> for SecretRefProto {
    fn from(value: SecretRef) -> Self {
        Self {
            scope: value.scope,
            name: value.name,
            version: value.version,
        }
    }
}

// Protobuf JSON is the one JSON shape: `version` is a decimal string on output
// and either spelling on input, so serde and descriptor codecs agree.
impl Serialize for SecretRef {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct as _;
        let mut fields = serializer.serialize_struct("SecretRef", 3)?;
        fields.serialize_field("scope", &self.scope)?;
        fields.serialize_field("name", &self.name)?;
        fields.serialize_field("version", &self.version.to_string())?;
        fields.end()
    }
}

impl<'de> serde::Deserialize<'de> for SecretRef {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        enum Version {
            Number(u64),
            Text(String),
        }
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            scope: String,
            name: String,
            version: Version,
        }
        let fields = Fields::deserialize(deserializer)?;
        let version = match fields.version {
            Version::Number(version) => version,
            Version::Text(text) => text
                .parse()
                .map_err(|_| serde::de::Error::custom("secret version must be a decimal u64"))?,
        };
        Self::new(fields.scope, fields.name, version)
            .map_err(|_| serde::de::Error::custom("invalid secret reference"))
    }
}

/// A resolved value, held only by the host: never printed, zeroed on drop.
pub struct SecretValue(Zeroizing<Vec<u8>>);

impl SecretValue {
    /// Takes ownership of `value`.
    pub fn new(value: Vec<u8>) -> Self {
        Self(Zeroizing::new(value))
    }
    /// The bytes, for the original host credential adapter.
    pub fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SecretValue(..)")
    }
}

/// Why a reference did not resolve. Never carries the value or provider text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ResolveError {
    /// The variable does not exist.
    #[error("secret reference not found")]
    NotFound,
    /// The variable exists at another version; a reference never resolves to
    /// bytes other than the ones its version wrote.
    #[error("secret reference version is no longer current")]
    VersionGone,
    /// No secrets grant covers the reference.
    #[error("secret reference permission denied")]
    PermissionDenied,
    /// This host has no secrets source.
    #[error("secret references are unavailable on this host")]
    Unavailable,
}

/// Host-side resolution future.
#[cfg(not(target_arch = "wasm32"))]
pub type ResolveFuture<'a> =
    Pin<Box<dyn Future<Output = Result<SecretValue, ResolveError>> + Send + 'a>>;
/// Host-side resolution future, with JavaScript's single-threaded ownership.
#[cfg(target_arch = "wasm32")]
pub type ResolveFuture<'a> = Pin<Box<dyn Future<Output = Result<SecretValue, ResolveError>> + 'a>>;

/// The host's one secrets source. It is never reachable from the API: the
/// public surface has no read action. Only host credential adapters hold a
/// resolver; resolved values never enter guest execution.
#[cfg(not(target_arch = "wasm32"))]
pub trait Resolver: Send + Sync {
    /// Resolves exactly `reference`, or fails typed.
    fn resolve<'a>(&'a self, reference: &'a SecretRef) -> ResolveFuture<'a>;
}
/// The host's one secrets source, with JavaScript's single-threaded ownership.
#[cfg(target_arch = "wasm32")]
pub trait Resolver {
    /// Resolves exactly `reference`, or fails typed.
    fn resolve<'a>(&'a self, reference: &'a SecretRef) -> ResolveFuture<'a>;
}

/// Wire reply. Debug deliberately does not include the bytes.
pub struct Response {
    /// Actual HTTP status.
    pub status: u16,
    /// Caller-owned transport bytes, scrubbed after decoding.
    pub body: Zeroizing<Vec<u8>>,
}

/// Native request future; cancellation drops the transport request.
#[cfg(not(target_arch = "wasm32"))]
pub type TransportFuture<'a> = Pin<Box<dyn Future<Output = Result<Response, Error>> + Send + 'a>>;
/// Browser request future, with JavaScript's single-threaded ownership.
#[cfg(target_arch = "wasm32")]
pub type TransportFuture<'a> = Pin<Box<dyn Future<Output = Result<Response, Error>> + 'a>>;

/// Thin HTTP adapter. Implementations must use the fixed path unchanged, carry
/// credentials only as a sensitive header, reject redirects and suppress body
/// diagnostics. No retries, interpretation or authority logic lives here.
pub trait Transport {
    /// Send exactly one POST. Borrowed inputs remain owned until completion.
    fn post<'a>(
        &'a self,
        path: &'static str,
        bearer: &'a str,
        body: &'a [u8],
    ) -> TransportFuture<'a>;
}

/// One client implementation used by native callers, the CLI and WASM.
pub struct Client<T> {
    transport: T,
    credentials: CredentialProvider,
}

impl<T: Transport> Client<T> {
    /// Use an existing credential provider, resolved freshly for each call.
    pub fn new(transport: T, credentials: CredentialProvider) -> Self {
        Self {
            transport,
            credentials,
        }
    }

    async fn post<R: DeserializeOwned>(
        &self,
        path: &'static str,
        body: &impl Serialize,
    ) -> Result<R, Error> {
        let body = Zeroizing::new(serde_json::to_vec(body).map_err(|_| Error::InvalidRequest)?);
        let changed = self
            .credentials
            .watch()
            .await
            .map_err(|_| Error::Credential)?;
        let operation = async {
            let bearer = self
                .credentials
                .resolve()
                .await
                .map_err(|_| Error::Credential)?;
            bearer.check().await.map_err(|_| Error::Credential)?;
            let response = self.transport.post(path, &bearer, &body).await?;
            match response.status {
                200..=299 => {
                    bearer.check().await.map_err(|_| Error::Credential)?;
                    serde_json::from_slice(&response.body).map_err(|_| Error::InvalidResponse)
                }
                401 => {
                    // Only an authenticated rejection retires the exact credential.
                    let _ = bearer.reject().await;
                    Err(Error::PermissionDenied)
                }
                403 => Err(Error::PermissionDenied),
                409 => Err(Error::Conflict),
                status => Err(Error::Http(status)),
            }
        };
        if let Some(changed) = changed {
            use futures::FutureExt;
            let changed = changed.fuse();
            let operation = operation.fuse();
            futures::pin_mut!(changed, operation);
            futures::select_biased! {
                _ = changed => Err(Error::Credential),
                result = operation => result,
            }
        } else {
            operation.await
        }
    }

    /// Create or replace exactly at the supplied scope generation. No replay or
    /// automatic retry can turn a conflict into a successful overwrite.
    pub async fn put(&self, request: &SecretPutRequest) -> Result<SecretMutationReply, Error> {
        validate_put(request).map_err(|_| Error::InvalidRequest)?;
        let reply: SecretMutationReply = self.post("/v1/identity/secrets/put", request).await?;
        mutation(
            reply,
            &request.name,
            request.kind,
            request.expected_generation,
        )
    }

    /// One page of one generation. Plain values and protected metadata only; a
    /// protected row carrying a value is unrepresentable and refused on decode.
    /// A cursor whose generation is no longer current fails `ListingChanged`,
    /// and a page from another generation fails `InvalidResponse`: no listing
    /// is ever silently mixed across generations.
    pub async fn list(&self, request: &SecretListRequest) -> Result<SecretListReply, Error> {
        validate_list(request).map_err(|_| Error::InvalidRequest)?;
        if let Some(cursor) = &request.cursor {
            validate_cursor(cursor).map_err(|_| Error::InvalidRequest)?;
        }
        let reply: SecretListReply = match self.post("/v1/identity/secrets/list", request).await {
            Err(Error::Conflict) => return Err(Error::ListingChanged),
            reply => reply?,
        };
        page(request, reply)
    }

    /// Delete at the supplied scope generation, returning metadata only.
    pub async fn delete(
        &self,
        request: &SecretDeleteRequest,
    ) -> Result<SecretMutationReply, Error> {
        validate_delete(request).map_err(|_| Error::InvalidRequest)?;
        let reply: SecretMutationReply = self.post("/v1/identity/secrets/delete", request).await?;
        mutation(
            reply,
            &request.name,
            SecretKind::Deleted,
            request.expected_generation,
        )
    }
}

// The version of a mutation is its own generation, which is what makes a
// SecretRef version immutable: a later write of the same name gets a new one.
fn mutation(
    reply: SecretMutationReply,
    name: &str,
    kind: SecretKind,
    expected: u64,
) -> Result<SecretMutationReply, Error> {
    if reply.name != name
        || reply.kind != kind
        || reply.generation <= expected
        || reply.version != reply.generation
    {
        return Err(Error::InvalidResponse);
    }
    Ok(reply)
}

// O(page): one pass over the returned rows, no state beyond the previous name.
fn page(request: &SecretListRequest, reply: SecretListReply) -> Result<SecretListReply, Error> {
    let fenced = request
        .cursor
        .as_ref()
        .is_none_or(|cursor| cursor.generation == reply.generation);
    let bounded = request.page_size == 0 || reply.variables.len() <= request.page_size as usize;
    let mut previous = request.cursor.as_ref().map(|cursor| cursor.after.as_str());
    for variable in &reply.variables {
        let (name, version) = variable.identity();
        if !label(name)
            || version == 0
            || version > reply.generation
            || previous.is_some_and(|previous| previous.as_bytes() >= name.as_bytes())
        {
            return Err(Error::InvalidResponse);
        }
        previous = Some(name);
    }
    let continues = reply.next_cursor.as_ref().is_none_or(|next| {
        next.generation == reply.generation
            && Some(next.after.as_str()) == previous
            && !reply.variables.is_empty()
    });
    if !fenced || !bounded || !continues {
        return Err(Error::InvalidResponse);
    }
    Ok(reply)
}

/// Native HTTPS adapter for the shared client. Local HTTP is restricted to
/// loopback, allowing real in-process conformance providers without bearer leaks.
#[cfg(all(feature = "client", not(target_arch = "wasm32")))]
pub struct HttpTransport {
    endpoint: url::Url,
    client: reqwest::Client,
}

#[cfg(all(feature = "client", not(target_arch = "wasm32")))]
impl HttpTransport {
    /// Create a redirect-free HTTP adapter. Origin validation happens before
    /// any credential resolution or network operation.
    pub fn new(origin: &str) -> Result<Self, Error> {
        Ok(Self {
            endpoint: crate::credential::endpoint(origin).map_err(|_| Error::InvalidRequest)?,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .build()
                .map_err(|_| Error::Transport)?,
        })
    }
}

#[cfg(all(feature = "client", not(target_arch = "wasm32")))]
impl Transport for HttpTransport {
    fn post<'a>(
        &'a self,
        path: &'static str,
        bearer: &'a str,
        body: &'a [u8],
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let mut authorization =
                reqwest::header::HeaderValue::from_bytes(&crate::credential::bearer_bytes(bearer))
                    .map_err(|_| Error::Credential)?;
            authorization.set_sensitive(true);
            let response = self
                .client
                .post(
                    self.endpoint
                        .join(path)
                        .map_err(|_| Error::InvalidRequest)?,
                )
                .header(reqwest::header::AUTHORIZATION, authorization)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.to_vec())
                .send()
                .await
                .map_err(|_| Error::Transport)?;
            let status = response.status().as_u16();
            // Never consume an error body: even a malicious provider cannot
            // attach a planted value to a propagated error or source chain.
            let body = if (200..300).contains(&status) {
                response
                    .bytes()
                    .await
                    .map_err(|_| Error::Transport)?
                    .to_vec()
            } else {
                Vec::new()
            };
            Ok(Response {
                status,
                body: Zeroizing::new(body),
            })
        })
    }
}

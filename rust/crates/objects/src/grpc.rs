//! Authenticated gRPC client and account-resolving service adapter.
use crate::convert;
use crate::{
    Blobs, ByteRange, Collection, Deletion, Digest, Error, Head, Manifest, Object, Page, Put,
    Retained, RootId, Usage, wire,
};
pub use acyclic_auth::credential::CredentialProvider;
use acyclic_stream::wire_codec::{cut_from_wire, revision_from_wire, revision_to_wire};
use acyclic_stream::{OwnedBytes, Revision, RevisionCut, RevisionTracker, StreamError};
use prost::Message;
use std::sync::Arc;
use tonic::{
    Request,
    metadata::{Ascii, Binary, MetadataMap, MetadataValue},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity},
};

/// Request metadata: one minimum revision per authority, as the protobuf bytes
/// of the shared `acyclic.stream.v1.Revision`.
pub const MINIMUM_REVISION: &str = "acyclic-min-revision-bin";
/// Response metadata: the revision committing a write or serving a read.
pub const REVISION: &str = "acyclic-revision-bin";

const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// The largest deadline `grpc-timeout` can carry: eight digits of hours.
const MAX_DEADLINE: std::time::Duration = std::time::Duration::from_secs(99_999_999 * 3600);

fn credential_header(token: &str) -> Result<MetadataValue<Ascii>, ConnectError> {
    if !acyclic_auth::credential::valid_credential(token) {
        return Err(ConnectError::InvalidConfiguration);
    }
    let mut value: MetadataValue<Ascii> = format!("Bearer {token}")
        .parse()
        .map_err(|_| ConnectError::InvalidConfiguration)?;
    value.set_sensitive(true);
    Ok(value)
}

/// Client configuration or connection failure.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// Endpoint, credential, or private CA violates the client contract.
    #[error("invalid Objects gRPC configuration")]
    InvalidConfiguration,
    /// Connection could not be established.
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    /// The platform certificate verifier could not be initialized.
    #[error("Objects platform certificate verifier: {0}")]
    PlatformVerifier(#[source] rustls::Error),
}

/// Blob client. Every request carries a sensitive bearer credential; the
/// service derives the account from it.
#[derive(Clone)]
pub struct GrpcObjects {
    channel: Channel,
    authorization: Option<MetadataValue<Ascii>>,
    credential: Option<CredentialProvider>,
    transfer_timeout: std::time::Duration,
    revisions: Arc<RevisionTracker>,
}

impl GrpcObjects {
    /// The last revision observed per authority, shared by every clone and
    /// forwarded as the minimum of each GET, HEAD and LIST. Observing a
    /// revision another client returned makes later reads cover it.
    #[must_use]
    pub fn revisions(&self) -> &RevisionTracker {
        &self.revisions
    }

    /// Sets the caller's finite deadline for body-carrying calls; a slow or
    /// large transfer is bounded by this deadline alone.
    pub fn with_transfer_timeout(
        mut self,
        timeout: std::time::Duration,
    ) -> Result<Self, ConnectError> {
        if timeout.is_zero() || timeout > MAX_DEADLINE {
            return Err(ConnectError::InvalidConfiguration);
        }
        self.transfer_timeout = timeout;
        Ok(self)
    }

    /// Connects with optional PEM client identity, trusting only the declared CA or otherwise platform roots.
    pub async fn connect(
        endpoint: &str,
        token: &str,
        ca: Option<&[u8]>,
        identity: Option<(&[u8], &[u8])>,
    ) -> Result<Self, ConnectError> {
        Self::connect_inner(endpoint, Some(token), None, ca, identity).await
    }

    /// Connects without a fixed bearer, resolving the callback before each RPC.
    pub async fn connect_with_credential(
        endpoint: &str,
        credential: CredentialProvider,
        ca: Option<&[u8]>,
        identity: Option<(&[u8], &[u8])>,
    ) -> Result<Self, ConnectError> {
        Self::connect_inner(endpoint, None, Some(credential), ca, identity).await
    }

    async fn connect_inner(
        endpoint: &str,
        token: Option<&str>,
        credential: Option<CredentialProvider>,
        ca: Option<&[u8]>,
        identity: Option<(&[u8], &[u8])>,
    ) -> Result<Self, ConnectError> {
        let identity = identity
            .map(|(certificate, key)| {
                if certificate.is_empty()
                    || certificate.len() > crate::MAX_PEM_BYTES
                    || key.is_empty()
                    || key.len() > crate::MAX_PEM_BYTES
                {
                    return Err(ConnectError::InvalidConfiguration);
                }
                Ok(Identity::from_pem(certificate, key))
            })
            .transpose()?;
        let endpoint = Endpoint::from_shared(endpoint.to_owned())?;
        if endpoint.uri().scheme_str() != Some("https")
            || endpoint
                .uri()
                .authority()
                .is_none_or(|authority| authority.as_str().contains('@'))
            || endpoint.uri().query().is_some()
            || endpoint.uri().path() != "/"
        {
            return Err(ConnectError::InvalidConfiguration);
        }
        let authorization = token.map(credential_header).transpose()?;
        let mut tls = ClientTlsConfig::new();
        if let Some(ca) = ca {
            if ca.is_empty() || ca.len() > crate::MAX_PEM_BYTES {
                return Err(ConnectError::InvalidConfiguration);
            }
            tls = tls.ca_certificate(Certificate::from_pem(ca));
        }
        if let Some(identity) = identity {
            tls = tls.identity(identity);
        }
        let endpoint = if ca.is_some() {
            endpoint.tls_config(tls)?
        } else {
            let verifier = rustls_platform_verifier::Verifier::new(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .map_err(ConnectError::PlatformVerifier)?;
            endpoint.tls_config_with_verifier(tls, Arc::new(verifier))?
        };
        let channel = endpoint
            // Every request carries its own deadline; the channel adds none.
            .connect_timeout(std::time::Duration::from_secs(10))
            .connect()
            .await?;
        Ok(Self::over(channel, authorization, credential))
    }

    fn over(
        channel: Channel,
        authorization: Option<MetadataValue<Ascii>>,
        credential: Option<CredentialProvider>,
    ) -> Self {
        Self {
            channel,
            authorization,
            credential,
            transfer_timeout: REQUEST_TIMEOUT,
            revisions: Arc::default(),
        }
    }

    async fn request<T>(
        &self,
        value: T,
        timeout: std::time::Duration,
        floor: Option<&RevisionCut>,
    ) -> Result<Request<T>, Error> {
        let authorization = if let Some(provider) = &self.credential {
            let token = provider
                .resolve()
                .await
                .map_err(|_| Error::from(wire::ErrorCode::AccessDenied))?;
            credential_header(&token).map_err(|_| Error::from(wire::ErrorCode::AccessDenied))?
        } else {
            self.authorization
                .clone()
                .ok_or_else(|| Error::from(wire::ErrorCode::AccessDenied))?
        };
        let mut request = Request::new(value);
        request.set_timeout(timeout);
        request
            .metadata_mut()
            .insert("authorization", authorization);
        for minimum in floor.into_iter().flat_map(RevisionCut::iter) {
            request.metadata_mut().append_bin(
                MINIMUM_REVISION,
                MetadataValue::from_bytes(&revision_to_wire(minimum).encode_to_vec()),
            );
        }
        Ok(request)
    }

    /// Observes the revision a reply carries. A read served below the minimum
    /// it forwarded for that authority, or without any revision, is refused,
    /// never exposed.
    fn observed<T>(
        &self,
        reply: Result<tonic::Response<T>, tonic::Status>,
        floor: Option<&RevisionCut>,
    ) -> Result<(T, Option<Revision>), Error> {
        let (metadata, outcome) = match reply {
            Ok(response) => {
                let (metadata, value, _) = response.into_parts();
                (metadata, Ok(value))
            }
            Err(status) => (status.metadata().clone(), Err(error_from_status(&status))),
        };
        if let Err(error) = &outcome
            && error.code == wire::ErrorCode::RevisionForeign
        {
            self.revisions.refused(&StreamError::RevisionForeign);
        }
        let revision = issued(&metadata)?;
        // A server that ignored the minimum reports no revision; its read
        // proves nothing about the floor.
        if outcome.is_ok() && revision.is_none() && floor.is_some_and(|floor| !floor.is_empty()) {
            return Err(wire::ErrorCode::Unavailable.into());
        }
        if let Some(revision) = &revision {
            // Only a reply that served a read can be stale; a refused wait
            // reports its own failure.
            if outcome.is_ok()
                && floor
                    .and_then(|floor| floor.get(&revision.scope))
                    .is_some_and(|minimum| {
                        minimum.same_authority(revision) && minimum.index > revision.index
                    })
            {
                return Err(wire::ErrorCode::Unavailable.into());
            }
            self.revisions.observe(revision);
        }
        outcome.map(|value| (value, revision))
    }

    fn client(&self) -> wire::blobs_service_client::BlobsServiceClient<Channel> {
        wire::blobs_service_client::BlobsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX)
    }
}

/// Maps canonical service details, falling back to stable transport categories.
fn error_from_status(status: &tonic::Status) -> Error {
    if let Ok(detail) = wire::ErrorDetail::decode(status.details())
        && let Ok(code) = wire::ErrorCode::try_from(detail.code)
        && code != wire::ErrorCode::Unspecified
    {
        return code.into();
    }
    match status.code() {
        tonic::Code::InvalidArgument => wire::ErrorCode::InvalidArgument,
        tonic::Code::NotFound => wire::ErrorCode::NotFound,
        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
            wire::ErrorCode::AccessDenied
        }
        tonic::Code::ResourceExhausted => wire::ErrorCode::QuotaExceeded,
        tonic::Code::OutOfRange => wire::ErrorCode::RangeNotSatisfiable,
        tonic::Code::Unimplemented => wire::ErrorCode::Unsupported,
        tonic::Code::DataLoss => wire::ErrorCode::Corrupt,
        tonic::Code::DeadlineExceeded => wire::ErrorCode::DeadlineElapsed,
        _ => wire::ErrorCode::Unavailable,
    }
    .into()
}

fn status(error: Error) -> tonic::Status {
    let code = match error.code {
        wire::ErrorCode::InvalidArgument
        | wire::ErrorCode::DigestMismatch
        | wire::ErrorCode::Unspecified => tonic::Code::InvalidArgument,
        wire::ErrorCode::NotFound => tonic::Code::NotFound,
        wire::ErrorCode::MissingReference
        | wire::ErrorCode::Conflict
        | wire::ErrorCode::Suspended
        | wire::ErrorCode::RevisionForeign => tonic::Code::FailedPrecondition,
        wire::ErrorCode::DeadlineElapsed => tonic::Code::DeadlineExceeded,
        wire::ErrorCode::QuotaExceeded => tonic::Code::ResourceExhausted,
        wire::ErrorCode::Unsupported => tonic::Code::Unimplemented,
        wire::ErrorCode::AccessDenied => tonic::Code::PermissionDenied,
        wire::ErrorCode::RangeNotSatisfiable => tonic::Code::OutOfRange,
        wire::ErrorCode::Corrupt => tonic::Code::DataLoss,
        wire::ErrorCode::Unavailable | wire::ErrorCode::OutcomeUnknown => tonic::Code::Unavailable,
    };
    let detail = wire::ErrorDetail {
        code: error.code as i32,
        request_id: String::new(),
    };
    tonic::Status::with_details(
        code,
        "objects operation failed",
        detail.encode_to_vec().into(),
    )
}

/// The revision a response carries, if any; a malformed one is unavailable.
fn issued(metadata: &MetadataMap) -> Result<Option<Revision>, Error> {
    metadata
        .get_bin(REVISION)
        .map(|value| {
            let bytes = value
                .to_bytes()
                .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?;
            acyclic_stream::wire::Revision::decode(bytes)
                .ok()
                .and_then(|revision| revision_from_wire(revision).ok())
                .ok_or_else(|| wire::ErrorCode::Unavailable.into())
        })
        .transpose()
}

/// Writes carry no minimum; GET, HEAD and LIST forward the tracked floor.
macro_rules! call {
    ($self:ident, $rpc:ident, $value:expr, $timeout:expr) => {
        call!($self, $rpc, $value, $timeout, None)
    };
    ($self:ident, $rpc:ident, $value:expr, $timeout:expr, read) => {{
        let floor = $self.revisions.floor(&RevisionCut::default());
        call!($self, $rpc, $value, $timeout, Some(&floor))
    }};
    ($self:ident, $rpc:ident, $value:expr, $timeout:expr, $floor:expr) => {{
        let floor: Option<&RevisionCut> = $floor;
        let request = $self.request($value, $timeout, floor).await?;
        // The client owns its deadline: a failure once it elapsed is the
        // deadline, whichever side's cancellation reported it first.
        let started = tokio::time::Instant::now();
        match tokio::time::timeout($timeout, $self.client().$rpc(request)).await {
            Ok(Err(_)) if started.elapsed() >= $timeout => {
                Err(wire::ErrorCode::DeadlineElapsed.into())
            }
            Ok(reply) => $self.observed(reply, floor),
            Err(_) => Err(wire::ErrorCode::DeadlineElapsed.into()),
        }
    }};
}

#[async_trait::async_trait]
impl Blobs for GrpcObjects {
    async fn put(&self, body: OwnedBytes, references: Vec<Digest>) -> Result<Put, Error> {
        let digest = Digest::of(&body);
        let reply = call!(
            self,
            put_blob,
            wire::PutBlobRequest {
                digest: Some(digest.into()),
                body: bytes::Bytes::from_owner(body.clone()),
                references: convert::wire_digests(&references),
            },
            self.transfer_timeout
        )?;
        Ok(committed(
            convert::verified_put(&reply.0, digest, body.len() as u64)?,
            reply.1,
        ))
    }

    async fn put_manifest(&self, manifest: Manifest) -> Result<Put, Error> {
        let digest = Digest::of(&manifest.encode());
        let length = manifest.length()?;
        let reply = call!(
            self,
            put_manifest,
            wire::PutManifestRequest {
                manifest: Some(convert::manifest(&manifest)),
            },
            REQUEST_TIMEOUT
        )?;
        Ok(committed(
            convert::verified_put(&reply.0, digest, length)?,
            reply.1,
        ))
    }

    async fn get(
        &self,
        digest: Digest,
        range: ByteRange,
        maximum_bytes: u64,
    ) -> Result<Object, Error> {
        let reply = call!(
            self,
            get_blob,
            wire::GetBlobRequest {
                digest: Some(digest.into()),
                range: Some(convert::range(range)),
                maximum_bytes,
            },
            self.transfer_timeout,
            read
        )?;
        Ok(convert::object(reply.0, digest, range, maximum_bytes)?.served_at(reply.1))
    }

    async fn head(&self, digest: Digest) -> Result<Head, Error> {
        let reply = call!(
            self,
            head_blob,
            wire::HeadBlobRequest {
                digest: Some(digest.into()),
            },
            REQUEST_TIMEOUT,
            read
        )?;
        let mut head = convert::headed(&reply.0, digest)?;
        head.revision = reply.1;
        Ok(head)
    }

    async fn delete(&self, digest: Digest) -> Result<Deletion, Error> {
        let reply = call!(
            self,
            delete_blob,
            wire::DeleteBlobRequest {
                digest: Some(digest.into()),
            },
            REQUEST_TIMEOUT
        )?;
        Ok(Deletion {
            outcome: convert::delete(&reply.0)?,
            revision: reply.1,
        })
    }

    async fn list(&self, after: Option<Digest>, limit: u32) -> Result<Page, Error> {
        let reply = call!(
            self,
            list_blobs,
            wire::ListBlobsRequest {
                after: after.map(Into::into),
                limit,
            },
            REQUEST_TIMEOUT,
            read
        )?;
        let mut page = convert::listed(&reply.0, after, limit)?;
        page.revision = reply.1;
        Ok(page)
    }

    /// The server acknowledges a put only once it is durable.
    async fn flush(&self, _only: Option<&[Digest]>) -> Result<(), Error> {
        Ok(())
    }

    async fn retain(&self, root: &RootId, references: Vec<Digest>) -> Result<Retained, Error> {
        let reply = call!(
            self,
            retain,
            wire::RetainRequest {
                root: root.as_bytes().to_vec(),
                references: convert::wire_digests(&references),
            },
            REQUEST_TIMEOUT
        )?;
        let mut retained = convert::retained(&reply.0);
        retained.revision = reply.1;
        Ok(retained)
    }

    async fn release(&self, root: &RootId) -> Result<crate::Released, Error> {
        let reply = call!(
            self,
            release,
            wire::ReleaseRequest {
                root: root.as_bytes().to_vec(),
            },
            REQUEST_TIMEOUT
        )?;
        Ok(crate::Released {
            existed: reply.0.existed,
            revision: reply.1,
        })
    }

    async fn collect(&self) -> Result<Collection, Error> {
        let reply = call!(
            self,
            collect,
            wire::CollectRequest {},
            self.transfer_timeout
        )?;
        let mut collection = convert::collection(&reply.0);
        collection.revision = reply.1;
        Ok(collection)
    }

    async fn usage(&self) -> Result<Usage, Error> {
        let reply = call!(self, usage, wire::UsageRequest {}, REQUEST_TIMEOUT)?;
        Ok(convert::usage(&reply.0))
    }
}

/// Stamps a durable put with the revision its reply committed.
fn committed(put: Put, revision: Option<Revision>) -> Put {
    match put {
        Put::Created { mut verified } => {
            verified.revision = revision;
            Put::Created { verified }
        }
        Put::AlreadyExists { mut verified } => {
            verified.revision = revision;
            Put::AlreadyExists { verified }
        }
        Put::Unknown => Put::Unknown,
    }
}

/// Resolves the verified caller's account namespace from request metadata.
pub type AccountResolver = Arc<dyn Fn(&MetadataMap) -> Result<Arc<dyn Blobs>, Error> + Send + Sync>;

/// Serves [`wire::blobs_service_server::BlobsService`] over any [`Blobs`]
/// provider, one account per authenticated request.
#[derive(Clone)]
pub struct BlobsServer {
    resolve: AccountResolver,
}

impl BlobsServer {
    /// Serves the account `resolve` derives from each request.
    #[must_use]
    pub fn new(resolve: AccountResolver) -> Self {
        Self { resolve }
    }

    /// The generated tonic service.
    #[must_use]
    pub fn into_service(self) -> wire::blobs_service_server::BlobsServiceServer<Self> {
        wire::blobs_service_server::BlobsServiceServer::new(self)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX)
    }

    fn account<T>(&self, request: &Request<T>) -> Result<Arc<dyn Blobs>, tonic::Status> {
        (self.resolve)(request.metadata()).map_err(status)
    }
}

type Reply<T> = Result<tonic::Response<T>, tonic::Status>;

/// Waits until `blobs` applies the request's minimum, bounded by its call
/// deadline. A minimum the account cannot prove is foreign.
async fn applied(blobs: &dyn Blobs, metadata: &MetadataMap) -> Result<(), Error> {
    let minimum = cut_from_wire(
        metadata
            .get_all_bin(MINIMUM_REVISION)
            .iter()
            .map(|value| {
                let bytes = value
                    .to_bytes()
                    .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
                acyclic_stream::wire::Revision::decode(bytes)
                    .map_err(|_| wire::ErrorCode::InvalidArgument.into())
            })
            .collect::<Result<Vec<_>, Error>>()?,
    )
    .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
    let minimum = blobs.admitted(minimum).await?;
    if minimum.is_empty() {
        return Ok(());
    }
    let revisions = blobs
        .revisions()
        .await?
        .ok_or(Error::from(wire::ErrorCode::RevisionForeign))?;
    let wait = std::pin::pin!(revisions.wait(&minimum));
    let deadline = std::pin::pin!(deadline(metadata));
    match futures::future::select(wait, deadline).await {
        futures::future::Either::Left((Ok(_), _)) => Ok(()),
        futures::future::Either::Left((Err(StreamError::RevisionForeign), _)) => {
            Err(wire::ErrorCode::RevisionForeign.into())
        }
        futures::future::Either::Left((Err(_), _)) => Err(wire::ErrorCode::Unavailable.into()),
        futures::future::Either::Right(((), _)) => Err(wire::ErrorCode::DeadlineElapsed.into()),
    }
}

/// Resolves at the call's `grpc-timeout`; a call without one waits as long as
/// its transport keeps it.
async fn deadline(metadata: &MetadataMap) {
    match metadata
        .get("grpc-timeout")
        .and_then(|value| value.to_str().ok())
        .and_then(grpc_timeout)
    {
        Some(timeout) => tokio::time::sleep(timeout).await,
        None => std::future::pending().await,
    }
}

/// Parses the gRPC `TimeoutValue TimeoutUnit` spelling.
fn grpc_timeout(value: &str) -> Option<std::time::Duration> {
    let (amount, unit) = value.split_at(value.len().checked_sub(1)?);
    if amount.is_empty() || amount.len() > 8 || !amount.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let amount: u64 = amount.parse().ok()?;
    Some(match unit {
        "H" => std::time::Duration::from_secs(amount.checked_mul(3600)?),
        "M" => std::time::Duration::from_secs(amount.checked_mul(60)?),
        "S" => std::time::Duration::from_secs(amount),
        "m" => std::time::Duration::from_millis(amount),
        "u" => std::time::Duration::from_micros(amount),
        "n" => std::time::Duration::from_nanos(amount),
        _ => return None,
    })
}

fn stamp(metadata: &mut MetadataMap, revision: Option<&Revision>) {
    if let Some(revision) = revision {
        metadata.insert_bin(
            REVISION,
            MetadataValue::<Binary>::from_bytes(&revision_to_wire(revision).encode_to_vec()),
        );
    }
}

/// Serves one route: a read first waits for its minimum; every reply carries
/// its revision, and a failure the account's current one.
async fn serve<T, F>(blobs: &dyn Blobs, metadata: &MetadataMap, read: bool, route: F) -> Reply<T>
where
    F: Future<Output = Result<crate::route::Served<T>, Error>>,
{
    let outcome = if read {
        match applied(blobs, metadata).await {
            Ok(()) => route.await,
            Err(error) => Err(error),
        }
    } else {
        route.await
    };
    match outcome {
        Ok((value, revision)) => {
            let mut response = tonic::Response::new(value);
            stamp(response.metadata_mut(), revision.as_ref());
            Ok(response)
        }
        Err(error) => {
            let mut status = status(error);
            let current = blobs
                .revisions()
                .await
                .ok()
                .flatten()
                .map(acyclic_stream::AppliedRevisions::current);
            stamp(status.metadata_mut(), current.as_ref());
            Err(status)
        }
    }
}

macro_rules! serve {
    ($self:ident, $request:ident, $route:ident, $read:expr) => {{
        let blobs = $self.account(&$request)?;
        let (metadata, _, message) = $request.into_parts();
        serve(
            &*blobs,
            &metadata,
            $read,
            crate::route::$route(&*blobs, message),
        )
        .await
    }};
}

#[tonic::async_trait]
impl wire::blobs_service_server::BlobsService for BlobsServer {
    async fn put_blob(&self, request: Request<wire::PutBlobRequest>) -> Reply<wire::PutResponse> {
        serve!(self, request, put_blob, false)
    }
    async fn put_manifest(
        &self,
        request: Request<wire::PutManifestRequest>,
    ) -> Reply<wire::PutResponse> {
        serve!(self, request, put_manifest, false)
    }
    async fn get_blob(
        &self,
        request: Request<wire::GetBlobRequest>,
    ) -> Reply<wire::GetBlobResponse> {
        serve!(self, request, get_blob, true)
    }
    async fn head_blob(&self, request: Request<wire::HeadBlobRequest>) -> Reply<wire::BlobInfo> {
        serve!(self, request, head_blob, true)
    }
    async fn delete_blob(
        &self,
        request: Request<wire::DeleteBlobRequest>,
    ) -> Reply<wire::DeleteBlobResponse> {
        serve!(self, request, delete_blob, false)
    }
    async fn list_blobs(
        &self,
        request: Request<wire::ListBlobsRequest>,
    ) -> Reply<wire::ListBlobsResponse> {
        serve!(self, request, list_blobs, true)
    }
    async fn retain(&self, request: Request<wire::RetainRequest>) -> Reply<wire::RetainResponse> {
        serve!(self, request, retain, false)
    }
    async fn release(
        &self,
        request: Request<wire::ReleaseRequest>,
    ) -> Reply<wire::ReleaseResponse> {
        serve!(self, request, release, false)
    }
    async fn collect(
        &self,
        request: Request<wire::CollectRequest>,
    ) -> Reply<wire::CollectResponse> {
        serve!(self, request, collect, false)
    }
    async fn usage(&self, request: Request<wire::UsageRequest>) -> Reply<wire::UsageResponse> {
        serve!(self, request, usage, false)
    }
}

#[cfg(test)]
#[path = "grpc_tests.rs"]
mod tests;

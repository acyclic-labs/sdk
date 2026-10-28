//! Authenticated gRPC client and account-resolving service adapter.
use crate::convert;
use crate::{
    Blobs, ByteRange, Collection, Delete, Digest, Error, Head, Manifest, Object, Page, Put,
    Retained, RootId, Usage, wire,
};
pub use acyclic_auth::credential::CredentialProvider;
use acyclic_stream::OwnedBytes;
use prost::Message;
use std::sync::Arc;
use tonic::{
    Request,
    metadata::{Ascii, MetadataMap, MetadataValue},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity},
};

const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_TRANSFER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1800);

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
}

impl GrpcObjects {
    /// Sets the finite deadline for body-carrying calls.
    pub fn with_transfer_timeout(
        mut self,
        timeout: std::time::Duration,
    ) -> Result<Self, ConnectError> {
        if timeout.is_zero() || timeout > MAX_TRANSFER_TIMEOUT {
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
            .connect_timeout(std::time::Duration::from_secs(10))
            // Requests carry their own deadlines; this ceiling admits the largest transfer budget.
            .timeout(MAX_TRANSFER_TIMEOUT)
            .connect()
            .await?;
        Ok(Self {
            channel,
            authorization,
            credential,
            transfer_timeout: REQUEST_TIMEOUT,
        })
    }

    async fn request<T>(
        &self,
        value: T,
        timeout: std::time::Duration,
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
        Ok(request)
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
        | wire::ErrorCode::Suspended => tonic::Code::FailedPrecondition,
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

macro_rules! call {
    ($self:ident, $rpc:ident, $value:expr, $timeout:expr) => {
        $self
            .client()
            .$rpc($self.request($value, $timeout).await?)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| error_from_status(&status))
    };
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
                body: body.to_vec(),
                references: convert::wire_digests(&references),
            },
            self.transfer_timeout
        )?;
        let put = convert::put(&reply)?;
        if put.verified().is_some_and(|verified| {
            verified.digest != digest || verified.length != body.len() as u64
        }) {
            return Err(wire::ErrorCode::Unavailable.into());
        }
        Ok(put)
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
        let put = convert::put(&reply)?;
        if put
            .verified()
            .is_some_and(|verified| verified.digest != digest || verified.length != length)
        {
            return Err(wire::ErrorCode::Unavailable.into());
        }
        Ok(put)
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
            self.transfer_timeout
        )?;
        convert::object(reply, digest, range, maximum_bytes)
    }

    async fn head(&self, digest: Digest) -> Result<Head, Error> {
        let reply = call!(
            self,
            head_blob,
            wire::HeadBlobRequest {
                digest: Some(digest.into()),
            },
            REQUEST_TIMEOUT
        )?;
        let head = convert::head(&reply)?;
        if head.digest != digest {
            return Err(wire::ErrorCode::Unavailable.into());
        }
        Ok(head)
    }

    async fn delete(&self, digest: Digest) -> Result<Delete, Error> {
        let reply = call!(
            self,
            delete_blob,
            wire::DeleteBlobRequest {
                digest: Some(digest.into()),
            },
            REQUEST_TIMEOUT
        )?;
        convert::delete(&reply)
    }

    async fn list(&self, after: Option<Digest>, limit: u32) -> Result<Page, Error> {
        let reply = call!(
            self,
            list_blobs,
            wire::ListBlobsRequest {
                after: after.map(Into::into),
                limit,
            },
            REQUEST_TIMEOUT
        )?;
        let page = convert::page(&reply)?;
        if page.digests.len() > limit as usize
            || after.is_some_and(|after| page.digests.first().is_some_and(|first| *first <= after))
        {
            return Err(wire::ErrorCode::Unavailable.into());
        }
        Ok(page)
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
        Ok(convert::retained(&reply))
    }

    async fn release(&self, root: &RootId) -> Result<bool, Error> {
        let reply = call!(
            self,
            release,
            wire::ReleaseRequest {
                root: root.as_bytes().to_vec(),
            },
            REQUEST_TIMEOUT
        )?;
        Ok(reply.existed)
    }

    async fn collect(&self) -> Result<Collection, Error> {
        let reply = call!(
            self,
            collect,
            wire::CollectRequest {},
            self.transfer_timeout
        )?;
        Ok(convert::collection(&reply))
    }

    async fn usage(&self) -> Result<Usage, Error> {
        let reply = call!(self, usage, wire::UsageRequest {}, REQUEST_TIMEOUT)?;
        Ok(convert::usage(&reply))
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

fn reply<T>(value: Result<T, Error>) -> Reply<T> {
    value.map(tonic::Response::new).map_err(status)
}

#[tonic::async_trait]
impl wire::blobs_service_server::BlobsService for BlobsServer {
    async fn put_blob(&self, request: Request<wire::PutBlobRequest>) -> Reply<wire::PutResponse> {
        let blobs = self.account(&request)?;
        reply(crate::route::put_blob(&*blobs, request.into_inner()).await)
    }
    async fn put_manifest(
        &self,
        request: Request<wire::PutManifestRequest>,
    ) -> Reply<wire::PutResponse> {
        let blobs = self.account(&request)?;
        reply(crate::route::put_manifest(&*blobs, request.into_inner()).await)
    }
    async fn get_blob(
        &self,
        request: Request<wire::GetBlobRequest>,
    ) -> Reply<wire::GetBlobResponse> {
        let blobs = self.account(&request)?;
        reply(crate::route::get_blob(&*blobs, request.into_inner()).await)
    }
    async fn head_blob(&self, request: Request<wire::HeadBlobRequest>) -> Reply<wire::BlobInfo> {
        let blobs = self.account(&request)?;
        reply(crate::route::head_blob(&*blobs, request.into_inner()).await)
    }
    async fn delete_blob(
        &self,
        request: Request<wire::DeleteBlobRequest>,
    ) -> Reply<wire::DeleteBlobResponse> {
        let blobs = self.account(&request)?;
        reply(crate::route::delete_blob(&*blobs, request.into_inner()).await)
    }
    async fn list_blobs(
        &self,
        request: Request<wire::ListBlobsRequest>,
    ) -> Reply<wire::ListBlobsResponse> {
        let blobs = self.account(&request)?;
        reply(crate::route::list_blobs(&*blobs, request.into_inner()).await)
    }
    async fn retain(&self, request: Request<wire::RetainRequest>) -> Reply<wire::RetainResponse> {
        let blobs = self.account(&request)?;
        reply(crate::route::retain(&*blobs, request.into_inner()).await)
    }
    async fn release(
        &self,
        request: Request<wire::ReleaseRequest>,
    ) -> Reply<wire::ReleaseResponse> {
        let blobs = self.account(&request)?;
        reply(crate::route::release(&*blobs, request.into_inner()).await)
    }
    async fn collect(
        &self,
        request: Request<wire::CollectRequest>,
    ) -> Reply<wire::CollectResponse> {
        let blobs = self.account(&request)?;
        reply(crate::route::collect(&*blobs, request.into_inner()).await)
    }
    async fn usage(&self, request: Request<wire::UsageRequest>) -> Reply<wire::UsageResponse> {
        let blobs = self.account(&request)?;
        reply(crate::route::usage(&*blobs, request.into_inner()).await)
    }
}

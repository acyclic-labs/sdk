//! Authenticated blob client for the Protobuf-JSON HTTP gateway.
use crate::{
    Blobs, ByteRange, Collection, Digest, Error, Head, Manifest, Object, Page, Put, Retained,
    RootId, Usage, convert, json, wire,
};
pub use acyclic_auth::credential::CredentialProvider;
use acyclic_stream::OwnedBytes;
use prost::Message;
use reqwest::{
    Client, Response, Url,
    header::{AUTHORIZATION, HeaderValue},
};
mod availability;
#[cfg(test)]
mod tests;

fn credential_header(token: &str) -> Result<HeaderValue, Error> {
    let invalid = || Error::from(wire::ErrorCode::InvalidArgument);
    if !acyclic_auth::credential::valid_credential(token) {
        return Err(invalid());
    }
    let mut value = HeaderValue::try_from(acyclic_auth::credential::bearer_bytes(token))
        .map_err(|_| invalid())?;
    value.set_sensitive(true);
    Ok(value)
}

fn unavailable() -> Error {
    wire::ErrorCode::Unavailable.into()
}

/// Authenticated native HTTP client for the Objects v1 blob gateway.
#[derive(Clone)]
pub struct HttpObjects {
    transport: Client,
    endpoint: Url,
    authorization: Option<HeaderValue>,
    credential: Option<CredentialProvider>,
    context: Option<acyclic_auth::operation::Context>,
    maximum: usize,
}

impl HttpObjects {
    /// Bind credentials, connection, body reading, decoding and semantic
    /// admission to one caller-owned absolute deadline and cancellation event.
    /// Clones retain that same budget; no stage starts another timer.
    pub fn with_context(mut self, context: acyclic_auth::operation::Context) -> Self {
        self.context = Some(context);
        self
    }

    /// Creates a bounded client with optional exclusive PEM trust.
    pub fn new(
        endpoint: &str,
        token: &str,
        maximum_response_bytes: usize,
        ca: Option<&[u8]>,
    ) -> Result<Self, Error> {
        Self::build(endpoint, Some(token), None, maximum_response_bytes, ca)
    }

    /// Resolves the callback for every physical request.
    pub fn with_credential_provider(
        endpoint: &str,
        credential: CredentialProvider,
        maximum: usize,
        ca: Option<&[u8]>,
    ) -> Result<Self, Error> {
        Self::build(endpoint, None, Some(credential), maximum, ca)
    }

    fn build(
        endpoint: &str,
        token: Option<&str>,
        credential: Option<CredentialProvider>,
        maximum_response_bytes: usize,
        ca: Option<&[u8]>,
    ) -> Result<Self, Error> {
        let invalid = || Error::from(wire::ErrorCode::InvalidArgument);
        let mut endpoint = Url::parse(endpoint).map_err(|_| invalid())?;
        let loopback = endpoint.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && loopback)
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || maximum_response_bytes == 0
        {
            return Err(invalid());
        }
        if !endpoint.path().ends_with('/') {
            endpoint.set_path(&format!("{}/", endpoint.path()));
        }
        let authorization = token.map(credential_header).transpose()?;
        let mut transport = Client::builder().redirect(reqwest::redirect::Policy::none());
        if let Some(ca) = ca {
            if ca.is_empty() || ca.len() > crate::MAX_PEM_BYTES {
                return Err(invalid());
            }
            transport = transport
                .add_root_certificate(reqwest::Certificate::from_pem(ca).map_err(|_| invalid())?);
        }
        Ok(Self {
            transport: transport.build().map_err(|_| invalid())?,
            endpoint: endpoint.join("v1/objects/").map_err(|_| invalid())?,
            authorization,
            credential,
            context: None,
            maximum: maximum_response_bytes,
        })
    }

    async fn authorization(
        &self,
    ) -> Result<
        (
            HeaderValue,
            Option<acyclic_auth::credential::ResolvedCredential>,
        ),
        Error,
    > {
        if let Some(provider) = &self.credential {
            let token = provider
                .resolve()
                .await
                .map_err(|_| Error::from(wire::ErrorCode::AccessDenied))?;
            token
                .check()
                .await
                .map_err(|_| Error::from(wire::ErrorCode::AccessDenied))?;
            let header = credential_header(&token)
                .map_err(|_| Error::from(wire::ErrorCode::AccessDenied))?;
            Ok((header, Some(token)))
        } else {
            self.authorization
                .clone()
                .map(|header| (header, None))
                .ok_or_else(|| Error::from(wire::ErrorCode::AccessDenied))
        }
    }

    /// Posts one route's input and decodes its canonical output.
    async fn call<I: Message, O: Message + Default>(
        &self,
        route: &str,
        input: &I,
    ) -> Result<O, Error> {
        crate::operation::run(self.context.as_ref(), async {
            let (_, input_name, output_name) = crate::HTTP_ROUTES
                .iter()
                .find(|(path, _, _)| *path == route)
                .ok_or_else(unavailable)?;
            let url = self
                .endpoint
                .join(route)
                .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
            let (authorization, lease) = self.authorization().await?;
            let mut request = self
                .transport
                .post(url)
                .header(AUTHORIZATION, authorization)
                .header("content-type", "application/json")
                .body(json::encode(input_name, input)?);
            if self.context.is_none() {
                request = request.timeout(std::time::Duration::from_secs(30));
            }
            if let Some(context) = &self.context {
                context.check()?;
            }
            let response = request.send().await.map_err(|_| unavailable())?;
            if response.status().as_u16() == 401 {
                if let Some(lease) = &lease {
                    lease
                        .reject()
                        .await
                        .map_err(|_| Error::from(wire::ErrorCode::AccessDenied))?;
                }
            }
            if response.status().as_u16() != 200 {
                return Err(self.failure(response).await);
            }
            let bytes = self.bytes(response).await?;
            if let Some(lease) = &lease {
                lease
                    .check()
                    .await
                    .map_err(|_| Error::from(wire::ErrorCode::AccessDenied))?;
            }
            if let Some(context) = &self.context {
                context.check()?;
            }
            let decoded =
                json::decode(output_name, &bytes, self.maximum).map_err(|_| unavailable())?;
            if let Some(lease) = &lease {
                lease
                    .check()
                    .await
                    .map_err(|_| Error::from(wire::ErrorCode::AccessDenied))?;
            }
            Ok(decoded)
        })
        .await
    }

    async fn bytes(&self, mut response: Response) -> Result<Vec<u8>, Error> {
        if response
            .content_length()
            .is_some_and(|size| size > self.maximum as u64)
        {
            return Err(wire::ErrorCode::QuotaExceeded.into());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| unavailable())? {
            if chunk.len() > self.maximum.saturating_sub(bytes.len()) {
                return Err(wire::ErrorCode::QuotaExceeded.into());
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }

    async fn failure(&self, response: Response) -> Error {
        let status = response.status().as_u16();
        let bytes = match self.bytes(response).await {
            Ok(bytes) => bytes,
            Err(error) => return error,
        };
        if let Ok(detail) = json::decode::<wire::ErrorDetail>("ErrorDetail", &bytes, self.maximum)
            && let Ok(code) = wire::ErrorCode::try_from(detail.code)
            && code != wire::ErrorCode::Unspecified
        {
            return code.into();
        }
        match status {
            400 => wire::ErrorCode::InvalidArgument,
            401 | 403 => wire::ErrorCode::AccessDenied,
            404 => wire::ErrorCode::NotFound,
            409 => wire::ErrorCode::Conflict,
            413 | 429 => wire::ErrorCode::QuotaExceeded,
            416 => wire::ErrorCode::RangeNotSatisfiable,
            501 => wire::ErrorCode::Unsupported,
            _ => wire::ErrorCode::Unavailable,
        }
        .into()
    }
}

#[cfg(test)]
#[path = "http_context_tests.rs"]
mod context_tests;

#[async_trait::async_trait]
impl Blobs for HttpObjects {
    async fn availability(&self, digest: Digest) -> Result<crate::Availability, Error> {
        self.observe_availability(digest).await
    }

    async fn put(&self, body: OwnedBytes, references: Vec<Digest>) -> Result<Put, Error> {
        if let Some(context) = &self.context {
            context.check()?;
        }
        self.put_sealed(crate::Sealed::new(body), references).await
    }

    async fn put_sealed(&self, body: crate::Sealed, references: Vec<Digest>) -> Result<Put, Error> {
        crate::operation::admit(self.context.as_ref(), async {
            let digest = Digest::sealed(&body);
            let body = body.into_bytes();
            let reply: wire::PutResponse = self
                .call(
                    "blobs/put",
                    &wire::PutBlobRequest {
                        digest: Some(digest.into()),
                        body: bytes::Bytes::from_owner(body.clone()),
                        references: convert::wire_digests(&references),
                    },
                )
                .await?;
            convert::verified_put(&reply, digest, body.len() as u64)
        })
        .await
    }

    async fn put_manifest(&self, manifest: Manifest) -> Result<Put, Error> {
        crate::operation::admit(self.context.as_ref(), async {
            let digest = Digest::of(&manifest.encode());
            let length = manifest.length()?;
            let reply: wire::PutResponse = self
                .call(
                    "blobs/put-manifest",
                    &wire::PutManifestRequest {
                        manifest: Some(convert::manifest(&manifest)),
                    },
                )
                .await?;
            convert::verified_put(&reply, digest, length)
        })
        .await
    }

    async fn get(
        &self,
        digest: Digest,
        range: ByteRange,
        maximum_bytes: u64,
    ) -> Result<Object, Error> {
        crate::operation::admit(self.context.as_ref(), async {
            let reply: wire::GetBlobResponse = self
                .call(
                    "blobs/get",
                    &wire::GetBlobRequest {
                        digest: Some(digest.into()),
                        range: Some(convert::range(range)),
                        maximum_bytes,
                    },
                )
                .await?;
            convert::object(reply, digest, range, maximum_bytes)
        })
        .await
    }

    async fn head(&self, digest: Digest) -> Result<Head, Error> {
        crate::operation::admit(self.context.as_ref(), async {
            let reply: wire::BlobInfo = self
                .call(
                    "blobs/head",
                    &wire::HeadBlobRequest {
                        digest: Some(digest.into()),
                    },
                )
                .await?;
            convert::headed(&reply, digest)
        })
        .await
    }

    async fn delete(&self, digest: Digest) -> Result<crate::Deletion, Error> {
        crate::operation::admit(self.context.as_ref(), async {
            let reply: wire::DeleteBlobResponse = self
                .call(
                    "blobs/delete",
                    &wire::DeleteBlobRequest {
                        digest: Some(digest.into()),
                    },
                )
                .await?;
            Ok(crate::Deletion {
                outcome: convert::delete(&reply)?,
                revision: None,
            })
        })
        .await
    }

    async fn list(&self, after: Option<Digest>, limit: u32) -> Result<Page, Error> {
        crate::operation::admit(self.context.as_ref(), async {
            let reply: wire::ListBlobsResponse = self
                .call(
                    "blobs/list",
                    &wire::ListBlobsRequest {
                        after: after.map(Into::into),
                        limit,
                    },
                )
                .await?;
            convert::listed(&reply, after, limit)
        })
        .await
    }

    /// The server acknowledges a put only once it is durable.
    async fn flush(&self, _only: Option<&[Digest]>) -> Result<(), Error> {
        crate::operation::admit(self.context.as_ref(), async { Ok(()) }).await
    }

    async fn retain(&self, root: &RootId, references: Vec<Digest>) -> Result<Retained, Error> {
        crate::operation::admit(self.context.as_ref(), async {
            let reply: wire::RetainResponse = self
                .call(
                    "blobs/retain",
                    &wire::RetainRequest {
                        root: root.as_bytes().to_vec(),
                        references: convert::wire_digests(&references),
                    },
                )
                .await?;
            Ok(convert::retained(&reply))
        })
        .await
    }

    async fn release(&self, root: &RootId) -> Result<crate::Released, Error> {
        crate::operation::admit(self.context.as_ref(), async {
            let reply: wire::ReleaseResponse = self
                .call(
                    "blobs/release",
                    &wire::ReleaseRequest {
                        root: root.as_bytes().to_vec(),
                    },
                )
                .await?;
            Ok(crate::Released {
                existed: reply.existed,
                revision: None,
            })
        })
        .await
    }

    async fn collect(&self) -> Result<Collection, Error> {
        crate::operation::admit(self.context.as_ref(), async {
            let reply: wire::CollectResponse =
                self.call("blobs/collect", &wire::CollectRequest {}).await?;
            Ok(convert::collection(&reply))
        })
        .await
    }

    async fn usage(&self) -> Result<Usage, Error> {
        crate::operation::admit(self.context.as_ref(), async {
            let reply: wire::UsageResponse =
                self.call("blobs/usage", &wire::UsageRequest {}).await?;
            Ok(convert::usage(&reply))
        })
        .await
    }
}

use super::*;
use tonic::metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue};

/// Invalid fixed native request metadata, including credential overrides.
#[derive(Debug, thiserror::Error)]
#[error("invalid native request metadata")]
pub struct InvalidRequestMetadata;

/// Immutable routing metadata shared by every physical call of a client.
#[derive(Clone, Default)]
pub struct RequestMetadata(Arc<MetadataMap>);

impl RequestMetadata {
    /// Validates fixed ASCII entries. Authorization belongs to the credential
    /// owner and cannot be supplied through routing metadata.
    ///
    /// # Errors
    /// Rejects invalid ASCII keys or values and an authorization entry.
    pub fn new<K: AsRef<str>, V: AsRef<str>>(
        entries: impl IntoIterator<Item = (K, V)>,
    ) -> Result<Self, InvalidRequestMetadata> {
        let mut metadata = MetadataMap::new();
        for (key, value) in entries {
            let key = MetadataKey::<Ascii>::from_bytes(key.as_ref().as_bytes())
                .map_err(|_| InvalidRequestMetadata)?;
            if key == "authorization" || !value.as_ref().is_ascii() {
                return Err(InvalidRequestMetadata);
            }
            let value = MetadataValue::<Ascii>::try_from(value.as_ref())
                .map_err(|_| InvalidRequestMetadata)?;
            metadata.append(key, value);
        }
        Ok(Self(Arc::new(metadata)))
    }

    /// Adds fixed entries while preserving the original request's credential,
    /// deadline and revision metadata when a key occurs in both maps.
    pub fn apply<T>(&self, request: &mut Request<T>) {
        if self.0.is_empty() {
            return;
        }
        let mut headers = (*self.0).clone().into_headers();
        headers.extend(std::mem::take(request.metadata_mut()).into_headers());
        *request.metadata_mut() = MetadataMap::from_headers(headers);
    }

    /// Builds a request with its exact credential lease, or with no bearer
    /// when the caller explicitly selected transport authentication.
    pub fn request<T>(
        &self,
        body: T,
        authorization: Option<MetadataValue<Ascii>>,
        lease: Option<ResolvedCredential>,
    ) -> Request<T> {
        let mut request = Request::new(body);
        *request.metadata_mut() = (*self.0).clone();
        if let Some(lease) = lease {
            request.extensions_mut().insert(lease);
        }
        if let Some(authorization) = authorization {
            request
                .metadata_mut()
                .insert("authorization", authorization);
        }
        request
    }
}

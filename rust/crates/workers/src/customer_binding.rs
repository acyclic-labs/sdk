//! Native customer custody. Only the public key and actual HTTP response leave Rust.
use crate::account_binding_native::{AccountIssuedCredential, NativeClock, error, lifetime};
use crate::client_binding::WorkersCancellation;
use acyclic_native_runtime::account;
use acyclic_native_runtime::customer_custody::{CustomerCustodyNamespace, PendingCustomerLeaf, RestoredCustomerLeaf};
use napi::bindgen_prelude::{BigInt, Buffer};
use napi::{Result, Status};
use napi_derive::napi;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

fn origin(value: &str) -> Result<url::Url> {
    let url = url::Url::parse(value).map_err(|_| error("invalid customer login origin"))?;
    if url.scheme() != "https" || url.host_str().is_none() || !url.username().is_empty()
        || url.password().is_some() || url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        return Err(error("customer login requires an HTTPS origin"));
    }
    Ok(url)
}
fn namespace(origin: &url::Url, public_key: &str, birth: &str, certificate: &str) -> Result<CustomerCustodyNamespace> {
    let key = account::public_key(public_key).map_err(error)?;
    let metadata = account::inspect_holder(&key, birth, certificate).map_err(error)?;
    CustomerCustodyNamespace::new(&origin.origin().ascii_serialization(), metadata.environment(), metadata.account_id(), metadata.key_id()).map_err(error)
}
fn cancelled(token: &CancellationToken) -> Result<()> {
    if token.is_cancelled() { return Err(napi::Error::new(Status::GenericFailure, "customer operation cancelled")); }
    Ok(())
}
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(work).await.map_err(|_| error("customer custody worker failed"))?
}

/// Generated own leaf. No private key is a JavaScript value.
#[napi]
pub struct NativePendingCustomerLeaf { inner: Mutex<Option<PendingCustomerLeaf>> }
#[napi]
impl NativePendingCustomerLeaf {
    /// Exportable own public key for the actual identity enrollment request.
    #[napi(getter)]
    pub fn public_key(&self) -> Result<String> {
        let guard = self.inner.lock().map_err(|_| error("pending customer leaf unavailable"))?;
        let pending = guard.as_ref().ok_or_else(|| error("pending customer leaf consumed"))?;
        let key = ed25519_dalek::VerifyingKey::from_bytes(&pending.public_key()).map_err(error)?;
        Ok(account::encode_key(&key))
    }
    /// Atomically seal the original own leaf and distinct SQL session in the OS vault.
    #[napi]
    pub async fn commit(&self, login_origin: String, birth: String, certificate: String, sql_session: String) -> Result<NativeCustomerCredential> {
        let origin = origin(&login_origin)?;
        let namespace = namespace(&origin, &self.public_key()?, &birth, &certificate)?;
        let pending = self.inner.lock().map_err(|_| error("pending customer leaf unavailable"))?
            .take().ok_or_else(|| error("pending customer leaf consumed"))?;
        let session = Zeroizing::new(sql_session);
        let leaf = blocking(move || pending.commit(namespace, &birth, &certificate, session).map_err(error)).await?;
        NativeCustomerCredential::new(leaf, origin)
    }
    /// Erase an uncommitted own leaf without touching an existing OS credential.
    #[napi]
    pub fn dispose(&self) -> Result<()> {
        self.inner.lock().map_err(|_| error("pending customer leaf unavailable"))?.take();
        Ok(())
    }
}

/// Generate an opaque own leaf; this operation grants no Root authority.
#[napi]
pub fn generate_native_customer_leaf() -> Result<NativePendingCustomerLeaf> {
    Ok(NativePendingCustomerLeaf { inner: Mutex::new(Some(PendingCustomerLeaf::generate().map_err(error)?)) })
}

/// Actual configured-origin identity response, not an exposed SQL session callback.
#[napi(object)]
pub struct NativeIdentityResponse {
    /// Upstream HTTP status, including permission and revocation refusal.
    pub status: u32,
    /// Exact upstream bytes bounded by the existing Workers message ceiling.
    pub body: Buffer,
}

/// Generation-bound OS handle. There is no private-key/session serializer or getter.
#[napi]
pub struct NativeCustomerCredential {
    inner: Arc<RestoredCustomerLeaf>,
    origin: url::Url,
    http: reqwest::Client,
}
impl NativeCustomerCredential {
    fn new(inner: RestoredCustomerLeaf, origin: url::Url) -> Result<Self> {
        let http = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build()
            .map_err(|_| error("customer identity HTTPS transport unavailable"))?;
        Ok(Self { inner: Arc::new(inner), origin, http })
    }
}
#[napi]
impl NativeCustomerCredential {
    /// The actual original configured HTTPS origin, not caller-supplied mutable metadata.
    #[napi(getter)]
    pub fn origin(&self) -> String { self.origin.origin().ascii_serialization() }
    /// Only exportable key material.
    #[napi(getter)]
    pub fn public_key(&self) -> Result<String> {
        let key = ed25519_dalek::VerifyingKey::from_bytes(&self.inner.public_key()).map_err(error)?;
        Ok(account::encode_key(&key))
    }
    /// Mint using the original sealed leaf and the one canonical holder codec.
    #[napi]
    pub async fn mint(&self, birth: String, certificate: String, credential_id: String, lifetime_seconds: BigInt, cancellation: &WorkersCancellation) -> Result<AccountIssuedCredential> {
        let token = cancellation.token();
        cancelled(&token)?;
        let lifetime = lifetime(&lifetime_seconds)?;
        let leaf = self.inner.clone();
        let issued = blocking(move || {
            cancelled(&token)?;
            leaf.mint(&birth, &certificate, &credential_id, &NativeClock, lifetime).map_err(error)
        }).await?;
        Ok(AccountIssuedCredential::from_inner(issued))
    }
    /// Renew the public certificate while retaining the original sealed own leaf/session.
    #[napi]
    pub async fn renew(&self, birth: String, certificate: String) -> Result<NativeCustomerCredential> {
        let namespace = namespace(&self.origin, &self.public_key()?, &birth, &certificate)?;
        let leaf = self.inner.clone();
        let restored = blocking(move || leaf.recertify(namespace, &birth, &certificate).map_err(error)).await?;
        Ok(NativeCustomerCredential { inner: Arc::new(restored), origin: self.origin.clone(), http: self.http.clone() })
    }
    /// Send a SQL-session identity request only to the originally configured HTTPS origin.
    /// No redirect, permission retry, leaf-bearer substitution or mutation redispatch occurs.
    #[napi]
    pub async fn request_identity(&self, method: String, path: String, body: Option<Buffer>, cancellation: &WorkersCancellation) -> Result<NativeIdentityResponse> {
        let token = cancellation.token();
        cancelled(&token)?;
        if !path.starts_with("/v1/identity/") || path.starts_with("//") {
            return Err(error("customer SQL session is restricted to identity routes"));
        }
        let url = self.origin.join(&path).map_err(|_| error("invalid identity path"))?;
        if url.origin() != self.origin.origin() || !url.path().starts_with("/v1/identity/") || url.fragment().is_some() {
            return Err(error("identity route escaped the configured origin"));
        }
        let method = reqwest::Method::from_bytes(method.as_bytes()).map_err(|_| error("invalid identity HTTP method"))?;
        if body.as_ref().is_some_and(|bytes| bytes.len() > crate::MAX_MESSAGE_BYTES) {
            return Err(error("identity request exceeds message ceiling"));
        }
        let leaf = self.inner.clone();
        let authorization = blocking(move || {
            leaf.with_sql_session(|session| {
                let header = Zeroizing::new(format!("Bearer {session}"));
                let mut value = reqwest::header::HeaderValue::from_str(&header)
                    .map_err(|_| error("invalid sealed SQL session"))?;
                value.set_sensitive(true);
                Ok(value)
            }).map_err(error)?
        }).await?;
        cancelled(&token)?;
        let mut request = self.http.request(method, url).header(reqwest::header::AUTHORIZATION, authorization);
        if let Some(body) = body { request = request.header(reqwest::header::CONTENT_TYPE, "application/json").body(body.to_vec()); }
        let work = async {
            let mut response = request.send().await.map_err(|_| error("customer identity HTTPS request failed"))?;
            let status = u32::from(response.status().as_u16());
            if response.content_length().is_some_and(|length| length > crate::MAX_MESSAGE_BYTES as u64) {
                return Err(error("identity response exceeds message ceiling"));
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| error("customer identity response interrupted"))? {
                if bytes.len().saturating_add(chunk.len()) > crate::MAX_MESSAGE_BYTES { return Err(error("identity response exceeds message ceiling")); }
                bytes.extend_from_slice(&chunk);
            }
            Ok(NativeIdentityResponse { status, body: bytes.into() })
        };
        tokio::select! {
            biased;
            _ = token.cancelled() => Err(napi::Error::new(Status::GenericFailure, "customer identity request cancelled")),
            result = work => result,
        }
    }
    /// Delete only this original generation. Server revocation is a separate operation.
    #[napi]
    pub async fn delete(&self) -> Result<()> {
        let leaf = self.inner.clone();
        blocking(move || leaf.delete().map_err(error)).await
    }
}

/// Restore the original namespace, including an expired public certificate for SQL renewal.
#[napi]
pub async fn open_native_customer_credential(login_origin: String, public_key: String, birth: String, certificate: String) -> Result<NativeCustomerCredential> {
    let origin = origin(&login_origin)?;
    let namespace = namespace(&origin, &public_key, &birth, &certificate)?;
    let expected = account::public_key(&public_key).map_err(error)?.to_bytes();
    let leaf = blocking(move || {
        let leaf = RestoredCustomerLeaf::open(namespace).map_err(error)?;
        if leaf.public_key() != expected { return Err(error("stored customer leaf does not match its certificate")); }
        Ok(leaf)
    }).await?;
    NativeCustomerCredential::new(leaf, origin)
}

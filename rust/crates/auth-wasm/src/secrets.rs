//! Browser/Node fetch adapter. Authority, JSON and redaction live in acyclic-auth.
use acyclic_auth::{
    contract::*,
    credential::{CredentialError, CredentialProvider},
    secrets::{Client, Error, Response, Transport, TransportFuture},
};
use js_sys::{Function, Promise, Reflect, Uint8Array};
use prost::Message;
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;
use zeroize::Zeroizing;

fn error(value: Error) -> JsValue {
    let error = js_sys::Error::new(&value.to_string());
    error.set_name("SecretsError");
    let _ = Reflect::set(error.as_ref(), &"code".into(), &value.code().into());
    error.into()
}

struct FetchTransport {
    endpoint: url::Url,
}

// Dropping a browser operation retires its physical fetch as well as its Rust
// request buffers. The controller remains owned through response-body reading.
struct FetchOwner(web_sys::AbortController);
impl Drop for FetchOwner {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Transport for FetchTransport {
    fn post<'a>(
        &'a self,
        path: &'static str,
        bearer: &'a str,
        body: &'a [u8],
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let init = web_sys::RequestInit::new();
            let owner = FetchOwner(web_sys::AbortController::new().map_err(|_| Error::Transport)?);
            let budget = acyclic_auth::credential::HTTP_REQUEST_TIMEOUT;
            let timeout_ms = u32::try_from(budget.as_millis()).map_err(|_| Error::Transport)?;
            let deadline = web_sys::AbortSignal::timeout_with_u32(timeout_ms);
            let signals = js_sys::Array::new();
            signals.push(owner.0.signal().as_ref());
            signals.push(deadline.as_ref());
            let signal = web_sys::AbortSignal::any(signals.as_ref());
            init.set_signal(Some(&signal));
            init.set_method("POST");
            init.set_redirect(web_sys::RequestRedirect::Error);
            init.set_credentials(web_sys::RequestCredentials::Omit);
            init.set_cache(web_sys::RequestCache::NoStore);
            init.set_referrer_policy(web_sys::ReferrerPolicy::NoReferrer);
            init.set_body(&Uint8Array::from(body));
            let url = self
                .endpoint
                .join(path)
                .map_err(|_| Error::InvalidRequest)?;
            let request = web_sys::Request::new_with_str_and_init(url.as_str(), &init)
                .map_err(|_| Error::Transport)?;
            request
                .headers()
                .set("authorization", &format!("Bearer {bearer}"))
                .map_err(|_| Error::Credential)?;
            request
                .headers()
                .set("content-type", "application/json")
                .map_err(|_| Error::Transport)?;
            let global = js_sys::global();
            let fetch = Reflect::get(&global, &"fetch".into())
                .and_then(|value| value.dyn_into::<Function>())
                .map_err(|_| Error::Transport)?;
            let pending = fetch
                .call1(&global, &request)
                .map_err(|_| Error::Transport)?;
            let response = JsFuture::from(Promise::resolve(&pending))
                .await
                .map_err(|_| Error::Transport)?
                .dyn_into::<web_sys::Response>()
                .map_err(|_| Error::Transport)?;
            let status = response.status();
            let body = if (200..300).contains(&status) {
                let bytes = JsFuture::from(response.array_buffer().map_err(|_| Error::Transport)?)
                    .await
                    .map_err(|_| Error::Transport)?;
                Uint8Array::new(&bytes).to_vec()
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

/// Browser and Node call the same Rust client as the CLI. Requests and replies
/// cross this boundary as protobuf, preserving all 64-bit generation values.
#[wasm_bindgen]
pub struct SecretsClient(Client<FetchTransport>);

#[wasm_bindgen]
impl SecretsClient {
    /// Credential callbacks run for every operation. Rejections and malformed
    /// credentials become redacted shared errors, without JavaScript source chains.
    #[wasm_bindgen(constructor)]
    pub fn new(endpoint: &str, credential: Function) -> Result<Self, JsValue> {
        let endpoint = acyclic_auth::credential::endpoint(endpoint)
            .map_err(|_| error(Error::InvalidRequest))?;
        let credentials = CredentialProvider::new(move || {
            let credential = credential.clone();
            async move {
                let result = credential
                    .call0(&JsValue::UNDEFINED)
                    .map_err(|_| CredentialError::Invalid)?;
                JsFuture::from(Promise::resolve(&result))
                    .await
                    .map_err(|_| CredentialError::Invalid)?
                    .as_string()
                    .ok_or(CredentialError::Invalid)
            }
        });
        Ok(Self(Client::new(FetchTransport { endpoint }, credentials)))
    }

    /// Generation-fenced put; no retry or reveal route exists.
    pub async fn put(&self, input: &[u8]) -> Result<Vec<u8>, JsValue> {
        let request: SecretPutRequest =
            acyclic_auth::secrets::decode_request::<SecretPutRequestProto, _>(input)
                .map_err(error)?;
        let request = Zeroizing::new(request);
        self.0
            .put(&request)
            .await
            .map(|reply| SecretMutationReplyProto::from(reply).encode_to_vec())
            .map_err(error)
    }

    /// List plain values and protected metadata only.
    pub async fn list(&self, input: &[u8]) -> Result<Vec<u8>, JsValue> {
        let request: SecretListRequest =
            acyclic_auth::secrets::decode_request::<SecretListRequestProto, _>(input)
                .map_err(error)?;
        self.0
            .list(&request)
            .await
            .map(|reply| SecretListReplyProto::from(reply).encode_to_vec())
            .map_err(error)
    }

    /// Generation-fenced deletion, returning a deleted receipt.
    pub async fn delete(&self, input: &[u8]) -> Result<Vec<u8>, JsValue> {
        let request: SecretDeleteRequest =
            acyclic_auth::secrets::decode_request::<SecretDeleteRequestProto, _>(input)
                .map_err(error)?;
        self.0
            .delete(&request)
            .await
            .map(|reply| SecretMutationReplyProto::from(reply).encode_to_vec())
            .map_err(error)
    }
}

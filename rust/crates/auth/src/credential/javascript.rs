//! Lossless adapter for the public JavaScript CredentialProvider contract.
use super::*;
use js_sys::{Function, JsString, Reflect};
use std::task::{Context, Poll};
use wasm_bindgen::{JsCast, prelude::*};
use web_sys::{AbortController, AbortSignal};

#[wasm_bindgen(inline_js = r#"
const encoder = new TextEncoder();
const encodeInto = encoder.encodeInto.bind(encoder);
export function encodeCredentialInto(text, output) {
    const encoded = encodeInto(text, output);
    return encoded.read === text.length ? encoded.written : -1;
}
"#)]
extern "C" {
    #[wasm_bindgen(catch, js_name = encodeCredentialInto)]
    fn encode_credential_into(text: &JsString, output: &js_sys::Uint8Array)
    -> Result<i32, JsValue>;
}

/// Admit JavaScript bearer text with the native transport predicate, without
/// returning or formatting any credential bytes.
pub fn valid_credential(value: &JsValue) -> bool {
    protected_text(value).is_ok_and(|text| super::valid_credential(&text))
}

fn protected_text(value: &JsValue) -> Result<Zeroizing<String>, CredentialError> {
    let text = value
        .dyn_ref::<JsString>()
        .ok_or(CredentialError::Invalid)?;
    let units = text.length() as usize;
    // Three bytes per UTF-16 unit covers BMP characters and lone surrogates;
    // a surrogate pair takes fewer.
    // Encode once into the final protected backing, never through the generated
    // string glue's reallocations, which cannot scrub their old allocations.
    let capacity = units.checked_mul(3).ok_or(CredentialError::Invalid)?;
    let mut bytes = Zeroizing::new(vec![0; capacity]);
    let written = encode_protected(text, &mut bytes)?;
    bytes.truncate(written);
    match String::from_utf8(std::mem::take(&mut *bytes)) {
        Ok(text) => Ok(Zeroizing::new(text)),
        Err(error) => {
            drop(Zeroizing::new(error.into_bytes()));
            Err(CredentialError::Invalid)
        }
    }
}

#[allow(unsafe_code)] // Synchronous JavaScript FFI borrows this fixed backing.
fn encode_protected(text: &JsString, bytes: &mut [u8]) -> Result<usize, CredentialError> {
    // SAFETY: the view is used only by the retained native TextEncoder method,
    // which cannot reenter Wasm or grow its memory. No Rust allocation, await or
    // backing replacement occurs while it is borrowed; JavaScript retains no
    // view after this synchronous call.
    let output = unsafe { js_sys::Uint8Array::view(bytes) };
    let written = encode_credential_into(text, &output).map_err(invalid)?;
    usize::try_from(written)
        .ok()
        .filter(|written| *written <= bytes.len())
        .ok_or(CredentialError::Invalid)
}

/// Retain the existing JavaScript owner, including exact request rejection and
/// drop-owned watch/cancellation. Raw callbacks remain raw, without a binding.
pub fn provider(callback: Function) -> CredentialProvider {
    CredentialProvider::from_custodian(Source(callback))
}

struct Source(Function);
struct Cancel(Option<AbortController>);
impl Drop for Cancel {
    fn drop(&mut self) {
        if let Some(controller) = self.0.take() {
            controller.abort();
        }
    }
}

impl CredentialSource for Source {
    fn resolve(&self) -> CredentialResolution<'_> {
        Box::pin(async {
            let controller = AbortController::new().map_err(invalid)?;
            let signal = controller.signal();
            let mut cancel = Cancel(Some(controller));
            let resolve = get(&self.0, "resolveRequest")?;
            let is_request = !resolve.is_undefined();
            let function = if is_request {
                resolve.dyn_into::<Function>().map_err(invalid)?
            } else {
                self.0.clone()
            };
            let result = function.call1(&self.0, &signal).map_err(invalid)?;
            let object = wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&result))
                .await
                .map_err(invalid)?;
            // The source has settled. A raw request can retain this signal for
            // its own check, so successful resolution must not abort it.
            cancel.0.take();
            if !is_request {
                let token = protected_text(&object)?;
                if !super::valid_credential(&token) {
                    return Err(CredentialError::Invalid);
                }
                return Ok(ResolvedCredential(Resolved::Raw(Shared::new(token))));
            }
            let binding = get(&object, "binding")?;
            let binding = if binding.is_undefined() {
                None
            } else {
                Some(CredentialBinding {
                    revision: revision(get(&binding, "revision")?)?,
                    expires_at_unix_seconds: seconds(get(&binding, "expiresAtUnixSeconds")?)?,
                })
            };
            let token = protected_text(&invoke(&object, "token")?)?;
            ResolvedCredential::from_custodian(Request {
                object,
                token,
                binding,
            })
            .await
        })
    }

    fn watch(&self) -> CredentialAction<Option<CredentialWatch>> {
        (|| {
            if get(&self.0, "resolveRequest")?.is_undefined() {
                return Ok(None);
            }
            let function = get(&self.0, "watch")?;
            if function.is_undefined() {
                return Ok(None);
            }
            let object = function
                .dyn_into::<Function>()
                .map_err(invalid)?
                .call0(&self.0)
                .map_err(invalid)?;
            // Close even if malformed metadata prevents constructing a registration.
            let owner = Close(object);
            let validity = get(&owner.0, "validity")?;
            let source_revision = get(&validity, "revision")?;
            let validity = CredentialValidity {
                now_unix_seconds: seconds(get(&validity, "nowUnixSeconds")?)?,
                revision: if source_revision.is_undefined() {
                    None
                } else {
                    Some(revision(source_revision)?)
                },
            };
            let signal = get(&owner.0, "changed")?
                .dyn_into::<AbortSignal>()
                .map_err(invalid)?;
            let changed = crate::operation::javascript::listen(signal).map_err(invalid)?;
            Ok(Some(CredentialWatch::from_custodian(
                validity,
                Registration {
                    changed,
                    _owner: owner,
                },
            )))
        })()
        .into()
    }
}

struct Request {
    object: JsValue,
    token: Zeroizing<String>,
    binding: Option<CredentialBinding>,
}
impl Request {
    fn synchronous_check(&self) -> Result<(), CredentialError> {
        // The public JS owner contract is synchronous and returns void. An
        // asynchronous or malformed owner must not be admitted as ready.
        if invoke(&self.object, "check")?.is_undefined() {
            Ok(())
        } else {
            Err(CredentialError::Invalid)
        }
    }
}
impl CredentialRequest for Request {
    fn token(&self) -> &str {
        &self.token
    }
    fn binding(&self) -> Option<CredentialBinding> {
        self.binding.clone()
    }
    fn check(&self) -> CredentialAction<()> {
        self.synchronous_check().into()
    }
    fn retained_access(&self) -> Option<Result<(), CredentialError>> {
        // JS's admitted request owns a synchronous check. Forward the same
        // object; no async invocation, copied validity or new identity exists.
        self.binding.as_ref()?;
        Some(self.synchronous_check())
    }
    fn reject(&self) -> CredentialAction<bool> {
        invoke(&self.object, "reject")
            .and_then(|value| value.as_bool().ok_or(CredentialError::Invalid))
            .into()
    }
}

struct Close(JsValue);
impl Drop for Close {
    fn drop(&mut self) {
        let _ = invoke(&self.0, "close");
    }
}
struct Registration {
    // Unregister before closing the original source owner.
    changed: crate::operation::javascript::SignalStop,
    _owner: Close,
}
impl Future for Registration {
    type Output = Result<(), CredentialCacheError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.changed).poll(cx).map(|result| {
            result
                .map(|_| ())
                .map_err(|_| CredentialCacheError::Unavailable)
        })
    }
}

fn invalid(_: JsValue) -> CredentialError {
    CredentialError::Invalid
}
fn get(object: &JsValue, name: &str) -> Result<JsValue, CredentialError> {
    Reflect::get(object, &JsValue::from_str(name)).map_err(invalid)
}
fn invoke(object: &JsValue, name: &str) -> Result<JsValue, CredentialError> {
    get(object, name)?
        .dyn_into::<Function>()
        .map_err(invalid)?
        .call0(object)
        .map_err(invalid)
}
fn revision(value: JsValue) -> Result<CredentialRevision, CredentialError> {
    if !value.is_string() {
        return Err(CredentialError::Invalid);
    }
    let text = value.unchecked_into::<JsString>();
    if text.length() == 0 {
        return Err(CredentialError::Invalid);
    }
    Ok(CredentialRevision::from(text.iter().collect::<Vec<u16>>()))
}
fn seconds(value: JsValue) -> Result<u64, CredentialError> {
    let value = value.as_f64().ok_or(CredentialError::Invalid)?;
    if !(0.0..=9_007_199_254_740_991.0).contains(&value) || value.fract() != 0.0 {
        return Err(CredentialError::Invalid);
    }
    Ok(value as u64)
}

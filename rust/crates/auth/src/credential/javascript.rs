//! Lossless adapter for the public JavaScript CredentialProvider contract.
use super::*;
use futures::channel::oneshot;
use js_sys::{Function, JsString, Reflect};
use std::{
    cell::RefCell,
    rc::Rc,
    task::{Context, Poll},
};
use wasm_bindgen::{JsCast, prelude::*};
use web_sys::{AbortController, AbortSignal};

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
                let token = Zeroizing::new(object.as_string().ok_or(CredentialError::Invalid)?);
                if !valid_credential(&token) {
                    return Err(CredentialError::Invalid);
                }
                return Ok(ResolvedCredential(Resolved::Raw(token)));
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
            let token = invoke(&object, "token")?
                .as_string()
                .ok_or(CredentialError::Invalid)?;
            ResolvedCredential::from_custodian(Request {
                object,
                token: Zeroizing::new(token),
                binding,
            })
        })
    }

    fn watch(&self) -> Result<Option<CredentialWatch>, CredentialError> {
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
        let (send, receive) = oneshot::channel();
        let send = Rc::new(RefCell::new(Some(send)));
        let changed_send = send.clone();
        let changed = Closure::new(move |_: web_sys::Event| {
            if let Some(send) = changed_send.borrow_mut().take() {
                let _ = send.send(());
            }
        });
        signal
            .add_event_listener_with_callback("abort", changed.as_ref().unchecked_ref())
            .map_err(invalid)?;
        if signal.aborted() {
            if let Some(send) = send.borrow_mut().take() {
                let _ = send.send(());
            }
        }
        Ok(Some(CredentialWatch::from_custodian(
            validity,
            Registration {
                _owner: owner,
                signal,
                changed,
                receive,
            },
        )))
    }
}

struct Request {
    object: JsValue,
    token: Zeroizing<String>,
    binding: Option<CredentialBinding>,
}
impl CredentialRequest for Request {
    fn token(&self) -> &str {
        &self.token
    }
    fn binding(&self) -> Option<CredentialBinding> {
        self.binding.clone()
    }
    fn check(&self) -> Result<(), CredentialError> {
        invoke(&self.object, "check").map(|_| ())
    }
    fn reject(&self) -> Result<bool, CredentialError> {
        invoke(&self.object, "reject")?
            .as_bool()
            .ok_or(CredentialError::Invalid)
    }
}

struct Close(JsValue);
impl Drop for Close {
    fn drop(&mut self) {
        let _ = invoke(&self.0, "close");
    }
}
struct Registration {
    _owner: Close,
    signal: AbortSignal,
    changed: Closure<dyn FnMut(web_sys::Event)>,
    receive: oneshot::Receiver<()>,
}
impl Future for Registration {
    type Output = Result<(), CredentialCacheError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receive)
            .poll(cx)
            .map(|result| result.map_err(|_| CredentialCacheError::Unavailable))
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        let _ = self
            .signal
            .remove_event_listener_with_callback("abort", self.changed.as_ref().unchecked_ref());
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

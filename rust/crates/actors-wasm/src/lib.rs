//! WebAssembly boundary for the Rust Actors client.
// Keep the bridge type-checkable on the host so contract generators inventory
// the same exported implementation without executing JavaScript operations.

use acyclic_actors::{client, domain, guest};
use js_sys::{BigInt, Function, Reflect, Uint8Array};
use tokio_util::sync::CancellationToken;
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;

fn js_error(code: &str, message: impl std::fmt::Display) -> JsValue {
    let error = js_sys::Error::new(&message.to_string());
    error.set_name("ActorsError");
    let _ = js_sys::Reflect::set(&error, &JsValue::from_str("code"), &JsValue::from_str(code));
    error.into()
}

fn map_error(error: client::Error) -> JsValue {
    let metadata = error.metadata();
    let error = js_error(&metadata.code, &metadata.message);
    match serde_wasm_bindgen::to_value(&metadata) {
        Ok(metadata) => {
            let _ = Reflect::set(&error, &JsValue::from_str("metadata"), &metadata);
            error
        }
        Err(error) => js_error("internal", error),
    }
}

fn nominal_error(error: domain::DomainError) -> JsValue {
    let code = error.code_name();
    js_error(code, error)
}

/// Admit a deployment declaration through Rust before materializing a JS env handle.
#[wasm_bindgen(js_name = validateDeploymentBinding)]
pub fn validate_deployment_binding(
    name: String,
    capability: String,
    resource: String,
    account: &[u8],
    namespace: Option<String>,
    worker: Option<String>,
    state_transaction: bool,
    guest_admission: Option<String>,
) -> Result<(), JsValue> {
    domain::Binding::try_from(acyclic_actors::wire::Binding {
        state_transaction,
        guest_admission,
        name,
        capability,
        resource,
        target_account: account.to_vec(),
        actor_namespace: namespace,
        worker_service: worker,
    })
    .map(|_| ())
    .map_err(nominal_error)
}

fn nominal_string(value: &JsValue, field: &str) -> Result<String, JsValue> {
    value
        .as_string()
        .ok_or_else(|| js_error("invalid_argument", format!("{field} must be a string")))
}

/// Rust-backed nominal constructor for `ActorId`.
#[wasm_bindgen(js_name = "ActorId")]
pub fn actor_id(
    #[wasm_bindgen(unchecked_param_type = "string")] value: &JsValue,
) -> Result<String, JsValue> {
    let value = nominal_string(value, "actor_id")?;
    domain::ActorId::new(value)
        .map(String::from)
        .map_err(nominal_error)
}

/// Rust-backed exact namespace label constructor.
#[wasm_bindgen(js_name = "ActorNamespace")]
pub fn actor_namespace(
    #[wasm_bindgen(unchecked_param_type = "string")] value: &JsValue,
) -> Result<String, JsValue> {
    domain::ActorNamespace::new(nominal_string(value, "namespace")?)
        .map(String::from)
        .map_err(nominal_error)
}

/// Rust-backed nominal constructor for `CodeSha256`.
#[wasm_bindgen(js_name = "CodeSha256")]
pub fn code_sha256(
    #[wasm_bindgen(unchecked_param_type = "Uint8Array")] value: JsValue,
) -> Result<Vec<u8>, JsValue> {
    let value: Uint8Array = value
        .dyn_into()
        .map_err(|_| js_error("invalid_argument", "code_sha256 must be a Uint8Array"))?;
    domain::CodeSha256::new(value.to_vec())
        .map(|value| value.as_bytes().to_vec())
        .map_err(nominal_error)
}

/// Rust-backed nominal constructor for `PositiveU64`.
#[wasm_bindgen(js_name = "PositiveU64")]
pub fn positive_u64(value: BigInt) -> Result<u64, JsValue> {
    let value = u64::try_from(value)
        .map_err(|_| js_error("not_positive", "value must be a lossless positive u64"))?;
    domain::PositiveU64::new(value)
        .map(domain::PositiveU64::get)
        .map_err(nominal_error)
}

/// Rust-backed nominal constructor for the true-only current-head marker.
#[wasm_bindgen(js_name = "CurrentHeadMarker")]
pub fn current_head_marker(
    #[wasm_bindgen(unchecked_param_type = "boolean")] value: &JsValue,
) -> Result<bool, JsValue> {
    let value = value
        .as_bool()
        .ok_or_else(|| js_error("invalid_argument", "current_head_marker must be a boolean"))?;
    domain::subscription_start::CurrentHeadMarker::try_from(value)
        .map(bool::from)
        .map_err(nominal_error)
}

fn guest_error(error: guest::GuestError) -> JsValue {
    let code = match error {
        guest::GuestError::UnsupportedFormat => "unsupported_format",
        _ => "invalid_argument",
    };
    js_error(code, error)
}

fn object(entries: &[(&str, JsValue)]) -> JsValue {
    let object = js_sys::Object::new();
    for (name, value) in entries {
        let _ = Reflect::set(&object, &JsValue::from_str(name), value);
    }
    object.into()
}

/// Decode one strict `guest::wire` delivery for a JavaScript guest: u64 fields
/// as bigint, bytes as Uint8Array, and a "record" trigger carrying its record.
#[wasm_bindgen(js_name = decodeDelivery)]
pub fn decode_delivery(bytes: &[u8]) -> Result<JsValue, JsValue> {
    let (delivery, checkpoint) = guest::wire::decode_delivery(bytes).map_err(guest_error)?;
    let u64 = |value: u64| JsValue::from(BigInt::from(value));
    let bytes = |value: &[u8]| JsValue::from(Uint8Array::from(value));
    let trigger = match (delivery.trigger, &delivery.record) {
        (
            guest::Trigger::Record {
                subscription,
                sequence,
            },
            Some(record),
        ) => object(&[
            ("type", "record".into()),
            ("subscription", subscription.into()),
            ("sequence", u64(sequence)),
            (
                "record",
                object(&[
                    ("sequence", u64(record.sequence)),
                    ("value", bytes(&record.value)),
                    ("commitId", bytes(record.commit_id.as_bytes())),
                    ("committedAtMicros", u64(record.committed_at_micros)),
                    ("origin", record.origin.as_str().into()),
                ]),
            ),
        ]),
        (
            guest::Trigger::Alarm {
                generation,
                now_unix_ms,
            },
            None,
        ) => object(&[
            ("type", "alarm".into()),
            ("generation", u64(generation)),
            ("nowUnixMs", u64(now_unix_ms)),
        ]),
        _ => {
            return Err(guest_error(guest::GuestError::Decode(
                "guest delivery record differs from its trigger".into(),
            )));
        }
    };
    let checkpoint = checkpoint.map_or(JsValue::NULL, |checkpoint| {
        object(&[
            (
                "codec",
                object(&[
                    ("name", checkpoint.codec.name.into()),
                    ("version", checkpoint.codec.version.into()),
                ]),
            ),
            ("bytes", bytes(&checkpoint.bytes)),
        ])
    });
    Ok(object(&[
        ("request", u64(delivery.request.get())),
        ("actor", bytes(&delivery.actor.encode())),
        ("epoch", u64(delivery.epoch)),
        ("nowUnixMs", u64(delivery.now_unix_ms)),
        ("trigger", trigger),
        ("checkpoint", checkpoint),
    ]))
}

/// Encode a JavaScript guest's transition: its checkpoint and optional reply,
/// keeping the alarm, with no effects, participant or subscription changes.
#[wasm_bindgen(js_name = encodeTransition)]
pub fn encode_transition(
    codec_name: String,
    codec_version: String,
    checkpoint: Vec<u8>,
    response: Option<Vec<u8>>,
) -> Result<Vec<u8>, JsValue> {
    guest::wire::encode_transition(&guest::Transition {
        response: response.map(Into::into),
        pause: false,
        checkpoint: guest::Checkpoint {
            codec: guest::CodecIdentity {
                name: codec_name,
                version: codec_version,
            },
            bytes: checkpoint.into(),
        },
        alarm: guest::AlarmChange::Keep,
        effects: Vec::new(),
        participant: None,
        subscriptions: Vec::new(),
    })
    .map_err(guest_error)
}

struct AbortRegistration {
    signal: JsValue,
    callback: Closure<dyn FnMut(JsValue)>,
    token: CancellationToken,
}

impl AbortRegistration {
    fn new(signal: Option<JsValue>) -> Result<Option<Self>, JsValue> {
        let Some(signal) = signal.filter(|value| !value.is_undefined() && !value.is_null()) else {
            return Ok(None);
        };
        let token = CancellationToken::new();
        let callback_token = token.clone();
        let callback = Closure::wrap(Box::new(move |_event: JsValue| {
            callback_token.cancel();
        }) as Box<dyn FnMut(JsValue)>);
        let add = Reflect::get(&signal, &JsValue::from_str("addEventListener"))?
            .dyn_into::<Function>()?;
        add.call2(
            &signal,
            &JsValue::from_str("abort"),
            callback.as_ref().unchecked_ref(),
        )?;
        let registration = Self {
            signal,
            callback,
            token,
        };
        let aborted = Reflect::get(&registration.signal, &JsValue::from_str("aborted"))?
            .as_bool()
            .unwrap_or(false);
        if aborted {
            registration.token.cancel();
        }
        Ok(Some(registration))
    }
}

impl Drop for AbortRegistration {
    fn drop(&mut self) {
        if let Ok(remove) = Reflect::get(&self.signal, &JsValue::from_str("removeEventListener"))
            .and_then(|value| value.dyn_into::<Function>())
        {
            let _ = remove.call2(
                &self.signal,
                &JsValue::from_str("abort"),
                self.callback.as_ref().unchecked_ref(),
            );
        }
    }
}

/// Browser binding for the shared Actors client.
#[wasm_bindgen]
pub struct ActorsClient {
    inner: client::Client,
}

#[wasm_bindgen]
impl ActorsClient {
    /// Connect to an HTTPS endpoint using the canonical browser gRPC-Web transport.
    #[wasm_bindgen]
    pub async fn connect(
        endpoint: String,
        token: String,
        signal: Option<JsValue>,
    ) -> Result<ActorsClient, JsValue> {
        let registration = AbortRegistration::new(signal)?;
        let cancellation = registration
            .as_ref()
            .map(|registration| registration.token.clone());
        let result =
            client::run_with_cancellation(client::connect(&endpoint, &token, None), cancellation)
                .await;
        drop(registration);
        result.map(|inner| Self { inner }).map_err(map_error)
    }

    async fn run<T, F>(&self, signal: Option<JsValue>, operation: F) -> Result<T, JsValue>
    where
        F: std::future::Future<Output = Result<T, client::Error>>,
    {
        let registration = AbortRegistration::new(signal)?;
        let cancellation = registration
            .as_ref()
            .map(|registration| registration.token.clone());
        let result = client::run_with_cancellation(operation, cancellation).await;
        drop(registration);
        result.map_err(map_error)
    }

    /// Returns the transport selected by the Rust client.
    #[wasm_bindgen(getter)]
    pub fn transport(&self) -> String {
        self.inner.transport().to_owned()
    }

    /// Execute one operation with a request-local credential on the existing connection.
    pub async fn call_with_credential(
        &self,
        method: &str,
        request: &[u8],
        token: &str,
        signal: Option<JsValue>,
    ) -> Result<Vec<u8>, JsValue> {
        self.run(
            signal,
            self.inner.call_with_credential(method, request, token),
        )
        .await
    }
}

macro_rules! operation_wrappers {
    ($(($method:ident, $name:literal)),+ $(,)?) => {
        #[acyclic_contract_derive::implementation_methods(RPC_METHODS, wasm_bindgen)]
        #[wasm_bindgen]
        impl ActorsClient {
            $(
            #[doc = concat!("Execute `", $name, "` with an encoded protobuf request.")]
            pub async fn $method(
                &self,
                request: &[u8],
                signal: Option<JsValue>,
            ) -> Result<Vec<u8>, JsValue> {
                self.run(signal, self.inner.call($name, request)).await
            }
            )+
        }
    };
}

operation_wrappers! {
    (deploy_namespace, "DeployNamespace"),
    (deploy_worker, "DeployWorker"),
    (invoke_worker, "InvokeWorker"),
    (delete_worker, "DeleteWorker"),
    (update_namespace, "UpdateNamespace"),
    (inspect_actor, "InspectActor"),
    (add_subscription, "AddSubscription"),
    (remove_subscription, "RemoveSubscription"),
    (resume_subscription, "ResumeSubscription"),
    (checkpoint_actor, "CheckpointActor"),
    (fork_actor, "ForkActor"),
    (invoke_actor, "InvokeActor"),
    (delete_all, "DeleteAll"),
}

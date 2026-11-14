//! WebAssembly boundary for the Rust Actors client.
// Keep the bridge type-checkable on the host so contract generators inventory
// the same exported implementation without executing JavaScript operations.

use acyclic_actors::{client, domain, guest};
use js_sys::{BigInt, Function, Reflect, Uint8Array};
use serde::Serialize as _;
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
    let socket = delivery
        .socket()
        .map_err(guest_error)?
        .map(|socket| guest::GuestEvent::Socket {
            connection_id: socket.connection_id,
            event: socket.event,
        });
    let socket = socket
        .map(|event| {
            event.serialize(&serde_wasm_bindgen::Serializer::new().serialize_missing_as_null(true))
        })
        .transpose()
        .map_err(|_| js_error("invalid_argument", "socket event does not encode"))?
        .unwrap_or(JsValue::NULL);
    let u64 = |value: u64| JsValue::from(BigInt::from(value));
    let bytes = |value: &[u8]| JsValue::from(Uint8Array::from(value));
    let trigger = match (delivery.trigger, &delivery.record) {
        (
            guest::Trigger::Record {
                subscription,
                sequence,
            }
            | guest::Trigger::KeyedRecord {
                subscription,
                sequence,
                ..
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
        ("socket", socket),
        ("checkpoint", checkpoint),
    ]))
}

/// The HTTP request of one `InvokeActor` delivery, for a host whose every
/// invocation is a front-door `InvokeActorRequest`: method, URL, ordered header
/// pairs and body. Ordinary records and alarms yield null. Only such a host
/// calls this; elsewhere invocation payloads are application-defined bytes
/// that stay on the guest's event path.
#[wasm_bindgen(js_name = decodeInvokeRequest)]
pub fn decode_invoke_request(bytes: &[u8]) -> Result<JsValue, JsValue> {
    use prost::Message as _;
    let (delivery, _) = guest::wire::decode_delivery(bytes).map_err(guest_error)?;
    if delivery.socket().map_err(guest_error)?.is_some() {
        return Ok(JsValue::NULL);
    }
    let Some(invocation) = delivery.invocation().map_err(guest_error)? else {
        return Ok(JsValue::NULL);
    };
    let request = acyclic_actors::wire::InvokeActorRequest::decode(invocation.request().as_ref())
        .map_err(|_| {
        js_error(
            "invalid_argument",
            "invocation request is not InvokeActorRequest",
        )
    })?;
    let headers = js_sys::Array::new();
    for header in request.headers {
        headers.push(&js_sys::Array::of2(
            &header.name.into(),
            &header.value.into(),
        ));
    }
    Ok(object(&[
        ("method", request.method.into()),
        ("url", request.url.into()),
        ("headers", headers.into()),
        ("body", Uint8Array::from(request.body.as_slice()).into()),
    ]))
}

/// The canonical actor answer to a socket Open, with the same validated
/// GuestEventResult encoding the native SDK socket host consumes.
#[wasm_bindgen(js_name = encodeSocketResult)]
pub fn encode_socket_result(result: JsValue) -> Result<Vec<u8>, JsValue> {
    let result: guest::GuestEventResult = serde_wasm_bindgen::from_value(result)
        .map_err(|_| js_error("invalid_argument", "invalid socket result"))?;
    Ok(acyclic_actors::sockets::socket_reply(&result)
        .map_err(guest_error)?
        .to_vec())
}

/// Encode the SDK-owned socket command and binary body for an admitted host port.
#[wasm_bindgen(js_name = encodeSocketRequest)]
pub fn encode_socket_request(command: JsValue, body: Vec<u8>) -> Result<Vec<u8>, JsValue> {
    let command: guest::SocketCommand = serde_wasm_bindgen::from_value(command)
        .map_err(|_| js_error("invalid_argument", "invalid socket command"))?;
    Ok(guest::encode_socket_request(&command, body.into())
        .map_err(guest_error)?
        .to_vec())
}

/// Decode a native socket result without JSON/base64-encoding attachment bytes.
#[wasm_bindgen(js_name = decodeSocketReply)]
pub fn decode_socket_reply(bytes: &[u8]) -> Result<JsValue, JsValue> {
    let (reply, body) = guest::decode_socket_reply(bytes).map_err(guest_error)?;
    let reply = reply
        .serialize(&serde_wasm_bindgen::Serializer::new().serialize_missing_as_null(true))
        .map_err(|_| js_error("invalid_argument", "socket result does not encode"))?;
    Ok(object(&[
        ("reply", reply),
        ("body", Uint8Array::from(body.as_ref()).into()),
    ]))
}

/// The canonical `InvokeActorResponse` reply of one invocation: an HTTP status,
/// ordered header pairs and the complete body. `names` and `values` pair by index.
#[wasm_bindgen(js_name = encodeInvokeResponse)]
pub fn encode_invoke_response(
    status: u16,
    names: Vec<String>,
    values: Vec<String>,
    body: Vec<u8>,
) -> Result<Vec<u8>, JsValue> {
    use prost::Message as _;
    if names.len() != values.len() {
        return Err(js_error(
            "invalid_argument",
            "response header names and values differ in length",
        ));
    }
    Ok(acyclic_actors::wire::InvokeActorResponse {
        status: status.into(),
        body,
        headers: names
            .into_iter()
            .zip(values)
            .map(|(name, value)| acyclic_actors::wire::Header { name, value })
            .collect(),
        ..Default::default()
    }
    .encode_to_vec())
}

/// The strict-wire manifest of a JavaScript `ActorDefinition` package: the
/// deployment's admitted code digest and subscription ids, the definition's
/// codec, and the deployment's optional limits. It declares no interrupts and
/// no bindings; a definition reaches its bindings through its environment.
#[wasm_bindgen(js_name = encodeManifest)]
pub fn encode_manifest(
    code: Vec<u8>,
    codec_name: String,
    codec_version: String,
    subscriptions: Vec<String>,
    handler_timeout_millis: Option<u64>,
    memory_bytes: Option<u64>,
    checkpoint_bytes: Option<u64>,
) -> Result<Vec<u8>, JsValue> {
    Ok(guest::wire::encode_manifest(&guest::Manifest {
        code: domain::CodeSha256::try_from(code).map_err(nominal_error)?,
        codec: guest::CodecIdentity {
            name: codec_name,
            version: codec_version,
        },
        subscriptions,
        interrupts: Vec::new(),
        bindings: Vec::new(),
        limits: domain::ActorLimits::new(handler_timeout_millis, memory_bytes, checkpoint_bytes)
            .map_err(nominal_error)?,
    }))
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
        pause: false,
        response: response.map(Into::into),
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

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message as _;

    #[test]
    fn definition_manifest_is_the_strict_wire_the_core_admits() -> Result<(), guest::GuestError> {
        let code = vec![7; 32];
        let bytes = encode_manifest(
            code.clone(),
            "routes".into(),
            "1".into(),
            vec!["routes".into(), "inbox".into()],
            Some(5_000),
            None,
            Some(1 << 20),
        )
        .map_err(|_| guest::GuestError::Decode("manifest refused".into()))?;
        let manifest = guest::wire::decode_manifest(&bytes)?;
        assert_eq!(manifest.code.as_bytes().as_slice(), code.as_slice());
        assert_eq!(
            manifest.codec,
            guest::CodecIdentity {
                name: "routes".into(),
                version: "1".into()
            }
        );
        assert_eq!(manifest.subscriptions, ["routes", "inbox"]);
        assert!(manifest.interrupts.is_empty() && manifest.bindings.is_empty());
        assert_eq!(manifest.limits.handler_timeout_millis(), Some(5_000));
        assert_eq!(manifest.limits.memory_bytes(), None);
        assert_eq!(manifest.limits.checkpoint_bytes(), Some(1 << 20));
        Ok(())
    }

    #[test]
    fn invocation_reply_is_the_front_door_response() -> Result<(), prost::DecodeError> {
        let bytes = encode_invoke_response(
            201,
            vec!["location".into(), "x-a".into()],
            vec!["/result".into(), "b".into()],
            b"body".to_vec(),
        )
        .unwrap_or_default();
        let response = acyclic_actors::wire::InvokeActorResponse::decode(bytes.as_slice())?;
        assert_eq!(response.status, 201);
        assert_eq!(response.body, b"body");
        assert_eq!(
            response
                .headers
                .iter()
                .map(|header| (header.name.as_str(), header.value.as_str()))
                .collect::<Vec<_>>(),
            [("location", "/result"), ("x-a", "b")]
        );
        Ok(())
    }
}

//! Guest lifecycle context and retry-key contract.
use crate::{ActorRuntime, AdmittedCaller, GuestError};
use ts_rs::TS;

type RetryKey = Box<dyn Fn(&str) -> Result<String, GuestError> + Send + Sync>;

/// One context for every lifecycle hook and event. Hosts inject scoped handles.
#[derive(TS)]
#[ts(bound = "Bindings: TS", concrete(Socket = ()), rename_all = "camelCase")]
#[non_exhaustive]
pub struct GuestContext<Bindings, Socket: ?Sized> {
    /// Scoped binding handles, never deployment secrets in plain configuration.
    pub env: Bindings,
    /// Injectable Unix millisecond clock; shared with the local runtime.
    #[ts(type = "() => bigint")]
    pub clock: Box<dyn Fn() -> u64 + Send + Sync>,
    /// Stable across retries; null only for restore and quiesced checkpoint.
    pub delivery_id: Option<String>,
    /// Host-admitted caller, null for Stream/timer/restore/checkpoint deliveries.
    pub admitted_identity: Option<AdmittedCaller>,
    /// Reserved runtime namespace, independent of manifest bindings.
    pub actor: ActorRuntime,
    /// Delivery-scoped socket owner. The JavaScript host port uses the SDK's
    /// binary request/reply codecs; it carries no actor-selected authority.
    #[ts(
        optional,
        type = "(request: Uint8Array, signal?: AbortSignal) => Promise<Uint8Array>"
    )]
    pub socket_host: Option<std::sync::Arc<Socket>>,
    /// Effect retry keys derived from the admitted delivery identity.
    #[ts(type = "(effect: string) => string")]
    pub retry_key: RetryKey,
}

impl<Bindings, Socket: ?Sized> GuestContext<Bindings, Socket> {
    /// Start host context construction; later fields are optional builder extensions.
    pub fn builder(
        env: Bindings,
        clock: Box<dyn Fn() -> u64 + Send + Sync>,
    ) -> GuestContextBuilder<Bindings, Socket> {
        GuestContextBuilder {
            env,
            clock,
            delivery_id: None,
            admitted_identity: None,
            actor: ActorRuntime::default(),
            socket_host: None,
        }
    }
}

/// Host context builder with optional, additive runtime and caller metadata.
pub struct GuestContextBuilder<Bindings, Socket: ?Sized> {
    env: Bindings,
    clock: Box<dyn Fn() -> u64 + Send + Sync>,
    delivery_id: Option<String>,
    admitted_identity: Option<AdmittedCaller>,
    actor: ActorRuntime,
    socket_host: Option<std::sync::Arc<Socket>>,
}

impl<Bindings, Socket: ?Sized> GuestContextBuilder<Bindings, Socket> {
    /// Supply the host's stable delivery identity.
    pub fn delivery(mut self, identity: String) -> Self {
        self.delivery_id = Some(identity);
        self
    }
    /// Supply identity only after host authority admission; never from public headers.
    pub fn admitted_identity(mut self, identity: AdmittedCaller) -> Self {
        self.admitted_identity = Some(identity);
        self
    }
    /// Supply runtime handles without adding manifest env properties.
    pub fn actor(mut self, actor: ActorRuntime) -> Self {
        self.actor = actor;
        self
    }
    /// Supply the socket host scoped to this admitted actor's delivery.
    pub fn socket_host(mut self, host: std::sync::Arc<Socket>) -> Self {
        self.socket_host = Some(host);
        self
    }
    /// Finish context construction and bind effect keys to the admitted delivery.
    pub fn build(self) -> GuestContext<Bindings, Socket> {
        let admitted = self.delivery_id.clone();
        GuestContext {
            env: self.env,
            clock: self.clock,
            delivery_id: self.delivery_id,
            admitted_identity: self.admitted_identity,
            actor: self.actor,
            socket_host: self.socket_host,
            retry_key: Box::new(move |effect| {
                retry_key(admitted.as_deref().unwrap_or_default(), effect)
            }),
        }
    }
}

/// Derive an unambiguous effect retry key from a stable delivery and effect name.
/// Hosts must reuse delivery identity on retries; this promises no external atomicity.
pub fn retry_key(delivery_id: &str, effect: &str) -> Result<String, GuestError> {
    if delivery_id.is_empty() || effect.is_empty() {
        return Err(GuestError::MissingIdentity);
    }
    Ok(format!(
        "guest-v1:{}:{delivery_id}:{effect}",
        delivery_id.len()
    ))
}

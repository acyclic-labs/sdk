//! Thin adapter over the authenticated platform's invocation-owned capability.
use super::*;
use acyclic_auth::{
    contract::CallerContext,
    operation::{Context, Stopped},
};
use acyclic_stream::BoxProviderFuture;
use js_sys::{Function, Promise};
use serde::de::DeserializeOwned;
use std::sync::Arc;
use wasm_bindgen_futures::JsFuture;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Admission {
    issuer: String,
    audience: Authority,
    scope: Scope,
    trust: acyclic_auth::trust::Snapshot,
    checkpoint_key: Vec<u8>,
    caller: Vec<u8>,
    operation: OperationId,
    deadline_unix_millis: u64,
}

#[derive(Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum Reply<T> {
    Ok(T),
    Denied,
    Closed,
    Indeterminate,
}

struct Platform {
    owner: JsValue,
    check: Function,
    trust_changed: Function,
    trust_closed: Function,
    derive: Function,
    checkpoint: Function,
}

struct Clock {
    owner: JsValue,
    now: Function,
    expires: Function,
}

impl acyclic_auth::operation::Clock for Clock {
    fn now_unix_millis(&self) -> std::result::Result<u64, Stopped> {
        self.now
            .call0(&self.owner)
            .ok()
            .and_then(|value| from_js(value).ok())
            .ok_or(Stopped::Cancelled)
    }

    fn expires_at(
        &self,
        deadline: u64,
    ) -> std::result::Result<acyclic_auth::operation::StopFuture, Stopped> {
        // Register eagerly on the original owner; cancellation owns exactly this
        // registration, never the invocation or remote physical operation.
        let registration = crate::wasm::cancel::Cancel::new().map_err(|_| Stopped::Cancelled)?;
        let promise = self
            .expires
            .call2(
                &self.owner,
                &js_sys::BigInt::from(deadline),
                &registration.signal,
            )
            .map_err(|_| Stopped::Cancelled)?
            .dyn_into::<Promise>()
            .map_err(|_| Stopped::Cancelled)?;
        Ok(Box::pin(async move {
            let _registration = registration;
            stop(promise).await
        }))
    }
}

async fn stop(promise: Promise) -> Stopped {
    match JsFuture::from(promise)
        .await
        .ok()
        .and_then(|value| value.as_string())
        .as_deref()
    {
        Some("deadline") => Stopped::Deadline,
        _ => Stopped::Cancelled,
    }
}

impl Platform {
    async fn call<T: DeserializeOwned>(
        &self,
        method: &Function,
        operation: OperationId,
        request: &impl Serialize,
    ) -> crate::Result<T> {
        let mut cancel = crate::wasm::cancel::Cancel::new().map_err(|_| crate::Error::Cancelled)?;
        let request = to_js(request)
            .map_err(|_| crate::Error::Invalid("invalid authority operation".into()))?;
        let result = method
            .call2(&self.owner, &request, &cancel.signal)
            .map_err(|_| crate::Error::Indeterminate(operation))?;
        let result = JsFuture::from(Promise::resolve(&result)).await;
        cancel.finish();
        let value = result.map_err(|_| crate::Error::Indeterminate(operation))?;
        match from_js::<Reply<T>>(value).map_err(|_| crate::Error::Indeterminate(operation))? {
            Reply::Ok(value) => Ok(value),
            Reply::Denied => Err(crate::Error::Unauthorized(
                "platform refused the admitted operation".into(),
            )),
            Reply::Closed | Reply::Indeterminate => Err(crate::Error::Indeterminate(operation)),
        }
    }
}

impl acyclic_auth::trust::ChangeSource for Platform {
    fn check_open(&self) -> std::result::Result<(), acyclic_auth::trust::SourceClosed> {
        match self
            .trust_closed
            .call0(&self.owner)
            .ok()
            .and_then(|value| value.as_bool())
        {
            Some(false) => Ok(()),
            _ => Err(acyclic_auth::trust::SourceClosed),
        }
    }
    fn changed(&self) -> acyclic_auth::trust::Changed {
        // Register now, before the consumer's synchronous authority check.
        let registered = (|| {
            let cancel = crate::wasm::cancel::Cancel::new()
                .map_err(|_| acyclic_auth::trust::SourceClosed)?;
            let event = self
                .trust_changed
                .call1(&self.owner, &cancel.signal)
                .map_err(|_| acyclic_auth::trust::SourceClosed)?;
            let event = event
                .dyn_into::<Promise>()
                .map_err(|_| acyclic_auth::trust::SourceClosed)?;
            Ok((cancel, event))
        })();
        Box::pin(async move {
            let (mut cancel, event) = registered?;
            let result = JsFuture::from(event).await;
            cancel.finish();
            result
                .map(|_| ())
                .map_err(|_| acyclic_auth::trust::SourceClosed)
        })
    }
}

impl crate::core::AuthorityOperations for Platform {
    fn check(&self, scope: &Scope) -> crate::Result<()> {
        let scope = to_js(scope)
            .map_err(|_| crate::Error::Invalid("invalid authority observation".into()))?;
        let state = self
            .check
            .call1(&self.owner, &scope)
            .map_err(|_| crate::Error::Cancelled)?;
        match state.as_string().as_deref() {
            Some("current") => Ok(()),
            Some("denied") => Err(crate::Error::Unauthorized(
                "original platform admission is no longer current".into(),
            )),
            Some("closed") => Err(crate::Error::Cancelled),
            _ => Err(crate::Error::Invalid(
                "platform admission needs a synchronous retained validity guard".into(),
            )),
        }
    }

    fn derive<'a>(
        &'a self,
        context: &'a Context,
        operation: OperationId,
        parent: &'a Scope,
        id: &'a str,
        derivation: crate::core::ScopeDerivation<'a>,
    ) -> BoxProviderFuture<'a, crate::Result<Scope>> {
        #[derive(Serialize)]
        struct Request<'a> {
            operation: OperationId,
            parent: &'a Scope,
            id: &'a str,
            derivation: crate::core::ScopeDerivation<'a>,
            deadline_unix_millis: u64,
        }
        Box::pin(async move {
            self.call(
                &self.derive,
                operation,
                &Request {
                    operation,
                    parent,
                    id,
                    derivation,
                    deadline_unix_millis: context.deadline_unix_millis(),
                },
            )
            .await
        })
    }

    fn checkpoint<'a>(
        &'a self,
        context: &'a Context,
        operation: OperationId,
        kind: crate::core::CheckpointKind,
        digest: [u8; 32],
    ) -> BoxProviderFuture<'a, crate::Result<[u8; 64]>> {
        #[derive(Serialize)]
        struct Request {
            operation: OperationId,
            kind: crate::core::CheckpointKind,
            deadline_unix_millis: u64,
            digest: [u8; 32],
        }
        Box::pin(async move {
            let proof: Vec<u8> = self
                .call(
                    &self.checkpoint,
                    operation,
                    &Request {
                        operation,
                        kind,
                        digest,
                        deadline_unix_millis: context.deadline_unix_millis(),
                    },
                )
                .await?;
            proof
                .try_into()
                .map_err(|_| crate::Error::Indeterminate(operation))
        })
    }
}

pub(super) fn open(owner: JsValue) -> Result<WasmAgentAuthority, JsValue> {
    let field = |name: &str| js_sys::Reflect::get(&owner, &JsValue::from_str(name));
    let admission: Admission = from_js(field("admission")?)?;
    let caller = CallerContext::try_from(
        acyclic_auth::contract::CallerContextProto::decode(admission.caller.as_slice())
            .map_err(|_| JsValue::from_str("invalid caller context encoding"))?,
    )
    .map_err(|_| JsValue::from_str("invalid caller context contract"))?;
    if u128::from(admission.deadline_unix_millis)
        > u128::from(caller.expires_at_unix_seconds) * 1000
    {
        return Err(js_error(crate::Error::Invalid(
            "operation deadline exceeds the original caller's expiry".into(),
        )));
    }
    let stopped = field("stopped")?.dyn_into::<Promise>()?;
    let clock = field("clock")?;
    let context = if clock.is_undefined() {
        // Event-only authority remains explicitly unqualified for child budgets.
        Context::from_stopped(admission.deadline_unix_millis, stop(stopped))
    } else {
        let method = |name: &str| {
            js_sys::Reflect::get(&clock, &JsValue::from_str(name))?.dyn_into::<Function>()
        };
        let clock = Clock {
            now: method("nowUnixMilliseconds")?,
            expires: method("expiresAt")?,
            owner: clock,
        };
        Context::from_clock(
            admission.deadline_unix_millis,
            stop(stopped),
            Arc::new(clock),
        )
    };
    let platform = Arc::new(Platform {
        check: field("check")?.dyn_into()?,
        trust_changed: field("trustChanged")?.dyn_into()?,
        trust_closed: field("trustClosed")?.dyn_into()?,
        derive: field("derive")?.dyn_into()?,
        checkpoint: field("checkpoint")?.dyn_into()?,
        owner,
    });
    let verifier = crate::core::AuthorityVerifier::from_public(
        admission.issuer,
        admission.audience,
        admission.trust,
        admission.checkpoint_key.try_into().map_err(|_| {
            JsValue::from_str("public checkpoint key must contain exactly 32 bytes")
        })?,
        platform.clone(),
    )
    .map_err(js_error)?;
    let authority = crate::core::AdmittedAuthority::from_platform(
        verifier,
        admission.scope,
        context,
        platform.clone(),
        platform,
    )
    .map_err(js_error)?;
    Ok(WasmAgentAuthority {
        issuer: crate::core::RuntimeAuthority::admitted(authority, admission.operation),
        caller,
    })
}

#[wasm_bindgen(typescript_custom_section)]
const CAPABILITY: &str = r#"
export type WasmAuthorityReply<T> = { kind: "ok"; value: T }
    | { kind: "denied" | "closed" | "indeterminate" };
export type WasmScopeDerivation =
    | { kind: "narrow"; capabilities: string[]; agent: string | null }
    | { kind: "private_file_read"; file: WasmFileRefWire; reader: string }
    | { kind: "private_directory_read"; volume: WasmVolumeRefWire; prefix: string; reader: string };
export interface WasmAuthorityDerivation {
    operation: string;
    deadline_unix_millis: bigint;
    parent: Scope;
    id: string;
    derivation: WasmScopeDerivation;
}
export interface WasmAuthorityCheckpoint {
    operation: string;
    deadline_unix_millis: bigint;
    kind: "execution" | "working" | "model_request";
    digest: number[];
}
export interface WasmAuthorityAdmission {
    issuer: string;
    audience: Authority;
    /** Sealed public evidence. Restricted append authority is rejected. */
    scope: Scope;
    trust: { environment: string; environment_keys: number[][]; account: string;
        issuer_keys: number[][]; minimum_key_epoch: bigint;
        revoked_key_ids: string[]; revoked_block_ids: number[][] };
    /** Authenticated public verification key, never a signing key. */
    checkpoint_key: number[];
    /** Canonical Auth CallerContext bytes: original customer and every delegate. */
    caller: Uint8Array;
    operation: string;
    deadline_unix_millis: bigint;
}
/** Obtained from authenticated platform dispatch, never decoded from a guest
 * request. The owner binds admission, caller handle, recipient and current trust
 * to this invocation. Its methods retain the original customer even across
 * accounts and recheck the actual target and owner policy before signing.
 * No signing key, append seed or portable account bearer crosses this port. */
export interface WasmAuthorityCapability {
    readonly admission: WasmAuthorityAdmission;
    /** Original deadline/cancel event; producer loss rejects or resolves cancelled.
     * This is the platform's existing event, never a guest timer or polling loop. */
    readonly stopped: Promise<"deadline" | "cancelled">;
    /** SAME invocation clock. Event-only owners without this port explicitly
     * refuse remaining-time and child-budget qualification. Never substitute a
     * resource transport's clock, Date.now or a guest timer. */
    readonly clock?: WasmOperationClock;
    /** Synchronous, non-dispatching owner observation, including original expiry,
     * source revocation and exact rejection. Async-only owners are unsupported. */
    check(scope: Scope): "current" | "denied" | "closed";
    /** Synchronously register the next publication from the SAME authenticated
     * trust owner. Resolve once its changes are visible to check; reject when
     * that producer is lost. Abort releases only this registration. Registering
     * never probes, starts a timer or creates an independent trust channel. */
    trustChanged(signal: AbortSignal): Promise<void>;
    /** Synchronous producer-loss observation from the SAME owner. This does not
     * register work or consume the pending publication; async owners must retain
     * a local guard. Throwing or a non-boolean result is closed. */
    trustClosed(): boolean;
    /** Return only exact sealed descendants. Ordinary narrowing may change an
     * acting agent only when the actual resource producer independently admits
     * it. The full request identifies the operation; uncertainty is not replayed. */
    derive(request: WasmAuthorityDerivation, signal: AbortSignal): WasmAuthorityReply<Scope> | Promise<WasmAuthorityReply<Scope>>;
    /** Sign the typed checkpoint under the retained issuer and audience. */
    checkpoint(request: WasmAuthorityCheckpoint, signal: AbortSignal): WasmAuthorityReply<number[]> | Promise<WasmAuthorityReply<number[]>>;
}
/** Retained only from authenticated platform admission. Its monotonic wall-time
 * projection and expiry registration share the original invocation's clock. */
export interface WasmOperationClock {
    /** No IO; throw after owner loss. */
    nowUnixMilliseconds(): bigint;
    /** Eager registration on the original clock. Abort unregisters it; owner
     * loss must settle cancelled or reject. No timer renewal between frames. */
    expiresAt(deadlineUnixMilliseconds: bigint, signal: AbortSignal): Promise<"deadline" | "cancelled">;
}
"#;

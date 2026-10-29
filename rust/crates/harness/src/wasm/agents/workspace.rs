//! Opaque ownership of the public admitted Machines workspace.
use crate::{machines::MachinesWorkspace, tool::ToolExecutor};
use std::sync::Arc;
use wasm_bindgen::prelude::*;

/// A platform's admitted Machine, selected volume and authenticated provider.
/// Native hosts wrap the public `MachinesWorkspace`; JavaScript hosts use
/// `open` in the same initialized module as the registry and server. Both retain
/// the original caller, selected volume and the one shared shell executor.
#[wasm_bindgen]
pub struct WasmMachinesWorkspace {
    workspace: Arc<MachinesWorkspace>,
}

impl From<MachinesWorkspace> for WasmMachinesWorkspace {
    fn from(workspace: MachinesWorkspace) -> Self {
        Self {
            workspace: Arc::new(workspace),
        }
    }
}

impl WasmMachinesWorkspace {
    pub(super) fn executor(&self) -> Arc<dyn ToolExecutor> {
        self.workspace.clone()
    }
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
impl WasmMachinesWorkspace {
    /// Materialize a trusted `machines:/` grant, without performing an RPC.
    /// The source is supplied by authenticated platform admission, never by
    /// request metadata. Its transport and budget belong to this same caller.
    pub fn open(
        owner: &super::WasmAgentAuthority,
        #[wasm_bindgen(unchecked_param_type = "WasmMachinesWorkspaceSource")] source: JsValue,
    ) -> Result<Self, JsValue> {
        use crate::{conversation::VolumeRef, machines::MachinesHost, resources::ProviderRef};
        use acyclic_machines::{MachineObservation, MachinesProvider, grpc, wire};
        use prost::Message as _;

        fn field<T: JsCast>(source: &JsValue, name: &str) -> Result<T, JsValue> {
            js_sys::Reflect::get(source, &JsValue::from_str(name))?
                .dyn_into()
                .map_err(|_| super::js_error(crate::Error::Invalid(format!(
                    "Machines workspace source field {name} has the wrong type; recovery: retain the original admitted platform source"
                ))))
        }

        // Observe the original owner before accessing the admitted source.
        let verifier = owner.issuer.verifier(&mut None).map_err(super::js_error)?;
        let (_, scope) = owner
            .issuer
            .caller()
            .ok_or_else(|| super::js_error(crate::Error::Unauthorized(
                "Machines workspace requires platform admission; recovery: use the server's original admitted authority".into(),
            )))?;
        let provider: js_sys::Uint8Array = field(&source, "provider")?;
        let provider = crate::wire::ProviderRef::decode(provider.to_vec().as_slice())
            .map_err(|_| super::js_error(crate::Error::Invalid(
                "invalid Machines provider reference protobuf; recovery: retain the original canonical ProviderRef".into(),
            )))?;
        let provider_ref = ProviderRef::try_from(provider).map_err(super::js_error)?;
        let observation: js_sys::Uint8Array = field(&source, "observation")?;
        let observation = wire::MachineState::decode(observation.to_vec().as_slice())
            .map_err(|_| super::js_error(crate::Error::Invalid(
                "invalid admitted Machine observation protobuf; recovery: retain the original settled MachineState".into(),
            )))?;
        let observation = MachineObservation::try_from(observation)
            .map_err(|error| super::js_error(crate::Error::Machines(error)))?;
        let volume = observation
            .contract
            .selected_volume_ref
            .clone()
            .ok_or_else(|| super::js_error(crate::Error::Unauthorized(
                "admitted Machine has no selected volume; recovery: retain the original selected-volume admission".into(),
            )))?;
        let volume = VolumeRef::try_from(volume).map_err(super::js_error)?;
        let open = field::<js_sys::Function>(&source, "open")?;
        let budget = field::<js_sys::Function>(&source, "budget")?;
        let authority = owner.issuer.clone();
        let context = owner.context.clone();
        let endpoint = grpc::javascript::Endpoint::new(open, move |timeout, expiry| {
            deadline::open(&source, &budget, &authority, &context, timeout, expiry)
        });
        let provider: Arc<dyn MachinesProvider> = Arc::new(grpc::Provider::over(endpoint));
        let workspace = MachinesHost::new(provider, provider_ref)
            .and_then(|host| host.workspace(observation, &volume, scope.clone(), verifier))
            .map_err(super::js_error)?;
        // Source getters can run host code: a same-turn stop must beat success.
        owner.issuer.verifier(&mut None).map_err(super::js_error)?;
        Ok(workspace.into())
    }
}

#[cfg(target_arch = "wasm32")]
mod deadline {
    use acyclic_auth::operation::{Context, Stopped};
    use acyclic_machines::{ProviderError, grpc::transport::Deadline};
    use acyclic_stream::ProviderTask;
    use js_sys::{Function, Promise};
    use std::{future::Future, time::Duration};
    use wasm_bindgen::prelude::*;
    use wasm_bindgen_futures::JsFuture;

    pub(super) struct Budget {
        original: Context,
        narrowed: Context,
        authority: crate::core::RuntimeAuthority,
        observation: JsValue,
        remaining: Function,
        _registration: crate::wasm::cancel::Cancel,
    }

    pub(super) fn open(
        source: &JsValue,
        budget: &Function,
        authority: &crate::core::RuntimeAuthority,
        original: &Context,
        timeout: Option<Duration>,
        expiry: Option<u64>,
    ) -> Result<Budget, ProviderError> {
        authority
            .verifier(&mut None)
            .map_err(|_| ProviderError::Cancelled)?;
        let registration =
            crate::wasm::cancel::Cancel::new().map_err(|_| ProviderError::Unavailable)?;
        let timeout = timeout
            .map(|value| u64::try_from(value.as_millis()))
            .transpose()
            .map_err(|_| ProviderError::Invalid("command timeout is not representable".into()))?;
        let timeout = timeout.map_or(JsValue::UNDEFINED, |value| {
            js_sys::BigInt::from(value).into()
        });
        let expiry = expiry.map_or(JsValue::UNDEFINED, |value| {
            js_sys::BigInt::from(value).into()
        });
        let observation = budget
            .call3(source, &timeout, &expiry, &registration.signal)
            .map_err(|_| ProviderError::Cancelled)?;
        let field = |name: &str| {
            js_sys::Reflect::get(&observation, &JsValue::from_str(name))
                .map_err(|_| ProviderError::Unavailable)
        };
        let remaining = field("remainingMilliseconds")?
            .dyn_into::<Function>()
            .map_err(|_| ProviderError::Unavailable)?;
        let stopped = field("stopped")?
            .dyn_into::<Promise>()
            .map_err(|_| ProviderError::Unavailable)?;
        let narrowed = Context::from_stopped(original.deadline_unix_millis(), async move {
            match JsFuture::from(stopped)
                .await
                .ok()
                .and_then(|value| value.as_string())
                .as_deref()
            {
                Some("deadline") => Stopped::Deadline,
                _ => Stopped::Cancelled,
            }
        });
        let budget = Budget {
            original: original.clone(),
            narrowed,
            authority: authority.clone(),
            observation,
            remaining,
            _registration: registration,
        };
        if budget.remaining().is_zero() || !budget.current() {
            return Err(ProviderError::Cancelled);
        }
        Ok(budget)
    }

    impl Budget {
        fn current(&self) -> bool {
            self.original.check().is_ok()
                && self.narrowed.check().is_ok()
                && self.authority.verifier(&mut None).is_ok()
        }
    }

    impl Deadline for Budget {
        fn remaining(&self) -> Duration {
            self.remaining
                .call0(&self.observation)
                .ok()
                .and_then(|value| crate::wasm::from_js::<u64>(value).ok())
                .map_or(Duration::ZERO, Duration::from_millis)
        }

        async fn run<F>(&self, future: F) -> Option<F::Output>
        where
            F: Future + ProviderTask,
            F::Output: ProviderTask,
        {
            if self.remaining().is_zero() || !self.current() {
                return None;
            }
            let result = self
                .original
                .run(self.narrowed.run(future))
                .await
                .ok()?
                .ok()?;
            (!self.remaining().is_zero() && self.current()).then_some(result)
        }
    }
}

#[wasm_bindgen(typescript_custom_section)]
const SOURCE: &str = r#"
/** Trusted projection of the ORIGINAL resource admission, never guest config.
 * The authority passed to open and this source must belong to the same caller,
 * Machine incarnation, physical selected volume and invocation lifetime. */
export interface WasmMachinesWorkspaceSource {
    /** Canonical harness.v2.ProviderRef protobuf. */
    readonly provider: Uint8Array;
    /** Original settled machines.v1.MachineState protobuf, including the exact
     * selected_volume_ref. No second volume or scope input is accepted. */
    readonly observation: Uint8Array;
    /** createMachinesRpcTransport(originalAuthenticatedConnectTransport).
     * Provider credentials remain behind this protected transport. */
    readonly open: ReturnType<typeof import("@acyclic-labs/machines/transport").createMachinesRpcTransport>;
    /** Register ONCE on the ORIGINAL platform clock before credentials/dispatch:
     * min(original caller deadline, command start + timeout, authenticated expiry).
     * Absence never installs a fallback timeout. No guest timer, new invocation,
     * renewed budget or uncertain replay is permitted. Abort releases only this
     * registration; it does not claim remote physical cancellation. */
    budget(timeoutMilliseconds: bigint | undefined, expiryUnixMilliseconds: bigint | undefined,
        signal: AbortSignal): {
        /** Synchronous original-clock observation; zero on stop/producer loss.
         * Throwing or invalid output is closed, including in a completing poll. */
        remainingMilliseconds(): bigint;
        /** Original earlier-deadline/caller-stop event. Producer loss must
         * settle or reject this promise, waking every pending observer. */
        readonly stopped: Promise<"deadline" | "cancelled">;
    };
}
"#;

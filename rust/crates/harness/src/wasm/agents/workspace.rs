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

    /// The tree's home target: this provisioned Machine, guarded by the
    /// tree's human approver. Hosted Machines run Linux.
    pub(super) fn target(&self) -> crate::agent::TargetRecord {
        crate::runtime::InProcessTargets::record(
            ("machine", crate::approvals::HUMAN_APPROVER),
            (
                crate::agent::TargetClass::Provisioned,
                crate::agent::DeviceKind::VirtualMachine,
            ),
            "linux",
            self.workspace.shell_spec(),
        )
    }
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
impl WasmMachinesWorkspace {
    pub(super) async fn admit(
        owner: &super::WasmAgentAuthority,
        providers: &super::WasmAgentProviders,
        source: &JsValue,
        request: u64,
    ) -> Result<Self, JsValue> {
        owner.issuer.verifier(&mut None).map_err(super::js_error)?;
        let admit =
            js_sys::Reflect::get(source, &"admit".into())?.dyn_into::<js_sys::Function>()?;
        let admitted = admit.call1(source, &js_sys::BigInt::from(request).into())?;
        let admitted =
            wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&admitted)).await?;
        Self::open(owner, providers, admitted).await
    }
    /// Binds a trusted `machines:/` grant and copies the selected workspace when
    /// the provider has no native attachment capability.
    /// The source is supplied by authenticated platform admission, never by
    /// request metadata. Its transport and budget belong to this same caller.
    pub async fn open(
        owner: &super::WasmAgentAuthority,
        providers: &super::WasmAgentProviders,
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
        let (provider, ()) = acyclic_stream::javascript_bytes::snapshot(&provider, |_| Ok(()))?;
        let provider = crate::wire::ProviderRef::decode(provider.as_slice())
            .map_err(|_| super::js_error(crate::Error::Invalid(
                "invalid Machines provider reference protobuf; recovery: retain the original canonical ProviderRef".into(),
            )))?;
        let provider_ref = ProviderRef::try_from(provider).map_err(super::js_error)?;
        let observation: js_sys::Uint8Array = field(&source, "observation")?;
        let (observation, ()) =
            acyclic_stream::javascript_bytes::snapshot(&observation, |_| Ok(()))?;
        let observation = wire::MachineState::decode(observation.as_slice())
            .map_err(|_| super::js_error(crate::Error::Invalid(
                "invalid admitted Machine observation protobuf; recovery: retain the original settled MachineState".into(),
            )))?;
        let observation = MachineObservation::try_from(observation)
            .map_err(|error| super::js_error(crate::Error::Machines(error)))?;
        let provider: Arc<dyn MachinesProvider> = match (provider_ref.namespace(), provider_ref.family()) {
            ("acyclic", "machines") => {
        let open = field::<js_sys::Function>(&source, "open")?;
        let authority = owner.issuer.clone();
        let context = authority.operation_context().ok_or_else(|| super::js_error(crate::Error::Unauthorized(
            "Machines workspace requires an original operation context; recovery: retain platform admission".into(),
        )))?.clone();
        let endpoint = grpc::javascript::Endpoint::new(open, move |timeout, expiry| {
            deadline::open(&authority, &context, timeout, expiry)
        });
        Arc::new(grpc::Provider::over(endpoint))

            }
            ("io.daytona", "machines") => {
                use acyclic_harness_core::http::{BrowserCredentialResolver, BrowserTransport};
                use acyclic_machines_daytona::{DaytonaConfig, DaytonaProvider};
                let reference = js_sys::Reflect::get(&source, &"secretReference".into())?;
                let reference: acyclic_auth::secrets::SecretRef = serde_wasm_bindgen::from_value(reference).map_err(|_| super::js_error(crate::Error::Invalid("Daytona requires a canonical SecretRef object; recovery: retain the protected sandbox binding".into())))?;
                let resolver = BrowserCredentialResolver(field(&source, "credentialResolver")?);
                let config = DaytonaConfig::new(reference, observation.contract.platform);
                Arc::new(DaytonaProvider::new(config, Arc::new(resolver), Arc::new(BrowserTransport::global().map_err(super::js_error)?)).map_err(|error| super::js_error(crate::Error::Machines(error)))?)
            }
            _ => return Err(super::js_error(crate::Error::Unsupported("Machines provider reference is not admitted; recovery: retain the original provider selection".into()))),
        };
        let volume = if provider.capability(acyclic_machines::ProviderCapability::AttachVolume)
            == acyclic_machines::Support::Unsupported
        {
            let selected: js_sys::Uint8Array = field(&source, "selectedVolume")?;
            let (bytes, ()) = acyclic_stream::javascript_bytes::snapshot(&selected, |_| Ok(()))?;
            let selected = crate::wire::VolumeRef::decode(bytes.as_slice()).map_err(|_| {
                super::js_error(crate::Error::Invalid(
                    "invalid admitted selected VolumeRef protobuf".into(),
                ))
            })?;
            VolumeRef::try_from(selected).map_err(super::js_error)?
        } else {
            let selected = observation
                .contract
                .selected_volume_ref
                .clone()
                .ok_or_else(|| {
                    super::js_error(crate::Error::Unauthorized(
                        "admitted Machine has no selected volume".into(),
                    ))
                })?;
            VolumeRef::try_from(selected).map_err(super::js_error)?
        };
        let workspace = MachinesHost::new(provider, provider_ref)
            .and_then(|host| host.workspace(observation, &volume, scope.clone(), verifier))
            .map_err(super::js_error)?;
        let workspace = providers
            .providers
            .filesystem
            .bind_machine_workspace(workspace)
            .await
            .map_err(super::js_error)?;
        // Source getters can run host code: a same-turn stop must beat success.
        owner.issuer.verifier(&mut None).map_err(super::js_error)?;
        Ok(workspace.into())
    }
}

#[cfg(target_arch = "wasm32")]
mod deadline {
    use acyclic_auth::operation::{BudgetError, Context};
    use acyclic_machines::{DeadlineStop, ProviderError, grpc::transport::Deadline};
    use acyclic_stream::ProviderTask;
    use std::{future::Future, time::Duration};

    pub(super) struct Budget {
        context: Context,
        authority: crate::core::RuntimeAuthority,
    }

    pub(super) fn open(
        authority: &crate::core::RuntimeAuthority,
        original: &Context,
        timeout: Option<Duration>,
        expiry: Option<u64>,
    ) -> Result<Budget, ProviderError> {
        authority
            .verifier(&mut None)
            .map_err(|_| ProviderError::Cancelled)?;
        let context = original.narrow(timeout, expiry).map_err(|error| match error {
            BudgetError::ClockUnavailable => ProviderError::Unsupported(
                "original operation clock unavailable; recovery: retain the admitted platform clock".into(),
            ),
            BudgetError::Stopped(_) => ProviderError::Cancelled,
        })?;
        let budget = Budget {
            context,
            authority: authority.clone(),
        };
        if budget
            .remaining()
            .is_some_and(|remaining| remaining.is_zero())
            || !budget.current()
        {
            return Err(ProviderError::Cancelled);
        }
        Ok(budget)
    }

    impl Budget {
        fn current(&self) -> bool {
            self.context.check().is_ok() && self.authority.verifier(&mut None).is_ok()
        }
    }

    impl Deadline for Budget {
        fn remaining(&self) -> Option<Duration> {
            // This context always retains its original authority deadline.
            // A failed clock/stop check exhausts that budget; None would
            // incorrectly advertise an unbounded transport lifetime.
            Some(self.context.remaining().unwrap_or(Duration::ZERO))
        }

        async fn run<F>(&self, future: F) -> Result<F::Output, DeadlineStop>
        where
            F: Future + ProviderTask,
            F::Output: ProviderTask,
        {
            let check = || -> Result<(), DeadlineStop> {
                self.context.remaining().map_err(|error| match error {
                    BudgetError::Stopped(acyclic_auth::operation::Stopped::Deadline) => {
                        DeadlineStop::Expired
                    }
                    BudgetError::ClockUnavailable
                    | BudgetError::Stopped(acyclic_auth::operation::Stopped::Cancelled) => {
                        DeadlineStop::Cancelled
                    }
                })?;
                self.authority
                    .verifier(&mut None)
                    .map_err(|_| DeadlineStop::Cancelled)?;
                Ok(())
            };
            check()?;
            let result = self.context.run(future).await.map_err(|stop| match stop {
                acyclic_auth::operation::Stopped::Deadline => DeadlineStop::Expired,
                acyclic_auth::operation::Stopped::Cancelled => DeadlineStop::Cancelled,
            })?;
            check()?;
            Ok(result)
        }
    }
}

#[wasm_bindgen(typescript_custom_section)]
const SOURCE: &str = r#"
/** Trusted projection of the ORIGINAL resource admission, never guest config.
 * The authority passed to open and this source must belong to the same caller,
 * Machine incarnation, selected volume and invocation lifetime. */
export interface WasmMachinesWorkspaceAdmission {
    /** Host resolves this opaque request id to its actor and original attempt.
     * Called within onRecord; the returned Machine is retained only by that attempt. */
    admit(requestId: bigint): WasmMachinesWorkspaceSource | Promise<WasmMachinesWorkspaceSource>;
}

export interface WasmMachinesWorkspaceSource {
    /** Canonical harness.v2.ProviderRef protobuf. */
    readonly provider: Uint8Array;
    /** Original settled machines.v1.MachineState protobuf, including its exact
     * physical selected_volume_ref when the provider supports attachment. */
    readonly observation: Uint8Array;
    /** Original platform-admitted logical VolumeRef protobuf for a provider
     * declaring attach_volume Unsupported. Its Machine has no physical selection. */
    readonly selectedVolume?: Uint8Array;
    /** createMachinesRpcTransport(originalAuthenticatedConnectTransport).
     * Provider credentials remain behind this protected transport. */
    /** Required for io.daytona/machines; a plain string is refused. */
    readonly secretReference?: { readonly scope: string; readonly name: string; readonly version: string };
    /** The same host-owned resolver used for model credentials. */
    readonly credentialResolver?: (origin: string, reference: { scope: string; name: string; version: string }, scheme: 'bearer') => { credentialScope: Uint8Array; fetch: typeof fetch };
    readonly open?: ReturnType<typeof import("@acyclic-labs/machines/transport").createMachinesRpcTransport>;
}
"#;

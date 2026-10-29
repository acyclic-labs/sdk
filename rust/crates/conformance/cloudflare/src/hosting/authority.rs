//! The qualification host's custody lives in this module's separate Wasm memory.
use acyclic_auth::{
    contract::*,
    operation::{Context, Stopped},
};
use acyclic_harness_core::{
    Capabilities, OperationId,
    conversation::{FileRef, VolumeRef},
    core::{AdmittedAuthority, AggregateKind, Authority, AuthorityRoot, RuntimeAuthority, Scope},
};
use prost::Message as _;
use serde::Deserialize;
use wasm_bindgen::prelude::*;

/// Real host issuer, never instantiated in the harness guest's memory.
#[wasm_bindgen]
pub struct HostFixtureAuthority {
    root: AuthorityRoot,
    admitted: AdmittedAuthority,
    binding: acyclic_auth::guest::GuestAdmission<AdmittedAuthority>,
    parent: acyclic_auth::verify::VerifiedCapability,
    customer: acyclic_auth::verify::VerifiedCapability,
    context: Context,
    operation: OperationId,
    caller: CallerContext,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Derive {
    operation: OperationId,
    deadline_unix_millis: u64,
    parent: Scope,
    id: String,
    derivation: Derivation,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Derivation {
    Narrow {
        capabilities: Capabilities,
        agent: Option<acyclic_harness_core::AgentId>,
    },
    PrivateFileRead {
        file: FileRef,
        reader: acyclic_harness_core::AgentId,
    },
    PrivateDirectoryRead {
        volume: VolumeRef,
        prefix: String,
        reader: acyclic_harness_core::AgentId,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    operation: OperationId,
    deadline_unix_millis: u64,
    kind: acyclic_harness_core::core::CheckpointKind,
    digest: [u8; 32],
}

fn error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

fn authenticate(
    scope: &Scope,
    root: &AuthorityRoot,
) -> Result<acyclic_auth::verify::VerifiedCapability, JsValue> {
    use base64::Engine as _;
    let value = serde_json::to_value(scope).map_err(error)?;
    let encoded = value["workload"]
        .as_str()
        .ok_or_else(|| error("missing workload"))?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(error)?;
    use acyclic_stream::UnixMillisClock as _;
    acyclic_auth::verify::verify(
        &bytes,
        &root.public_workload_trust().map_err(error)?,
        acyclic_stream::SystemUnixMillisClock.now_unix_millis() / 1000,
    )
    .map_err(error)
}

#[wasm_bindgen]
impl HostFixtureAuthority {
    /// The host owns every agent in this isolated fixture's admitted task. Its
    /// explicit grant ceiling is fixed before the guest or its requests exist.
    #[wasm_bindgen(constructor)]
    pub fn new(
        task: String,
        grants: JsValue,
        recipient_account: String,
        stopped: js_sys::Promise,
        deadline_unix_millis: u64,
    ) -> Result<Self, JsValue> {
        let root = AuthorityRoot::new(
            "browser-agents",
            [23; 32],
            Authority {
                kind: AggregateKind::Task,
                id: task,
            },
        );
        let grants: Vec<String> = serde_wasm_bindgen::from_value(grants).map_err(error)?;
        let scope = root
            .bootstrap_scope(
                "fixture-admission",
                &Capabilities::new(grants),
                None,
                &mut None,
            )
            .map_err(error)?;
        let parent = authenticate(&scope, &root)?;
        use acyclic_stream::UnixMillisClock as _;
        let customer = acyclic_auth::verify::verify(
            &super::credential(),
            &acyclic_auth::testing::Snapshot::default(),
            acyclic_stream::SystemUnixMillisClock.now_unix_millis() / 1000,
        )
        .map_err(error)?;
        let caller = customer.caller_authority(b"customers").map_err(error)?;
        let expires_at_unix_seconds = caller.expires_at_unix_seconds;
        if u128::from(deadline_unix_millis) > u128::from(expires_at_unix_seconds) * 1000 {
            return Err(error("fixture deadline exceeds customer expiry"));
        }
        let operation = OperationId::from_bytes([24; 16]);
        let caller = CallerContext {
            version: 1,
            caller,
            audience: CallerIdentity {
                account: recipient_account.into_bytes(),
                ns: b"agents".to_vec(),
                id: b"host".to_vec(),
            },
            delegation: Vec::new(),
            expires_at_unix_seconds,
            capability_handle: operation.to_string().into_bytes(),
        };
        let context = Context::from_stopped(deadline_unix_millis, async move {
            match wasm_bindgen_futures::JsFuture::from(stopped)
                .await
                .ok()
                .and_then(|value| value.as_string())
                .as_deref()
            {
                Some("deadline") => Stopped::Deadline,
                _ => Stopped::Cancelled,
            }
        });
        let admitted = root.admit(scope, context.clone()).map_err(error)?;
        let binding =
            acyclic_auth::guest::GuestAdmission::admit(admitted.clone()).map_err(error)?;
        Ok(Self {
            root,
            admitted,
            binding,
            parent,
            customer,
            context,
            operation,
            caller,
        })
    }

    /// Host-only projection admission. The JavaScript adapter returns the SAME
    /// invocation ports after this boundary; no guest descriptor supplies them.
    pub fn binding(&self, kind: &str, project: &js_sys::Function) -> Result<JsValue, JsValue> {
        let kind = acyclic_auth::guest::GuestAdmissionKind::parse(kind)
            .ok_or_else(|| error("invalid guest admission projection"))?;
        self.binding
            .project(kind, |_, _| Ok(project.call0(&JsValue::UNDEFINED)))
            .map_err(error)?
    }

    /// Only sealed scoped session evidence crosses into the guest.
    #[wasm_bindgen(js_name = bindingCredential)]
    pub fn binding_credential(&self) -> Result<Vec<u8>, JsValue> {
        self.admitted.check().map_err(error)?;
        self.customer
            .encode_sealed()
            .map(|mut value| std::mem::take(&mut *value))
            .map_err(error)
    }

    #[wasm_bindgen(js_name = trustClosed)]
    pub fn trust_closed(&self) -> bool {
        self.context.check().is_err()
            || acyclic_auth::trust::ChangeSource::check_open(&self.admitted).is_err()
    }

    pub fn admission(&self) -> Result<JsValue, JsValue> {
        #[derive(serde::Serialize)]
        struct Admission<'a> {
            issuer: &'a str,
            audience: &'a Authority,
            scope: Scope,
            trust: acyclic_auth::trust::Snapshot,
            checkpoint_key: [u8; 32],
            caller: Vec<u8>,
            operation: OperationId,
            deadline_unix_millis: u64,
        }
        super::to_value(&Admission {
            issuer: "browser-agents",
            audience: self.admitted.caller().0,
            scope: self.admitted.public_scope().map_err(error)?,
            trust: self.root.public_workload_trust().map_err(error)?,
            checkpoint_key: self
                .root
                .verifier(&mut None)
                .map_err(error)?
                .public_checkpoint_key(),
            caller: CallerContextProto::from(self.caller.clone()).encode_to_vec(),
            operation: self.operation,
            deadline_unix_millis: self.context.deadline_unix_millis(),
        })
        .map_err(error)
    }

    pub fn check(&self, scope: JsValue) -> JsValue {
        let current = serde_wasm_bindgen::from_value::<Scope>(scope)
            .map_err(error)
            .and_then(|scope| {
                acyclic_harness_core::core::AuthorityOperations::check(&self.admitted, &scope)
                    .map_err(error)
            });
        if current.is_ok() {
            JsValue::from_str("current")
        } else {
            JsValue::from_str("closed")
        }
    }

    /// The real host Auth publication event, registered before returning.
    #[wasm_bindgen(js_name = trustChanged)]
    pub fn trust_changed(&self, signal: JsValue) -> Result<js_sys::Promise, JsValue> {
        use js_sys::{Function, Reflect};
        use wasm_bindgen::JsCast as _;
        if Reflect::get(&signal, &"aborted".into())?.as_bool() != Some(false) {
            return Err(error("trust observation cancelled"));
        }
        let changed = self.root.trust_changes().changed();
        let context = self.context.clone();
        let (abort, registration) = futures::future::AbortHandle::new_pair();
        let listener = Closure::<dyn FnMut()>::new(move || abort.abort());
        let add: Function = Reflect::get(&signal, &"addEventListener".into())?.dyn_into()?;
        let remove: Function = Reflect::get(&signal, &"removeEventListener".into())?.dyn_into()?;
        add.call2(&signal, &"abort".into(), listener.as_ref())?;
        Ok(wasm_bindgen_futures::future_to_promise(async move {
            let result = futures::future::Abortable::new(context.run(changed), registration).await;
            let _ = remove.call2(&signal, &"abort".into(), listener.as_ref());
            drop(listener);
            result.map_err(error)?.map_err(error)?.map_err(error)?;
            Ok(JsValue::UNDEFINED)
        }))
    }

    /// Change current host trust without replacing the guest's older snapshot.
    pub fn revoke(&self, scope: JsValue) -> Result<(), JsValue> {
        let scope: Scope = serde_wasm_bindgen::from_value(scope).map_err(error)?;
        self.root.revoke_workload(&scope).map_err(error)
    }

    pub async fn derive(&self, request: JsValue) -> Result<JsValue, JsValue> {
        let request: Derive = serde_wasm_bindgen::from_value(request).map_err(error)?;
        self.admitted.check().map_err(error)?;
        if request.operation != self.operation
            || request.deadline_unix_millis != self.context.deadline_unix_millis()
            || !self
                .parent
                .preserved_by(&authenticate(&request.parent, &self.root)?)
                .map_err(error)?
        {
            return Err(error("fixture invocation or ancestry differs"));
        }
        let scope = match request.derivation {
            Derivation::Narrow {
                capabilities,
                agent,
            } => {
                if !capabilities.is_subset_of(request.parent.capabilities())
                    || request
                        .parent
                        .agent()
                        .is_some_and(|bound| Some(bound) != agent)
                {
                    return Err(error("fixture derivation widens its parent"));
                }
                RuntimeAuthority::from(self.root.clone())
                    .issue(Some(&request.parent), &request.id, &capabilities, agent)
                    .await
            }
            Derivation::PrivateFileRead { file, reader } => {
                self.root
                    .delegate_private_file_read(&request.parent, reader, &request.id, &file)
            }
            Derivation::PrivateDirectoryRead {
                volume,
                prefix,
                reader,
            } => self.root.delegate_private_directory_read(
                &request.parent,
                reader,
                &request.id,
                &volume,
                &prefix,
            ),
        }
        .map_err(error)?
        .seal(&self.root.verifier(&mut None).map_err(error)?)
        .map_err(error)?;
        self.admitted.check().map_err(error)?;
        super::to_value(&serde_json::json!({ "kind": "ok", "value": scope })).map_err(error)
    }

    pub async fn checkpoint(&self, request: JsValue) -> Result<JsValue, JsValue> {
        let request: Checkpoint = serde_wasm_bindgen::from_value(request).map_err(error)?;
        if request.operation != self.operation
            || request.deadline_unix_millis != self.context.deadline_unix_millis()
        {
            return Err(error("fixture invocation differs"));
        }
        let proof = self
            .admitted
            .checkpoint(
                &self.context,
                request.operation,
                request.kind,
                request.digest,
            )
            .await
            .map_err(error)?;
        super::to_value(&serde_json::json!({ "kind": "ok", "value": proof.as_slice() }))
            .map_err(error)
    }
}

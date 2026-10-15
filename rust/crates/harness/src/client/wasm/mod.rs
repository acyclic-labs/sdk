use super::{
    Begin, BranchId, Client, Correspondence, Dependency, DependencyRequirement, Domain, Error,
    Fact, Hypothesis, Limits, Observation, Provenance,
};
use js_sys::{Function, Object, Reflect};
use serde::Serialize;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

mod values;
use values::*;
mod domain;
use domain::JavaScriptDomain;
mod branches;
mod views;

/// Browser client using the kernel's transitions, limits and reconciliation.
#[wasm_bindgen]
pub struct WasmClientViews {
    client: Client<JavaScriptDomain>,
}

#[wasm_bindgen]
impl WasmClientViews {
    /// Pure construction; callbacks run only on explicit transitions.
    #[wasm_bindgen(constructor)]
    pub fn new(
        #[wasm_bindgen(unchecked_param_type = "string")] identity: &JsValue,
        #[wasm_bindgen(unchecked_param_type = "string")] namespace: &JsValue,
        #[wasm_bindgen(unchecked_param_type = "bigint")] sequence: JsValue,
        limits: &JsValue,
        validate: Function,
        observe: Function,
        corresponds: Function,
        restore: Option<Function>,
    ) -> Result<Self, JsValue> {
        let identity = text(identity, 160)
            .map_err(failure)?
            .parse()
            .map_err(|_| failure(Error::Conflict))?;
        let namespace = text(namespace, 160)
            .map_err(failure)?
            .parse()
            .map_err(|_| failure(Error::Conflict))?;
        let sequence = decode(sequence).map_err(failure)?;
        let limits = (|| {
            Ok::<_, Error>(Limits {
                records: decode(field(limits, "records")?)?,
                branches: decode(field(limits, "branches")?)?,
                edges: decode(field(limits, "edges")?)?,
                bytes: decode(field(limits, "bytes")?)?,
                work: decode(field(limits, "work")?)?,
                retention: decode(field(limits, "retention")?)?,
                visible: decode(field(limits, "visible")?)?,
            })
        })()
        .map_err(failure)?;
        let domain = JavaScriptDomain {
            identity,
            validate,
            observe,
            corresponds,
            maximum_bytes: limits.bytes,
            restore,
        };
        Ok(Self {
            client: Client::new(domain, namespace, sequence, limits).map_err(failure)?,
        })
    }
}

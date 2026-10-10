//! Reference-preserving JavaScript binding of the existing client kernel.
//!
//! The embedding host supplies the same trusted, pure, bounded Domain obligations
//! as native Rust. Callbacks are not evidence authentication or an authority grant.

use super::{
    Begin, BranchId, Client, Correspondence, Dependency, DependencyRequirement, Domain, Error,
    Fact, Limits, Observation, Provenance,
};
use js_sys::{Function, Object, Reflect};
use serde::Serialize;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

fn field(value: &JsValue, name: &str) -> Result<JsValue, Error> {
    Reflect::get(value, &JsValue::from_str(name)).map_err(|_| Error::Conflict)
}

fn decode<T: serde::de::DeserializeOwned>(value: JsValue) -> Result<T, Error> {
    if value.is_string() {
        text(&value, 256)?;
    }
    serde_wasm_bindgen::from_value(value).map_err(|_| Error::Conflict)
}

fn encode<T: Serialize>(value: &T) -> Result<JsValue, Error> {
    value
        .serialize(&serde_wasm_bindgen::Serializer::new())
        .map_err(|_| Error::Conflict)
}

fn set(object: &JsValue, name: &str, value: &JsValue) -> Result<(), Error> {
    Reflect::set(object, &JsValue::from_str(name), value)
        .map(|_| ())
        .map_err(|_| Error::Conflict)
}

fn failure(error: Error) -> JsValue {
    js_sys::Error::new(&format!("Harness client: {error:?}")).into()
}

fn id(value: &str) -> Result<BranchId, Error> {
    let (namespace, sequence) = value.split_once(':').ok_or(Error::Conflict)?;
    let parsed = BranchId {
        namespace: namespace.parse().map_err(|_| Error::Conflict)?,
        sequence: sequence.parse().map_err(|_| Error::Conflict)?,
    };
    // Cache routing uses the same textual identity returned by narrow changes.
    if id_text(parsed) != value {
        return Err(Error::Conflict);
    }
    Ok(parsed)
}

fn id_text(value: BranchId) -> String {
    format!("{}:{}", value.namespace, value.sequence)
}

fn fact_object(fact: &Fact<String, String, JsValue>) -> Result<JsValue, Error> {
    let object: JsValue = Object::new().into();
    set(&object, "key", &JsValue::from_str(&fact.key))?;
    set(&object, "basis", &JsValue::from_str(&fact.basis))?;
    set(&object, "value", &fact.value)?;
    set(&object, "bytes", &encode(&fact.bytes)?)?;
    Ok(object)
}

struct JavaScriptDomain {
    identity: u128,
    validate: Function,
    observe: Function,
    corresponds: Function,
    maximum_bytes: usize,
}

fn text(value: &JsValue, maximum_bytes: usize) -> Result<String, Error> {
    let string = value.dyn_ref::<js_sys::JsString>().ok_or(Error::Conflict)?;
    if usize::try_from(string.length()).map_err(|_| Error::Budget)? > maximum_bytes / 4 {
        return Err(Error::Budget);
    }
    value.as_string().ok_or(Error::Conflict)
}

fn array(value: &JsValue, maximum: usize) -> Result<&js_sys::Array, Error> {
    let array = value.dyn_ref::<js_sys::Array>().ok_or(Error::Conflict)?;
    if usize::try_from(array.length()).map_err(|_| Error::Budget)? > maximum {
        return Err(Error::Budget);
    }
    Ok(array)
}

impl Domain for JavaScriptDomain {
    type Key = String;
    type Basis = String;
    type Operation = String;
    type Assumption = JsValue;
    type Value = JsValue;
    type Evidence = JsValue;

    fn identity(&self) -> u128 {
        self.identity
    }

    fn validate(
        &self,
        fact: &Fact<String, String, JsValue>,
        value: &JsValue,
        assumption: &JsValue,
        work: usize,
    ) -> Result<(usize, usize), Error> {
        let args = js_sys::Array::new();
        args.push(&fact_object(fact)?);
        args.push(value);
        args.push(assumption);
        args.push(&encode(&work)?);
        let result = self
            .validate
            .apply(&JsValue::UNDEFINED, &args)
            .map_err(|_| Error::Unsupported)?;
        Ok((
            decode(field(&result, "bytes")?)?,
            decode(field(&result, "work")?)?,
        ))
    }

    fn observe(
        &self,
        evidence: &JsValue,
        current: Option<&Fact<String, String, JsValue>>,
        work: usize,
    ) -> Result<Observation<String, String, String, JsValue>, Error> {
        let current = current
            .map(fact_object)
            .transpose()?
            .unwrap_or(JsValue::NULL);
        let result = self
            .observe
            .call3(&JsValue::UNDEFINED, evidence, &current, &encode(&work)?)
            .map_err(|_| Error::Unsupported)?;
        let operation = field(&result, "operation")?;
        let operation = if operation.is_null() || operation.is_undefined() {
            None
        } else {
            let operation = array(&operation, 2)?;
            if operation.length() != 2 {
                return Err(Error::Conflict);
            }
            Some((
                text(&operation.get(0), self.maximum_bytes)?,
                decode(operation.get(1))?,
            ))
        };
        let value = field(&result, "fact")?;
        let fact = if value.is_null() || value.is_undefined() {
            None
        } else {
            Some(Fact {
                key: text(&field(&value, "key")?, self.maximum_bytes)?,
                basis: Arc::new(text(&field(&value, "basis")?, self.maximum_bytes)?),
                value: Arc::new(field(&value, "value")?),
                bytes: decode(field(&value, "bytes")?)?,
            })
        };
        Ok(Observation {
            fact,
            operation,
            work: decode(field(&result, "work")?)?,
        })
    }

    fn corresponds(
        &self,
        predicted: &JsValue,
        canonical: &JsValue,
        work: usize,
    ) -> Result<(Correspondence, usize), Error> {
        let result = self
            .corresponds
            .call3(&JsValue::UNDEFINED, predicted, canonical, &encode(&work)?)
            .map_err(|_| Error::Unsupported)?;
        let matches: bool = decode(field(&result, "matches")?)?;
        Ok((
            if matches {
                Correspondence::Match
            } else {
                Correspondence::Different
            },
            decode(field(&result, "work")?)?,
        ))
    }
}

/// Browser facade. All transitions, bounds and reconciliation use Client<D>.
#[wasm_bindgen]
pub struct WasmClientViews {
    client: Client<JavaScriptDomain>,
    // Immutable configuration is copied solely to bound boundary allocations
    // before invoking the kernel; no canonical or hypothesis state is duplicated.
    limits: Limits,
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
        let limits = Limits {
            records: decode(field(limits, "records").map_err(failure)?).map_err(failure)?,
            branches: decode(field(limits, "branches").map_err(failure)?).map_err(failure)?,
            edges: decode(field(limits, "edges").map_err(failure)?).map_err(failure)?,
            bytes: decode(field(limits, "bytes").map_err(failure)?).map_err(failure)?,
            work: decode(field(limits, "work").map_err(failure)?).map_err(failure)?,
            retention: decode(field(limits, "retention").map_err(failure)?).map_err(failure)?,
            visible: decode(field(limits, "visible").map_err(failure)?).map_err(failure)?,
        };
        let domain = JavaScriptDomain {
            identity,
            validate,
            observe,
            corresponds,
            maximum_bytes: limits.bytes,
        };
        Ok(Self {
            client: Client::new(domain, namespace, sequence, limits).map_err(failure)?,
            limits,
        })
    }

    /// Begin one explicit hypothesis; values stay as JavaScript references.
    pub fn begin(
        &mut self,
        metadata: &JsValue,
        predicted: JsValue,
        assumption: JsValue,
    ) -> Result<String, JsValue> {
        let edges = field(metadata, "dependencies").map_err(failure)?;
        let edges = array(&edges, self.limits.edges.min(self.limits.work)).map_err(failure)?;
        let dependencies = edges
            .iter()
            .map(|edge| {
                Ok(Dependency {
                    branch: id(&text(&field(&edge, "branch")?, 256)?)?,
                    requirement: decode::<DependencyRequirement>(field(&edge, "requirement")?)?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()
            .map_err(failure)?;
        let operation = field(metadata, "operation").map_err(failure)?;
        let operation = if operation.is_null() || operation.is_undefined() {
            None
        } else {
            Some(text(&operation, self.limits.bytes).map_err(failure)?)
        };
        self.client
            .begin(Begin {
                key: text(&field(metadata, "key").map_err(failure)?, self.limits.bytes)
                    .map_err(failure)?,
                basis: Arc::new(
                    text(
                        &field(metadata, "basis").map_err(failure)?,
                        self.limits.bytes,
                    )
                    .map_err(failure)?,
                ),
                operation,
                assumption,
                predicted: Arc::new(predicted),
                dependencies,
                expires: decode(field(metadata, "expires").map_err(failure)?).map_err(failure)?,
            })
            .map(id_text)
            .map_err(failure)
    }

    /// Select only one demanded record and explicitly supplied overlays.
    pub fn view(
        &self,
        #[wasm_bindgen(unchecked_param_type = "string")] key: &JsValue,
        overlays: &JsValue,
    ) -> Result<JsValue, JsValue> {
        let overlays = array(overlays, self.limits.visible).map_err(failure)?;
        let overlays = overlays
            .iter()
            .map(|value| id(&text(&value, 256)?))
            .collect::<Result<Vec<_>, _>>()
            .map_err(failure)?;
        let view = self
            .client
            .view(&text(key, self.limits.bytes).map_err(failure)?, &overlays)
            .map_err(failure)?;
        let object: JsValue = Object::new().into();
        set(&object, "value", &view.value).map_err(failure)?;
        let (basis, branch, adapter) = match view.provenance {
            Provenance::Authoritative(basis) => (basis, JsValue::NULL, JsValue::NULL),
            Provenance::Hypothesis { id, basis, adapter } => (
                basis,
                JsValue::from_str(&id_text(id)),
                JsValue::from_str(&adapter.to_string()),
            ),
        };
        set(&object, "basis", &JsValue::from_str(&basis)).map_err(failure)?;
        set(&object, "branch", &branch).map_err(failure)?;
        set(&object, "adapter", &adapter).map_err(failure)?;
        Ok(object)
    }

    /// Narrow changed hypothesis IDs and work; no full-state export.
    pub fn observe(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "string")] key: &JsValue,
        evidence: &JsValue,
    ) -> Result<JsValue, JsValue> {
        let key = text(key, self.limits.bytes).map_err(failure)?;
        let changes = self.client.observe(key, evidence).map_err(failure)?;
        encode(&(
            changes.authoritative,
            changes
                .hypotheses
                .into_iter()
                .map(id_text)
                .collect::<Vec<_>>(),
            changes.work,
        ))
        .map_err(failure)
    }

    /// Inspect outcome metadata without serializing hypothesis bodies.
    pub fn status(
        &self,
        #[wasm_bindgen(unchecked_param_type = "string")] branch: &JsValue,
    ) -> Result<JsValue, JsValue> {
        let branch = text(branch, 256).map_err(failure)?;
        let Some(branch) = self.client.hypothesis(id(&branch).map_err(failure)?) else {
            return Ok(JsValue::NULL);
        };
        encode(&(branch.prediction, branch.outcome)).map_err(failure)
    }

    /// Remove one local prediction; never cancels a durable effect.
    pub fn discard(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "string")] branch: &JsValue,
    ) -> Result<JsValue, JsValue> {
        let changed = self
            .client
            .discard(id(&text(branch, 256).map_err(failure)?).map_err(failure)?)
            .map_err(failure)?;
        encode(&changed.into_iter().map(id_text).collect::<Vec<_>>()).map_err(failure)
    }

    /// Explicit logical retention tick, never a hidden timer.
    pub fn advance(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "bigint")] now: JsValue,
    ) -> Result<JsValue, JsValue> {
        let changed = self
            .client
            .advance(decode(now).map_err(failure)?)
            .map_err(failure)?;
        encode(&changed.into_iter().map(id_text).collect::<Vec<_>>()).map_err(failure)
    }

    /// Release an unreferenced demanded record.
    pub fn release(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "string")] key: &JsValue,
    ) -> Result<(), JsValue> {
        self.client
            .release(&text(key, self.limits.bytes).map_err(failure)?)
            .map_err(failure)
    }

    /// Resident record/branch/edge counts and conservatively accounted bytes.
    pub fn residency(&self) -> Result<JsValue, JsValue> {
        encode(&self.client.residency()).map_err(failure)
    }
}

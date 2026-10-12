use super::*;

pub(super) fn field(value: &JsValue, name: &str) -> Result<JsValue, Error> {
    Reflect::get(value, &JsValue::from_str(name)).map_err(|_| Error::Conflict)
}

pub(super) fn decode<T: serde::de::DeserializeOwned>(value: JsValue) -> Result<T, Error> {
    if value.is_string() {
        text(&value, 256)?;
    }
    serde_wasm_bindgen::from_value(value).map_err(|_| Error::Conflict)
}

pub(super) fn encode<T: Serialize>(value: &T) -> Result<JsValue, Error> {
    value
        .serialize(&serde_wasm_bindgen::Serializer::new())
        .map_err(|_| Error::Conflict)
}

pub(super) fn failure(error: Error) -> JsValue {
    js_sys::Error::new(&format!("Harness client: {error:?}")).into()
}

pub(super) fn id(value: &str) -> Result<BranchId, Error> {
    let (namespace, sequence) = value.split_once(':').ok_or(Error::Conflict)?;
    let parsed = BranchId {
        namespace: namespace.parse().map_err(|_| Error::Conflict)?,
        sequence: sequence.parse().map_err(|_| Error::Conflict)?,
    };
    if id_text(parsed) != value {
        return Err(Error::Conflict);
    }
    Ok(parsed)
}

pub(super) fn id_text(value: BranchId) -> String {
    format!("{}:{}", value.namespace, value.sequence)
}

pub(super) fn text(value: &JsValue, maximum_bytes: usize) -> Result<String, Error> {
    let string = value.dyn_ref::<js_sys::JsString>().ok_or(Error::Conflict)?;
    if usize::try_from(string.length()).map_err(|_| Error::Budget)? > maximum_bytes / 4 {
        return Err(Error::Budget);
    }
    value.as_string().ok_or(Error::Conflict)
}

pub(super) fn array(value: &JsValue, maximum: usize) -> Result<&js_sys::Array, Error> {
    let array = value.dyn_ref::<js_sys::Array>().ok_or(Error::Conflict)?;
    if usize::try_from(array.length()).map_err(|_| Error::Budget)? > maximum {
        return Err(Error::Budget);
    }
    Ok(array)
}

pub(super) fn object(fields: &[(&str, JsValue)]) -> Result<JsValue, Error> {
    let object: JsValue = Object::new().into();
    for (key, value) in fields {
        Reflect::set(&object, &JsValue::from_str(key), value).map_err(|_| Error::Conflict)?;
    }
    Ok(object)
}

pub(super) fn fact_object(fact: &Fact<String, String, JsValue>) -> Result<JsValue, Error> {
    object(&[
        ("key", JsValue::from_str(&fact.key)),
        ("basis", JsValue::from_str(&fact.basis)),
        ("value", fact.value.as_ref().clone()),
        ("bytes", encode(&fact.bytes)?),
    ])
}

pub(super) fn hypothesis_object(
    branch: &super::super::ClientHypothesis<JavaScriptDomain>,
) -> Result<JsValue, Error> {
    let edges = js_sys::Array::new();
    for edge in &branch.dependencies {
        edges.push(&object(&[
            ("branch", JsValue::from_str(&id_text(edge.branch))),
            ("requirement", encode(&edge.requirement)?),
        ])?);
    }
    object(&[
        ("branch", JsValue::from_str(&id_text(branch.id))),
        ("adapter", JsValue::from_str(&branch.adapter.to_string())),
        ("key", JsValue::from_str(&branch.key)),
        ("basis", JsValue::from_str(&branch.basis)),
        (
            "operation",
            branch
                .operation
                .as_ref()
                .map_or(JsValue::NULL, |value| JsValue::from_str(value)),
        ),
        ("dependencies", edges.into()),
        ("expires", encode(&branch.expires)?),
        ("prediction", encode(&branch.prediction)?),
        ("outcome", encode(&branch.outcome)?),
        ("predicted", branch.predicted.as_ref().clone()),
        ("assumption", branch.assumption.clone()),
    ])
}

pub(super) fn dependencies(metadata: &JsValue, limits: Limits) -> Result<Vec<Dependency>, Error> {
    let edges = field(metadata, "dependencies")?;
    let edges = array(&edges, limits.edges.min(limits.work))?;
    edges
        .iter()
        .map(|edge| {
            Ok(Dependency {
                branch: id(&text(&field(&edge, "branch")?, 256)?)?,
                requirement: decode::<DependencyRequirement>(field(&edge, "requirement")?)?,
            })
        })
        .collect()
}

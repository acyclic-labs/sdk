use super::serialization::Scope;
use super::*;

fn string(value: &str, scope: &mut Scope<'_>) -> Result<String> {
    let bytes = value.len() as u64;
    scope.event(JsonCodecCharge::CopyAdmission { bytes })?;
    if bytes != 0 {
        scope.event(JsonCodecCharge::AllocationOperation { bytes })?;
    }
    let mut copied = String::new();
    copied
        .try_reserve_exact(value.len())
        .map_err(Error::Allocation)?;
    copied.push_str(value);
    scope.event(JsonCodecCharge::Copy { bytes })?;
    Ok(copied)
}

pub(super) fn value(value: &Value, scope: &mut Scope<'_>) -> Result<Value> {
    Ok(match value {
        Value::String(text) => Value::String(string(text, scope)?),
        Value::Array(values) => {
            let mut copied = Vec::new();
            if !values.is_empty() {
                scope.event(JsonCodecCharge::AllocationOperation {
                    bytes: checked(
                        (values.len() as u64).checked_mul(std::mem::size_of::<Value>() as u64),
                        "JSON capacity overflow",
                    )?,
                })?;
                copied
                    .try_reserve_exact(values.len())
                    .map_err(Error::Allocation)?;
            }
            for child in values {
                copied.push(self::value(child, scope)?);
            }
            Value::Array(copied)
        }
        Value::Object(values) => {
            let mut copied = serde_json::Map::new();
            for (key, child) in values {
                let key = string(key, scope)?;
                let child = self::value(child, scope)?;
                scope.map_nodes(copied.len())?;
                copied.insert(key, child);
            }
            Value::Object(copied)
        }
        value => value.clone(),
    })
}

use super::*;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn control_reply(
    mut state: Value,
    work: &acyclic_fs::NativeMaterializationWork,
) -> acyclic_harness::Result<(Option<PiControlReply>, acyclic_fs::NativeAllocation)> {
    let (metadata, custody) = native_metadata::<PiTaskState>(
        &MetadataValue(&state, MetadataRole::State),
        acyclic_harness::JsonValueSchema::Root,
        STATE_LABEL_BYTES,
        work,
    )?;
    let PiTaskState::Hosting {
        reply: Some(mut reply),
        ..
    } = metadata
    else {
        return Ok((None, custody));
    };
    reply.value = state
        .get_mut("reply")
        .and_then(Value::as_object_mut)
        .and_then(|reply| reply.remove("value"))
        .ok_or_else(|| transition_error("Pi control reply has no value"))?;
    Ok((Some(reply), custody))
}

pub(super) enum MetadataRole {
    State,
    Results,
    Replies,
    Reply,
}
pub(super) struct MetadataValue<'a>(pub(super) &'a Value, pub(super) MetadataRole);

impl Serialize for MetadataValue<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{SerializeMap, SerializeSeq};
        match (&self.1, self.0) {
            (MetadataRole::Replies, Value::Array(values)) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    sequence.serialize_element(&MetadataValue(value, MetadataRole::Reply))?;
                }
                sequence.end()
            }
            (role, Value::Object(fields)) => {
                let mut map = serializer.serialize_map(Some(fields.len()))?;
                for (name, value) in fields {
                    match (role, name.as_str()) {
                        (MetadataRole::State, "reply")
                            if fields.get("state").and_then(Value::as_str) == Some("hosting") =>
                        {
                            map.serialize_entry(name, &MetadataValue(value, MetadataRole::Reply))?;
                        }
                        (MetadataRole::Results, "commands") => {
                            map.serialize_entry(
                                name,
                                &MetadataValue(value, MetadataRole::Replies),
                            )?;
                        }
                        (MetadataRole::Reply, "value") => map.serialize_entry(name, &())?,
                        _ => map.serialize_entry(name, value)?,
                    }
                }
                map.end()
            }
            _ => self.0.serialize(serializer),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) const NATIVE_METADATA_MAXIMUM: u64 = crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8;

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn native_metadata<T: serde::de::DeserializeOwned + acyclic_harness::HeapLayout>(
    value: &impl Serialize,
    schema: &'static acyclic_harness::JsonValueSchema,
    label_bytes: u64,
    work: &acyclic_fs::NativeMaterializationWork,
) -> acyclic_harness::Result<(T, acyclic_fs::NativeAllocation)> {
    let encoded = crate::pi_root_provider::guarded_pi_bytes(value, NATIVE_METADATA_MAXIMUM, work)?;
    crate::pi_root_provider::guarded_pi_decode::<T>(
        &encoded.0,
        NATIVE_METADATA_MAXIMUM,
        acyclic_harness::wire::ErrorCode::Invalid,
        schema,
        label_bytes,
        work,
    )
    .map_err(|error| match error {
        acyclic_harness::Error::Invalid(_) => transition_error("Pi native metadata is malformed"),
        error => error,
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn require_single_command(input: &Value) -> acyclic_harness::Result<()> {
    if input
        .get("commands")
        .and_then(Value::as_array)
        .is_some_and(|commands| commands.len() == 1)
    {
        Ok(())
    } else {
        Err(transition_error(
            "Pi result differs from its retained command",
        ))
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn take_result_body(input: &mut Value) -> acyclic_harness::Result<Value> {
    input
        .get_mut("commands")
        .and_then(Value::as_array_mut)
        .and_then(|commands| commands.first_mut())
        .and_then(Value::as_object_mut)
        .and_then(|reply| reply.remove("value"))
        .ok_or_else(|| transition_error("Pi control reply has no value"))
}

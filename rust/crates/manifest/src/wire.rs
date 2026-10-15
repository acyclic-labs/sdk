//! JSON adapters over the existing protobuf descriptors, without a second schema.
use prost::{Message, Name};
use prost_reflect::{DescriptorPool, DynamicMessage, Kind, MessageDescriptor};
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{borrow::Cow, cell::RefCell, collections::HashMap, fmt::Debug, ops::Deref};

// Immutable owner metadata, initialized synchronously per thread. No shared cache
// or lock crosses a validation call or await; untrusted input cannot select a pool.
thread_local! {
    static POOLS: RefCell<HashMap<&'static str, DescriptorPool>> = RefCell::new(HashMap::new());
}

/// Shared semantic contract with its owning descriptor and wire conversion.
pub trait ContractType: Clone + Debug + PartialEq {
    /// Compiler-generated wire shadow.
    type Proto: Message + Name + Default + From<Self>;
    /// Owning contract's descriptor set.
    const DESCRIPTOR: &'static [u8];
    /// Apply the owner's admission checks after JSON decoding.
    fn from_proto(proto: Self::Proto) -> Result<Self, String>;
}

/// Retains the canonical Rust type while using its descriptor's Protobuf JSON.
#[derive(Clone, Debug, PartialEq)]
pub struct Contract<T: ContractType>(pub T);
impl<T: ContractType> Deref for Contract<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}
impl<T: ContractType> From<T> for Contract<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

/// Resolve the owner's message, without maintaining separate field metadata.
pub fn descriptor<T: ContractType>() -> Result<MessageDescriptor, String> {
    POOLS.with_borrow_mut(|pools| {
        let pool = match pools.entry(T::Proto::PACKAGE) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => entry
                .insert(DescriptorPool::decode(T::DESCRIPTOR).map_err(|error| error.to_string())?),
        };
        pool.get_message_by_name(&T::Proto::full_name())
            .ok_or_else(|| "missing contract descriptor".into())
    })
}
impl<T: ContractType> Serialize for Contract<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let message = DynamicMessage::decode(
            descriptor::<T>().map_err(serde::ser::Error::custom)?,
            T::Proto::from(self.0.clone()).encode_to_vec().as_slice(),
        )
        .map_err(serde::ser::Error::custom)?;
        message.serialize(serializer)
    }
}
impl<'de, T: ContractType> Deserialize<'de> for Contract<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let message = DynamicMessage::deserialize(
            descriptor::<T>().map_err(serde::de::Error::custom)?,
            deserializer,
        )?;
        let proto = message
            .transcode_to::<T::Proto>()
            .map_err(serde::de::Error::custom)?;
        T::from_proto(proto)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}
impl<T: ContractType> JsonSchema for Contract<T> {
    fn schema_name() -> Cow<'static, str> {
        T::Proto::NAME.into()
    }
    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        match descriptor::<T>() {
            Ok(descriptor) => Schema::try_from(schema(&descriptor))
                .unwrap_or_else(|_| schemars::json_schema!(false)),
            Err(_) => schemars::json_schema!(false),
        }
    }
}

fn field_schema(kind: Kind) -> serde_json::Value {
    use serde_json::json;
    match kind {
        Kind::Message(message) => schema(&message),
        Kind::String => json!({"type":"string"}),
        Kind::Bool => json!({"type":"boolean"}),
        Kind::Bytes => json!({"type":"string", "contentEncoding":"base64"}),
        Kind::Uint64 | Kind::Int64 | Kind::Sint64 | Kind::Fixed64 | Kind::Sfixed64 => {
            json!({"anyOf":[{"type":"string","pattern":"^-?[0-9]+$"},{"type":"integer"}]})
        }
        Kind::Enum(enumeration) => {
            json!({"enum": enumeration.values().map(|v| v.name().to_string()).collect::<Vec<_>>()})
        }
        _ => json!({"type":"integer"}),
    }
}
fn schema(message: &MessageDescriptor) -> serde_json::Value {
    use serde_json::json;
    let properties: serde_json::Map<String, serde_json::Value> = message
        .fields()
        .map(|field| {
            let schema = field_schema(field.kind());
            (
                field.json_name().to_string(),
                if field.is_list() {
                    json!({"type":"array", "items":schema})
                } else {
                    schema
                },
            )
        })
        .collect();
    json!({"type":"object", "properties":properties, "additionalProperties":false})
}

/// Generate the imported contract's Protobuf JSON representation.
pub fn typescript<T: ContractType>() -> Result<String, String> {
    fn emit(
        message: MessageDescriptor,
        seen: &mut std::collections::HashSet<String>,
        out: &mut String,
    ) {
        if !seen.insert(message.full_name().to_string()) {
            return;
        }
        for field in message.fields() {
            if let Kind::Message(child) = field.kind() {
                emit(child, seen, out);
            }
        }
        out.push_str(&format!("export type {} = {{\n", message.name()));
        for field in message.fields() {
            let ty = match field.kind() {
                Kind::Message(child) => child.name().to_string(),
                Kind::String | Kind::Bytes => "string".into(),
                Kind::Bool => "boolean".into(),
                Kind::Uint64 | Kind::Int64 | Kind::Sint64 | Kind::Fixed64 | Kind::Sfixed64 => {
                    "string | number".into()
                }
                _ => "number".into(),
            };
            out.push_str(&format!(
                "  {}?: {}{};\n",
                field.json_name(),
                if field.is_list() {
                    format!("({ty})[]")
                } else {
                    ty
                },
                ""
            ));
        }
        out.push_str("};\n");
    }
    let mut output = String::new();
    emit(
        descriptor::<T>()?,
        &mut std::collections::HashSet::new(),
        &mut output,
    );
    Ok(output)
}

macro_rules! contract {
    ($ty:ty, $proto:ty, $descriptor:expr) => {
        impl ContractType for $ty {
            type Proto = $proto;
            const DESCRIPTOR: &'static [u8] = $descriptor;
            fn from_proto(proto: Self::Proto) -> Result<Self, String> {
                Self::try_from(proto).map_err(|error| error.to_string())
            }
        }
    };
}
contract!(
    acyclic_actors::domain::Binding,
    acyclic_actors::contract::BindingProto,
    acyclic_actors::FILE_DESCRIPTOR_SET
);
contract!(
    acyclic_actors::domain::ActorLimits,
    acyclic_actors::contract::ActorLimitsProto,
    acyclic_actors::FILE_DESCRIPTOR_SET
);
contract!(
    acyclic_actors::domain::SubscriptionSpec,
    acyclic_actors::contract::SubscriptionSpecProto,
    acyclic_actors::FILE_DESCRIPTOR_SET
);
contract!(
    acyclic_auth::contract::ResourcePattern,
    acyclic_auth::contract::ResourcePatternProto,
    acyclic_auth::FILE_DESCRIPTOR_SET
);
contract!(
    acyclic_auth::contract::ResourceGrant,
    acyclic_auth::contract::ResourceGrantProto,
    acyclic_auth::FILE_DESCRIPTOR_SET
);
contract!(
    acyclic_auth::contract::Limits,
    acyclic_auth::contract::LimitsProto,
    acyclic_auth::FILE_DESCRIPTOR_SET
);

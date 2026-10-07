use std::num::NonZeroU64;

use protify::{define_proto_file, proto_message, proto_oneof, proto_package};
use ts_rs::TS;

#[cfg(test)]
use std::path::Path;
#[cfg(test)]
use prost::Message;

proto_package!(ACTORS, name = "acyclic.actors.v1", files = [ACTORS_FILE]);
define_proto_file!(
    ACTORS_FILE,
    name = "actors.proto",
    package = ACTORS,
    messages = [NestedActorProto, RegisterActorProto, SubscriptionStartProto]
);

#[derive(Debug, Clone, PartialEq, Eq, TS)]
#[ts(type = "string")]
pub struct ActorId(pub String);

impl From<ActorId> for String {
    fn from(value: ActorId) -> Self { value.0 }
}

impl From<String> for ActorId {
    fn from(value: String) -> Self { Self(value) }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, TS)]
#[ts(type = "bigint")]
pub struct PositiveU64(pub NonZeroU64);

impl From<PositiveU64> for u64 {
    fn from(value: PositiveU64) -> Self { value.0.get() }
}

impl From<u64> for PositiveU64 {
    fn from(value: u64) -> Self { Self(NonZeroU64::new(value).expect("positive")) }
}

#[derive(Debug, Clone, PartialEq, Eq, TS)]
#[ts(type = "Uint8Array")]
pub struct CodeSha256(pub [u8; 32]);

impl From<CodeSha256> for protify::Bytes {
    fn from(value: CodeSha256) -> Self { value.0.to_vec().into() }
}

impl From<protify::Bytes> for CodeSha256 {
    fn from(value: protify::Bytes) -> Self {
        Self(value.as_ref().try_into().expect("CodeSha256 must contain 32 bytes"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressError {
    EmptyActorId,
    NonPositiveU64,
    CodeSha256Length(usize),
    MissingNestedActor,
}

impl std::fmt::Display for IngressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyActorId => f.write_str("actor id must not be empty"),
            Self::NonPositiveU64 => f.write_str("value must be positive"),
            Self::CodeSha256Length(length) => write!(f, "CodeSha256 must contain 32 bytes, got {length}"),
            Self::MissingNestedActor => f.write_str("nested actor is required"),
        }
    }
}

impl std::error::Error for IngressError {}

fn parse_actor_id(value: String) -> Result<ActorId, IngressError> {
    (!value.is_empty()).then_some(ActorId(value)).ok_or(IngressError::EmptyActorId)
}

fn parse_positive_u64(value: u64) -> Result<PositiveU64, IngressError> {
    NonZeroU64::new(value).map(PositiveU64).ok_or(IngressError::NonPositiveU64)
}

fn parse_code_sha256(value: protify::Bytes) -> Result<CodeSha256, IngressError> {
    let length = value.len();
    value.as_ref().try_into().map(CodeSha256).map_err(|_| IngressError::CodeSha256Length(length))
}

fn parse_nested_actor(value: Option<NestedActorProto>) -> Result<NestedActor, IngressError> {
    value.ok_or(IngressError::MissingNestedActor).and_then(NestedActor::try_from)
}

fn parse_optional_nested_actor(value: Option<NestedActorProto>) -> Result<Option<NestedActor>, IngressError> {
    value.map(NestedActor::try_from).transpose()
}

fn parse_optional_start(value: Option<StartProto>) -> Result<Option<Start>, IngressError> {
    value.map(Start::try_from).transpose()
}

#[derive(Debug, Clone, PartialEq, Eq, TS)]
#[proto_message(proxied, fallible = IngressError)]
pub struct NestedActor {
    #[proto(string, tag = 1, from_proto = parse_actor_id)]
    pub actor_id: ActorId,
}

#[derive(Debug, Clone, PartialEq, Eq, TS)]
#[proto_oneof(proxied, fallible = IngressError)]
pub enum Start {
    #[proto(tag = 1)]
    Cursor(u64),
    #[proto(tag = 2)]
    CurrentHead(bool),
}

#[derive(Debug, Clone, PartialEq, Eq, TS)]
#[ts(export, export_to = "typescript/subscription-start.ts")]
#[proto_message(proxied, fallible = IngressError)]
pub struct SubscriptionStart {
    #[proto(oneof(tags(1, 2), proxied), from_proto = parse_optional_start)]
    pub start: Option<Start>,
    #[proto(message(proxied), tag = 3, from_proto = parse_optional_nested_actor)]
    pub nested: Option<NestedActor>,
}

#[derive(Debug, Clone, PartialEq, Eq, TS)]
#[ts(export, export_to = "typescript/contract.ts")]
#[proto_message(proxied, fallible = IngressError)]
pub struct RegisterActor {
    #[proto(string, tag = 1, from_proto = parse_actor_id)]
    pub actor_id: ActorId,
    #[proto(uint64, tag = 2, from_proto = parse_positive_u64)]
    pub max_children: PositiveU64,
    #[proto(bytes, tag = 3, from_proto = parse_code_sha256)]
    pub code_sha256: CodeSha256,
    #[proto(message(default, proxied), tag = 4, from_proto = parse_nested_actor)]
    pub nested: NestedActor,
}

#[test]
fn one_authored_type_renders_wire_schema_and_round_trips() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("rendered");
    let _ = std::fs::remove_dir_all(&root);
    ACTORS::get_package().render_files(&root).unwrap();

    let proto = std::fs::read_to_string(root.join("actors.proto")).unwrap();
    assert!(proto.contains("package acyclic.actors.v1;"));
    assert!(proto.contains("message RegisterActor"));
    assert!(proto.contains("message NestedActor"));
    assert!(proto.contains("message SubscriptionStart"));
    assert!(proto.contains("string actor_id = 1;"));
    assert!(proto.contains("uint64 max_children = 2;"));
    assert!(proto.contains("bytes code_sha256 = 3;"));

    let authored = RegisterActor {
        actor_id: ActorId("actor-1".into()),
        max_children: PositiveU64::from(9),
        code_sha256: CodeSha256([7; 32]),
        nested: NestedActor { actor_id: ActorId("nested-1".into()) },
    };
    let wire: RegisterActorProto = authored.clone().into();
    assert_eq!(wire.actor_id, "actor-1");
    assert_eq!(wire.max_children, 9);
    assert_eq!(wire.code_sha256, vec![7; 32]);
    let encoded = wire.encode_to_vec();
    let wire = RegisterActorProto::decode(encoded.as_slice()).unwrap();
    let decoded = RegisterActor::try_from(wire).unwrap();
    assert_eq!(decoded, authored);
}

#[test]
fn fallible_proxy_round_trips_valid_values() {
    let authored = RegisterActor {
        actor_id: ActorId("actor-1".into()),
        max_children: PositiveU64::from(9),
        code_sha256: CodeSha256([7; 32]),
        nested: NestedActor { actor_id: ActorId("nested-1".into()) },
    };
    let wire: RegisterActorProto = authored.clone().into();
    let encoded = wire.encode_to_vec();
    let wire = RegisterActorProto::decode(encoded.as_slice()).unwrap();
    let decoded = RegisterActor::try_from(wire).unwrap();
    assert_eq!(decoded, authored);
}

#[test]
fn fallible_oneof_and_optional_nested_presence_round_trip() {
    let authored = SubscriptionStart {
        start: Some(Start::CurrentHead(true)),
        nested: Some(NestedActor { actor_id: ActorId("nested-1".into()) }),
    };
    let wire: SubscriptionStartProto = authored.clone().into();
    assert!(wire.start.is_some());
    assert!(wire.nested.is_some());
    let encoded = wire.encode_to_vec();
    let wire = SubscriptionStartProto::decode(encoded.as_slice()).unwrap();
    assert_eq!(SubscriptionStart::try_from(wire).unwrap(), authored);
}

#[test]
fn fallible_oneof_and_optional_nested_absence_round_trip() {
    let authored = SubscriptionStart { start: None, nested: None };
    let wire: SubscriptionStartProto = authored.clone().into();
    assert!(wire.start.is_none());
    assert!(wire.nested.is_none());
    assert_eq!(SubscriptionStart::try_from(wire).unwrap(), authored);
}

#[test]
fn fallible_proxy_rejects_invalid_nested_actor() {
    let wire = RegisterActorProto {
        actor_id: "actor-1".into(),
        max_children: 1,
        code_sha256: vec![0; 32].into(),
        nested: Some(NestedActorProto { actor_id: "".into() }),
    };
    assert_eq!(
        RegisterActor::try_from(wire),
        Err(IngressError::EmptyActorId)
    );
}

#[test]
fn fallible_proxy_rejects_missing_required_nested_actor() {
    let wire = RegisterActorProto {
        actor_id: "actor-1".into(),
        max_children: 1,
        code_sha256: vec![0; 32].into(),
        nested: None,
    };
    assert_eq!(
        RegisterActor::try_from(wire),
        Err(IngressError::MissingNestedActor)
    );
}

#[test]
fn fallible_proxy_rejects_bad_actor_id() {
    let wire = RegisterActorProto {
        actor_id: "".into(),
        max_children: 1,
        code_sha256: vec![0; 32].into(),
        nested: None,
    };
    assert_eq!(RegisterActor::try_from(wire), Err(IngressError::EmptyActorId));
}

#[test]
fn fallible_proxy_rejects_zero_positive_u64() {
    let wire = RegisterActorProto {
        actor_id: "actor-1".into(),
        max_children: 0,
        code_sha256: vec![0; 32].into(),
        nested: None,
    };
    assert_eq!(RegisterActor::try_from(wire), Err(IngressError::NonPositiveU64));
}

#[test]
fn fallible_proxy_rejects_bad_code_sha256_length() {
    let wire = RegisterActorProto {
        actor_id: "actor-1".into(),
        max_children: 1,
        code_sha256: vec![0; 31].into(),
        nested: None,
    };
    assert_eq!(
        RegisterActor::try_from(wire),
        Err(IngressError::CodeSha256Length(31))
    );
}

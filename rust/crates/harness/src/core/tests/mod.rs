use super::*;
use crate::conversation::FileDescriptor;
use serde_json::json;

#[derive(Clone, Debug)]
pub(super) enum Step {
    Lifecycle(usize),
    Custom(u8),
    Bind(u8),
    Resubmit(usize),
    ConflictingIntent(usize),
    StaleRevision,
}

pub(in crate::core) fn step() -> impl proptest::strategy::Strategy<Value = Step> {
    use proptest::prelude::*;
    prop_oneof![
        (0..6_usize).prop_map(Step::Lifecycle),
        any::<u8>().prop_map(Step::Custom),
        any::<u8>().prop_map(Step::Bind),
        any::<usize>().prop_map(Step::Resubmit),
        any::<usize>().prop_map(Step::ConflictingIntent),
        Just(Step::StaleRevision)
    ]
}

pub(in crate::core) fn operation(value: u8) -> OperationId {
    OperationId::from_bytes([value; 16])
}

pub(in crate::core) fn request_file(bytes: &[u8]) -> Result<FileRef> {
    use crate::conversation::VolumeClass;
    let volume = VolumeRef::new(
        crate::resources::ProviderRef::new(
            "test".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?,
        "effects".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([8; 16])),
        &mut None,
    )?;
    FileRef::new(
        volume,
        "requests/request.json".into(),
        "generation-1".into(),
        FileDescriptor::from_bytes(bytes, "application/json", &mut None)?,
        "request.json".into(),
        &mut None,
    )
}

pub(in crate::core) fn schema_file(schema: &Value) -> Result<FileRef> {
    let bytes = serde_json::to_vec(schema).map_err(|error| Error::Invalid(error.to_string()))?;
    FileRef::new(
        request_file(b"null")?.volume().clone(),
        "schemas/result.json".into(),
        "generation-1".into(),
        FileDescriptor::from_bytes(&bytes, "application/schema+json", &mut None)?,
        "result.json".into(),
        &mut None,
    )
}

pub(in crate::core) fn issuer() -> AuthorityIssuer {
    AuthorityIssuer::new(
        "test",
        [7; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "conversation-1".into(),
        },
    )
}

pub(in crate::core) fn schemas() -> SchemaRegistry {
    let mut schemas = SchemaRegistry::new();
    schemas
        .register(
            "example.message",
            1,
            json!({"type":"object","properties":{"text":{"type":"string"}},
        "additionalProperties":true}),
            [9; 32],
            ExtensionForkPolicy::Inherit,
            [],
            None,
        )
        .expect("fixture schema");
    schemas
}

pub(in crate::core) fn reducer_fixture() -> Reducer {
    let owner = issuer();
    Reducer::new(
        owner.audience.clone(),
        owner.verifier(&mut None).expect("fixture verifier"),
        schemas(),
    )
}

pub(in crate::core) fn command(
    operation_id: OperationId,
    expected_revision: u64,
    action: Action,
) -> Command {
    let mut grants: Vec<String> = [
        "event:append",
        "lifecycle:manage",
        "effect:run",
        "effect:plan",
        "effect:provider:example.provider",
        "interaction:open",
        "interaction:respond",
        "fork:publish",
        "conversation:bind",
        "conversation:append",
        "conversation:select_context",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    if let Action::PublishFork { seed } = &action {
        for resource in &seed.resources {
            if let crate::fork::ResourceRevision::PrivateVolume { volume, .. }
            | crate::fork::ResourceRevision::Project { volume, .. } = &resource.source
            {
                grants.push(
                    volume
                        .capability(VolumeOperation::Read, &mut None)
                        .expect("source reference"),
                );
            }
        }
    }
    let agent =
        matches!(&action, Action::PublishFork { .. }).then_some(AgentId::from_bytes([1; 16]));
    let scope = issuer()
        .issue("root", &Capabilities::new(grants), agent, None, &mut None)
        .expect("fixture scope");
    Command {
        operation_id,
        idempotency_key: IdempotencyKey(format!("key-{operation_id}")),
        expected_revision,
        scope,
        causal_parent: None,
        action,
    }
}

mod authority;
mod conversation;
mod effects;
mod extensions;
mod fork;
mod model;

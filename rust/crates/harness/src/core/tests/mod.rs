use super::*;
use crate::conversation::FileDescriptor;
use crate::resources::ProviderRef;
use serde_json::json;

pub(in crate::core) fn operation(value: u8) -> OperationId {
    OperationId::from_bytes([value; 16])
}

pub(in crate::core) fn request_file(bytes: &[u8]) -> Result<FileRef> {
    use crate::conversation::VolumeClass;
    let provider = ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?;
    let volume = VolumeRef::new(
        provider,
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
    let verifier = owner.verifier(&mut None).expect("fixture verifier");
    Reducer::new(owner.audience, verifier, schemas())
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

fn apply(reducer: &mut Reducer, id: u8, action: Action) -> Result<ApplyResult> {
    reducer.apply(command(operation(id), reducer.revision(), action))
}

pub(in crate::core) fn restore(reducer: &Reducer, owner: &AuthorityIssuer) -> Result<Reducer> {
    let bytes = crate::contract::canonical_json_bytes(&reducer.snapshot(usize::MAX, &mut None)?.0)?;
    Reducer::restore(
        crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?,
        owner.verifier(&mut None)?,
        reducer.schemas.clone(),
        &mut None,
    )
}

mod authority;
mod conversation;
mod effects;
mod extensions;
mod fork;
mod model;

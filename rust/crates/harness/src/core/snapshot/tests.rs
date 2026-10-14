use super::*;
use crate::core::tests::{command, issuer, operation, reducer_fixture, request_file};
use serde_json::json;

fn restore(
    snapshot: Snapshot,
    owner: &AuthorityIssuer,
    registry: SchemaRegistry,
) -> Result<Reducer> {
    Reducer::restore(snapshot, owner.verifier(&mut None)?, registry, &mut None)
}

#[test]
fn checkpoint_proof_binds_issuer_key_identity_audience_and_format() -> Result<()> {
    let mut reducer = reducer_fixture();
    let content = request_file(br#"{"text":"hello"}"#)?;
    let action = Action::AppendCustom {
        schema: "example.message".into(),
        version: 1,
        content,
    };
    let ApplyResult::Applied { .. } = reducer.apply(&command(operation(70), 0, action))? else {
        panic!("fresh append replayed")
    };
    let snapshot = reducer.snapshot(usize::MAX, &mut None)?.0;
    for other in [
        AuthorityIssuer::new("test", [8; 32], reducer.authority.clone()),
        AuthorityIssuer::new("other", [7; 32], reducer.authority.clone()),
        AuthorityIssuer::new(
            "test",
            [7; 32],
            Authority {
                kind: AggregateKind::Agent,
                id: "other-agent".into(),
            },
        ),
    ] {
        assert!(restore(snapshot.copy(&mut None)?, &other, reducer.schemas.clone()).is_err());
    }
    let mut old = snapshot.copy(&mut None)?;
    old.format_version = 2;
    assert!(restore(old, &issuer(), reducer.schemas.clone()).is_err());
    assert!(restore(snapshot.copy(&mut None)?, &issuer(), SchemaRegistry::new()).is_err());
    let mut changed = SchemaRegistry::new();
    changed.register(
        "example.message",
        1,
        json!({"type":"string"}),
        [8; 32],
        ExtensionForkPolicy::Reset,
        [],
        None,
    )?;
    assert!(restore(snapshot.copy(&mut None)?, &issuer(), changed).is_err());
    assert!(
        reducer
            .schemas
            .register(
                "example.message",
                1,
                json!({"type":"object"}),
                [8; 32],
                ExtensionForkPolicy::Inherit,
                [],
                None
            )
            .is_err()
    );
    let mut expanded = reducer.schemas.clone();
    expanded.register(
        "example.additional",
        1,
        json!({"type":"string"}),
        [9; 32],
        ExtensionForkPolicy::Reset,
        [],
        None,
    )?;
    let restored = restore(snapshot, &issuer(), expanded.clone())?;
    assert_eq!(restored.schemas, expanded);
    Ok(())
}

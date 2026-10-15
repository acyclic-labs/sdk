use super::*;

#[test]
fn duplicate_reducer_registration_never_replaces_the_original() -> Result<()> {
    let entrypoint = EntrypointRef {
        name: "example.reducer".into(),
        version: "1".into(),
        digest: [3; 32],
        result_schema: serde_json::json!({"type": "number"}),
    };
    let mut registry = ReducerRegistry::default();
    registry.register(entrypoint.clone(), |_| {
        Ok(crate::Outcome::Succeeded(Value::from(1)))
    })?;
    assert!(matches!(
        registry.register(entrypoint.clone(), |_| Ok(crate::Outcome::Succeeded(
            Value::from(2)
        ))),
        Err(Error::Conflict(_))
    ));
    let retained = registry
        .get(&entrypoint)
        .ok_or_else(|| Error::NotFound("reducer".into()))?;
    assert_eq!(retained(&[])?, crate::Outcome::Succeeded(Value::from(1)));
    Ok(())
}

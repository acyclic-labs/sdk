use super::*;

#[test]
fn family_shell_binding_keeps_one_selection_and_the_original_capability() -> Result<()> {
    for profile in [
        NativeToolProfile::Anthropic,
        NativeToolProfile::Openai,
        NativeToolProfile::Google,
    ] {
        let mut registry = crate::tools::registry(&["shell".into()], |_| true, Vec::new())?;
        let original = registry
            .get("shell", None, None)?
            .unwrap()
            .definition
            .clone();
        bind(&mut registry, profile)?;
        let selected = registry
            .get("shell", None, None)?
            .expect("the canonical shell remains directly callable");
        {
            let offered = definitions(profile)?
                .into_iter()
                .find(|definition| definition.name == "shell")
                .expect("the family declares its canonical shell");
            assert_eq!(selected.definition.revision, offered.revision);
            assert_eq!(selected.definition.input_schema, offered.input_schema);
            let capability = selected
                .executor
                .capability()
                .expect("the family adapter retains the original capability");
            assert_eq!(capability.definition, original);
            assert_eq!(selected.definition.output_schema, original.output_schema);
            assert_eq!(
                selected.definition.projection_schema,
                original.projection_schema
            );
        }
        let expected = vec![selected.definition.clone()];
        assert_eq!(registry.definitions(None)?, expected);
        assert_eq!(
            registry
                .definitions_for_grants(&crate::Capabilities::new(["tool:call:shell"]), None)?,
            expected
        );
        assert!(
            registry
                .definitions_for_grants(&crate::Capabilities::new(["tool:call:read"]), None)?
                .is_empty()
        );
        for retired in ["Bash", "run_shell_command"] {
            assert!(registry.get(retired, None, None)?.is_none());
        }
    }
    // Canonical presentation cannot create a missing selection.
    assert!(matches!(
        ToolRegistry::default().present_natively("shell", vec!["shell".into()]),
        Err(Error::NotFound(_)),
    ));
    Ok(())
}

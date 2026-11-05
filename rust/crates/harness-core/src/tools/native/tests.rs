use super::*;

#[test]
fn family_shell_binding_keeps_one_selection_and_the_original_capability() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    for profile in [
        NativeToolProfile::Anthropic,
        NativeToolProfile::Openai,
        NativeToolProfile::Google,
    ] {
        let cache = Cache::default();
        let calls = AtomicUsize::new(0);
        let refused = cache.get(|| Err(Error::Unsupported("original profile refusal".into())));
        assert!(
            matches!(refused, Err(Error::Unsupported(reason)) if reason == "original profile refusal")
        );

        let start = std::sync::Barrier::new(4);
        let bindings = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..4)
                .map(|_| {
                    scope.spawn(|| {
                        start.wait();
                        cache.get(|| {
                            calls.fetch_add(1, Ordering::SeqCst);
                            Ok(Some(profile))
                        })
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|worker| worker.join().expect("native binding worker"))
                .collect::<Result<Vec<_>>>()
        })?;
        let original = bindings[0].as_ref().expect("native family binding");
        // A cached hit does not even ask the immutable selection for its profile.
        let retained = cache
            .get(|| {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(Error::Unsupported("cached binding was read again".into()))
            })?
            .expect("retained native family binding");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "native profile was read more than once"
        );
        assert!(Arc::ptr_eq(original, &retained));
        for binding in bindings.iter().flatten() {
            assert!(Arc::ptr_eq(original, binding));
        }
        let mut registry = crate::tools::registry(&["shell".into()], |_| true, Vec::new())?;
        let original = registry
            .get("shell", None, None)?
            .unwrap()
            .definition
            .clone();
        // A module can change the declaration without replacing authority or
        // making the declaration's result schemas authoritative.
        let entries = || -> Result<Vec<_>> {
            decoded(profile)?
                .filter_map(|entry| match entry {
                    Ok((mapping, definition)) if mapping.capability == "shell" => {
                        Some(Ok((mapping, definition)))
                    }
                    Ok(_) => None,
                    Err(error) => Some(Err(error)),
                })
                .collect()
        };
        let mut admitted = entries()?;
        assert!(!admitted.is_empty());
        for (_, definition) in &mut admitted {
            definition
                .description
                .push_str(" Admitted module description.");
            definition.output_schema = Value::Bool(false);
            definition.projection_schema = Value::Bool(false);
        }
        let mut changed = registry.clone();
        NativeTools::from_definitions(admitted)?.bind(&mut changed)?;
        let selected = changed.get("shell", None, None)?.unwrap();
        assert!(
            selected
                .definition
                .description
                .ends_with("Admitted module description.")
        );
        assert_eq!(selected.definition.output_schema, original.output_schema);
        assert_eq!(
            selected.definition.projection_schema,
            original.projection_schema
        );
        assert_eq!(selected.executor.capability().unwrap().definition, original);
        for alter_name in [false, true] {
            let mut mismatched = entries()?;
            for (_, definition) in &mut mismatched {
                if alter_name {
                    definition.name.push_str("_mismatched");
                } else {
                    definition.revision.push_str("_mismatched");
                }
            }
            assert!(
                matches!(
                    NativeTools::from_definitions(mismatched),
                    Err(Error::Invalid(reason))
                        if reason == "native declaration identity differs from its mapping"
                ),
                "mismatched native identity was admitted"
            );
        }
        let mut missing = ToolRegistry::default();
        NativeTools::from_definitions(entries()?)?.bind(&mut missing)?;
        assert!(missing.definitions(None)?.is_empty());
        retained.bind(&mut registry)?;
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
    let absent = Cache::default();
    assert!(absent.get(|| Ok(None))?.is_none());
    assert!(
        absent
            .get(|| Err(Error::Unsupported("absent profile was read again".into())))?
            .is_none()
    );
    // Canonical presentation cannot create a missing selection.
    assert!(matches!(
        ToolRegistry::default().present_natively("shell", vec!["shell".into()]),
        Err(Error::NotFound(_)),
    ));
    Ok(())
}

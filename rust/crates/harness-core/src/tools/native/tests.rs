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
                            NativeTools::new(profile).map(|tools| Some(Arc::new(tools)))
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
        let mut admitted = shell_entries(profile)?;
        assert!(!admitted.is_empty());
        for (_, definition) in &mut admitted {
            definition
                .description
                .push_str(" Admitted module description.");
            definition.output_schema = Value::Bool(false);
            definition.projection_schema = Value::Bool(false);
        }
        let mut changed = registry.clone();
        let admitted = Arc::new(NativeTools::from_definitions(admitted)?);
        let selected = Cache::default()
            .get(|| Ok(Some(Arc::clone(&admitted))))?
            .unwrap();
        assert!(Arc::ptr_eq(&selected, &admitted));
        selected.bind(&mut changed)?;
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
            let mut mismatched = shell_entries(profile)?;
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
        NativeTools::from_definitions(shell_entries(profile)?)?.bind(&mut missing)?;
        assert!(missing.definitions(None)?.is_empty());
        check_aliases(profile, &registry)?;
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

fn check_aliases(profile: NativeToolProfile, registry: &ToolRegistry) -> Result<()> {
    let original = registry.get("shell", None, None)?.unwrap();
    for reverse in [false, true] {
        // Derive a module edit from each family's retained real declaration;
        // the alias changes spelling while retaining its argument template.
        let mut declarations = shell_entries(profile)?;
        let (mut mapping, mut definition) = shell_entries(profile)?.pop().unwrap();
        mapping.variant = "native_shell_alias".into();
        definition.name.clone_from(&mapping.variant);
        declarations.push((mapping, definition));
        if reverse {
            declarations.reverse();
        }
        let mut selected = registry.clone();
        NativeTools::from_definitions(declarations)?.bind(&mut selected)?;
        let mut visible = selected
            .definitions_for_grants(&crate::Capabilities::new(["tool:call:shell"]), None)?;
        assert_eq!(
            visible.len(),
            2,
            "canonical declaration was hidden by aliases"
        );
        visible.sort_by(|left, right| left.name.cmp(&right.name));
        assert_eq!(selected.definitions(None)?, visible);
        for definition in &visible {
            let tool = selected.get(&definition.name, None, None)?.unwrap();
            assert_eq!(tool.definition, *definition);
            assert!(
                std::ptr::eq(tool.executor.capability().unwrap(), original),
                "native alias replaced the original capability owner"
            );
        }
        assert!(
            selected
                .definitions_for_grants(&crate::Capabilities::new(["tool:call:read"]), None)?
                .is_empty()
        );
        #[cfg(feature = "filesystem")]
        {
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .unwrap();
            let (copied, custody) = selected.clone_with_native_work(&work)?;
            assert_eq!(copied.definitions(None)?, visible);
            assert!(custody.bytes() > 0);
        }
    }
    Ok(())
}

fn shell_entries(
    profile: NativeToolProfile,
) -> Result<Vec<(crate::profile::ToolRevision, ToolDefinition)>> {
    decoded(profile)?
        .filter_map(|entry| match entry {
            Ok((mapping, definition)) if mapping.capability == "shell" => {
                Some(Ok((mapping, definition)))
            }
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

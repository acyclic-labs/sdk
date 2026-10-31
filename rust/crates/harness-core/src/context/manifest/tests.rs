use super::*;
use proptest::prelude::*;
use serde_json::json;

fn skill(path: String) -> Result<SkillMetadata> {
    use crate::{
        conversation::{VolumeClass, VolumeOwner, VolumeRef},
        resources::{GenerationRef, ProviderRef},
    };
    let provider = ProviderRef::new("test".into(), "filesystem".into(), "1".into(), &mut None)?;
    Ok(SkillMetadata {
        name: "inspect".into(),
        description: "Inspect files".into(),
        fields: BTreeMap::new(),
        source: PinnedContextPath {
            generation: GenerationRef::new(&provider, &[1], None, None)?.0,
            root: ContextRoot {
                volume: VolumeRef::new(
                    provider,
                    "v".into(),
                    VolumeClass::Project,
                    VolumeOwner::Project("test".into()),
                    &mut None,
                )?,
                directory: "skills".into(),
            },
            path,
        },
    })
}

fn definition(name: String, description: String) -> ToolDefinition {
    ToolDefinition {
        name,
        revision: "1".into(),
        description,
        input_schema: json!({"type":"object"}),
        output_schema: json!({"type":"object"}),
        projection_schema: crate::tool::json_projection_schema(json!({})),
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn callable_registry_admission_preserves_original_custody_on_refusal() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .expect("original callable work");
    let (mut registry, custody) = ToolRegistry::new(&work)?;
    let mut custody = Some(custody);
    let mut declared = skill("skills/inspect/SKILL.md".into())?;
    declared
        .fields
        .insert("input_schema".into(), json!({"type":"object"}));
    declared.fields.insert(
        "runtime".into(),
        json!({"kind":"code","script":"skills/inspect/run.ts"}),
    );
    assert!(declared.register_callable(&mut registry, &mut custody, Some(&work))?);
    let before = registry.definitions(None)?;
    let retained = custody.as_ref().expect("original custody").bytes();
    // Original native registration refuses missing custody and rolls back a
    // malformed replacement without changing the admitted revision or its hold.
    let mut absent = ToolRegistry::default();
    assert!(matches!(
        declared.register_callable(&mut absent, &mut None, Some(&work)),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert!(absent.definitions(None)?.is_empty());
    declared
        .fields
        .insert("input_schema".into(), json!({"type":"unknown"}));
    assert!(
        declared
            .register_callable(&mut registry, &mut custody, Some(&work))
            .is_err()
    );
    assert_eq!(registry.definitions(None)?, before);
    assert_eq!(
        custody.as_ref().expect("retained custody").bytes(),
        retained
    );
    cancellation.cancel();
    assert!(
        declared
            .register_callable(&mut registry, &mut custody, Some(&work))
            .is_err()
    );
    assert_eq!(
        custody.as_ref().expect("cancelled custody").bytes(),
        retained
    );
    Ok(())
}

// Property: canonical encoding is idempotent, preserves array order and round-trips
// arbitrary descriptions; byte admission rejects before accepting an oversized manifest.
proptest! {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn callable_registration_retains_pins_and_refuses_invalid_replacements(
        text in ".{0,80}", code in any::<bool>(),
    ) {
        let mut declared = skill("skills/inspect/SKILL.md".into())?;
        let mut registry = ToolRegistry::default();
        prop_assert!(!declared.register_callable(&mut registry, &mut None, None)?);
        prop_assert!(registry.definitions(None)?.is_empty());
        declared.fields.insert("input_schema".into(), json!({"const": text}));
        declared.fields.insert("runtime".into(), json!({
            "kind": if code { "code" } else { "shell" },
            "script": "skills/inspect/run",
        }));
        prop_assert!(declared.register_callable(&mut registry, &mut None, None)?);
        let registered = registry.get("inspect", None, None)?.ok_or_else(|| TestCaseError::fail("missing registered skill"))?;
        let revision = registered.definition.revision.clone();
        let source = registered.skill_source.as_ref().ok_or_else(|| TestCaseError::fail("missing skill provenance"))?;
        prop_assert_eq!(source.source(), &declared.source);
        prop_assert_eq!(&registered.definition.input_schema, &declared.fields["input_schema"]);
        prop_assert_eq!(registered.definition.validate_projection(
            &json!({"kind":"json","value":text}), Limits::default(), None,
        )?, crate::model::ToolResultContent::Json { value: json!(text) });
        prop_assert!(registered.definition.validate_projection(
            &json!({"kind":"parts","value":text}), Limits::default(), None,
        ).is_err());
        let before = registry.definitions(None)?;
        for runtime in [
            json!({"kind":"unknown","script":"skills/inspect/run"}),
            json!({"kind":"code","script":"skills/inspect/run","unknown":true}),
            json!({"kind":"code","script":"skills-other/run"}),
        ] {
            let mut replacement = declared.clone();
            replacement.fields.insert("runtime".into(), runtime);
            prop_assert!(replacement.register_callable(&mut registry, &mut None, None).is_err());
            prop_assert_eq!(registry.definitions(None)?, before.clone());
        }
        let mut replacement = declared.clone();
        replacement.fields.insert("input_schema".into(), json!({"type":"unknown"}));
        prop_assert!(replacement.register_callable(&mut registry, &mut None, None).is_err());
        prop_assert_eq!(registry.definitions(None)?, before);
        // A different pinned source is a different immutable executable revision.
        replacement = declared;
        replacement.source.path = "skills/other/SKILL.md".into();
        let mut other = ToolRegistry::default();
        prop_assert!(replacement.register_callable(&mut other, &mut None, None)?);
        let other = other.get("inspect", None, None)?.ok_or_else(|| TestCaseError::fail("missing replacement"))?;
        prop_assert_ne!(&other.definition.revision, &revision);
    }

    // Both catalog discovery and restored manifests reject partial callables.
    // Opaque runtime data and the original pinned source survive publication;
    // executable/schema admission remains the registry's responsibility.
    #[test]
    fn callable_metadata_preserves_source_and_refuses_partial_declarations(
        text in ".{0,80}", boolean_schema in any::<bool>(),
    ) {
        let original = skill("skills/inspect/SKILL.md".into())?;
        // Keep the minimized raw-JSON/YAML failure in every generated header.
        let text = format!("\u{7f}{text}");
        let schema = if boolean_schema { json!(false) } else { json!({"description": text}) };
        let runtime = json!({"opaque": text});
        let header = json!({
            "name": original.name, "description": original.description,
            "input_schema": schema, "runtime": runtime,
        });
        let prefix = format!("---\n{}\n---\nBODY", serde_saphyr::to_string(&header)?);
        let declared = parse_skill_metadata(prefix.as_bytes(), original.source.clone())?;
        let fields = declared.call_fields(&mut None)?.ok_or_else(|| TestCaseError::fail("missing callable fields"))?;
        prop_assert_eq!(fields.input_schema, &schema);
        prop_assert_eq!(fields.runtime, &runtime);
        let manifest = ContextManifest {
            discovery: DiscoveredContext { instructions: Vec::new(), skills: vec![declared] },
            ..Default::default()
        };
        let limits = Limits { render_bytes: 32_768, ..Default::default() };
        let encoded = manifest.encode(limits)?;
        let restored = ContextManifest::decode(&encoded, limits)?;
        prop_assert_eq!(&restored, &manifest);
        prop_assert_eq!(&restored.discovery.skills[0].source, &original.source);
        for missing in ["input_schema", "runtime"] {
            let mut incomplete = header.clone();
            incomplete.as_object_mut().ok_or_else(|| TestCaseError::fail("header is not an object"))?.remove(missing);
            let prefix = format!("---\n{}\n---\nBODY", serde_saphyr::to_string(&incomplete)?);
            prop_assert!(parse_skill_metadata(prefix.as_bytes(), original.source.clone()).is_err());
            let mut restored = serde_json::to_value(&manifest)?;
            restored["discovery"]["skills"][0]["fields"].as_object_mut()
                .ok_or_else(|| TestCaseError::fail("fields are not an object"))?.remove(missing);
            prop_assert!(ContextManifest::decode(&serde_json::to_vec(&restored)?, limits).is_err());
        }
    }

    // Property: display names do not identify sources; explicit name-only
    // invocations must be unambiguous, and tool names remain unique.
    #[test]
    fn source_names_and_invocations_have_distinct_identity_rules(names in prop::collection::vec("[a-z]{1,8}", 0..16)) {
        let unique = names.iter().collect::<std::collections::BTreeSet<_>>().len() == names.len();
        let mut skills = Vec::new();
        for (index, name) in names.iter().enumerate() {
            let mut skill = skill(format!("skills/x{index}/SKILL.md"))?;
            skill.name = name.clone();
            skills.push(skill);
        }
        let discovery = DiscoveredContext { instructions: Vec::new(), skills };
        prop_assert!(discovery.validate(&mut None).is_ok());
        let mut selected = names.clone();
        selected.sort_unstable();
        selected.dedup();
        let manifest = ContextManifest { discovery: discovery.clone(), selected_skills: selected, ..Default::default() };
        prop_assert_eq!(manifest.validate(Limits::default()).is_ok(), unique);
        if let Some(first) = discovery.skills.first() {
            let mut other = first.clone();
            other.source.path = "skills/other/SKILL.md".into();
            let manifest = ContextManifest {
                discovery: DiscoveredContext { instructions: Vec::new(), skills: vec![first.clone(), other] },
                selected_skills: vec![first.name.clone(), "missing-selection".into()],
                ..Default::default()
            };
            prop_assert!(matches!(manifest.validate(Limits::default()), Err(Error::Conflict(_))),
                         "duplicate matches cannot conceal a missing selected skill");
        }
        let manifest = ContextManifest {
            tools: names.iter().map(|name| definition(name.clone(), "test".into())).collect(),
            ..Default::default()
        };
        prop_assert_eq!(manifest.validate(Limits::default()).is_ok(), unique);
        let mut discovery = discovery;
        discovery.skills.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        discovery.skills.dedup_by(|a, b| a.name == b.name);
        let manifest = ContextManifest { discovery, selected_skills: names, ..Default::default() };
        prop_assert_eq!(manifest.validate(Limits::default()).is_ok(), unique);
    }

    // Property: the manifest round-trips its immutable sources, and
    // configuration files obey the same bounds as instructions.
    #[cfg(feature = "filesystem")]
    #[test]
    fn configuration_sources_round_trip(path in "[a-zé猫]{1,40}", text in ".{0,80}", bound in 1usize..256) {
        use crate::conversation::{FileDescriptor, FileRef};
        let reference = FileRef::new(
            skill("skills/x/SKILL.md".into())?.source.root.volume,
            format!("context/{path}.json"), "pinned-1".into(),
            FileDescriptor::from_bytes(text.as_bytes(), "application/json", &mut None)?,
            "manifest.json".into(), &mut None,
        )?;
        let manifest = ContextManifest { sources: vec![reference.clone()], ..Default::default() };
        let limits = Limits { render_bytes: 32_768, ..Default::default() };
        let bytes = manifest.encode(limits)?;
        prop_assert_eq!(ContextManifest::decode(&bytes, limits)?, manifest.clone());
        let limits = Limits { path_bytes: bound, ..limits };
        prop_assert_eq!(manifest.validate(limits).is_ok(), reference.path().len() <= bound);
        prop_assert_eq!(ContextManifest::decode(&bytes, limits).is_ok(), reference.path().len() <= bound);
    }

    // Property: every declared skill path fits its UTF-8 byte bound, whether
    // selected or lazy. Validation, publication encoding and decoding agree at
    // arbitrary bounds and immediately below/at/above the exact byte length.
    #[test]
    fn skill_paths_obey_byte_limits(
        component in "[a-zé猫]{1,48}",
        bound in 1usize..256,
        selected in any::<bool>(),
    ) {
        let path = format!("skills/{component}/SKILL.md");
        let length = path.len();
        let manifest = ContextManifest {
            discovery: DiscoveredContext {
                instructions: Vec::new(),
                skills: vec![skill(path)?],
            },
            selected_skills: if selected { vec!["inspect".into()] } else { Vec::new() },
            tools: Vec::new(),
            ..Default::default()
        };
        let limits = Limits { render_bytes: 32_768, ..Default::default() };
        let bytes = manifest.encode(limits)?;
        for path_bytes in [bound, length - 1, length, length + 1] {
            let limits = Limits { path_bytes, ..limits };
            let admitted = length <= path_bytes;
            prop_assert_eq!(manifest.validate(limits).is_ok(), admitted);
            prop_assert_eq!(manifest.encode(limits).is_ok(), admitted);
            prop_assert_eq!(ContextManifest::decode(&bytes, limits).is_ok(), admitted);
        }
    }

    #[test]
    fn bounded_manifest_round_trip(descriptions in prop::collection::vec(".{0,80}", 0..8)) {
        let manifest = ContextManifest {
            tools: descriptions.into_iter().enumerate()
                .map(|(index, description)| definition(format!("tool{index}"), description)).collect(),
            ..Default::default()
        };
        let limits = Limits { render_bytes: 32_768, ..Default::default() };
        let encoded = manifest.encode(limits)?;
        let decoded = ContextManifest::decode(&encoded, limits)?;
        prop_assert_eq!(&decoded, &manifest);
        prop_assert_eq!(decoded.encode(limits)?, encoded.clone());
        prop_assert!(ContextManifest::decode(&encoded, Limits {
            render_bytes: encoded.len() as u64 - 1, ..limits
        }).is_err(), "oversized manifest was admitted");
    }

    // Property: any selected definition mismatch rejects atomically. On success
    // the exact requested selection replaces visibility while admitted versions remain.
    #[test]
    fn pinning_is_exact_and_transactional(description in ".{0,80}", hide in any::<bool>(), tamper in any::<bool>()) {
        let mut tools = ToolRegistry::default();
        let mut tool = crate::tool::files::write()?;
        tool.definition.description = description;
        tools.register((tool, None), &mut None, None)?;
        let before = tools.definitions(None)?;
        let first = before.first().ok_or_else(|| TestCaseError::fail("registered definition missing"))?;
        let name = &first.name;
        let revision = &first.revision;
        let mut selected = if hide { Vec::new() } else { before.clone() };
        if tamper && !hide {
            selected.first_mut().ok_or_else(|| TestCaseError::fail("selected definition missing"))?.description.push('!');
        }
        let result = tools.pin_model_definitions(&selected, None);
        if tamper && !hide {
            prop_assert!(matches!(result, Err(Error::Conflict(_))));
            prop_assert_eq!(tools.definitions(None)?, before.clone());
        } else {
            prop_assert!(result.is_ok());
            prop_assert_eq!(tools.definitions(None)?, selected);
        }
        prop_assert!(tools.get(name, Some(revision), None)?.is_some());
    }
}

#[test]
fn malformed_manifests_fail_before_any_binding() {
    let limits = Limits {
        render_bytes: 32_768,
        ..Default::default()
    };
    for input in [
        r#"{"discovery":{"instructions":[],"skills":[]},"selected_skills":[],"tools":[],"extra":true}"#,
        r#"{"discovery":{"instructions":[],"skills":[]},"selected_skills":[],"tools":[],"tools":[]}"#,
        r#"{"discovery":{"instructions":[],"skills":[]},"selected_skills":["missing"],"tools":[]}"#,
        r#"{"discovery":{"instructions":[],"skills":[]},"selected_skills":[],"tools":[]} false"#,
        r#"{"discovery":{"instructions":[],"skills":[]},"selected_skills":false,"tools":[]}"#,
        r#"{"discovery":{"instructions":[],"skills":[]},"selected_skills":[],"tools":[],"x": [[["#,
    ] {
        assert!(
            ContextManifest::decode(input.as_bytes(), limits).is_err(),
            "{input}"
        );
    }
    let mut repeated = ContextManifest {
        tools: vec![definition("read".into(), "Read".into()); 2],
        ..Default::default()
    };
    assert!(matches!(repeated.validate(limits), Err(Error::Conflict(_))));
    repeated.tools.truncate(1);
    repeated.tools[0].input_schema = json!({"type":"invalid"});
    assert!(repeated.validate(limits).is_err());
}

#[tokio::test]
async fn optional_environment_joins_after_all_required_stages_on_public_pipeline_routes()
-> Result<()> {
    let required = Context {
        messages: vec![ModelMessage {
            role: ModelRole::System,
            content: ModelContent::Text("required".into()),
        }],
        ..Default::default()
    };
    let pipeline = ContextPipeline::new([
        Arc::new(ManifestStage {
            context: Context::default(),
            sources: Vec::new(),
            environment: Some(EnvironmentFacts {
                cwd: Some("/bound".into()),
                ..Default::default()
            }),
        }) as Arc<dyn ContextStage>,
        Arc::new(SourceStage::new(
            "required",
            "1",
            Arc::new(required),
            ContextPlacement::Prepend,
        )),
    ]);
    let input = ContextInput {
        input: ModelContent::Text("current".into()),
        selected_context: None,
        step: 0,
        prior_messages: Vec::new(),
    };
    for (message_limit, current_input_index) in [(2, 1), (3, 2)] {
        let limits = Limits {
            context_messages: message_limit,
            ..Limits::default()
        };
        let run = pipeline.run(&input, limits, None).await?;
        let base = ContextPipeline::base_context(&input, limits, None)?;
        assert_eq!(pipeline.transform(&input, base, limits, None).await?, run);
        assert_eq!(run.messages.len(), message_limit);
        assert_eq!(run.current_input_index, Some(current_input_index));
        assert_eq!(
            run.messages[message_limit - 2].content,
            ModelContent::Text("required".into())
        );
        if message_limit == 3 {
            assert!(
                matches!(&run.messages[0].content, ModelContent::Text(text) if text.starts_with("<environment_context>"))
            );
        }
    }
    Ok(())
}

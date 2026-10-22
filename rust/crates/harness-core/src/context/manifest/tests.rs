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

// Property: canonical encoding is idempotent, preserves array order and round-trips
// arbitrary descriptions; byte admission rejects before accepting an oversized manifest.
proptest! {
    // Property: order never changes uniqueness or membership; duplicate source
    // metadata, selected invocations and tool names are all rejected.
    #[test]
    fn catalog_uniqueness_is_independent_of_order(names in prop::collection::vec("[a-z]{1,8}", 0..16)) {
        let unique = names.iter().collect::<std::collections::BTreeSet<_>>().len() == names.len();
        let mut skills = Vec::new();
        for name in &names {
            let mut skill = skill("skills/x/SKILL.md".into())?;
            skill.name = name.clone();
            skills.push(skill);
        }
        let discovery = DiscoveredContext { instructions: Vec::new(), skills };
        prop_assert_eq!(discovery.validate(&mut None).is_ok(), unique);
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

    // Property: command serialization preserves the entire immutable manifest
    // identity, and configuration files obey the same bounds as instructions.
    #[cfg(feature = "filesystem")]
    #[test]
    fn command_and_configuration_sources_round_trip(path in "[a-zé猫]{1,40}", text in ".{0,80}", bound in 1usize..256, summary in any::<bool>()) {
        use crate::conversation::{FileDescriptor, FileRef};
        let reference = FileRef::new(
            skill("skills/x/SKILL.md".into())?.source.root.volume,
            format!("context/{path}.json"), "pinned-1".into(),
            FileDescriptor::from_bytes(text.as_bytes(), "application/json", &mut None)?,
            "manifest.json".into(), &mut None,
        )?;
        let command = crate::filesystem::ModelTaskCommand {
            summary_source: summary.then(|| Box::new(reference.clone())),
            context_manifest: reference.clone(), input: ModelContent::Text(text),
            selected_context: None, max_steps: 1, max_output_tokens: None,
            mailbox_operation: None, selected_output_volume: None,
        };
        let encoded = serde_json::to_vec(&command).unwrap();
        prop_assert_eq!(serde_json::from_slice::<Value>(&encoded).unwrap().get("summary_source").is_some(), summary);
        prop_assert_eq!(serde_json::from_slice::<crate::filesystem::ModelTaskCommand>(&encoded).unwrap(), command);
        let mut omitted = serde_json::from_slice::<Value>(&encoded).unwrap();
        omitted.as_object_mut().unwrap().remove("context_manifest");
        prop_assert!(serde_json::from_value::<crate::filesystem::ModelTaskCommand>(omitted.clone()).is_err());
        omitted["context_manifest"] = Value::Null;
        prop_assert!(serde_json::from_value::<crate::filesystem::ModelTaskCommand>(omitted).is_err());
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
    for message_limit in [2, 3] {
        let limits = Limits {
            context_messages: message_limit,
            ..Limits::default()
        };
        let run = pipeline.run(&input, limits, None).await?;
        let base = ContextPipeline::base_context(&input, limits, None)?;
        assert_eq!(pipeline.transform(&input, base, limits, None).await?, run);
        assert_eq!(run.messages.len(), message_limit);
        assert_eq!(run.current_input_index, Some((message_limit - 1) as u32));
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

//! Configuration inputs retain identity without rewriting the cached prefix.
use super::*;

#[tokio::test]
async fn optional_planning_and_model_selection_do_not_enter_worker_context() -> Result<()> {
    let tools = crate::tool::ToolRegistry::default();
    let mut baseline = None;
    for model in ["first-model", "second-model"] {
        let payloads = TestPayloadStore::new()?;
        let mut manifest = ContextManifest::new(DiscoveredContext::default(), Vec::new(), &tools)?;
        manifest.environment = Some(EnvironmentFacts {
            cwd: Some("/project".into()),
            os: "test".into(),
            shell: "sh".into(),
            tool_path: None,
            tooling: None,
            current_date: "2026-01-01".into(),
            timezone: "UTC".into(),
            model: model.into(),
        });
        let reference = manifest
            .publish(
                &payloads,
                crate::OperationId::new(),
                "manifest.json",
                payload_limits(),
            )
            .await?;
        let (_, stage) =
            ContextManifest::bind(reference, &payloads, tools.clone(), payload_limits()).await?;
        let projected = ContextPipeline::new([stage])
            .run(&input(), payload_limits(), None)
            .await?;
        let encoded = serde_json::to_string(&projected.messages)
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        assert!(
            encoded.contains("/project"),
            "environment projection was not exercised"
        );
        for noise in [model, "<model>", "git_status", "GOAL.md", "plans/"] {
            assert!(!encoded.contains(noise), "worker context contains {noise}");
        }
        let digest =
            crate::executor::json::canonical_json_digest(&projected.messages, None, &mut None)?;
        if let Some(expected) = baseline {
            assert_eq!(digest, expected, "model selection rewrote worker context");
        } else {
            baseline = Some(digest);
        }
    }
    // Optional guidance has one source in the seed, available through ordinary
    // skill discovery rather than an unconditional instruction frame.
    let plans = crate::seed::file("skills/plans/SKILL.md").unwrap();
    assert!(
        std::str::from_utf8(plans)
            .unwrap()
            .contains("volume:///home/GOAL.md")
    );
    assert_eq!(
        crate::seed::AGENT_PROMPT.len(),
        1023,
        "update the reviewed prompt byte pin"
    );
    assert!(!crate::seed::AGENT_PROMPT.contains("GOAL.md"));
    Ok(())
}

#[tokio::test]
async fn configuration_files_preserve_cached_prompt_prefix() -> Result<()> {
    let tools = crate::tool::ToolRegistry::default();
    let manifest = ContextManifest::new(DiscoveredContext::default(), Vec::new(), &tools)?;
    let mut baseline = None;
    // Configuration files are execution inputs. Adding or editing one must
    // retain its identity without adding a single byte to the worker's prompt.
    for body in [None, Some("feature off"), Some("feature on")] {
        // Each immutable snapshot owns its original file identities; publishing
        // another body at the same identity would correctly refuse.
        let payloads = TestPayloadStore::new()?;
        let mut selected = manifest.clone();
        if let Some(body) = body {
            selected.sources.push(
                payloads
                    .stage(
                        crate::OperationId::new(),
                        "feature.json",
                        body.as_bytes(),
                        "application/json",
                        "feature.json",
                        crate::conversation::ContentAccess::default(),
                    )?
                    .await?
                    .0,
            );
        }
        let reference = selected
            .publish(
                &payloads,
                crate::OperationId::new(),
                "manifest.json",
                payload_limits(),
            )
            .await?;
        let (_, stage) =
            ContextManifest::bind(reference, &payloads, tools.clone(), payload_limits()).await?;
        let projected = ContextPipeline::new([stage])
            .run(&input(), payload_limits(), None)
            .await?;
        let prefix: Vec<_> = projected
            .messages
            .iter()
            .take_while(|message| message.role == ModelRole::System)
            .collect();
        let digest = crate::executor::json::canonical_json_digest(&prefix, None, &mut None)?;
        if let Some(expected) = baseline {
            assert_eq!(digest, expected, "configuration rewrote the cached prefix");
        } else {
            baseline = Some(digest);
        }
        // Negative control: adding optional policy text to the worker is
        // observable even when the rest of the prefix stays identical.
        let mut changed = projected.messages.clone();
        changed.insert(0, text(ModelRole::System, "feature status"));
        let changed: Vec<_> = changed
            .iter()
            .take_while(|message| message.role == ModelRole::System)
            .collect();
        assert_ne!(
            crate::executor::json::canonical_json_digest(&changed, None, &mut None)?,
            digest
        );
    }
    Ok(())
}

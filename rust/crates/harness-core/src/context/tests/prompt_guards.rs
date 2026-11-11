//! Configuration inputs retain identity without rewriting the cached prefix.
use super::*;

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

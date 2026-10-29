use super::*;

#[derive(Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct RequestBackedProjectionControl {
    request: FileRef,
}
#[cfg(feature = "filesystem")]
impl PreparationRecord for RequestBackedProjectionControl {}

#[tokio::test]
async fn inline_projection_recovers_accounting_and_checkpoint_with_original_custody() -> Result<()>
{
    let storage =
        crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    let journal = journal.as_ref();
    let context = crate::context::Context {
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("paused on main".into()),
        }],
        current_input_index: Some(0),
        ..Default::default()
    };
    let checkpoint = stage_json(journal, OperationId::new(), "checkpoint", &context, None).await?;
    for compacted in [false, true] {
        let accounting = ContextAccounting {
            capacity: crate::context::ModelContextCapacity {
                context_tokens: 65_536,
                output_tokens: Some(4_096),
            },
            count: crate::context::ModelTokenCount {
                request_digest: [9; 32],
                input_tokens: 128,
                fixed_tokens: 0,
                message_tokens: vec![128],
            },
        };
        let fixture = serde_json::json!({
            "context": context, "canonical": null,
            "checkpoint": if compacted { Some(&checkpoint) } else { None },
            "accounting": accounting,
        });
        let bytes = crate::contract::canonical_json_bytes(&fixture)?;
        let rejected =
            crate::executor::payload::decode_json::<RequestBackedProjectionControl>(&bytes, None);
        assert!(
            rejected.is_err(),
            "request-backed reader accepted the inline envelope"
        );
        let control = serde_json::from_value::<RequestBackedProjectionControl>(
            serde_json::json!({"request": checkpoint}),
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(control.request, checkpoint);
        let reference = stage_json(
            journal,
            OperationId::new(),
            "main-projection",
            &fixture,
            None,
        )
        .await?;
        for native in [false, true] {
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(16 * 1024 * 1024, 16 * 1024 * 1024);
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let io = if native {
                PreparationIo::from_native_work(&work)?
            } else {
                PreparationIo::from_budget(budget, &cancellation)?
            };
            let (projection, hold) =
                load_projection(journal, &reference, Limits::default(), Some(&io)).await?;
            assert_eq!(projection.context, context);
            assert_eq!(
                projection.checkpoint.as_ref(),
                compacted.then_some(&checkpoint)
            );
            assert!(projection.working.is_none());
            assert_eq!(
                serde_json::to_value(&projection.accounting)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                fixture["accounting"]
            );
            drop(projection);
            drop(hold);
            assert_eq!(
                io.owned_bytes()?,
                0,
                "native={native} compacted={compacted}"
            );
        }
    }
    Ok(())
}

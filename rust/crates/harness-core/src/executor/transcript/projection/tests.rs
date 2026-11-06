use super::*;

#[tokio::test]
async fn inline_projection_recovers_accounting_and_checkpoint_with_original_custody() -> Result<()>
{
    let storage =
        crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    let journal = journal.as_ref();
    let mut context = crate::context::Context {
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("paused on main".into()),
        }],
        current_input_index: Some(0),
        ..Default::default()
    };
    let checkpoint = stage_json(journal, OperationId::new(), "checkpoint", &context, None).await?;
    context
        .metadata
        .insert("original".into(), checkpoint.clone());
    let prepared = crate::model::PreparedModelRequest::prepare(
        crate::model::ModelRequest {
            summary: false,
            compose_tools: false,
            model: crate::model::Model::new("fixture".into(), "projection".into(), "1".into())?,
            messages: context.messages.clone(),
            tools: Vec::new(),
            sampling: Default::default(),
            max_output_tokens: None,
        },
        Limits::default(),
        None,
    )?;
    for compacted in [false, true] {
        let accounting = ContextAccounting {
            capacity: crate::context::ModelContextCapacity {
                context_tokens: 65_536,
                output_tokens: Some(4_096),
            },
            count: crate::context::ModelTokenCount::from_upper_bounds(
                prepared.request_digest(),
                0,
                vec![128],
            ),
        };
        let expected_accounting =
            serde_json::to_value(&accounting).map_err(|error| Error::Invalid(error.to_string()))?;
        let projection = ResponseProjection {
            context: crate::context::Context {
                messages: Vec::new(),
                metadata: context.metadata.clone(),
                current_input_index: context.current_input_index,
            },
            steering: None,
            canonical: None,
            checkpoint: compacted.then(|| checkpoint.clone()),
            working: Some(checkpoint.clone()),
            accounting: None,
        };
        let reference = stage_projection(
            journal,
            OperationId::new(),
            "main-projection",
            &projection,
            &prepared,
            Some(&accounting),
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
            assert_eq!(projection.working.as_ref(), Some(&checkpoint));
            assert_eq!(
                serde_json::to_value(&projection.accounting)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                expected_accounting
            );
            let full_bytes = projection
                .decoded_owned_bytes()?
                .ok_or_else(|| invalid(Some(&io)))?;
            drop(projection);
            drop(hold);
            assert_eq!(
                io.owned_bytes()?,
                0,
                "native={native} compacted={compacted}"
            );
            let continuation =
                ResponseProjection::continuation_metadata(journal, &reference, Some(&io)).await?;
            assert!(continuation.context.messages.is_empty());
            assert_eq!(continuation.context.metadata, context.metadata);
            assert_eq!(
                continuation.context.current_input_index,
                context.current_input_index
            );
            assert_eq!(continuation.working.as_ref(), Some(&checkpoint));
            assert!(continuation.canonical.is_none());
            assert!(continuation.checkpoint.is_none());
            assert!(continuation.accounting.is_none());
            let retained = continuation
                .decoded_owned_bytes()?
                .ok_or_else(|| invalid(Some(&io)))?;
            assert!(retained < full_bytes);
            assert_eq!(io.owned_bytes()?, retained);
            let hold = io.guard_owned(retained)?;
            drop(continuation);
            drop(hold);
            assert_eq!(io.owned_bytes()?, 0);
        }
    }
    Ok(())
}

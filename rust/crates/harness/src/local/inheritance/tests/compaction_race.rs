use super::*;
use crate::test_support::ScriptedTurn;
use std::cell::RefCell;

thread_local! {
    // Consumed before awaiting: no borrow is held across real compaction I/O.
    static AFTER_CAPTURE: RefCell<Option<(TaskId, BoxProviderFuture<'static, Result<()>>)>> =
        const { RefCell::new(None) };
}

pub(in crate::local::inheritance) async fn after_capture(task: TaskId) -> Result<()> {
    let publish = AFTER_CAPTURE.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.as_ref().is_some_and(|(parent, _)| *parent == task) {
            slot.take().map(|(_, publish)| publish)
        } else {
            None
        }
    });
    if let Some(publish) = publish {
        publish.await?;
    }
    Ok(())
}

fn matches_observation(summary: &Value, selected: bool) -> bool {
    if selected {
        crate::context::SummarySelection::deserialize(summary)
            .is_ok_and(|selection| selection.through.is_some())
    } else {
        summary.is_null()
    }
}

#[tokio::test]
async fn first_compaction_admission_model_retains_only_the_captured_binding() -> Result<()> {
    // Exhaust first publication before capture, between capture and fallback,
    // and after admission. Fallback and value construction have no await; later
    // publication cannot change the value passed to immutable retention.
    for publication_cut in 0..3 {
        let directory = tempfile::tempdir().map_err(storage)?;
        let volume = super::super::super::tests::volume(directory.path()).await?;
        let marker = "FIRST_COMPACTION_ONLY_".repeat(256);
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text(marker.clone())]).with_context_capacity(
                crate::context::ModelContextCapacity {
                    context_tokens: 32_768,
                    output_tokens: Some(4096),
                },
            ),
        );
        let config = super::super::super::tests::volume_config(directory.path(), volume, provider)?
            .without_default_tools()
            .compaction(crate::context::CompactionPolicy::Disabled);
        let local = Arc::new(LocalHarness::open(config).await?);
        let parent = child(&local, None).await?;
        // Complete the compaction's separately admitted model command first.
        // Only publication races child admission; it never acquires admission.
        let operation = OperationId::new();
        let (receive, model) = local
            .stage_summary(operation, parent, "retain first summary")
            .await?;
        let task = crate::runtime::task_id_for_admission(operation);
        local.prepare_inheritance(task, None).await?;
        let task = super::super::super::commands::accepted(
            local.admit_turn(operation, None, receive, model).await?,
            operation,
        )?;
        let checkpoint =
            serde_json::from_value(local.resume(task).await?.metadata).map_err(storage)?;
        let compact = {
            let local = local.clone();
            Box::pin(async move {
                let (_, selected) = local
                    .publish_checkpoint(operation, parent, &checkpoint)
                    .await?;
                assert!(selected, "first compaction was not published");
                Ok(())
            }) as BoxProviderFuture<'static, Result<()>>
        };
        let mut later = None;
        match publication_cut {
            0 => compact.await?,
            1 => AFTER_CAPTURE.with(|slot| {
                assert!(slot.borrow_mut().replace((parent, compact)).is_none());
            }),
            _ => later = Some(compact),
        }
        let admitted = child(&local, Some(parent)).await?;
        if let Some(publish) = later {
            publish.await?;
        }
        assert!(AFTER_CAPTURE.with(|slot| slot.borrow().is_none()));
        let binding = read_inheritance(&local.task_files.inner, admitted, local.limits.file_bytes)
            .await?
            .unwrap();
        assert!(
            matches_observation(&binding["summary"], publication_cut == 0),
            "admission persisted context outside its captured observation: {binding}"
        );
        assert!(
            !serde_json::to_string(&binding)
                .map_err(storage)?
                .contains(&marker)
        );
        if publication_cut == 1 {
            // The former fallback itself: a fresh read resolves the newly
            // published checkpoint, and retention persists the entire text.
            let broken = crate::runtime::task_id_for_admission(OperationId::new());
            let summary = local.own_value(parent, "summary").await?.unwrap();
            assert!(summary.contains(&marker));
            retain_inheritance(
                &local.task_files.inner,
                broken,
                json!({"parent":parent,"model":null,"summary":summary}),
                local.limits.file_bytes,
            )
            .await?;
            let broken = read_inheritance(&local.task_files.inner, broken, local.limits.file_bytes)
                .await?
                .unwrap();
            assert!(
                !matches_observation(&broken["summary"], false),
                "fresh checkpoint resolution escaped the metadata detector"
            );
        }
        match Arc::try_unwrap(local) {
            Ok(local) => local.disconnect_unfinished().await?,
            Err(_) => panic!("compaction schedule retained the harness"),
        }
    }
    Ok(())
}

use super::*;
use crate::model::{ModelContentPart, ToolCall, ToolResultContent};

#[tokio::test]
async fn large_tool_outputs_keep_head_tail_and_full_files_while_old_previews_demote() -> Result<()>
{
    let storage = MemoryHarnessStorage::new(AgentId::new(), 262_144).await?;
    let body = format!("HEAD:{}:TAIL", "猫é".repeat(16_384));
    let source = storage
        .stage(
            OperationId::new(),
            "source.txt",
            body.as_bytes(),
            "text/plain",
            "source.txt",
        )
        .await?;
    let turns = (0..3)
        .map(|index| {
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                    format!("read-{index}"),
                    "read".into(),
                    serde_json::json!({"file":source}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ])
        })
        .chain([ScriptedTurn::Text("done".into())]);
    let provider = Arc::new(
        ScriptedModelProvider::new(turns).with_token_counter(|request| {
            Ok(crate::context::ModelTokenCount::from_upper_bounds(
                request.request_digest(),
                u32::try_from(request.bytes().len()).expect("bounded fixture request"),
                vec![0; request.request().messages.len()],
            ))
        }),
    );
    let bundle = storage
        .builder()
        .model(
            Model::new("test".into(), "bounded".into(), "1".into(), Value::Null)?,
            provider.clone(),
        )
        .grant("model:generate")
        .grant("tool:call:read")
        .compaction(crate::context::CompactionPolicy::Disabled)
        .build()?;
    assert_eq!(
        storage
            .run_prompt(&bundle, "read the source three times", Vec::new())
            .await?
            .text,
        "done"
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 4);
    let mut results = Vec::new();
    for message in &requests[3].messages {
        for part in message.content.parts() {
            if let ModelContentPart::ToolResult { content, .. } = part {
                results.push(content);
            }
        }
    }
    assert_eq!(results.len(), 3);
    let mut previews = Vec::new();
    for content in results {
        let ToolResultContent::Stored { file, preview } = content else {
            panic!("unbounded tool output reached the next request");
        };
        let full = storage.read(file).await?;
        let projection: Value = serde_json::from_slice(&full).unwrap();
        assert_eq!(projection["value"], body);
        previews.push(preview);
    }
    assert!(previews[0].is_empty());
    assert!(previews[1].len() <= 2048);
    assert!(previews[2].len() <= crate::executor::TOOL_OUTPUT_PREVIEW_BYTES);
    assert!(previews[2].contains("HEAD:") && previews[2].contains(":TAIL"));
    assert!(
        requests[3]
            .messages
            .iter()
            .map(|m| crate::contract::canonical_json_bytes(m).unwrap().len())
            .sum::<usize>()
            < 32_768
    );
    let ToolResultContent::Stored { file, .. } = results_from_request(&requests[3])[0] else {
        unreachable!()
    };
    let retained = file.clone();
    let foreign = FileRef::new(
        VolumeRef::new(
            retained.volume().provider().clone(),
            "foreign".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::new()),
            &mut None,
        )?,
        retained.path().to_owned(),
        retained.version().to_owned(),
        retained.descriptor().clone(),
        retained.display_name().to_owned(),
        &mut None,
    )?;
    run_admitted_file_case(&storage, Limits::default(), move |context| {
        let retained = retained.clone();
        let foreign = foreign.clone();
        async move {
            context.authorize_file_reference(&retained, None)?;
            assert!(matches!(
                context.authorize_file_reference(&foreign, None),
                Err(Error::Unauthorized(_))
            ));
            Ok(())
        }
    })
    .await?;
    Ok(())
}

fn results_from_request(request: &crate::model::ModelRequest) -> Vec<&ToolResultContent> {
    request
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            ModelContentPart::ToolResult { content, .. } => Some(content),
            _ => None,
        })
        .collect()
}

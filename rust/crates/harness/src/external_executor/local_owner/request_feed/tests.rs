use super::*;
use crate::{
    external_executor::{ExternalHost, request},
    task_volume::{Component, TaskVolume},
};
use acyclic_stream::MemoryStream;
use futures::StreamExt;
use proptest::prelude::*;
use serde_json::json;

fn tool(index: usize) -> ExternalRequest {
    request(
        ExternalHost::Pi,
        "tool_call",
        json!({
            "sessionId":"history", "toolCallId":format!("tool-{index}"),"toolName":"write",
            "input":{"path":"file.txt","content":format!("{index}")},
        }),
    )
    .unwrap()
}

async fn publish(files: &TaskVolume<MemoryStream>, request: &ExternalRequest) -> Result<()> {
    let file = request
        .to_file(Component::new(request.task_id().to_string()).map_err(storage)?)
        .map_err(storage)?;
    files
        .append(files.head().await.map_err(storage)?, &file)
        .await
        .map_err(storage)?;
    Ok(())
}

#[tokio::test]
async fn accepted_request_history_beyond_one_snapshot_page_replays_in_order() -> Result<()> {
    let provider = Arc::new(MemoryStream::default());
    let files = TaskVolume::open(
        provider.clone(),
        StreamPath::new("request-log").map_err(storage)?,
    )
    .await
    .map_err(storage)?;
    let attach = request(
        ExternalHost::Pi,
        "session_start",
        json!({"sessionId":"history","cwd":"/project"}),
    )?;
    publish(&files, &attach).await?;
    for index in 0..10_000 {
        publish(&files, &tool(index)).await?;
    }
    let mut accepted = follow(provider.as_ref(), files.path().clone()).await?;
    assert_eq!(accepted.next().await.unwrap()?, attach);
    for index in 0..10_000 {
        assert_eq!(accepted.next().await.unwrap()?, tool(index));
    }
    let next = tool(10_000);
    publish(&files, &next).await?;
    assert_eq!(accepted.next().await.unwrap()?, next);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    // Acceptance order is independent of operation UUID ordering and of whether
    // a publication precedes or follows construction of the live reader.
    #[test]
    fn replay_and_live_delivery_preserve_every_original_record(prefix in 0usize..24, suffix in 1usize..12) {
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
            let provider = Arc::new(MemoryStream::default());
            let files = TaskVolume::open(provider.clone(), StreamPath::new("property").unwrap()).await.unwrap();
            for index in 0..prefix {publish(&files, &tool(index)).await.unwrap();}
            let mut accepted = follow(provider.as_ref(), files.path().clone()).await.unwrap();
            for index in prefix..prefix+suffix {publish(&files, &tool(index)).await.unwrap();}
            for index in 0..prefix+suffix {assert_eq!(accepted.next().await.unwrap().unwrap(), tool(index));}
            let expected = tool(prefix+suffix);
            let file = expected.to_file(Component::new(expected.task_id().to_string()).unwrap()).unwrap();
            let record = Record {sequence:1, value:file.record_value(None).unwrap(),
                commit_id:Default::default(), committed_at_micros:0};
            assert_eq!(decode(&record).unwrap(), Some(expected));
            let forged = tool(prefix+suffix);
            let file = forged.to_file(Component::new("another-task").unwrap()).unwrap();
            let record = Record {sequence:1, value:file.record_value(None).unwrap(),
                commit_id:Default::default(), committed_at_micros:0};
            assert!(matches!(decode(&record), Err(Error::Unauthorized(_))));
            files.append(files.head().await.unwrap(), &file).await.unwrap();
            let mut invalid = tool(prefix+suffix).to_file(Component::new(forged.task_id().to_string()).unwrap()).unwrap();
            invalid.bytes = b"{malformed".as_slice().into();
            files.append(files.head().await.unwrap(), &invalid).await.unwrap();
            let sentinel = tool(prefix+suffix+1);
            publish(&files,&sentinel).await.unwrap();
            assert_eq!(accepted.next().await.unwrap().unwrap(),sentinel);
        });
    }
}

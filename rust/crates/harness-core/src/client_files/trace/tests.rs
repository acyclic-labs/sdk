use super::*;
use acyclic_stream::{
    MemoryStream,
    trace::{Mechanism, Outcome, SpanEvent, SpanId, TraceRecord},
};
use futures::StreamExt as _;
use std::sync::Arc;

#[tokio::test]
async fn bounded_trace_files_preserve_captured_cuts_and_follow_original_sources()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(MemoryStream::default());
    let agent = AgentPath::root(&crate::agent::AgentId::from_bytes([39; 16]));
    let path = source(&agent)?;
    let stream = acyclic_stream::Stream::new(provider.clone(), path.as_str())?;
    let files = TraceFiles;
    let requested = "live/agents/self/trace/0/2.json";
    assert_eq!(
        <TraceFiles as FileView<MemoryStream>>::source(
            &files,
            &agent,
            FileRoot::Private {},
            requested,
        )?,
        Some(path.clone())
    );
    assert_eq!(
        <TraceFiles as FileView<MemoryStream>>::source(
            &files,
            &agent,
            FileRoot::Home {},
            requested,
        )?,
        None
    );
    let id = SpanId {
        process: [1; 16],
        sequence: 0,
    };
    for event in [
        SpanEvent::Started {
            id,
            parent: None,
            name: "wait".into(),
            mechanism: Mechanism::Wait,
            at_ns: 10,
            guarantee: None,
        },
        SpanEvent::Finished {
            id,
            at_ns: 30,
            bytes: None,
            result: Outcome::Unsettled,
        },
    ] {
        stream
            .append(
                serde_json::to_vec(&TraceRecord::V1(event))?,
                acyclic_stream::IdempotencyKey::unique(),
            )
            .await?;
    }
    let mut follow = stream.follow(2, Default::default()).await?;
    // A later bad publication cannot contaminate an earlier captured cut.
    stream
        .append(
            b"bad JSON".to_vec(),
            acyclic_stream::IdempotencyKey::unique(),
        )
        .await?;
    let original = follow.next().await.ok_or("follow ended")??;
    assert_eq!(original.sequence, 2);
    let values = files
        .read(
            provider.as_ref(),
            &agent,
            &[requested],
            &path,
            2,
            Default::default(),
        )
        .await?;
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].input.sequence, 1);
    let value: serde_json::Value = serde_json::from_str(&values[0].text)?;
    assert_eq!(value["coveredFrom"], 0);
    assert_eq!(value["coveredThrough"], 2);
    assert_eq!(value["caughtUp"], true);
    assert_eq!(value["mechanisms"]["wait"]["elapsed_ns"], 20);
    assert_eq!(value["mechanisms"]["wait"]["unsettled"], 1);
    let suffix = files
        .read(
            provider.as_ref(),
            &agent,
            &["live/agents/self/trace/1/1.json"],
            &path,
            2,
            Default::default(),
        )
        .await?;
    let value: serde_json::Value = serde_json::from_str(&suffix[0].text)?;
    assert_eq!(value["coveredFrom"], 1);
    assert_eq!(value["unpairedFinishes"], 1);
    assert_eq!(value["mechanisms"], serde_json::json!({}));
    let error = files
        .read(
            provider.as_ref(),
            &agent,
            &["live/agents/self/trace/2/1.json"],
            &path,
            3,
            Default::default(),
        )
        .await
        .err()
        .ok_or("malformed original row was accepted")?;
    let mut cause: &(dyn std::error::Error + 'static) = &error;
    loop {
        if let Some(acyclic_stream::trace::QueryError::Decode { evidence, .. }) =
            cause.downcast_ref()
        {
            assert_eq!(evidence.commit_id, original.commit_id);
            assert_eq!(evidence.sequence, original.sequence);
            assert_eq!(evidence.path, original.origin);
            break;
        }
        cause = cause.source().ok_or("original decoder cause was lost")?;
    }
    Ok(())
}

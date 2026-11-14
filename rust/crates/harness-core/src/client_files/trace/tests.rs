use super::*;
use acyclic_stream::{
    MemoryStream, StreamProviderExt as _,
    trace::{Mechanism, Outcome, SpanEvent, SpanId, TraceRecord},
};
use futures::StreamExt as _;
use std::sync::Arc;

#[tokio::test]
async fn original_comparison_publications_project_through_trace_files()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    use acyclic_stream::trace::{Comparison, ComparisonKind, Recorder};
    let provider = Arc::new(MemoryStream::default());
    let agent = AgentPath::root(&crate::agent::AgentId::from_bytes([40; 16]));
    let path = source(&agent)?;
    let stream = acyclic_stream::Stream::new(provider.clone(), path.as_str())?;
    let originals = acyclic_stream::Stream::new(provider.clone(), "original-descriptor")?;
    let digest = [19_u8; 32];
    let acyclic_stream::AppendOutcome::Committed(receipt) = originals
        .append(
            serde_json::to_vec(&digest)?,
            acyclic_stream::IdempotencyKey::unique(),
        )
        .await?
    else {
        return Err("original publication missing".into());
    };
    let previous = acyclic_stream::RetainedRecord {
        commit_id: receipt.commit_id,
        path: originals.path().clone(),
        sequence: receipt.start,
    };
    let recorder = Arc::new(Recorder::new(stream, [10; 16]));
    let recording = recorder
        .start(
            "model.request",
            Mechanism::ProviderCall,
            None,
            Some("original_request_identity"),
        )
        .await?;
    for kind in [
        ComparisonKind::Request,
        ComparisonKind::Output,
        ComparisonKind::CachePrefix,
    ] {
        recording
            .compare(Comparison {
                kind,
                previous: previous.clone(),
                previous_digest: digest,
                current_digest: if kind == ComparisonKind::CachePrefix {
                    [20; 32]
                } else {
                    digest
                },
            })
            .await?;
    }
    recording.finish(None, Outcome::Succeeded).await?;
    let files = TraceFiles::new(acyclic_stream::trace::Registry::new(provider.clone()));
    // Exclude the owning start/finish: equality needs its original descriptor
    // evidence, but cannot depend on a complete span or a history scan.
    let values = files
        .read(
            provider.as_ref(),
            &agent,
            &["live/agents/self/trace/1/3.json"],
            &path,
            5,
            Default::default(),
        )
        .await?;
    let value: serde_json::Value = serde_json::from_str(&values[0].text)?;
    for field in ["repeatedCalls", "repeatedOutputs", "cachePrefixBreaks"] {
        assert_eq!(value[field], 1);
    }
    assert_eq!(value["coveredFrom"], 1);
    assert_eq!(value["coveredThrough"], 4);
    assert_eq!(value["activeSpans"], 0);
    assert!(
        value["mechanisms"]
            .as_object()
            .ok_or("mechanisms absent")?
            .is_empty()
    );
    assert_eq!(
        value["comparisons"]["cache_prefix"]["last_different"]["sequence"],
        3
    );
    Ok(())
}

#[tokio::test]
async fn bounded_trace_files_preserve_captured_cuts_and_follow_original_sources()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(MemoryStream::default());
    let agent = AgentPath::root(&crate::agent::AgentId::from_bytes([39; 16]));
    let path = source(&agent)?;
    let stream = acyclic_stream::Stream::new(provider.clone(), path.as_str())?;
    let files = TraceFiles::new(acyclic_stream::trace::Registry::new(provider.clone()));
    let requested = "live/agents/self/trace/0/2.json";
    assert_eq!(
        <TraceFiles<MemoryStream> as FileView<MemoryStream>>::source(
            &files,
            &agent,
            FileRoot::Private {},
            requested,
        )?,
        Some(path.clone())
    );
    assert_eq!(
        <TraceFiles<MemoryStream> as FileView<MemoryStream>>::source(
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
    assert!(value["mechanisms"]["wait"]["outliers"].is_null());
    for (threshold, outliers) in [(19, 1), (20, 0)] {
        let selected = format!("{PREFIX}0/2/outliers/wait/{threshold}.json");
        let values = files
            .read(
                provider.as_ref(),
                &agent,
                &[&selected],
                &path,
                2,
                Default::default(),
            )
            .await?;
        let policy: serde_json::Value = serde_json::from_str(&values[0].text)?;
        assert_eq!(policy["coveredThrough"], 2);
        assert_eq!(policy["mechanisms"]["wait"]["elapsed_ns"], 20);
        assert_eq!(policy["mechanisms"]["wait"]["outliers"], outliers);
    }
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

    // Pair a start in the first maximum-sized provider page with its finish
    // in the next. The caller's window is larger than one protocol page.
    let paired = SpanId { sequence: 1, ..id };
    let mut batch = Vec::new();
    for index in 0..=acyclic_stream::MAX_ITEMS {
        let event = if index == 0 {
            SpanEvent::Started {
                id: paired,
                parent: None,
                name: "wait".into(),
                mechanism: Mechanism::Wait,
                at_ns: 40,
                guarantee: None,
            }
        } else {
            SpanEvent::Finished {
                id: if index == acyclic_stream::MAX_ITEMS {
                    paired
                } else {
                    SpanId {
                        sequence: index as u64 + 1,
                        ..id
                    }
                },
                at_ns: 60,
                bytes: None,
                result: Outcome::Unsettled,
            }
        };
        batch.push(
            acyclic_stream::OwnedBytes::from(serde_json::to_vec(&TraceRecord::V1(event))?).into(),
        );
        if batch.len() == acyclic_stream::MAX_ITEMS || index == acyclic_stream::MAX_ITEMS {
            provider
                .append(acyclic_stream::AppendRequest {
                    path: path.clone(),
                    records: std::mem::take(&mut batch),
                    if_tail: None,
                    idempotency_key: acyclic_stream::IdempotencyKey::unique(),
                })
                .await?;
        }
    }
    let requested = format!("{PREFIX}3/{}.json", acyclic_stream::MAX_ITEMS + 1);
    let through = acyclic_stream::MAX_ITEMS as u64 + 4;
    let values = files
        .read(
            provider.as_ref(),
            &agent,
            &[&requested],
            &path,
            through,
            Default::default(),
        )
        .await?;
    let value: serde_json::Value = serde_json::from_str(&values[0].text)?;
    assert_eq!(value["coveredThrough"], through);
    assert_eq!(value["activeWaits"], 0);
    assert_eq!(value["mechanisms"]["wait"]["completed"], 1);
    assert_eq!(value["mechanisms"]["wait"]["elapsed_ns"], 20);
    assert_eq!(value["unpairedFinishes"], acyclic_stream::MAX_ITEMS - 1);
    Ok(())
}

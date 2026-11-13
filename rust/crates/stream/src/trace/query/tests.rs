use super::*;
use crate::{AppendOutcome, IdempotencyKey, MemoryStream, OwnedBytes};
use std::sync::Arc;

async fn append(
    stream: &crate::Stream<MemoryStream>,
    event: SpanEvent,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = stream
        .append(
            OwnedBytes::from(serde_json::to_vec(&TraceRecord::V1(event))?),
            IdempotencyKey::unique(),
        )
        .await?;
    assert!(matches!(result, AppendOutcome::Committed(_)));
    Ok(())
}

#[tokio::test]
async fn paged_and_live_queries_preserve_clock_domains_coverage_and_settlement()
-> Result<(), Box<dyn std::error::Error>> {
    let stream = crate::Stream::new(Arc::new(MemoryStream::default()), "agents/worker/trace")?;
    let wait = SpanId {
        process: [1; 16],
        sequence: 0,
    };
    let io = SpanId {
        process: [2; 16],
        sequence: 0,
    };
    append(
        &stream,
        SpanEvent::Started {
            id: wait,
            parent: None,
            name: "wait".into(),
            mechanism: Mechanism::Wait,
            at_ns: 10,
            guarantee: None,
        },
    )
    .await?;
    let mut page = TraceQuery::new(stream.read(0, 1).await?, Default::default());
    assert!(matches!(
        page.next().await?,
        Some(Observation::Started {
            missing_guarantee: true,
            ..
        })
    ));
    assert!(page.next().await?.is_none());
    assert_eq!(page.active_waits(), 1);
    assert!(page.totals().is_empty(), "EOF cannot settle an active wait");

    let mut live = TraceQuery::new(
        stream.follow(0, Default::default()).await?,
        BTreeMap::from([(Mechanism::Io, 10), (Mechanism::Wait, 100)]),
    );
    assert!(matches!(
        live.next().await?,
        Some(Observation::Started { .. })
    ));
    append(
        &stream,
        SpanEvent::Started {
            id: io,
            parent: Some(wait),
            name: "read".into(),
            mechanism: Mechanism::Io,
            at_ns: 1000,
            guarantee: Some("committed_prefix".into()),
        },
    )
    .await?;
    assert!(matches!(
        live.next().await?,
        Some(Observation::Started {
            missing_guarantee: false,
            ..
        })
    ));
    append(
        &stream,
        SpanEvent::Finished {
            id: io,
            at_ns: 1015,
            bytes: Some(64),
            result: Outcome::Succeeded,
        },
    )
    .await?;
    assert!(matches!(
        live.next().await?,
        Some(Observation::Completed {
            elapsed_ns: 15,
            outlier: Some(true),
            ..
        })
    ));
    append(
        &stream,
        SpanEvent::Finished {
            id: wait,
            at_ns: 30,
            bytes: None,
            result: Outcome::Unsettled,
            outlier: Some(false),
        },
    )
    .await?;
    assert!(matches!(
        live.next().await?,
        Some(Observation::Completed {
            elapsed_ns: 20,
            result: Outcome::Unsettled,
            ..
        })
    ));
    assert_eq!(live.active_waits(), 0);
    assert_eq!(live.active().count(), 0);
    let totals = &live.totals()[&Mechanism::Io];
    assert_eq!(
        (totals.completed, totals.elapsed_ns, totals.known_bytes),
        (1, 15, 64)
    );
    let (duration, original) = totals.longest.as_ref().ok_or("missing largest span")?;
    assert_eq!((*duration, original.sequence), (15, 1));
    assert_eq!(original.path, *stream.path());
    assert_eq!(totals.outliers, Some(1));
    let waits = &live.totals()[&Mechanism::Wait];
    assert_eq!((waits.unsettled, waits.unknown_bytes), (1, 1));
    assert_eq!(waits.outliers, Some(0));
    assert_eq!(live.missing_guarantees(), 1);
    let mut unclassified = TraceQuery::new(stream.read(1, 2).await?, Default::default());
    unclassified.next().await?;
    assert!(matches!(
        unclassified.next().await?,
        Some(Observation::Completed { outlier: None, .. })
    ));
    assert_eq!(unclassified.totals()[&Mechanism::Io].outliers, None);

    // A finish in another producer domain cannot pair merely by sequence.
    append(
        &stream,
        SpanEvent::Finished {
            id: SpanId {
                process: [3; 16],
                sequence: 0,
            },
            at_ns: 99,
            bytes: Some(1),
            result: Outcome::Succeeded,
        },
    )
    .await?;
    assert!(matches!(
        live.next().await?,
        Some(Observation::UnpairedFinish { .. })
    ));
    assert_eq!(live.unpaired_finishes(), 1);
    assert_eq!(live.totals()[&Mechanism::Io].completed, 1);

    stream
        .append(
            OwnedBytes::from(b"not JSON".to_vec()),
            IdempotencyKey::unique(),
        )
        .await?;
    let error = live.next().await.unwrap_err();
    let QueryError::Decode { evidence, source } = error else {
        panic!("lost original decode cause")
    };
    assert_eq!(evidence.sequence, 5);
    assert!(source.is_syntax());

    let next = SpanId {
        process: [1; 16],
        sequence: 1,
    };
    for _ in 0..2 {
        append(
            &stream,
            SpanEvent::Started {
                id: next,
                parent: None,
                name: "wait".into(),
                mechanism: Mechanism::Wait,
                at_ns: 40,
                guarantee: None,
            },
        )
        .await?;
    }
    live.next().await?;
    assert!(matches!(
        live.next().await,
        Err(QueryError::Invalid {
            source: QueryViolation::RepeatedStart,
            ..
        })
    ));
    append(
        &stream,
        SpanEvent::Finished {
            id: next,
            at_ns: 39,
            bytes: None,
            result: Outcome::Succeeded,
        },
    )
    .await?;
    assert!(matches!(
        live.next().await,
        Err(QueryError::Invalid {
            source: QueryViolation::ReversedTime,
            ..
        })
    ));
    assert_eq!(live.active_waits(), 1);
    assert_eq!(live.missing_guarantees(), 2);
    assert_eq!(live.totals()[&Mechanism::Wait].completed, 1);
    append(
        &stream,
        SpanEvent::Finished {
            id: next,
            at_ns: 50,
            bytes: Some(0),
            result: Outcome::Succeeded,
        },
    )
    .await?;
    assert!(matches!(
        live.next().await?,
        Some(Observation::Completed { elapsed_ns: 10, .. })
    ));
    assert_eq!(live.active_waits(), 0);
    assert_eq!(live.totals()[&Mechanism::Wait].elapsed_ns, 30);
    Ok(())
}

use super::*;
use acyclic_stream::{
    AppendOutcome, AppendRequest, IdempotencyKey, MemoryStream, ReadRequest, StreamError,
    StreamProviderExt,
};
use futures::TryStreamExt;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn terminal_source_releases_the_follow_without_reading_a_later_record()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = MemoryStream::default();
    let path = StreamPath::new("monitor/source")?;
    let AppendOutcome::Committed(published) = provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![
                acyclic_stream::AppendRecord::latest("first", "first", false)?,
                acyclic_stream::AppendRecord::latest("later", "later", false)?,
            ],
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::new(b"terminal-monitor".to_vec())?,
        })
        .await?
    else {
        return Err("source publication refused".into());
    };
    let definition = Monitor {
        version: 1,
        id: IntentId::try_from("terminal".to_owned())?,
        source: path.as_str().into(),
        cursor: MonitorCursor {
            next: 0,
            preceding: None,
        },
        filter: MonitorFilter::default(),
        wake: true,
        deadline_unix_ms: None,
        coalesce_key: None,
    };
    for fail in [false, true] {
        let source = provider
            .read(ReadRequest {
                key: None,
                path: path.clone(),
                from: 0,
                limit: 1,
                at_least: Default::default(),
            })
            .await?;
        let updates: FollowStream = if fail {
            // A real short at-most read cannot satisfy this required range.
            // The following original record is the negative control: a caller
            // continuing after the source error would incorrectly deliver it.
            let later = provider
                .read(ReadRequest {
                    key: None,
                    path: path.clone(),
                    from: 1,
                    limit: 1,
                    at_least: Default::default(),
                })
                .await?;
            Box::pin(
                acyclic_stream::pages::ExactRange::new(source, 0..2)?
                    .chain(later)
                    .map_ok(FollowUpdate::Record),
            )
        } else {
            Box::pin(source.map_ok(FollowUpdate::Record))
        };
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = polls.clone();
        let mut updates = updates;
        let mut follow = MonitorFollow {
            definition: definition.clone(),
            updates: Some(Box::pin(futures::stream::poll_fn(move |cx| {
                observed.fetch_add(1, Ordering::SeqCst);
                updates.poll_next_unpin(cx)
            }))),
        };
        let first = follow.next().await?.ok_or("source ended early")?;
        assert_eq!(first.cursor.next, 1);
        assert!(first.notice.is_some());
        if fail {
            assert!(matches!(
                follow.next().await,
                Err(Error::Stream(StreamError::Unavailable))
            ));
        } else {
            assert!(follow.next().await?.is_none());
        }
        let at_end = polls.load(Ordering::SeqCst);
        assert!(follow.next().await?.is_none());
        assert_eq!(polls.load(Ordering::SeqCst), at_end);
        assert_eq!(follow.definition.cursor.next, 1);
    }

    // A real later record cannot silently stand in for skipped source progress.
    // Conversely, replaying an earlier record cannot advance a committed cursor.
    for (next, from) in [(0, 1), (1, 0)] {
        let source = provider
            .read(ReadRequest {
                key: None,
                path: path.clone(),
                from,
                limit: 1,
                at_least: Default::default(),
            })
            .await?;
        let mut selected = definition.clone();
        selected.cursor.next = next;
        selected.cursor.preceding = (next != 0).then_some(*published.commit_id.as_bytes());
        selected.validate()?;
        let mut follow = MonitorFollow {
            definition: selected,
            updates: Some(Box::pin(source.map_ok(FollowUpdate::Record))),
        };
        assert!(matches!(follow.next().await, Err(Error::Record(_))));
        assert!(follow.next().await?.is_none());
        assert_eq!(follow.definition.cursor.next, next);
    }
    Ok(())
}

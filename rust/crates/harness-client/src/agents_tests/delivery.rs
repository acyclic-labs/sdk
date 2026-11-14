//! Real session delivery, the shared Markdown renderer and paged history.
use super::*;
use crate::reader::{Driver, Priority, Want};
use crate::{Reader, ReaderPolicy};
use std::task::Poll;

fn render(value: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    // This is the same Markdown implementation the native TUI uses.
    let mut markdown = acyclic_markdown::Markdown::default();
    markdown.push(std::str::from_utf8(value)?);
    Ok(markdown
        .lines()
        .iter()
        .flat_map(|line| line.spans.iter().map(|span| span.text.as_str()))
        .collect())
}

#[tokio::test]
async fn oversize_records_render_and_evicted_history_pages_back()
-> Result<(), Box<dyn std::error::Error>> {
    delivery(false).await
}

#[tokio::test]
async fn oversize_latest_values_render_and_evict_by_page() -> Result<(), Box<dyn std::error::Error>>
{
    delivery(true).await
}

async fn delivery(latest: bool) -> Result<(), Box<dyn std::error::Error>> {
    let memory = Arc::new(MemoryStream::default());
    let path = StreamPath::new(format!("reader/oversize-{latest}"))?;
    // Stay inside the canonical Streams record frame. This value exceeds the
    // former caller-selected 1 KiB display budget; a full page also exceeds
    // the former default 1 MiB budget.
    let text = "x".repeat(16 * 1024 + 1);
    let other_text = "y".repeat(text.len());
    let huge = acyclic_stream::OwnedBytes::from(text.clone().into_bytes());
    let other = acyclic_stream::OwnedBytes::from(other_text.clone().into_bytes());
    seed(&memory, &path, &huge, &other).await?;
    let pool = Client::with_transport(
        Arc::new(Endpoint(memory.clone())),
        Arc::new(|_| Box::pin(async { Err(StreamError::Unavailable.into()) })),
        Arc::new(PlatformExecutor),
    );
    pool.register_credential(0, session::testing::credential())?;
    let mut request = subscription(&path, Default::default(), 0, latest);
    request.filter = None;
    if let Some(wire::session_subscription::Kind::Follow(follow)) = request.kind.as_mut() {
        follow.bootstrap_head = false;
    }
    let mut session = WireSession::with_view_policy(
        pool,
        DemandPolicy::default(),
        crate::StreamViewPolicy::new(128)?,
    );
    session.replace_streams([(path.clone(), request)])?;
    let mut one_charge = 0;
    for delivered in 1usize..=130 {
        let frame = loop {
            let frame = poll_fn(|cx| session.poll_stream(&path, cx))
                .await?
                .ok_or("oversize follow ended")?;
            match frame.body {
                Some(wire::session_frame::Body::Data(_)) => break frame,
                Some(wire::session_frame::Body::Closed(_)) => {
                    return Err("oversize follow closed".into());
                }
                _ => {}
            }
        };
        let view = session.view(&path).ok_or("missing display page")?;
        assert_eq!(view.next_sequence(), u64::try_from(delivered)?);
        if delivered == 1 {
            let value = if latest {
                &view
                    .latest_value(b"key000")
                    .ok_or("missing latest value")?
                    .value
            } else {
                &view.record(0).ok_or("missing record")?.value
            };
            assert_eq!(render(value)?, text, "oversized display was truncated");
            one_charge = view.retained_bytes();
            let Some(wire::session_frame::Body::Data(data)) = frame.body else {
                unreachable!()
            };
            let original = match data.item.ok_or("missing delivered value")? {
                wire::session_data::Item::Record(record) => record.value,
                wire::session_data::Item::LatestValue(value) => value.value,
                _ => return Err("unexpected stream delivery".into()),
            };
            assert_eq!(original.as_ref(), huge.as_slice());
        }
        if latest {
            assert_eq!(view.latest_value(b"key000").is_some(), delivered <= 128);
            assert_eq!(view.records().len(), 0);
        } else {
            assert_eq!(view.records().len(), delivered.min(128));
            assert_eq!(
                view.first_sequence(),
                u64::try_from(delivered.saturating_sub(128))?
            );
        }
        assert_eq!(view.retained_bytes(), delivered.min(128) * one_charge,);
        if delivered > 128 {
            assert!(view.retained_bytes() > 1024 * 1024);
            assert!(session.retained_bytes() < 128 * 1024);
        }
        assert!(session.is_following(&path), "eviction ended delivery");
    }
    if !latest {
        backfill(memory, &path, &text, &other_text).await?;
        assert_eq!(
            session
                .view(&path)
                .ok_or("missing live page")?
                .next_sequence(),
            130
        );
    }
    Ok(())
}

async fn seed(
    memory: &MemoryStream,
    path: &StreamPath,
    huge: &acyclic_stream::OwnedBytes,
    other: &acyclic_stream::OwnedBytes,
) -> Result<(), StreamError> {
    // Fill more than one default display page, with the oversized row first.
    for start in (0..130).step_by(32) {
        append(
            memory,
            path.clone(),
            (start..(start + 32).min(130))
                .map(|index| {
                    AppendRecord::latest(
                        if index == 0 {
                            huge.clone()
                        } else {
                            other.clone()
                        },
                        format!("key{index:03}"),
                        true,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
        .await?;
    }
    Ok(())
}

// Re-fetch through the production Reader and Driver, with no secondary
// history buffer. Zero cache bytes forces page-only pass-throughs even
// for the oversized original record.
async fn backfill(
    memory: Arc<MemoryStream>,
    path: &StreamPath,
    text: &str,
    other_text: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut reader = Reader::new(ReaderPolicy {
        cache_bytes: 0,
        read_limit: 1,
        prefetch: false,
        ..ReaderPolicy::default()
    })?;
    let mut driver = Driver::new(memory);
    driver.attach(&mut reader, 0);
    for sequence in [0, 1, 0] {
        let want = Want::Range {
            stream: path.clone(),
            from: sequence,
            to: sequence + 1,
        };
        reader.plan([(want.clone(), Priority::Visible)]);
        poll_fn(|cx| {
            let before = reader.generation();
            driver.step(&mut reader, 0, cx);
            assert!(reader.issue(path).is_none(), "backfill refused");
            if reader.cache().record(path, sequence).is_some() {
                return Poll::Ready(());
            }
            if reader.generation() != before {
                reader.plan([(want.clone(), Priority::Visible)]);
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        })
        .await;
        assert_eq!(reader.cache().bytes(), 0);
        let restored = reader
            .cache()
            .record(path, sequence)
            .ok_or("missing backfill")?;
        assert_eq!(
            render(restored)?,
            if sequence == 0 { text } else { other_text }
        );
        assert!(reader.cache().record(path, 1 - sequence).is_none());
    }
    Ok(())
}

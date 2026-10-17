use super::*;
use std::{future::Future, hint::black_box, task::Poll};

// Measure only the poll that completes registration. The real public follow
// future runs unchanged; dial/registration allocations happen on earlier polls,
// and other Tokio tasks cannot execute within this synchronous measurement.
async fn completion_allocations(
    open: impl Future<Output = Result<RecordStream, StreamError>>,
) -> Result<acyclic_test_support::AllocationInfo, StreamError> {
    let mut open = std::pin::pin!(open);
    let (records, allocation) = future::poll_fn(|cx| {
        let mut outcome = Poll::Pending;
        let allocation = acyclic_test_support::measure(|| outcome = open.as_mut().poll(cx));
        match outcome {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => Poll::Ready(result.map(|records| (records, allocation))),
        }
    })
    .await?;
    drop(black_box(records));
    Ok(allocation)
}

// Restore precisely the reviewed 9f38e976e projection shape: boxed received
// stream, boxed canonical data projection, then boxed typed-record projection.
// It uses the same real registration, cursor, credit and observation as today.
async fn reviewed_follow(client: &Client, path: StreamPath) -> Result<RecordStream, StreamError> {
    drop(black_box(StreamPath::new(path.as_str())?));
    let received = client
        .open_kind(
            Kind::Follow(wire::SessionFollow {
                path: path.to_string(),
                delivery: wire::SessionDelivery::All as i32,
                ..Default::default()
            }),
            1,
            true,
        )
        .await?
        .boxed();
    let data = received
        .map(|item| match item? {
            Received::Data(data) => Ok(data),
            _ => Err(StreamError::Unavailable),
        })
        .boxed();
    Ok(data
        .map(|data| match data?.item {
            Some(Item::Record(record)) => wire_codec::record(record),
            _ => Err(StreamError::Unavailable),
        })
        .boxed())
}

fn public_box_budget(allocations: u64, follows: u64) {
    assert_eq!(
        allocations, follows,
        "typed follow must allocate only the final public stream box"
    );
}

async fn paired() -> Result<(u64, u64), Box<dyn std::error::Error + Send + Sync>> {
    const FOLLOWS: u64 = 128;
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", 1).await?;
    let fixture = Fixture::open(provider, "node").await?;
    let client = Client::open(
        fixture.channel.clone(),
        Some("Bearer fixture".parse()?),
        None,
        None,
        None,
    );
    // Keep one interest alive so both arms share one warm physical Session.
    let anchor = client.follow(StreamPath::new("events")?, 1).await?;
    acyclic_test_support::measure(|| {});
    let mut before = (0, 0);
    let mut after = (0, 0);
    for index in 0..FOLLOWS {
        // Alternate the order to expose a dependency on earlier arm setup.
        for legacy in if index % 2 == 0 {
            [true, false]
        } else {
            [false, true]
        } {
            let path = StreamPath::new("events")?;
            let allocation = if legacy {
                completion_allocations(reviewed_follow(&client, path)).await?
            } else {
                completion_allocations(client.follow(path, 1)).await?
            };
            let total = if legacy { &mut before } else { &mut after };
            total.0 += allocation.count_total;
            total.1 += allocation.bytes_total;
            if !legacy {
                public_box_budget(allocation.count_total, 1);
            }
        }
    }
    assert_eq!(before.0, 3 * FOLLOWS);
    public_box_budget(after.0, FOLLOWS);
    assert_eq!(
        before.1 - after.1,
        2 * FOLLOWS * std::mem::size_of::<RecordStream>() as u64
    );
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.counts.connections.load(Ordering::Relaxed), 1);
    eprintln!(
        "follow_projection_allocations follows={FOLLOWS} before_count={} after_count={} before_bytes={} after_bytes={}",
        before.0, after.0, before.1, after.1
    );
    drop(anchor);
    Ok((before.0, FOLLOWS))
}

#[tokio::test]
async fn typed_follow_boxes_only_at_its_public_boundary() -> TestResult {
    tokio::time::timeout(Duration::from_secs(3), paired()).await??;
    Ok(())
}

#[tokio::test]
#[should_panic(expected = "typed follow must allocate only the final public stream box")]
async fn negative_control_rejects_the_reviewed_redundant_boxes() {
    let (before, follows) = paired().await.expect("allocation pair failed");
    public_box_budget(before, follows);
}

#[derive(Default)]
struct Allocations {
    count: u64,
    bytes: u64,
    pending_polls: usize,
}

async fn all_poll_allocations(
    open: impl Future<Output = Result<RecordStream, StreamError>>,
    totals: &mut Allocations,
) -> Result<RecordStream, StreamError> {
    let mut open = std::pin::pin!(open);
    future::poll_fn(|cx| {
        let mut outcome = Poll::Pending;
        let measured = acyclic_test_support::measure(|| outcome = open.as_mut().poll(cx));
        totals.count += measured.count_total;
        totals.bytes += measured.bytes_total;
        if outcome.is_pending() {
            totals.pending_polls += 1;
        }
        outcome
    })
    .await
}

fn opening_budget(
    before: &Allocations,
    after: &Allocations,
    path: acyclic_test_support::AllocationInfo,
    follows: u64,
) {
    assert_eq!(
        before.count - after.count,
        follows * (2 + path.count_total),
        "every opening poll must avoid the discarded path and two internal boxes"
    );
    assert_eq!(
        before.bytes - after.bytes,
        follows * (2 * std::mem::size_of::<RecordStream>() as u64 + path.bytes_total)
    );
}

async fn paired_openings(follows: u64, broken: bool) -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", 1).await?;
    let fixture = Fixture::open(provider, "node").await?;
    let client = || {
        Client::open(
            fixture.channel.clone(),
            Some("Bearer fixture".parse().unwrap()),
            None,
            None,
            None,
        )
    };
    let before_client = client();
    let after_client = client();
    // Separate clients keep 10k live interests in each arm, with identical
    // registration/cache state and one warm physical Session per client.
    let before_anchor = before_client.follow(StreamPath::new("events")?, 1).await?;
    let after_anchor = after_client.follow(StreamPath::new("events")?, 1).await?;
    let path_copy = acyclic_test_support::measure(|| {
        drop(black_box(StreamPath::new("events").unwrap()));
    });
    assert!(path_copy.count_total > 0);
    let mut before = Allocations::default();
    let mut after = Allocations::default();
    let mut retained = Vec::with_capacity(2 * follows as usize);
    for index in 0..follows {
        for legacy in if index % 2 == 0 {
            [true, false]
        } else {
            [false, true]
        } {
            let path = StreamPath::new("events")?;
            let records = if legacy {
                all_poll_allocations(reviewed_follow(&before_client, path), &mut before).await?
            } else {
                let public = async {
                    if broken {
                        // Reintroduce the reviewed discarded owned validation
                        // inside the measured public opening future.
                        drop(black_box(StreamPath::new(path.as_str())?));
                    }
                    after_client.follow(path, 1).await
                };
                all_poll_allocations(public, &mut after).await?
            };
            retained.push(records);
        }
    }
    assert!(before.pending_polls >= follows as usize);
    assert!(after.pending_polls >= follows as usize);
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 2);
    eprintln!(
        "follow_opening_allocations follows={follows} before_count={} after_count={} before_bytes={} after_bytes={} before_pending={} after_pending={} path_count={} path_bytes={}",
        before.count,
        after.count,
        before.bytes,
        after.bytes,
        before.pending_polls,
        after.pending_polls,
        path_copy.count_total,
        path_copy.bytes_total
    );
    opening_budget(&before, &after, path_copy, follows);
    drop((retained, before_anchor, after_anchor));
    Ok(())
}

#[tokio::test]
async fn ten_thousand_follow_openings_count_pending_and_ready_allocations() -> TestResult {
    tokio::time::timeout(Duration::from_secs(5), paired_openings(10_000, false)).await?
}

#[tokio::test]
#[should_panic(
    expected = "every opening poll must avoid the discarded path and two internal boxes"
)]
async fn negative_control_rejects_owned_validation_during_a_pending_poll() {
    paired_openings(8, true)
        .await
        .expect("allocation pair failed");
}

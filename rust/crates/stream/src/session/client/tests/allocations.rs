use super::*;
use std::{future::Future, hint::black_box, task::Poll};

// Measure only the poll that completes registration. The real public follow
// future runs unchanged; dial/registration allocations happen on earlier polls,
// and other Tokio tasks cannot execute within this synchronous measurement.
async fn completion_allocations<T>(
    open: impl Future<Output = Result<T, StreamError>>,
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
async fn reviewed_follow(
    client: &Client,
    path: StreamPath,
) -> Result<crate::BoxProviderStream<'static, Result<crate::Record, StreamError>>, StreamError> {
    drop(black_box(StreamPath::new(path.as_str())?));
    let received = client
        .open_kind(
            Kind::Follow(wire::SessionFollow {
                resource: crate::wire_codec::session::stream_resource(path.as_str()),
                delivery: wire::SessionDelivery::All as i32,
                ..Default::default()
            }),
            1,
            true,
            Some(crate::FollowFilter::default().transport_filter()?),
        )
        .await?
        .boxed();
    let data = received.map(|item| item?.into_data()).boxed();
    Ok(data
        .map(move |data| match data?.item {
            Some(Item::Record(record)) => wire_codec::record(record, Some(&path)),
            _ => Err(StreamError::Unavailable),
        })
        .boxed())
}

/// The opening receipt's own cost: decoding the selected snapshot and
/// selection, which the public follow owns independently of the transport.
fn receipt<T>(opened: &crate::Opened<T>) -> acyclic_test_support::AllocationInfo {
    let opened = wire_codec::opened_to_wire(opened);
    acyclic_test_support::measure(|| drop(black_box(wire_codec::selection_from_opened(&opened))))
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
        fixture._channel.clone(),
        Some("Bearer fixture".parse()?),
        None,
        None,
        None,
        Default::default(),
    );
    client.register_credential(0, crate::session::testing::credential())?;
    // Keep one interest alive so both arms share one warm physical Session.
    let anchor = client
        .follow_records(crate::FollowRequest::new(StreamPath::new("events")?, 1))
        .await?;
    let receipt = receipt(&anchor);
    let mut before = (0, 0);
    let mut after = (0, 0);
    let mut legacy_poll = 0;
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
                completion_allocations(client.follow_records(crate::FollowRequest::new(path, 1)))
                    .await?
            };
            let total = if legacy { &mut before } else { &mut after };
            total.0 += allocation.count_total;
            total.1 += allocation.bytes_total;
            // Both arms share the session's registration work; the reviewed
            // arm adds three boxes to it, the public arm one box and its receipt.
            if legacy {
                legacy_poll = allocation.count_total;
            } else if legacy_poll > 0 {
                public_box_budget(
                    allocation.count_total + 3 - legacy_poll - receipt.count_total,
                    1,
                );
            }
        }
    }
    public_box_budget(
        after.0 + 3 * FOLLOWS - before.0 - FOLLOWS * receipt.count_total,
        FOLLOWS,
    );
    // The public arm, receipt included, never costs more bytes than the
    // reviewed shape it replaced.
    assert!(after.1 <= before.1 + FOLLOWS * receipt.bytes_total);
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.counts.connections.load(Ordering::Relaxed), 1);
    eprintln!(
        "follow_projection_allocations follows={FOLLOWS} before_count={} after_count={} before_bytes={} after_bytes={}",
        before.0, after.0, before.1, after.1
    );
    drop(anchor);
    // The reviewed shape's own boxes, relative to the public arm.
    Ok((
        before.0 + FOLLOWS * (1 + receipt.count_total) - after.0,
        FOLLOWS,
    ))
}

#[tokio::test]
async fn typed_follow_boxes_only_at_its_public_boundary() -> TestResult {
    // Allocation counts, not elapsed time, are the property; a wall-clock
    // bound would fail a correct run on a loaded host.
    paired().await?;
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

async fn all_poll_allocations<T>(
    open: impl Future<Output = Result<T, StreamError>>,
    totals: &mut Allocations,
) -> Result<T, StreamError> {
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
    receipt: acyclic_test_support::AllocationInfo,
    follows: u64,
) {
    // The reviewed shape predates the opening receipt; only the public
    // follow pays for it.
    assert_eq!(
        (before.count + follows * receipt.count_total).checked_sub(after.count),
        Some(follows * (2 + path.count_total)),
        "every opening poll must avoid the discarded path and two internal boxes"
    );
    assert!(
        after.bytes + follows * path.bytes_total <= before.bytes + follows * receipt.bytes_total,
        "the public opening, receipt included, must not outgrow the reviewed shape"
    );
}

async fn paired_openings(follows: u64, broken: bool) -> TestResult {
    let follow_count = usize::try_from(follows)?;
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", 1).await?;
    let fixture = Fixture::open(provider, "node").await?;
    let client = || {
        Client::open(
            fixture._channel.clone(),
            Some("Bearer fixture".parse().unwrap()),
            None,
            None,
            None,
            Default::default(),
        )
    };
    let before_client = client();
    let after_client = client();
    before_client.register_credential(0, crate::session::testing::credential())?;
    after_client.register_credential(0, crate::session::testing::credential())?;
    // Separate clients keep every live interest in each arm, with identical
    // registration/cache state and one warm physical Session per client.
    let before_anchor = before_client
        .follow_records(crate::FollowRequest::new(StreamPath::new("events")?, 1))
        .await?;
    let after_anchor = after_client
        .follow_records(crate::FollowRequest::new(StreamPath::new("events")?, 1))
        .await?;
    let path_copy = acyclic_test_support::measure(|| {
        drop(black_box(StreamPath::new("events").unwrap()));
    });
    assert!(path_copy.count_total > 0);
    let mut before = Allocations::default();
    let mut after = Allocations::default();
    let mut retained_before = Vec::with_capacity(follow_count);
    let mut retained_after = Vec::with_capacity(follow_count);
    for index in 0..follows {
        for legacy in if index % 2 == 0 {
            [true, false]
        } else {
            [false, true]
        } {
            let path = StreamPath::new("events")?;
            if legacy {
                retained_before.push(
                    all_poll_allocations(reviewed_follow(&before_client, path), &mut before)
                        .await?,
                );
            } else {
                let public = async {
                    if broken {
                        // Reintroduce the reviewed discarded owned validation
                        // inside the measured public opening future.
                        drop(black_box(StreamPath::new(path.as_str())?));
                    }
                    after_client
                        .follow_records(crate::FollowRequest::new(path, 1))
                        .await
                };
                retained_after.push(all_poll_allocations(public, &mut after).await?);
            }
        }
    }
    assert!(before.pending_polls >= follow_count);
    assert!(after.pending_polls >= follow_count);
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
    opening_budget(&before, &after, path_copy, receipt(&after_anchor), follows);
    drop((retained_before, retained_after, before_anchor, after_anchor));
    Ok(())
}

#[tokio::test]
async fn follow_opening_allocations_grow_only_by_the_public_per_interest_cost() -> TestResult {
    for follows in [8, 64, 512] {
        paired_openings(follows, false).await?;
    }
    Ok(())
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

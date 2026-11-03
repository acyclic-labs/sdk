//! Original interval, overlap and terminal-event controls.
#![cfg(all(feature = "research", not(target_arch = "wasm32")))]

use acyclic_observability::{
    Export, ExportReceiver, ExportRecord, IntervalError, IntervalOrigin, Metrics,
    OperationInterval, Operations, Outcome,
};
use std::{
    future::Future,
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};
use tokio::sync::Semaphore;
use tracing_subscriber::prelude::*;

acyclic_test_support::counting_allocator!();

fn take(receiver: &mut ExportReceiver) -> Result<ExportRecord, Box<dyn std::error::Error>> {
    match pin!(receiver.recv()).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Some(record)) => Ok(record),
        Poll::Ready(None) => Err("terminal producer closed before its observation".into()),
        Poll::Pending => Err("terminal event did not publish its observation".into()),
    }
}

#[test]
fn original_intervals_preserve_overlap_and_terminal_disposition()
-> Result<(), Box<dyn std::error::Error>> {
    for details in [false, true] {
        for composition in 0..3 {
            let (origin, anchor) = IntervalOrigin::capture_unix([1; 16])?;
            let (export, mut output) = Export::channel(Arc::new(Semaphore::new(5)), details);
            let dispatch = match composition {
                0 => {
                    tracing::Dispatch::new(tracing_subscriber::registry().with(Operations(export)))
                }
                1 => tracing::Dispatch::new(
                    tracing_subscriber::registry().with(Operations((Metrics::default(), export))),
                ),
                _ => tracing::Dispatch::new(
                    tracing_subscriber::registry().with(Operations((export, Metrics::default()))),
                ),
            };
            let root = tracing::dispatcher::with_default(&dispatch, || {
                let root = tracing::info_span!(
                    "acyclic.test.interval.root",
                    outcome = tracing::field::Empty
                );
                let first = tracing::info_span!(parent: &root, "acyclic.test.interval.first", outcome = tracing::field::Empty);
                let second = tracing::info_span!(parent: &root, "acyclic.test.interval.second", outcome = tracing::field::Empty);
                acyclic_observability::record_outcome(&second, None);
                acyclic_observability::record_outcome(&first, Some("refused"));
                // Creation-time terminal and abandoned producer are the other two
                // completion paths. Neither requires a timer or retained owner.
                drop(
                    tracing::info_span!(parent: &root, "acyclic.test.interval.created", outcome = "ok"),
                );
                drop(
                    tracing::info_span!(parent: &root, "acyclic.test.interval.abandoned", outcome = tracing::field::Empty),
                );
                acyclic_observability::record_outcome(&root, None);
                root
            });
            let second = take(&mut output)?;
            let first = take(&mut output)?;
            let created = take(&mut output)?;
            let abandoned = take(&mut output)?;
            let finished = take(&mut output)?;
            assert_eq!(second.sample().outcome, Outcome::Ok);
            assert_eq!(first.sample().outcome, Outcome::Err);
            assert_eq!(created.sample().outcome, Outcome::Ok);
            assert_eq!(abandoned.sample().outcome, Outcome::Incomplete);
            for record in [&second, &first, &created, &abandoned, &finished] {
                let sample = record.sample();
                assert_eq!(sample.interval.is_some(), details);
                if let Some(interval) = sample.interval {
                    let coordinates = origin.project(interval)?;
                    assert_eq!(
                        coordinates.finished_ns - coordinates.started_ns,
                        sample.elapsed.as_nanos()
                    );
                    let unix = anchor.project(coordinates)?;
                    assert_eq!(
                        unix.finished_unix_ns - unix.started_unix_ns,
                        sample.elapsed.as_nanos()
                    );
                    assert_eq!(unix.domain, coordinates.domain);
                    assert_eq!(unix.read_window_ns, anchor.read_window_ns);
                }
            }
            assert_eq!(first.sample().parent_id, root.id().as_ref());
            assert_eq!(second.sample().parent_id, first.sample().parent_id);
            if details {
                let first =
                    origin.project(first.sample().interval.ok_or("missing first interval")?)?;
                let second =
                    origin.project(second.sample().interval.ok_or("missing second interval")?)?;
                assert!(first.started_ns <= second.started_ns);
                assert!(second.finished_ns <= first.finished_ns);
            }
            // Completion was published while the root handle remained live. Its
            // later retirement must not emit a second interval or keep output open.
            drop(dispatch);
            drop(root);
            assert!(matches!(
                pin!(output.recv()).poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(None)
            ));
        }
    }
    Ok(())
}

#[test]
fn interval_projection_refuses_foreign_origins_without_allocating()
-> Result<(), Box<dyn std::error::Error>> {
    let mut captured = None;
    let setup = acyclic_test_support::measure(|| {
        captured = Some(IntervalOrigin::capture_unix([2; 16]));
    });
    assert_eq!((setup.count_total, setup.bytes_total), (0, 0));
    let (origin, anchor) = captured.ok_or("capture not invoked")??;
    let instant = origin.instant;
    // Internally generated coordinates, not fabricated external observations.
    let interval = OperationInterval {
        started: instant + Duration::from_nanos(1),
        finished: instant + Duration::from_nanos(3),
    };
    let mut result = Ok(());
    let measured = acyclic_test_support::measure(|| {
        result = (|| -> Result<(), IntervalError> {
            let coordinates = origin.project(interval)?;
            assert_eq!((coordinates.started_ns, coordinates.finished_ns), (1, 3));
            assert_eq!(
                IntervalOrigin {
                    instant: interval.finished,
                    ..origin
                }
                .project(interval),
                Err(IntervalError::BeforeOrigin)
            );
            assert_eq!(
                origin.project(OperationInterval {
                    started: interval.finished,
                    finished: interval.started
                }),
                Err(IntervalError::Reversed)
            );
            Ok(())
        })();
    });
    result?;
    assert_eq!((measured.count_total, measured.bytes_total), (0, 0));
    let allocated =
        acyclic_test_support::measure(|| drop(std::hint::black_box(Box::new(interval))));
    assert!(allocated.count_total > 0);
    // Elapsed-only negative control: equal durations can occupy disjoint
    // intervals. Original coordinates must preserve that difference.
    let shifted = OperationInterval {
        started: instant + Duration::from_nanos(4),
        finished: instant + Duration::from_nanos(6),
    };
    assert_eq!(
        interval.finished - interval.started,
        shifted.finished - shifted.started
    );
    let first = origin.project(interval)?;
    let second = origin.project(shifted)?;
    assert!(first.finished_ns < second.started_ns);
    for count in [1, 100, 1000] {
        let measured = acyclic_test_support::measure(|| {
            result = (|| -> Result<(), IntervalError> {
                for _ in 0..count {
                    let unix = anchor.project(first)?;
                    assert_eq!(unix.finished_unix_ns - unix.started_unix_ns, 2);
                    assert_eq!(
                        anchor.project(acyclic_observability::IntervalCoordinates {
                            domain: [3; 16],
                            ..first
                        }),
                        Err(IntervalError::ForeignDomain)
                    );
                    assert_eq!(
                        anchor.project(acyclic_observability::IntervalCoordinates {
                            started_ns: first.finished_ns,
                            finished_ns: first.started_ns,
                            ..first
                        }),
                        Err(IntervalError::Reversed)
                    );
                }
                Ok(())
            })();
        });
        result?;
        assert_eq!((measured.count_total, measured.bytes_total), (0, 0));
    }
    // Mutated original observations are refusal controls, not clock fixtures.
    let zero = acyclic_observability::IntervalCoordinates {
        started_ns: 0,
        finished_ns: 0,
        ..first
    };
    assert_eq!(
        acyclic_observability::UnixClockAnchor {
            observed_unix_ns: 0,
            read_window_ns: 2,
            ..anchor
        }
        .project(zero),
        Err(IntervalError::UnixRange)
    );
    let extreme = acyclic_observability::UnixClockAnchor {
        observed_unix_ns: u128::MAX,
        read_window_ns: 2,
        ..anchor
    };
    // Apply the midpoint before addition: the representable boundary succeeds.
    assert_eq!(
        extreme
            .project(acyclic_observability::IntervalCoordinates {
                started_ns: 1,
                finished_ns: 1,
                ..first
            })?
            .started_unix_ns,
        u128::MAX
    );
    assert_eq!(
        extreme.project(acyclic_observability::IntervalCoordinates {
            started_ns: 2,
            finished_ns: 2,
            ..first
        }),
        Err(IntervalError::UnixRange)
    );
    // A clock domain remains an original producer fact; projection never
    // silently replaces it with another process's identity.
    assert_ne!(
        origin.project(interval)?.domain,
        IntervalOrigin {
            domain: [3; 16],
            ..origin
        }
        .project(interval)?
        .domain
    );
    Ok(())
}

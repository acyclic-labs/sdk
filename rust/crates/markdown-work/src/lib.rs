//! Development-only Markdown work counters. Without `enabled`, recording
//! expands to nothing, including its argument expressions. The parser's dev
//! dependency enables this feature for tests and benchmarks without a cycle.

/// Record one instrumented operation in development builds.
#[cfg(feature = "enabled")]
#[macro_export]
macro_rules! record {
    ($operation:ident($($argument:expr),*)) => {
        $crate::$operation($($argument),*)
    };
}

/// Ordinary consumers have no counters, argument evaluation or runtime branch.
#[cfg(not(feature = "enabled"))]
#[macro_export]
macro_rules! record {
    ($operation:ident($($argument:expr),*)) => {};
}

#[cfg(feature = "enabled")]
mod counters;
#[cfg(feature = "enabled")]
pub use counters::{Counts, node, scan, snapshot, span_copy};

#[cfg(all(test, not(feature = "enabled")))]
#[test]
fn disabled_recording_does_not_evaluate_arguments() {
    record!(scan(panic!(
        "production instrumentation evaluated its argument"
    )));
}

#[cfg(all(test, feature = "enabled"))]
#[test]
fn snapshots_exclude_another_parsers_thread() -> Result<(), Box<dyn std::error::Error>> {
    let before = snapshot();
    let other = std::thread::spawn(|| {
        record!(scan(100));
        record!(node());
        record!(span_copy(200));
        snapshot()
    });
    record!(scan(3));
    record!(node());
    record!(span_copy(5));
    let other = other.join().map_err(|_| "counter thread panicked")?;
    assert_eq!(
        other,
        Counts {
            rescanned_bytes: 100,
            reparsed_nodes: 1,
            span_copy_bytes: 200
        }
    );
    assert_eq!(
        snapshot() - before,
        Counts {
            rescanned_bytes: 3,
            reparsed_nodes: 1,
            span_copy_bytes: 5
        }
    );
    Ok(())
}

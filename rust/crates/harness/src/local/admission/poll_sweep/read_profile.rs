//! Optional read accounting for one deterministic baseline.
use super::*;
use tracing_subscriber::{Layer, layer::Context, prelude::*};

struct Reads(Arc<Schedule>, std::time::Instant);

fn counts(schedule: &Schedule) -> std::collections::BTreeMap<&'static str, usize> {
    let mut counts = std::collections::BTreeMap::new();
    for point in schedule.points() {
        *counts.entry(point).or_insert(0usize) += 1;
    }
    counts
}
impl<S: tracing::Subscriber> Layer<S> for Reads {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        if event.metadata().target() != "acyclic_harness::read_profile" {
            return;
        }
        struct Phase<'a>(&'a Schedule, std::time::Instant);
        impl tracing::field::Visit for Phase<'_> {
            fn record_str(&mut self, field: &tracing::field::Field, phase: &str) {
                if field.name() == "phase" {
                    let counts = counts(self.0);
                    eprintln!(
                        "READ_PROFILE {phase}: elapsed={:?} ready={} admission={:?} boundaries={counts:?}",
                        self.1.elapsed(),
                        self.0.ready_reads(),
                        self.0.admission_submission(),
                    );
                }
            }
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
        }
        event.record(&mut Phase(&self.0, self.1));
    }
}

pub(super) fn start() -> Option<std::time::Instant> {
    crate::native_environment::variable("GC_LOCAL_READ_PROFILE").map(|_| std::time::Instant::now())
}

pub(super) fn install(
    schedule: &Arc<Schedule>,
    started: Option<std::time::Instant>,
    restored: Option<std::time::Instant>,
) -> Option<tracing::subscriber::DefaultGuard> {
    started.map(|started| {
        let restored = restored.expect("profile restore started");
        eprintln!(
            "READ_PROFILE setup: seed={:?} restore={:?}",
            restored.duration_since(started),
            restored.elapsed(),
        );
        tracing::subscriber::set_default(
            tracing_subscriber::registry().with(
                Reads(Arc::clone(schedule), started).with_filter(
                    tracing_subscriber::filter::Targets::new()
                        .with_target("acyclic_harness::read_profile", tracing::Level::TRACE),
                ),
            ),
        )
    })
}

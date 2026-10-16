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
impl<S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>> Layer<S>
    for Reads
{
    fn on_new_span(
        &self,
        attributes: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        _: Context<'_, S>,
    ) {
        if attributes.metadata().name() == "acyclic.harness.journal.stage" {
            eprintln!(
                "STAGE_BEGIN {id:?} ready={} fields={:?} boundaries={:?}",
                self.0.ready_reads(),
                attributes.values(),
                counts(&self.0)
            );
        }
    }

    fn on_close(&self, id: tracing::span::Id, context: Context<'_, S>) {
        if context
            .span(&id)
            .is_some_and(|span| span.name() == "acyclic.harness.journal.stage")
        {
            eprintln!(
                "STAGE_END {id:?} ready={} boundaries={:?}",
                self.0.ready_reads(),
                counts(&self.0)
            );
        }
    }

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
                        "READ_PROFILE {phase}: elapsed={:?} ready={} boundaries={counts:?}",
                        self.1.elapsed(),
                        self.0.ready_reads()
                    );
                }
            }
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
        }
        event.record(&mut Phase(&self.0, self.1));
    }
}

pub(super) fn install(schedule: &Arc<Schedule>) -> Option<impl Sized> {
    std::env::var_os("GC_LOCAL_READ_PROFILE").map(|_| {
        tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(Reads(Arc::clone(schedule), std::time::Instant::now())),
        )
    })
}

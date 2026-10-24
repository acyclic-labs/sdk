//! Logical journal work between model dispatches, retained in a fixed window.
//! Payload stage bytes are inputs to staging, not physical bytes written.
use acyclic_harness::{Error, Result};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

macro_rules! counters {
    ($($name:ident),+ $(,)?) => {
        #[derive(Default)]
        pub(super) struct Counters { overflowed: AtomicBool, $($name: AtomicU64),+ }

        #[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
        struct Snapshot { $($name: u64),+ }

        impl Counters {
            fn snapshot(&self) -> Result<Snapshot> {
                let snapshot = Snapshot { $($name: self.$name.load(Ordering::Relaxed)),+ };
                if self.overflowed.load(Ordering::Relaxed) {
                    return Err(Error::Invalid("journal work counter overflow".into()));
                }
                Ok(snapshot)
            }
        }
        impl Snapshot {
            fn difference(self, earlier: Self) -> Result<Self> {
                Ok(Self { $($name: self.$name.checked_sub(earlier.$name)
                    .ok_or_else(|| Error::Invalid("journal work counter moved backwards".into()))?),+ })
            }
            fn add(self, other: Self) -> Result<Self> {
                Ok(Self { $($name: self.$name.checked_add(other.$name)
                    .ok_or_else(|| Error::Invalid("journal work window overflow".into()))?),+ })
            }
            fn report(self, end: usize, count: u32) {
                println!("journal work ending at turn {end}: window_turns={count} {}",
                    [$(format!("{}={}", stringify!($name), self.$name)),+].join(" "));
            }
        }
        struct Visitor<'a>(&'a Counters);
        impl tracing::field::Visit for Visitor<'_> {
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                match field.name() {
                    $(stringify!($name) => {
                        if self.0.$name.fetch_update(Ordering::Relaxed, Ordering::Relaxed,
                            |previous| previous.checked_add(value)).is_err() {
                            self.0.overflowed.store(true, Ordering::Relaxed);
                        }
                    }),+
                    _ => {}
                }
            }
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
        }
    };
}
counters! {
    journal_reads, journal_records, journal_read_bytes,
    journal_appends, journal_append_bytes,
    payload_loads, payload_load_bytes, payload_stages, payload_stage_bytes,
}

pub(super) struct Layer(pub(super) Arc<Counters>);
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Layer {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() == "acyclic_harness::journal_work" {
            event.record(&mut Visitor(&self.0));
        }
    }
}

#[derive(Default)]
pub(super) struct Measurement {
    pub(super) counters: Arc<Counters>,
    previous: Option<Snapshot>,
    values: [Snapshot; 10],
    next: usize,
    count: u32,
}
impl Measurement {
    pub(super) fn begin(&mut self) -> Result<()> {
        self.previous = Some(self.counters.snapshot()?);
        Ok(())
    }
    pub(super) fn complete(&mut self) -> Result<()> {
        let previous = self
            .previous
            .take()
            .ok_or_else(|| Error::Invalid("journal work measurement has no start".into()))?;
        let value = self.counters.snapshot()?.difference(previous)?;
        let slot = self
            .values
            .get_mut(self.next)
            .ok_or_else(|| Error::Invalid("journal work window position is invalid".into()))?;
        *slot = value;
        self.next = (self.next + 1) % self.values.len();
        self.count = (self.count + 1).min(10);
        Ok(())
    }
    fn summary(&self) -> Result<Snapshot> {
        self.values
            .iter()
            .try_fold(Snapshot::default(), |sum, value| sum.add(*value))
    }
    pub(super) fn report(&self, end: usize) -> Result<()> {
        self.summary()?.report(end, self.count);
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn require_stock_events(&self) -> Result<()> {
        let summary = self.summary()?;
        if summary.journal_appends == 0 || summary.payload_stages == 0 {
            return Err(Error::Invalid(
                "stock journal work events were not measured".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::prelude::*;

    #[test]
    fn journal_work_filters_events_and_retains_only_the_last_ten_turns() -> Result<()> {
        let mut measurement = Measurement::default();
        let subscriber = tracing_subscriber::registry().with(Layer(measurement.counters.clone()));
        tracing::subscriber::with_default(subscriber, || -> Result<()> {
            for turn in 1..=1000_u64 {
                measurement.begin()?;
                tracing::info!(target: "unrelated", journal_reads = 999_u64);
                tracing::info!(target: "acyclic_harness::journal_work",
                    journal_reads = turn, journal_records = turn, journal_read_bytes = turn,
                    journal_appends = turn, journal_append_bytes = turn,
                    payload_loads = turn, payload_load_bytes = turn,
                    payload_stages = turn, payload_stage_bytes = turn);
                measurement.complete()?;
                let expected: u64 = (turn.saturating_sub(9).max(1)..=turn).sum();
                let summary = measurement.summary()?;
                assert_eq!(
                    summary,
                    Snapshot {
                        journal_reads: expected,
                        journal_records: expected,
                        journal_read_bytes: expected,
                        journal_appends: expected,
                        journal_append_bytes: expected,
                        payload_loads: expected,
                        payload_load_bytes: expected,
                        payload_stages: expected,
                        payload_stage_bytes: expected,
                    }
                );
            }
            Ok(())
        })?;
        assert_eq!(measurement.count, 10);
        assert!(
            measurement.complete().is_err(),
            "missing start was silently accepted"
        );
        Ok(())
    }

    #[test]
    fn overflowing_journal_events_fail_instead_of_looking_like_zero_work() {
        let mut measurement = Measurement::default();
        measurement
            .counters
            .journal_reads
            .store(u64::MAX, Ordering::Relaxed);
        let subscriber = tracing_subscriber::registry().with(Layer(measurement.counters.clone()));
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "acyclic_harness::journal_work", journal_reads = 1_u64);
        });
        assert!(
            measurement.begin().is_err(),
            "overflow was reported as a valid zero counter"
        );
    }
}

//! Pending primitive observations; reporting never changes their readiness.
use std::{
    collections::HashMap,
    fmt,
    io::{self, Write},
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tracing::{Id, Subscriber, field::Visit, span::Attributes};
use tracing_subscriber::{Layer, layer::Context};

#[derive(Clone, Default)]
pub(super) struct Waits(Arc<Mutex<HashMap<Id, Entry>>>);

struct Entry {
    fields: Fields,
    started: SystemTime,
    elapsed: Instant,
}

#[derive(Default)]
struct Fields {
    primitive: String,
    key: String,
}

impl Visit for Fields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "primitive" => self.primitive = value.to_owned(),
            "key" => self.key = value.to_owned(),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        self.record_str(field, &format!("{value:?}"));
    }
}

impl<S: Subscriber> Layer<S> for Waits {
    fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, _: Context<'_, S>) {
        if attributes.metadata().target() != "acyclic.wait" {
            return;
        }
        let mut fields = Fields::default();
        attributes.record(&mut fields);
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                id.clone(),
                Entry {
                    fields,
                    started: SystemTime::now(),
                    elapsed: Instant::now(),
                },
            );
    }

    fn on_close(&self, id: Id, _: Context<'_, S>) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
    }
}

impl Waits {
    pub(super) fn report(&self, output: &mut impl Write) -> io::Result<()> {
        // Copy the report before writing: a blocked stderr never holds the
        // registry lock needed by a producer completing on another thread.
        let lines: Vec<_> = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .map(|entry| {
                let since = entry.started.duration_since(UNIX_EPOCH).unwrap_or_default();
                format!(
                    "waiting: {} {} since {:.3} ({:.3}s elapsed)",
                    entry.fields.primitive,
                    entry.fields.key,
                    since.as_secs_f64(),
                    entry.elapsed.elapsed().as_secs_f64()
                )
            })
            .collect();
        for line in lines {
            writeln!(output, "{line}")?;
        }
        output.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;
    use tracing_subscriber::prelude::*;

    #[test]
    fn dropped_producer_closes_the_wait_and_pending_is_reported() {
        let waits = Waits::default();
        let subscriber = tracing_subscriber::registry().with(waits.clone());
        let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
        let future = tracing::subscriber::with_default(subscriber, || {
            acyclic_native_runtime::obs::waiting("channel-close", "request-1", receiver)
        });
        let mut future = Box::pin(future);
        assert!(future.as_mut().now_or_never().is_none());
        let mut report = Vec::new();
        waits.report(&mut report).unwrap();
        assert!(
            String::from_utf8(report)
                .unwrap()
                .contains("waiting: channel-close request-1 since ")
        );
        drop(sender);
        assert!(future.as_mut().now_or_never().unwrap().is_err());
        drop(future);
        let mut report = Vec::new();
        waits.report(&mut report).unwrap();
        assert!(report.is_empty());
    }
}

//! Observe the stock executor boundary without replacing its tool or authority.
use super::*;

pub(super) struct ToolReturn {
    enabled: bool,
    timing: Arc<Mutex<Timing>>,
}

impl ToolReturn {
    pub(super) fn new(enabled: bool, timing: Arc<Mutex<Timing>>) -> Self {
        Self { enabled, timing }
    }
}

#[derive(Default)]
struct Returned {
    executed: Option<bool>,
    succeeded: Option<bool>,
}

impl tracing::field::Visit for Returned {
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        match field.name() {
            "executed" => self.executed = Some(value),
            "succeeded" => self.succeeded = Some(value),
            _ => {}
        }
    }

    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ToolReturn {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if !self.enabled || event.metadata().target() != "acyclic_harness::tool_return" {
            return;
        }
        let mut returned = Returned::default();
        event.record(&mut returned);
        let mut timing = self.timing.lock().unwrap_or_else(PoisonError::into_inner);
        let result = if returned.executed == Some(true) && returned.succeeded == Some(true) {
            timing.tool_returned()
        } else {
            Err(Error::Invalid(
                "stock tool return lacked a successful execution".into(),
            ))
        };
        if let Err(error) = result {
            timing.measurement_error = Some(error);
        }
    }
}

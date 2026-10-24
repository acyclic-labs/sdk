//! Optional inclusive span-poll timing. Enable with `--profile`.
//! Waiting between polls is excluded; child span time is included.
use std::{
    cell::RefCell,
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};
use tracing::{Id, Subscriber};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

type Measurements = BTreeMap<&'static str, (Duration, u64)>;

#[derive(Clone, Default)]
pub(super) struct Profile(Option<Arc<Mutex<Measurements>>>);

thread_local! {
    static ENTERED: RefCell<Vec<(Id, Instant)>> = const { RefCell::new(Vec::new()) };
}

impl Profile {
    pub(super) fn from_arguments() -> Self {
        Self(
            std::env::args()
                .any(|arg| arg == "--profile")
                .then(Default::default),
        )
    }

    pub(super) fn report(&self, step: usize) {
        let Some(totals) = &self.0 else { return };
        let totals = std::mem::take(&mut *totals.lock().unwrap_or_else(PoisonError::into_inner));
        for (name, (elapsed, polls)) in totals {
            if name.starts_with("acyclic.harness.")
                || name.starts_with("acyclic.fs.")
                || name.starts_with("acyclic.stream.")
                || name.starts_with("acyclic.objects.")
            {
                println!(
                    "profile ending at turn {step}: {name} {:.3} ms / {polls} polls",
                    elapsed.as_secs_f64() * 1000.0
                );
            }
        }
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Profile {
    fn on_enter(&self, id: &Id, _: Context<'_, S>) {
        if self.0.is_some() {
            ENTERED.with(|entered| entered.borrow_mut().push((id.clone(), Instant::now())));
        }
    }

    fn on_exit(&self, id: &Id, context: Context<'_, S>) {
        let Some(totals) = &self.0 else { return };
        let started = ENTERED.with(|entered| {
            let mut entered = entered.borrow_mut();
            let position = entered.iter().rposition(|(entered, _)| entered == id)?;
            Some(entered.remove(position).1)
        });
        if let (Some(started), Some(span)) = (started, context.span(id)) {
            let elapsed = started.elapsed();
            let mut totals = totals.lock().unwrap_or_else(PoisonError::into_inner);
            let (total, polls) = totals.entry(span.metadata().name()).or_default();
            *total += elapsed;
            *polls += 1;
        }
    }
}

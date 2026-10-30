use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};
use tokio::sync::watch;
use tracing::{
    Id, Span, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Record},
};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

/// Records a stable, path-free terminal result on the explicitly owned span.
pub fn record_outcome(span: &Span, error: Option<&'static str>) {
    if span.is_disabled() {
        return;
    }
    if let Some(error) = error {
        span.record("error.kind", error);
    }
    span.record("outcome", if error.is_some() { "err" } else { "ok" });
}

/// Returns the original result after recording its terminal outcome.
pub fn finish<T, E>(
    span: &Span,
    result: Result<T, E>,
    kind: impl FnOnce(&E) -> &'static str,
) -> Result<T, E> {
    if !span.is_disabled() {
        record_outcome(span, result.as_ref().err().map(kind));
    }
    result
}

/// Owns the terminal observation, independently of retained child span handles.
/// Dropping unfinished work records `incomplete`; it never invents a failed
/// result for an operation whose effect may already have been accepted.
pub struct Completion {
    span: Option<Span>,
    abandoned_error: Option<&'static str>,
}

impl Completion {
    /// Bind the observation to the explicitly owned operation span.
    pub fn new(span: &Span) -> Self {
        Self {
            span: Some(span.clone()),
            abandoned_error: None,
        }
    }

    /// Classify abandonment as an error when the boundary owns cancellation
    /// (for example, a read or RPC body). Accepted mutations use `incomplete`.
    pub fn on_drop_error(mut self, kind: &'static str) -> Self {
        self.abandoned_error = Some(kind);
        self
    }

    /// Complete once and return the unchanged result, including uncertain errors.
    pub fn finish<T, E>(
        mut self,
        result: Result<T, E>,
        kind: impl FnOnce(&E) -> &'static str,
    ) -> Result<T, E> {
        if let Some(span) = self.span.take() {
            finish(&span, result, kind)
        } else {
            result
        }
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        if let Some(span) = &self.span
            && !span.is_disabled()
        {
            match self.abandoned_error {
                Some(kind) => record_outcome(span, Some(kind)),
                None => {
                    span.record("outcome", "incomplete");
                }
            }
        }
    }
}

/// Observe a future in place, retaining one producer and one completion owner.
/// The caller scopes it with the shared native operation context when needed.
pub fn observe<T, E, F, K>(
    span: &Span,
    future: F,
    kind: K,
) -> impl std::future::Future<Output = Result<T, E>> + use<T, E, F, K>
where
    F: std::future::Future<Output = Result<T, E>>,
    K: FnOnce(&E) -> &'static str,
{
    use futures::FutureExt as _;
    let completion = Completion::new(span);
    future.map(move |result| completion.finish(result, kind))
}

/// Counts and a base-two nanosecond latency histogram from one operation span.
///
/// Bucket zero contains 0 ns, bucket i contains [2^(i-1), 2^i) ns.
/// All u128 nanosecond durations are represented. These are representation
/// buckets, not a limit on operation duration or retained operations.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Snapshot {
    /// Exact operation selected by this query.
    pub operation: String,
    /// Static RPC method when one span factory serves several operations.
    pub rpc: Option<String>,
    /// Explicit successful completions.
    pub ok: u64,
    /// Explicit failed completions, including recorded cancellations.
    pub err: u64,
    /// Closed without an explicit completion, including abandoned operations.
    pub incomplete: u64,
    /// One sample per outcome, never one per span handle.
    pub latency_ns: Vec<u64>,
}

/// Exactly one disposition of an observed operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Explicit successful completion.
    Ok,
    /// Explicit failed completion, including recorded cancellation.
    Err,
    /// The operation span closed without an explicit result.
    Incomplete,
}

/// One operation's terminal evidence, shared by metrics and exporters.
pub struct Sample<'a> {
    /// Original static span metadata. Exporters must use bounded static labels.
    pub metadata: &'static tracing::Metadata<'static>,
    /// RPC method declared by the operation factory, never a resource label.
    pub rpc: Option<&'a str>,
    /// Wall time from creation to explicit completion, or incomplete close.
    pub elapsed: Duration,
    /// Explicit result or incomplete close.
    pub outcome: Outcome,
}

/// Receives terminal samples from the one operation lifecycle implementation.
/// An OpenTelemetry adapter implements this contract; it does not reconstruct
/// span completion or time operations again. A sink must not wait for I/O.
pub trait Sink: Clone + Send + Sync + 'static {
    /// Records the one latency histogram observation and outcome counter update.
    fn record(&self, sample: &Sample<'_>);
}

impl<A: Sink, B: Sink> Sink for (A, B) {
    fn record(&self, sample: &Sample<'_>) {
        self.0.record(sample);
        self.1.record(sample);
    }
}

impl Snapshot {
    fn sample(&mut self, outcome: Outcome, nanos: u128) {
        let count = match outcome {
            Outcome::Ok => &mut self.ok,
            Outcome::Err => &mut self.err,
            Outcome::Incomplete => &mut self.incomplete,
        };
        *count = count.saturating_add(1);
        let bucket = usize::try_from(u128::BITS - nanos.leading_zeros()).unwrap_or(usize::MAX);
        if let Some(count) = self.latency_ns.get_mut(bucket) {
            *count = count.saturating_add(1);
        }
    }
}

struct Series(watch::Sender<Snapshot>);

impl Series {
    fn complete(&self, sample: &Sample<'_>) {
        self.0
            .send_modify(|value| value.sample(sample.outcome, sample.elapsed.as_nanos()));
    }
}

impl Sink for Metrics {
    fn record(&self, sample: &Sample<'_>) {
        self.series(sample.metadata.name(), sample.rpc)
            .complete(sample);
    }
}

/// Queryable operation metrics owned by an attached subscriber, with no history.
#[derive(Clone, Default)]
pub struct Metrics(Arc<Mutex<Names>>);

// A byte radix index: each node has at most 256 children, so lookup is bounded
// by the static operation name's bytes, independently of unrelated operations.
// Unlike a hash table this bound does not assume a collision distribution.
#[derive(Default)]
struct Names {
    children: BTreeMap<u8, Names>,
    series: Option<Arc<Series>>,
}

impl Metrics {
    /// Select a single static operation name. Only this series is observed.
    ///
    /// Register the query before running the operation to include its first
    /// completion. This does not install a subscriber or enable tracing.
    pub fn query(&self, operation: &str) -> Query {
        Query(self.series(operation, None).0.subscribe())
    }

    /// Select an RPC method from a shared static span factory.
    pub fn query_rpc(&self, operation: &str, rpc: &str) -> Query {
        Query(self.series(operation, Some(rpc)).0.subscribe())
    }

    fn series(&self, operation: &str, rpc: Option<&str>) -> Arc<Series> {
        let mut names = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let mut node = &mut *names;
        for byte in operation
            .bytes()
            .chain([0])
            .chain(rpc.into_iter().flat_map(str::bytes))
        {
            #[cfg(test)]
            INDEX_VISITS.set(INDEX_VISITS.get() + 1);
            node = node.children.entry(byte).or_default();
        }
        Arc::clone(node.series.get_or_insert_with(|| {
            Arc::new(Series(
                watch::channel(Snapshot {
                    operation: operation.to_owned(),
                    rpc: rpc.map(str::to_owned),
                    latency_ns: vec![0; 129],
                    ..Snapshot::default()
                })
                .0,
            ))
        }))
    }
}

#[cfg(test)]
thread_local! {
    static INDEX_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_series_cost_is_independent_of_unrelated_series() {
        let metrics = Metrics::default();
        let query = metrics.query("acyclic.selected");
        let mut previous = 0;
        for population in [1, 1_000, 100_000] {
            for index in previous..population {
                let _ = metrics.query(&format!("acyclic.unrelated.{index}"));
            }
            previous = population;
            INDEX_VISITS.set(0);
            let selected = metrics.query("acyclic.selected");
            assert_eq!(INDEX_VISITS.get(), "acyclic.selected".len() + 1);
            assert_eq!(query.snapshot(), selected.snapshot());
        }
        // A full namespace visit, formerly typical of snapshot collectors,
        // exceeds the same bound even though it returns the identical series.
        fn full_visit(names: &Names) -> usize {
            1 + names.children.values().map(full_visit).sum::<usize>()
        }
        let names = metrics.0.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(full_visit(&names) > "acyclic.selected".len() + 1);
    }
}

/// A live query waits on the selected operation's completion event.
/// Dropping the subscriber and metrics producer ends the wait with [`QueryError`].
pub struct Query(watch::Receiver<Snapshot>);

/// The selected telemetry producer was dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryError;

impl fmt::Display for QueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("operation telemetry producer closed")
    }
}
impl std::error::Error for QueryError {}

impl Query {
    /// The current bounded projection, without consuming a completion event.
    pub fn snapshot(&self) -> Snapshot {
        self.0.borrow().clone()
    }

    /// Waits for a selected completion, or returns a typed producer-closed error.
    pub async fn changed(&mut self) -> Result<Snapshot, QueryError> {
        self.0.changed().await.map_err(|_| QueryError)?;
        Ok(self.0.borrow_and_update().clone())
    }
}

/// One instrumentation layer for every SDK operation family.
///
/// It observes spans named `acyclic.*` declaring `outcome`. Other diagnostic
/// spans and wait reports remain projections on the same registry, not operations.
/// No identifiers, paths, credentials, payloads or error text become metric labels.
#[derive(Clone, Default)]
pub struct Operations<C = Metrics>(pub C);

struct Operation {
    metadata: &'static tracing::Metadata<'static>,
    rpc: Option<String>,
    started: Instant,
    completed: bool,
}

#[derive(Default)]
struct Terminal(Option<Outcome>);

#[derive(Default)]
struct Method(Option<String>);
impl Visit for Method {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "rpc" {
            self.0 = Some(value.to_owned());
        }
    }
    fn record_debug(&mut self, _: &Field, _: &dyn fmt::Debug) {}
}

impl Visit for Terminal {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "outcome" {
            self.0 = match value {
                "ok" => Some(Outcome::Ok),
                "err" => Some(Outcome::Err),
                "incomplete" => Some(Outcome::Incomplete),
                _ => None,
            };
        }
    }
    fn record_debug(&mut self, _: &Field, _: &dyn fmt::Debug) {}
}

impl<C: Sink, S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Operations<C> {
    fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
        let metadata = attributes.metadata();
        if !metadata.name().starts_with("acyclic.") || metadata.fields().field("outcome").is_none()
        {
            return;
        }
        let Some(span) = context.span(id) else { return };
        let mut method = Method::default();
        attributes.record(&mut method);
        let mut operation = Operation {
            metadata,
            rpc: method.0,
            started: Instant::now(),
            completed: false,
        };
        let mut terminal = Terminal::default();
        attributes.record(&mut terminal);
        if let Some(outcome) = terminal.0 {
            self.0.record(&Sample {
                metadata,
                rpc: operation.rpc.as_deref(),
                elapsed: operation.started.elapsed(),
                outcome,
            });
            operation.completed = true;
        }
        span.extensions_mut().insert(operation);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, context: Context<'_, S>) {
        let mut terminal = Terminal::default();
        values.record(&mut terminal);
        let Some(outcome) = terminal.0 else { return };
        let Some(span) = context.span(id) else { return };
        let mut extensions = span.extensions_mut();
        let Some(operation) = extensions.get_mut::<Operation>() else {
            return;
        };
        if operation.completed {
            return;
        }
        operation.completed = true;
        let metadata = operation.metadata;
        let rpc = operation.rpc.take();
        let started = operation.started;
        drop(extensions);
        // Notification may run arbitrary wakers. Never call them while holding
        // the registry's extension lock.
        self.0.record(&Sample {
            metadata,
            rpc: rpc.as_deref(),
            elapsed: started.elapsed(),
            outcome,
        });
    }

    fn on_close(&self, id: Id, context: Context<'_, S>) {
        let Some(span) = context.span(&id) else {
            return;
        };
        let operation = span.extensions_mut().remove::<Operation>();
        if let Some(operation) = operation
            && !operation.completed
        {
            self.0.record(&Sample {
                metadata: operation.metadata,
                rpc: operation.rpc.as_deref(),
                elapsed: operation.started.elapsed(),
                outcome: Outcome::Incomplete,
            });
        }
    }
}

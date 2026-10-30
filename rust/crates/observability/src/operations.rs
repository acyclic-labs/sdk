use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, Mutex, PoisonError, Weak,
        atomic::{AtomicUsize, Ordering},
    },
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
        self,
        result: Result<T, E>,
        kind: impl FnOnce(&E) -> &'static str,
    ) -> Result<T, E> {
        self.complete(result, |result| result.as_ref().err().map(kind))
    }

    /// Observe an unchanged output with separately retained provider and
    /// observer errors. Classification is skipped for a disabled span.
    pub fn complete<T>(mut self, output: T, error: impl FnOnce(&T) -> Option<&'static str>) -> T {
        if let Some(span) = self.span.take()
            && !span.is_disabled()
        {
            record_outcome(&span, error(&output));
        }
        output
    }

    /// Transfer this completion owner into the original producer future,
    /// including work that detaches from its disposable caller observation.
    pub fn observe_output<T, F, K>(
        self,
        future: F,
        error: K,
    ) -> impl std::future::Future<Output = T> + use<T, F, K>
    where
        F: std::future::Future<Output = T>,
        K: FnOnce(&T) -> Option<&'static str>,
    {
        use futures::FutureExt as _;
        future.map(move |output| self.complete(output, error))
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
    observe_output(span, future, |result| result.as_ref().err().map(kind))
}

/// Observe outputs that keep physical and observer failures separate, without
/// translating, cloning or reconstructing their original terminal evidence.
pub fn observe_output<T, F, K>(
    span: &Span,
    future: F,
    error: K,
) -> impl std::future::Future<Output = T> + use<T, F, K>
where
    F: std::future::Future<Output = T>,
    K: FnOnce(&T) -> Option<&'static str>,
{
    Completion::new(span).observe_output(future, error)
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
    /// Fixed-width counters lost precision. Operations continue without a cap;
    /// SLO evaluation remains unknown until this cohort is released.
    pub overflowed: bool,
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
        let (next, overflowed) = count.overflowing_add(1);
        self.overflowed |= overflowed;
        *count = if overflowed { u64::MAX } else { next };
        let bucket = usize::try_from(u128::BITS - nanos.leading_zeros()).unwrap_or(usize::MAX);
        if let Some(count) = self.latency_ns.get_mut(bucket) {
            let (next, overflowed) = count.overflowing_add(1);
            self.overflowed |= overflowed;
            *count = if overflowed { u64::MAX } else { next };
        }
    }
}

struct Series {
    sender: watch::Sender<Snapshot>,
    // Changed only while the index is locked. Receiver fields can outlive a
    // Query destructor, so watch receiver_count is not its registration count.
    selections: AtomicUsize,
}

impl Series {
    fn complete(&self, sample: &Sample<'_>) {
        self.sender
            .send_modify(|value| value.sample(sample.outcome, sample.elapsed.as_nanos()));
    }
}

impl Sink for Metrics {
    fn record(&self, sample: &Sample<'_>) {
        let series = {
            let names = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            names
                .find(sample.metadata.name(), sample.rpc)
                .and_then(|node| node.series.clone())
        };
        if let Some(series) = series {
            series.complete(sample);
        }
    }
}

/// Queryable operation metrics owned by an attached subscriber, with no history.
/// Only selected series are retained. The last query releases the series; a
/// later query starts a new cohort, even if the operation ran in between.
#[derive(Clone, Default)]
pub struct Metrics(Arc<Mutex<Names>>);

// A byte radix index: each node has at most 256 children, so lookup is bounded
// by the static operation name's bytes, independently of unrelated operations.
// Unlike a hash table this bound does not assume a collision distribution.
#[derive(Default)]
struct Names {
    children: BTreeMap<u8, Names>,
    series: Option<Arc<Series>>,
    methods: Option<Box<Names>>,
}

impl Metrics {
    /// Select a single static operation name. Only selected series are retained.
    ///
    /// Register the query before running the operation to include its first
    /// completion. Concurrent queries share the cohort until the last query is
    /// dropped. This does not install a subscriber or enable tracing.
    pub fn query(&self, operation: &str) -> Query {
        self.selected(operation, None)
    }

    /// Select an RPC method from a shared static span factory.
    pub fn query_rpc(&self, operation: &str, rpc: &str) -> Query {
        self.selected(operation, Some(rpc))
    }

    fn selected(&self, operation: &str, rpc: Option<&str>) -> Query {
        let mut names = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let mut node = &mut *names;
        node = node.select(operation);
        if let Some(rpc) = rpc {
            // A separate method namespace preserves the typed key. A delimiter
            // alone aliases None with Some("") and embedded delimiters.
            node = node.methods.get_or_insert_with(Box::default).select(rpc);
        }
        let series = node.series.get_or_insert_with(|| {
            Arc::new(Series {
                sender: watch::channel(Snapshot {
                    operation: operation.to_owned(),
                    rpc: rpc.map(str::to_owned),
                    latency_ns: vec![0; 129],
                    ..Snapshot::default()
                })
                .0,
                selections: AtomicUsize::new(0),
            })
        });
        series.selections.fetch_add(1, Ordering::Relaxed);
        Query {
            receiver: series.sender.subscribe(),
            producer: Arc::downgrade(&self.0),
            operation: operation.to_owned(),
            rpc: rpc.map(str::to_owned),
        }
    }
}

impl Names {
    fn find(&self, operation: &str, rpc: Option<&str>) -> Option<&Self> {
        let mut node = self;
        for byte in operation.bytes() {
            #[cfg(test)]
            INDEX_VISITS.set(INDEX_VISITS.get() + 1);
            node = node.children.get(&byte)?;
        }
        if let Some(rpc) = rpc {
            node = node.methods.as_deref()?;
            for byte in rpc.bytes() {
                #[cfg(test)]
                INDEX_VISITS.set(INDEX_VISITS.get() + 1);
                node = node.children.get(&byte)?;
            }
        }
        Some(node)
    }

    fn select(&mut self, name: &str) -> &mut Self {
        let mut node = self;
        for byte in name.bytes() {
            #[cfg(test)]
            INDEX_VISITS.set(INDEX_VISITS.get() + 1);
            node = node.children.entry(byte).or_default();
        }
        node
    }

    fn empty(&self) -> bool {
        self.children.is_empty() && self.series.is_none() && self.methods.is_none()
    }

    // An ownership zipper prunes only the selected key. No recursion, scan or
    // key-depth limit, including when an operator supplies a long query name.
    fn edit(&mut self, name: &str, edit: impl FnOnce(&mut Self)) {
        let mut node = std::mem::take(self);
        let mut parents = Vec::new();
        let mut found = true;
        for byte in name.bytes() {
            let Some(child) = node.children.remove(&byte) else {
                found = false;
                break;
            };
            parents.push((node, byte));
            node = child;
        }
        if found {
            edit(&mut node);
        }
        for (mut parent, byte) in parents.into_iter().rev() {
            if !node.empty() {
                parent.children.insert(byte, node);
            }
            node = parent;
        }
        *self = node;
    }
}

impl Drop for Names {
    fn drop(&mut self) {
        let mut pending: Vec<_> = std::mem::take(&mut self.children).into_values().collect();
        if let Some(methods) = self.methods.take() {
            pending.push(*methods);
        }
        while let Some(mut node) = pending.pop() {
            pending.extend(std::mem::take(&mut node.children).into_values());
            if let Some(methods) = node.methods.take() {
                pending.push(*methods);
            }
        }
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
    fn counter_overflow_cannot_produce_decisive_slo_evidence() {
        let slo = crate::Slo {
            version: 1,
            operation: "acyclic.test".into(),
            rpc: None,
            success_ppm: 0,
            latency: None,
        };
        for outcome in [Outcome::Ok, Outcome::Err, Outcome::Incomplete] {
            for histogram in [false, true] {
                let mut snapshot = Snapshot {
                    operation: slo.operation.clone(),
                    ok: u64::MAX,
                    err: u64::MAX,
                    incomplete: u64::MAX,
                    latency_ns: vec![0; 129],
                    ..Snapshot::default()
                };
                if histogram {
                    snapshot.ok = 0;
                    snapshot.err = 0;
                    snapshot.incomplete = 0;
                    snapshot.latency_ns[0] = u64::MAX;
                }
                snapshot.sample(outcome, 0);
                assert!(snapshot.overflowed);
                assert_eq!(slo.evaluate(&snapshot).unwrap().success, None);
                // The former silent saturation invents a decisive verdict.
                snapshot.overflowed = false;
                assert_eq!(slo.evaluate(&snapshot).unwrap().success, Some(true));
            }
        }
    }

    #[test]
    fn cohort_release_precedes_deferred_receiver_destruction()
    -> Result<(), Box<dyn std::error::Error>> {
        use tracing_subscriber::prelude::*;
        let metrics = Metrics::default();
        let first = metrics.query("acyclic.selected");
        let second = metrics.query("acyclic.selected");
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry().with(Operations(metrics.clone())),
        );
        tracing::dispatcher::with_default(&dispatch, || {
            let span = tracing::info_span!("acyclic.selected", outcome = tracing::field::Empty);
            record_outcome(&span, None);
        });
        assert_eq!(first.snapshot().ok, 1);
        // Retain the receiver fields across both logical deregistrations: the
        // exact concurrent schedule where both destructors finish before either
        // receiver field is destroyed. No thread timing or readiness probe.
        let receivers = [first.receiver.clone(), second.receiver.clone()];
        {
            let names = metrics.0.lock().unwrap_or_else(PoisonError::into_inner);
            let series = names
                .find("acyclic.selected", None)
                .and_then(|node| node.series.as_ref())
                .ok_or("selected cohort absent")?;
            // Negative control: both former receiver-count decisions retain
            // the cohort in this schedule, even though no query will remain.
            assert!(series.sender.receiver_count() > 1);
        }
        drop(first);
        drop(second);
        let reopened = metrics.query("acyclic.selected");
        assert_eq!(reopened.snapshot().ok, 0);
        drop(receivers);
        drop(reopened);
        let names = metrics.0.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(names.empty());
        Ok(())
    }

    #[test]
    fn selected_series_cost_is_independent_of_unrelated_series() {
        use tracing_subscriber::prelude::*;
        let metrics = Metrics::default();
        let query = metrics.query("acyclic.selected");
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry().with(Operations(metrics.clone())),
        );
        let mut unrelated = Vec::new();
        let mut previous = 0;
        for (sample, population) in [1, 1_000, 100_000].into_iter().enumerate() {
            for index in previous..population {
                unrelated.push(metrics.query(&format!("acyclic.unrelated.{index}")));
            }
            previous = population;
            INDEX_VISITS.set(0);
            let selected = metrics.query("acyclic.selected");
            assert_eq!(INDEX_VISITS.get(), "acyclic.selected".len());
            assert_eq!(query.snapshot(), selected.snapshot());
            INDEX_VISITS.set(0);
            tracing::dispatcher::with_default(&dispatch, || {
                let span = tracing::info_span!("acyclic.selected", outcome = tracing::field::Empty);
                record_outcome(&span, None);
            });
            assert_eq!(INDEX_VISITS.get(), "acyclic.selected".len());
            let snapshot = query.snapshot();
            assert_eq!(snapshot.ok, sample as u64 + 1);
            assert_eq!(snapshot.latency_ns.iter().sum::<u64>(), snapshot.ok);
        }
        // A full namespace visit, formerly typical of snapshot collectors,
        // exceeds the same bound even though it returns the identical series.
        fn full_visit(names: &Names) -> usize {
            1 + names.children.values().map(full_visit).sum::<usize>()
        }
        {
            let names = metrics.0.lock().unwrap_or_else(PoisonError::into_inner);
            assert!(full_visit(&names) > "acyclic.selected".len());
        }
        drop(unrelated);
        let names = metrics.0.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(full_visit(&names), "acyclic.selected".len() + 1);
    }
}

/// A live query waits on the selected operation's completion event.
/// Dropping the subscriber and metrics producer ends the wait with [`QueryError`].
pub struct Query {
    receiver: watch::Receiver<Snapshot>,
    producer: Weak<Mutex<Names>>,
    operation: String,
    rpc: Option<String>,
}

impl Drop for Query {
    fn drop(&mut self) {
        let Some(producer) = self.producer.upgrade() else {
            return;
        };
        let mut names = producer.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(series) = names
            .find(&self.operation, self.rpc.as_deref())
            .and_then(|node| node.series.as_ref())
        else {
            return;
        };
        if series.selections.fetch_sub(1, Ordering::Relaxed) != 1 {
            return;
        }
        names.edit(&self.operation, |node| match &self.rpc {
            Some(rpc) => {
                if let Some(methods) = &mut node.methods {
                    methods.edit(rpc, |node| node.series = None);
                    if methods.empty() {
                        node.methods = None;
                    }
                }
            }
            None => node.series = None,
        });
    }
}

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
        self.receiver.borrow().clone()
    }

    /// Waits for a selected completion, or returns a typed producer-closed error.
    pub async fn changed(&mut self) -> Result<Snapshot, QueryError> {
        self.receiver.changed().await.map_err(|_| QueryError)?;
        Ok(self.receiver.borrow_and_update().clone())
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

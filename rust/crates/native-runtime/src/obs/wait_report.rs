use std::{
    fmt,
    io::{self, Write},
    sync::{Arc, Mutex, PoisonError, Weak},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tracing::{Id, Subscriber, field::Visit, span::Attributes};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

/// Collects the live [`super::waiting`] spans of one subscriber.
///
/// Install this layer alongside the host's diagnostic layer, without applying
/// the diagnostic verbosity filter to it. The process owner calls [`Self::snapshot`]
/// at shutdown or after its existing termination signal. This layer installs no
/// signal handler, task, timer or readiness mechanism.
///
/// Each span links and unlinks its own node in constant work, independent of
/// history or other waits. Reporting visits the live waits once and releases
/// the registry lock before writing. Keys are opaque operation identities;
/// callers must never supply prompts or credentials.
#[derive(Clone, Default)]
pub struct WaitReport(Arc<Mutex<Chain>>);

#[cfg(test)]
thread_local! {
    static REGISTRY_LOCKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static REGISTRY_ACQUIRE_NANOS: std::cell::Cell<u128> = const { std::cell::Cell::new(0) };
}

/// An owned point-in-time observation, independent of later span settlement.
pub struct WaitSnapshot {
    nodes: Vec<Arc<Node>>,
    captured: Instant,
}

#[derive(Default)]
struct Chain {
    first: Option<Arc<Node>>,
    last: Weak<Node>,
    len: usize,
}

struct Node {
    fields: Fields,
    started: SystemTime,
    elapsed: Instant,
    links: Mutex<Links>,
}

#[derive(Default)]
struct Links {
    previous: Weak<Node>,
    next: Option<Arc<Node>>,
}

struct SpanWait(Arc<Node>);

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
        match field.name() {
            "primitive" => self.primitive = format!("{value:?}"),
            "key" => self.key = format!("{value:?}"),
            _ => {}
        }
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for WaitReport {
    fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
        if attributes.metadata().target() != "acyclic.wait"
            || attributes.metadata().name() != "waiting"
        {
            return;
        }
        let Some(span) = context.span(id) else {
            return;
        };
        let mut fields = Fields::default();
        attributes.record(&mut fields);
        let node = Arc::new(Node {
            fields,
            started: SystemTime::now(),
            elapsed: Instant::now(),
            links: Mutex::default(),
        });
        let mut chain = self.registry();
        if let Some(last) = chain.last.upgrade() {
            node.links
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .previous = Arc::downgrade(&last);
            last.links
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .next = Some(Arc::clone(&node));
        } else {
            chain.first = Some(Arc::clone(&node));
        }
        chain.last = Arc::downgrade(&node);
        chain.len += 1;
        span.extensions_mut().insert(SpanWait(node));
    }

    fn on_close(&self, id: Id, context: Context<'_, S>) {
        let Some(span) = context.span(&id) else {
            return;
        };
        let Some(SpanWait(node)) = span.extensions_mut().remove::<SpanWait>() else {
            return;
        };
        let mut chain = self.registry();
        let mut links = node.links.lock().unwrap_or_else(PoisonError::into_inner);
        let previous = links.previous.upgrade();
        let next = links.next.take();
        links.previous = Weak::new();
        if let Some(previous) = &previous {
            previous
                .links
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .next = next.clone();
        } else {
            chain.first = next.clone();
        }
        if let Some(next) = next {
            next.links
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .previous = previous.as_ref().map_or_else(Weak::new, Arc::downgrade);
        } else {
            chain.last = previous.as_ref().map_or_else(Weak::new, Arc::downgrade);
        }
        chain.len -= 1;
    }
}

impl WaitReport {
    fn registry(&self) -> std::sync::MutexGuard<'_, Chain> {
        #[cfg(not(test))]
        {
            self.0.lock().unwrap_or_else(PoisonError::into_inner)
        }
        #[cfg(test)]
        {
            REGISTRY_LOCKS.with(|count| count.set(count.get() + 1));
            let started = Instant::now();
            let chain = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            REGISTRY_ACQUIRE_NANOS
                .with(|elapsed| elapsed.set(elapsed.get() + started.elapsed().as_nanos()));
            chain
        }
    }

    /// Captures live observations before cancelling their producers.
    ///
    /// This visits the W live waits once under one registry critical section.
    /// It clones owners, not strings or registry links. Output never holds the
    /// registry lock, and later span closure does not erase the captured fields.
    #[must_use]
    pub fn snapshot(&self) -> WaitSnapshot {
        let chain = self.registry();
        let mut next = chain.first.clone();
        let mut nodes = Vec::with_capacity(chain.len);
        while let Some(node) = next {
            next = node
                .links
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .next
                .clone();
            nodes.push(node);
        }
        WaitSnapshot {
            nodes,
            captured: Instant::now(),
        }
    }

    /// Writes a snapshot of live primitive/key/start observations, then flushes.
    ///
    /// This synchronous compatibility entry may block the calling thread.
    /// Coordination owners use [`Self::snapshot`] and
    /// [`WaitSnapshot::try_write_to`] to keep cancellation independent of output.
    pub fn report(&self, output: &mut impl Write) -> io::Result<()> {
        self.snapshot().write_to(output)
    }
}

impl WaitSnapshot {
    /// Whether the captured observation contains no live waits.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Writes on the caller's thread, flushing once at the report boundary.
    /// Output errors are returned unchanged; no registry lock is acquired.
    pub fn write_to(&self, output: &mut impl Write) -> io::Result<()> {
        for node in &self.nodes {
            let since = node.started.duration_since(UNIX_EPOCH).unwrap_or_default();
            writeln!(
                output,
                "waiting: {} {} since {:.3} ({:.3}s elapsed)",
                node.fields.primitive,
                node.fields.key,
                since.as_secs_f64(),
                self.captured
                    .saturating_duration_since(node.elapsed)
                    .as_secs_f64(),
            )?;
        }
        output.flush()
    }
}

#[cfg(test)]
mod tests;

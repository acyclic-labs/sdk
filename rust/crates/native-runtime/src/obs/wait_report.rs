use std::{
    fmt,
    io::{self, Write},
    sync::{Arc, Mutex, PoisonError, Weak},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tracing::{
    Id, Subscriber,
    field::Visit,
    span::{Attributes, Record},
};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

/// Collects the live [`super::waiting`] spans of one subscriber.
///
/// Install this layer alongside the host's diagnostic layer, without applying
/// the diagnostic verbosity filter to it. The process owner calls [`Self::report`]
/// at shutdown or after its existing termination signal. This layer installs no
/// signal handler, task, timer or readiness mechanism.
///
/// Each span links and unlinks its own node in constant work, independent of
/// history or other waits. Reporting visits the live waits once and releases
/// the registry lock before writing. Keys are opaque operation identities;
/// callers must never supply prompts or credentials.
/// Persistence jobs include their creating span as the causal origin. A file
/// predecessor identifies the exact native fence currently blocking that job.
#[derive(Clone, Default)]
pub struct WaitReport(Arc<Mutex<Chain>>);

#[derive(Default)]
struct Chain {
    first: Option<Arc<Node>>,
    last: Weak<Node>,
}

struct Node {
    fields: Fields,
    stage: std::sync::atomic::AtomicU64,
    job: u64,
    origin: Option<u64>,
    predecessor: std::sync::atomic::AtomicU64,
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
            stage: std::sync::atomic::AtomicU64::new(0),
            job: id.clone().into_u64(),
            origin: span.parent().map(|parent| parent.id().into_u64()),
            predecessor: std::sync::atomic::AtomicU64::new(0),
            started: SystemTime::now(),
            elapsed: Instant::now(),
            links: Mutex::default(),
        });
        let mut chain = self.0.lock().unwrap_or_else(PoisonError::into_inner);
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
        span.extensions_mut().insert(SpanWait(node));
    }

    fn on_close(&self, id: Id, context: Context<'_, S>) {
        self.retire(&id, &context);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, context: Context<'_, S>) {
        #[derive(Default)]
        struct Finished {
            finished: bool,
            stage: Option<u64>,
            predecessor: Option<u64>,
        }
        impl Visit for Finished {
            fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
                if field.name() == "finished" {
                    self.finished = value;
                }
            }
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn fmt::Debug) {}
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                if field.name() == "stage" {
                    self.stage = Some(value);
                } else if field.name() == "predecessor" {
                    self.predecessor = Some(value);
                }
            }
        }
        let mut finished = Finished::default();
        values.record(&mut finished);
        if finished.finished {
            self.retire(id, &context);
            return;
        }
        if let Some(span) = context.span(id)
            && let Some(SpanWait(node)) = span.extensions().get::<SpanWait>()
        {
            if let Some(stage) = finished.stage {
                node.stage
                    .store(stage, std::sync::atomic::Ordering::Relaxed);
            }
            if let Some(predecessor) = finished.predecessor {
                node.predecessor
                    .store(predecessor, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
}

impl WaitReport {
    fn retire<S: Subscriber + for<'a> LookupSpan<'a>>(&self, id: &Id, context: &Context<'_, S>) {
        let Some(span) = context.span(id) else {
            return;
        };
        let Some(SpanWait(node)) = span.extensions_mut().remove::<SpanWait>() else {
            return;
        };
        let mut chain = self.0.lock().unwrap_or_else(PoisonError::into_inner);
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
    }

    /// Writes a snapshot of live primitive/key/start observations, then flushes.
    ///
    /// A blocked or failed writer cannot hold the lock needed by a completing
    /// producer. Output errors are returned unchanged to the process owner.
    pub fn report(&self, output: &mut impl Write) -> io::Result<()> {
        let nodes = {
            let chain = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            let mut next = chain.first.clone();
            let mut nodes = Vec::new();
            while let Some(node) = next {
                next = node
                    .links
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .next
                    .clone();
                nodes.push(node);
            }
            nodes
        };
        for node in nodes {
            let since = node.started.duration_since(UNIX_EPOCH).unwrap_or_default();
            write!(
                output,
                "waiting: {} {}",
                node.fields.primitive, node.fields.key
            )?;
            if node.fields.primitive == "persistence-completion" {
                let stage = match node.stage.load(std::sync::atomic::Ordering::Relaxed) {
                    0 => "admitted",
                    1 => "queued",
                    2 => "running",
                    3 => "syscall",
                    4 => "published",
                    5 => "file-predecessor",
                    _ => "invalid",
                };
                write!(
                    output,
                    " job={} origin={:?} predecessor={:#x} stage={stage}",
                    node.job,
                    node.origin,
                    node.predecessor.load(std::sync::atomic::Ordering::Relaxed)
                )?;
            }
            writeln!(
                output,
                " since {:.3} ({:.3}s elapsed)",
                since.as_secs_f64(),
                node.elapsed.elapsed().as_secs_f64()
            )?;
        }
        output.flush()
    }
}

#[cfg(test)]
mod tests;

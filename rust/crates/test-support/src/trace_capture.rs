//! Shared tracing subscriber fixture for SDK tests.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use tracing_subscriber::{layer::Context, registry::LookupSpan};

/// A historical span, retained when registry IDs are reused.
#[derive(Clone, Debug, Default)]
pub struct CapturedSpan {
    /// Static operation name.
    pub name: &'static str,
    /// Registry identity.
    pub id: u64,
    /// Registry identity of the original parent.
    pub parent_id: Option<u64>,
    /// Historical index of the original parent.
    pub parent: Option<usize>,
    /// Historical indices of causal links, without retaining span references.
    pub follows_from: Vec<usize>,
    /// Every declared field, including fields never recorded.
    pub declared_fields: Vec<&'static str>,
    /// Latest recorded values, with strings unquoted.
    pub fields: BTreeMap<String, String>,
    /// Whether the final span reference has retired.
    pub closed: bool,
}

type EventFields = Vec<(String, String, String)>;

/// Collected span history, field updates, events and closes.
pub struct Evidence {
    /// Span history in creation order.
    pub spans: Vec<CapturedSpan>,
    live: HashMap<u64, usize>,
    /// Field updates as operation, field and debug value.
    pub records: Vec<(String, String, String)>,
    /// Closed operation names in retirement order.
    pub closes: Vec<String>,
    /// Event parents and debug fields.
    pub events: Vec<(Option<String>, EventFields)>,
    // Keep callsite registration independent of whichever thread first emits.
    // tracing-core's single-dispatcher path consults that thread's default,
    // which can be empty even while this observer is registered elsewhere.
    // This separate, non-recording subscriber cannot retain our capture.
    _registration: tracing::Dispatch,
}

impl Default for Evidence {
    fn default() -> Self {
        Self {
            spans: Vec::new(),
            live: HashMap::new(),
            records: Vec::new(),
            closes: Vec::new(),
            events: Vec::new(),
            _registration: tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()),
        }
    }
}

/// A cloneable capture layer and asynchronous change notification.
#[derive(Clone, Default)]
pub struct Capture(
    /// Collected history.
    pub Arc<Mutex<Evidence>>,
    /// Notification after a field update or close.
    pub Arc<tokio::sync::Notify>,
);

impl tracing::field::Visit for CapturedSpan {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().into(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields.insert(field.name().into(), value.into());
    }
}

struct DebugFields<'a>(&'a mut EventFields, &'a str);
impl tracing::field::Visit for DebugFields<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .push((self.1.into(), field.name().into(), format!("{value:?}")));
    }
}

impl<S> tracing_subscriber::Layer<S> for Capture
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::Id,
        ctx: Context<'_, S>,
    ) {
        let parent_id = ctx
            .span(id)
            .and_then(|span| span.parent().map(|parent| parent.id().into_u64()));
        let mut evidence = self.0.lock().unwrap();
        let mut captured = CapturedSpan {
            name: attrs.metadata().name(),
            id: id.into_u64(),
            parent_id,
            parent: parent_id.and_then(|id| evidence.live.get(&id).copied()),
            declared_fields: attrs
                .metadata()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect(),
            ..Default::default()
        };
        attrs.record(&mut captured);
        let index = evidence.spans.len();
        evidence.spans.push(captured);
        evidence.live.insert(id.into_u64(), index);
    }
    fn on_follows_from(&self, id: &tracing::Id, follows: &tracing::Id, _: Context<'_, S>) {
        // Capture locks are never nested and never held while calling tracing.
        let mut evidence = self.0.lock().unwrap();
        let target = evidence.live[&id.into_u64()];
        if let Some(source) = evidence.live.get(&follows.into_u64()).copied() {
            evidence.spans[target].follows_from.push(source);
        }
    }
    fn on_record(&self, id: &tracing::Id, record: &tracing::span::Record<'_>, _: Context<'_, S>) {
        let mut evidence = self.0.lock().unwrap();
        let index = evidence.live[&id.into_u64()];
        let name = evidence.spans[index].name;
        record.record(&mut evidence.spans[index]);
        record.record(&mut DebugFields(&mut evidence.records, name));
        self.1.notify_one();
    }
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let parent = ctx.event_span(event).map(|span| span.name().into());
        let mut fields = Vec::new();
        event.record(&mut DebugFields(&mut fields, event.metadata().target()));
        let mut evidence = self.0.lock().unwrap();
        evidence.events.push((parent, fields));
    }
    fn on_close(&self, id: tracing::Id, _: Context<'_, S>) {
        let mut evidence = self.0.lock().unwrap();
        let index = evidence.live.remove(&id.into_u64()).unwrap();
        evidence.spans[index].closed = true;
        let name = evidence.spans[index].name.into();
        evidence.closes.push(name);
        self.1.notify_one();
    }
}

impl Capture {
    /// Parent name of the first matching span.
    pub fn parent(&self, name: &str) -> Option<&'static str> {
        let evidence = self.0.lock().unwrap();
        evidence
            .spans
            .iter()
            .find(|span| span.name == name)
            .and_then(|span| span.parent)
            .map(|index| evidence.spans[index].name)
    }
    /// Whether the first matching span has the named ancestor.
    pub fn has_ancestor(&self, name: &str, ancestor: &str) -> bool {
        let evidence = self.0.lock().unwrap();
        let mut parent = evidence
            .spans
            .iter()
            .find(|span| span.name == name)
            .and_then(|span| span.parent);
        while let Some(index) = parent {
            let span = &evidence.spans[index];
            if span.name == ancestor {
                return true;
            }
            parent = span.parent;
        }
        false
    }
    /// Number of retired matching spans.
    pub fn closed(&self, name: &str) -> usize {
        self.0
            .lock()
            .unwrap()
            .spans
            .iter()
            .filter(|span| span.name == name && span.closed)
            .count()
    }
    /// Latest fields of every matching span.
    pub fn all(&self, name: &str) -> Vec<BTreeMap<String, String>> {
        self.0
            .lock()
            .unwrap()
            .spans
            .iter()
            .filter(|span| span.name == name)
            .map(|span| span.fields.clone())
            .collect()
    }
    /// Latest fields of the first matching span.
    pub fn fields(&self, name: &str) -> BTreeMap<String, String> {
        self.all(name).into_iter().next().unwrap_or_default()
    }
    /// Retired matching spans in creation order.
    pub fn spans(&self, name: &str) -> Vec<CapturedSpan> {
        self.0
            .lock()
            .unwrap()
            .spans
            .iter()
            .filter(|span| span.name == name && span.closed)
            .cloned()
            .collect()
    }
}

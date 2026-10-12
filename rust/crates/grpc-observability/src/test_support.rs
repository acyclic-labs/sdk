//! Shared tracing capture and isolated TLS fixtures for SDK tests.
#![allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::missing_panics_doc, clippy::type_complexity, reason = "test capture invariants fail immediately; plain fixture tuples avoid forwarding types")]

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
    /// Every declared field, including fields never recorded.
    pub declared_fields: Vec<&'static str>,
    /// Latest recorded values, with strings unquoted.
    pub fields: BTreeMap<String, String>,
    /// Whether the final span reference has retired.
    pub closed: bool,
}

/// Collected span history, field updates, events and closes.
#[derive(Default)]
pub struct Evidence {
    /// Span history in creation order.
    pub spans: Vec<CapturedSpan>,
    live: HashMap<u64, usize>,
    /// Field updates as operation, field and debug value.
    pub records: Vec<(String, String, String)>,
    /// Closed operation names in retirement order.
    pub closes: Vec<String>,
    /// Parent of the last observed event.
    pub event_parent: Option<String>,
    /// Event parents and debug fields.
    pub events: Vec<(Option<String>, Vec<(String, String, String)>)>,
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
        self.fields.insert(field.name().into(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields.insert(field.name().into(), value.into());
    }
}

struct DebugFields<'a>(&'a mut Vec<(String, String, String)>, &'a str);
impl tracing::field::Visit for DebugFields<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.push((self.1.into(), field.name().into(), format!("{value:?}")));
    }
}

impl<S> tracing_subscriber::Layer<S> for Capture
where S: tracing::Subscriber + for<'a> LookupSpan<'a> {
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &tracing::Id, ctx: Context<'_, S>) {
        let parent_id = ctx.span(id).and_then(|span| span.parent().map(|parent| parent.id().into_u64()));
        let mut evidence = self.0.lock().unwrap();
        let mut captured = CapturedSpan {
            name: attrs.metadata().name(), id: id.into_u64(), parent_id,
            parent: parent_id.and_then(|id| evidence.live.get(&id).copied()),
            declared_fields: attrs.metadata().fields().iter().map(|field| field.name()).collect(),
            ..Default::default()
        };
        attrs.record(&mut captured);
        let index = evidence.spans.len();
        evidence.spans.push(captured);
        evidence.live.insert(id.into_u64(), index);
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
        evidence.event_parent = parent.clone();
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
        evidence.spans.iter().find(|span| span.name == name)
            .and_then(|span| span.parent).map(|index| evidence.spans[index].name)
    }
    /// Whether the first matching span has the named ancestor.
    pub fn has_ancestor(&self, name: &str, ancestor: &str) -> bool {
        let evidence = self.0.lock().unwrap();
        let mut parent = evidence.spans.iter().find(|span| span.name == name).and_then(|span| span.parent);
        while let Some(index) = parent {
            let span = &evidence.spans[index];
            if span.name == ancestor { return true; }
            parent = span.parent;
        }
        false
    }
    /// Number of retired matching spans.
    pub fn closed(&self, name: &str) -> usize {
        self.0.lock().unwrap().spans.iter().filter(|span| span.name == name && span.closed).count()
    }
    /// Latest fields of every matching span.
    pub fn all(&self, name: &str) -> Vec<BTreeMap<String, String>> {
        self.0.lock().unwrap().spans.iter().filter(|span| span.name == name)
            .map(|span| span.fields.clone()).collect()
    }
    /// Latest fields of the first matching span.
    pub fn fields(&self, name: &str) -> BTreeMap<String, String> {
        self.all(name).into_iter().next().unwrap_or_default()
    }
    /// Retired matching spans in creation order.
    pub fn spans(&self, name: &str) -> Vec<CapturedSpan> {
        self.0.lock().unwrap().spans.iter().filter(|span| span.name == name && span.closed).cloned().collect()
    }
}

/// A gRPC body that emits one unavailable trailer after 50 ms.
pub fn delayed_unavailable_body() -> tonic::body::Body {
    tonic::body::Body::new(Trailers(Some(Box::pin(tokio::time::sleep(
        std::time::Duration::from_millis(50),
    )))))
}

struct Trailers(Option<std::pin::Pin<Box<tokio::time::Sleep>>>);
impl http_body::Body for Trailers {
    type Data = tonic::codegen::Bytes;
    type Error = std::convert::Infallible;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        use std::{future::Future as _, task::Poll};
        let Some(sleep) = self.0.as_mut() else { return Poll::Ready(None); };
        std::task::ready!(sleep.as_mut().poll(cx));
        self.0 = None;
        let mut trailers = tonic::codegen::http::HeaderMap::new();
        trailers.insert("grpc-status", 14.into());
        Poll::Ready(Some(Ok(http_body::Frame::trailers(trailers))))
    }
}

/// Re-executes one TLS test in isolated trust environments, or returns its PEM inputs and mode.
pub fn tls_fixture(module: &str) -> Result<Option<(String, String, Vec<u8>, String)>, Box<dyn std::error::Error + Send + Sync>> {
    const FIXTURE: &str = "ACYCLIC_TLS_FIXTURE";
    const MODE: &str = "ACYCLIC_TLS_MODE";
    let Ok(directory) = std::env::var(FIXTURE) else {
        let directory = tempfile::tempdir()?;
        let trusted = directory.path().join("trusted");
        std::fs::create_dir(&trusted)?;
        let certified = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
        std::fs::write(trusted.join("root.pem"), certified.cert.pem())?;
        std::fs::write(directory.path().join("key.pem"), certified.signing_key.serialize_pem())?;
        let unrelated = rcgen::generate_simple_self_signed(["unrelated".to_owned()])?;
        std::fs::write(directory.path().join("unrelated.pem"), unrelated.cert.pem())?;
        std::fs::write(directory.path().join("empty.pem"), "")?;
        let test = format!("{}::platform_roots_and_exclusive_ca", module.split_once("::").map_or(module, |(_, path)| path));
        let modes = if cfg!(target_os = "linux") { &["unrelated", "file", "dir", "empty"][..] } else { &["unrelated"][..] };
        for mode in modes {
            let mut child = std::process::Command::new(std::env::current_exe()?);
            #[cfg(windows)]
            {
                use std::os::windows::process::CommandExt as _;
                child.creation_flags(0x08000000);
            }
            child.args([test.as_str(), "--exact", "--nocapture"])
                .env(FIXTURE, directory.path()).env(MODE, mode)
                .env_remove("SSL_CERT_FILE").env_remove("SSL_CERT_DIR");
            match *mode {
                "file" => { child.env("SSL_CERT_FILE", trusted.join("root.pem")); }
                "dir" => { child.env("SSL_CERT_DIR", &trusted); }
                "empty" => { child.env("SSL_CERT_FILE", directory.path().join("empty.pem")); }
                _ => { child.env("SSL_CERT_FILE", directory.path().join("unrelated.pem")); }
            }
            let output = child.output()?;
            assert!(output.status.success(), "{mode}: {}\n{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        }
        return Ok(None);
    };
    let directory = std::path::Path::new(&directory);
    Ok(Some((std::fs::read_to_string(directory.join("trusted/root.pem"))?,
        std::fs::read_to_string(directory.join("key.pem"))?,
        std::fs::read(directory.join("unrelated.pem"))?, std::env::var(MODE)?)))
}

//! Tracing glue for `docs/observability.md`. Spans come from
//! `#[cfg_attr(not(target_arch = "wasm32"), tracing::instrument(..))]`; these
//! helpers compile to no-ops on wasm32, which never links `tracing`.

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use tracing::field::Empty;

/// Records `outcome` and `error.kind` on the current span, then returns `result`.
pub(crate) fn outcome<T>(result: crate::Result<T>) -> crate::Result<T> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let span = tracing::Span::current();
        match &result {
            Ok(_) => span.record("outcome", "ok"),
            Err(error) => span
                .record("outcome", "err")
                .record("error.kind", error.kind()),
        };
    }
    result
}

/// Records extra fields on the current span; expands to nothing on wasm32.
macro_rules! obs_record {
    ($($field:literal = $value:expr),+ $(,)?) => {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let span = ::tracing::Span::current();
            $(span.record($field, $value);)+
        }
    };
}
pub(crate) use obs_record;

/// Opens a span (`DEBUG` unless prefixed with `INFO,`) with `outcome`/`error.kind`
/// slots for [`traced`]; an empty [`Span`] on wasm32.
macro_rules! obs_span {
    (INFO, $($rest:tt)+) => { crate::obs::obs_span!(@ INFO, $($rest)+) };
    (@ $level:ident, $name:literal $(, $field:ident = $value:expr)* $(,)?) => {{
        #[cfg(not(target_arch = "wasm32"))]
        let span = ::tracing::span!(
            ::tracing::Level::$level,
            $name,
            $($field = $value,)*
            outcome = ::tracing::field::Empty,
            error.kind = ::tracing::field::Empty,
        );
        #[cfg(target_arch = "wasm32")]
        let span = crate::obs::Span;
        span
    }};
    ($($rest:tt)+) => { crate::obs::obs_span!(@ DEBUG, $($rest)+) };
}
pub(crate) use obs_span;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type Span = tracing::Span;
/// wasm32 records nothing.
#[cfg(target_arch = "wasm32")]
#[derive(Clone, Copy)]
pub(crate) struct Span;

/// Boxes a trait method's future inside `span` and records its outcome there.
pub(crate) fn traced<'a, T: 'a>(
    span: Span,
    future: impl Future<Output = crate::Result<T>> + acyclic_stream::ProviderTask + 'a,
) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<T>> {
    #[cfg(not(target_arch = "wasm32"))]
    return Box::pin(tracing::Instrument::instrument(
        async move { outcome(future.await) },
        span,
    ));
    #[cfg(target_arch = "wasm32")]
    {
        let Span = span;
        Box::pin(future)
    }
}

/// Span-test capture layer shared by the crate's test modules.
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod capture {
    use std::sync::{Arc, Mutex};
    use tracing::{
        field::{Field, Visit},
        span,
    };
    use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

    /// `(span, field, value)` for every declared and recorded span field.
    pub(crate) type Seen = Arc<Mutex<Vec<(&'static str, &'static str, String)>>>;

    struct Capture(Seen);
    struct Fields<'a>(
        &'static str,
        &'a mut Vec<(&'static str, &'static str, String)>,
    );
    impl Visit for Fields<'_> {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.1.push((self.0, field.name(), value.to_owned()));
        }
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.1.push((self.0, field.name(), format!("{value:?}")));
        }
    }
    impl<S: tracing::Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
        fn on_new_span(&self, attrs: &span::Attributes<'_>, _: &span::Id, _: Context<'_, S>) {
            let name = attrs.metadata().name();
            let mut seen = self.0.lock().unwrap();
            let fields = attrs.metadata().fields();
            seen.extend(fields.iter().map(|f| (name, f.name(), String::new())));
            attrs.record(&mut Fields(name, &mut seen));
        }
        fn on_record(&self, id: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
            let name = ctx.span(id).unwrap().name();
            values.record(&mut Fields(name, &mut self.0.lock().unwrap()));
        }
    }

    /// Captures this thread's spans until the guard drops. A second live
    /// dispatcher makes callsites that a concurrent test reached first
    /// consult this subscriber too, instead of caching that thread's interest.
    pub(crate) fn install() -> (Seen, impl Sized) {
        use tracing_subscriber::layer::SubscriberExt as _;
        let second = tracing::Dispatch::new(tracing_subscriber::registry());
        let seen = Seen::default();
        let guard = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(Capture(Arc::clone(&seen))),
        );
        (seen, (guard, second))
    }

    pub(crate) fn has(seen: &Seen, span: &str, field: &str, value: &str) -> bool {
        let seen = seen.lock().unwrap();
        seen.iter()
            .any(|(s, f, v)| *s == span && *f == field && v == value)
    }

    /// Asserts no span declares a forbidden field or records one of `leaks`.
    pub(crate) fn assert_clean(seen: &Seen, leaks: &[&str]) {
        for (span, field, value) in seen.lock().unwrap().iter() {
            assert!(
                !["path", "token", "content", "body", "authorization"].contains(field),
                "{span} records {field}"
            );
            assert!(
                !leaks.iter().any(|leak| value.contains(leak)),
                "{span}.{field}={value}"
            );
        }
    }
}

use super::OperationSpan;
use std::{
    convert::Infallible,
    fmt::Display,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::{
    Dispatch, Level, Metadata, Span,
    field::{FieldSet, Value},
    level_filters::{LevelFilter, STATIC_MAX_LEVEL},
};
use tracing_core::{
    callsite::{DefaultCallsite, Identifier},
    metadata::Kind,
};

/// Retires a reported wait when dropped, including on unwind.
struct Retire(OperationSpan);

impl Drop for Retire {
    fn drop(&mut self) {
        self.0.record("finished", true);
    }
}

enum Report<K> {
    /// No subscriber recorded waits when this one began, or it has retired.
    Unreported,
    /// Opens under the dispatcher that created the wait once its producer is
    /// first pending; a wait ready on its first poll never opens a span.
    Deferred {
        primitive: &'static str,
        key: K,
        dispatch: Dispatch,
    },
    Open(Retire),
}

pin_project_lite::pin_project! {
    /// A primitive wait, reported from its first pending poll until its future
    /// completes or is dropped.
    pub struct Waiting<F, K> {
        #[pin]
        inner: Option<F>,
        report: Report<K>,
    }
    impl<F, K> PinnedDrop for Waiting<F, K> {
        fn drop(this: Pin<&mut Self>) {
            let mut this = this.project();
            if let Report::Open(Retire(context)) = this.report {
                context.in_scope(|| this.inner.set(None));
            }
        }
    }
}

/// The one callsite of every wait span. [`waits_reported`] asks a subscriber
/// about this same metadata, so no filter can admit the span yet refuse the
/// check, or the reverse.
static WAITING: DefaultCallsite = DefaultCallsite::new(&WAITING_METADATA);
static WAITING_METADATA: Metadata<'static> = Metadata::new(
    "waiting",
    "acyclic.wait",
    Level::INFO,
    Some(file!()),
    Some(line!()),
    Some(module_path!()),
    FieldSet::new(
        &["primitive", "key", "stage", "predecessor", "finished"],
        Identifier(&WAITING),
    ),
    Kind::SPAN,
);

fn span(primitive: &'static str, key: &dyn Display) -> OperationSpan {
    let fields = WAITING_METADATA.fields();
    let mut field = fields.iter();
    let span = match (
        waits_reported(),
        field.next(),
        field.next(),
        field.next(),
        field.next(),
        field.next(),
    ) {
        (true, Some(name), Some(id), Some(stage), Some(predecessor), Some(finished)) => Span::new(
            &WAITING_METADATA,
            &fields.value_set(&[
                (&name, Some(&primitive as &dyn Value)),
                (&id, Some(&tracing::field::display(key) as &dyn Value)),
                (&stage, Some(&0_u64 as &dyn Value)),
                (&predecessor, Some(&0_u64 as &dyn Value)),
                (&finished, None),
            ]),
        ),
        _ => Span::none(),
    };
    OperationSpan::new(span)
}

impl<F: Future, K: Display> Future for Waiting<F, K> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let Some(mut inner) = this.inner.as_mut().as_pin_mut() else {
            return Poll::Pending;
        };
        let result = match this.report {
            Report::Open(Retire(context)) => context.in_scope(|| inner.as_mut().poll(cx)),
            _ => inner.as_mut().poll(cx),
        };
        if result.is_ready() {
            match this.report {
                Report::Open(Retire(context)) => context.in_scope(|| this.inner.set(None)),
                _ => this.inner.set(None),
            }
            *this.report = Report::Unreported;
        } else if let Report::Deferred {
            primitive,
            key,
            dispatch,
        } = this.report
        {
            let context = super::with_dispatch(dispatch, || span(primitive, &*key));
            *this.report = Report::Open(Retire(context));
        }
        result
    }
}

/// Names the primitive and opaque operation key a future is waiting for.
///
/// A wait is reported from its first pending poll, so one that is ready on its
/// first poll costs only the subscriber interest check: it allocates nothing
/// and opens no span. The report is recorded by the subscriber that was current
/// when the wait was created, and retires with the future, even if descendants
/// retain its tracing span. Work done during the first poll runs before the
/// report opens, so a wait started there, including one nested in another
/// wait's first poll, appears as its own root rather than as a child.
///
/// This adds no task, timer, wake or completion flag governing readiness:
/// only the producer determines readiness.
/// Keys must identify operations, never contain prompts or credentials.
pub fn waiting<F: Future, K: Display>(primitive: &'static str, key: K, future: F) -> Waiting<F, K> {
    let report = if waits_reported() {
        Report::Deferred {
            primitive,
            key,
            dispatch: tracing::dispatcher::get_default(Dispatch::clone),
        }
    } else {
        Report::Unreported
    };
    Waiting {
        inner: Some(future),
        report,
    }
}

/// A wait reported from creation, with its context, for producers that run
/// work under the wait before first polling it.
pub(crate) fn waiting_now<F>(
    primitive: &'static str,
    key: impl Display,
    future: F,
) -> (Waiting<F, Infallible>, OperationSpan) {
    let context = span(primitive, &key);
    let wait = Waiting {
        inner: Some(future),
        report: Report::Open(Retire(context.clone())),
    };
    (wait, context)
}

/// Whether tracing admits a [`waiting`] span for the current subscriber.
/// This asks about the span's own callsite and metadata, exactly as creating
/// the span does. A producer that would allocate to wrap a wait asks this first.
/// Like tracing's enabled checks, this is conservative: a subscriber's
/// per-layer filters can still exclude the admitted span from every layer.
#[must_use]
pub fn waits_reported() -> bool {
    if Level::INFO > STATIC_MAX_LEVEL || Level::INFO > LevelFilter::current() {
        return false;
    }
    let interest = WAITING.interest();
    !interest.is_never()
        && (interest.is_always()
            || tracing::dispatcher::get_default(|dispatch| dispatch.enabled(&WAITING_METADATA)))
}

/// Names an existing synchronous wait while its closure runs.
///
/// Like [`waiting`], this only observes the original producer's wait. It adds
/// no readiness check, thread or completion signal. The report retires on
/// return or unwind, even when a descendant retains the tracing span.
/// Keys must identify operations, never contain prompts or credentials.
pub fn waiting_sync<T>(primitive: &'static str, key: impl Display, work: impl FnOnce() -> T) -> T {
    Retire(span(primitive, &key)).0.in_scope(work)
}

/// Creates a wait and polls it once, requiring its producer to be pending, so
/// it is reported until dropped.
#[cfg(test)]
pub(crate) fn blocked<F: Future, K: Display>(
    primitive: &'static str,
    key: K,
    future: F,
) -> Pin<Box<Waiting<F, K>>> {
    let mut wait = Box::pin(waiting(primitive, key, future));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(wait.as_mut().poll(&mut cx).is_pending());
    wait
}

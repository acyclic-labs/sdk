//! Tracing glue for `docs/observability.md`. Each operation owns its completion
//! span and dispatcher; filtered children cannot overwrite caller fields.
//! These helpers erase tracing on wasm32, which never links `tracing`.
//!
//! Only [`Observe::observe_on`] emits the `acyclic.work` event, and only facade
//! operations returning an [`OperationReceipt`] call it, while no observed
//! facade operation calls another. Summing those events therefore counts each
//! unit of work once; nested spans record just the `work.*` summary fields.

use crate::performance::{MeasuredResult, OperationReceipt, WorkCounters};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use tracing::field::Empty;

/// One operation owns its span and originating dispatcher, even if filtered.
#[derive(Clone)]
pub(crate) struct OperationSpan {
    #[cfg(not(target_arch = "wasm32"))]
    span: tracing::Span,
    #[cfg(not(target_arch = "wasm32"))]
    context: tracing::Span,
    #[cfg(not(target_arch = "wasm32"))]
    dispatch: tracing::Dispatch,
}

#[cfg(not(target_arch = "wasm32"))]
impl OperationSpan {
    pub(crate) fn new(span: tracing::Span) -> Self {
        let dispatch = dispatch_for(&span);
        let context = if span.is_disabled() {
            tracing::Span::current()
        } else {
            span.clone()
        };
        Self {
            span,
            context,
            dispatch,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for OperationSpan {
    fn drop(&mut self) {
        // Subscriber registries can close a retained parent via the current
        // dispatcher when their last child is destroyed.
        tracing::dispatcher::with_default(&self.dispatch, || {
            drop(std::mem::replace(&mut self.span, tracing::Span::none()));
            drop(std::mem::replace(&mut self.context, tracing::Span::none()));
        });
    }
}

/// Creates an explicitly owned operation span without evaluating fields on WASM.
macro_rules! span {
    ($level:ident, $name:literal $(, $($fields:tt)*)?) => {{
        #[cfg(not(target_arch = "wasm32"))]
        let span = $crate::obs::OperationSpan::new(tracing::span!(tracing::Level::$level, $name $(, $($fields)*)?));
        #[cfg(target_arch = "wasm32")]
        let span = $crate::obs::OperationSpan {};
        span
    }};
}
pub(crate) use span;

/// Enters an owned operation span only while its future is being polled.
#[cfg_attr(
    target_arch = "wasm32",
    allow(unused_variables, reason = "WASM records no spans")
)]
pub(crate) async fn in_span<F: std::future::Future>(span: &OperationSpan, future: F) -> F::Output {
    #[cfg(not(target_arch = "wasm32"))]
    {
        use tracing::{Instrument as _, instrument::WithSubscriber as _};
        let dispatch = span.dispatch.clone();
        future
            .instrument(span.context.clone())
            .with_subscriber(dispatch)
            .await
    }
    #[cfg(target_arch = "wasm32")]
    future.await
}

/// Runs synchronous work under the exact span's subscriber, including events.
#[cfg_attr(
    target_arch = "wasm32",
    allow(
        unused_variables,
        dead_code,
        reason = "WASM records no spans or synchronous native work"
    )
)]
pub(crate) fn scope<T>(span: &OperationSpan, job: impl FnOnce() -> T) -> T {
    #[cfg(not(target_arch = "wasm32"))]
    return tracing::dispatcher::with_default(&span.dispatch, || span.context.in_scope(job));
    #[cfg(target_arch = "wasm32")]
    job()
}

#[cfg(not(target_arch = "wasm32"))]
fn dispatch_for(span: &tracing::Span) -> tracing::Dispatch {
    span.with_subscriber(|(_, dispatch)| dispatch.clone())
        .unwrap_or_else(|| tracing::dispatcher::get_default(Clone::clone))
}

/// Captures ancestry at a boundary with no operation span of its own.
/// This context is for worker attribution, never for recording completion.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn caller_context() -> OperationSpan {
    OperationSpan::new(tracing::Span::current())
}

/// Carries an explicitly selected operation/ancestry context through actual work.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn in_context<T>(
    span: OperationSpan,
    job: impl FnOnce() -> T + Send + 'static,
) -> impl FnOnce() -> T + Send + 'static {
    move || scope(&span, job)
}

/// A failure with a stable, path-free name to record as `error.kind`: its
/// variant name, from `strum::IntoStaticStr`.
pub(crate) trait ErrorKind {
    #[cfg_attr(
        target_arch = "wasm32",
        allow(dead_code, reason = "wasm32 records no spans")
    )]
    fn kind(&self) -> &'static str;
}

impl<E> ErrorKind for E
where
    for<'a> &'a E: Into<&'static str>,
{
    fn kind(&self) -> &'static str {
        self.into()
    }
}

/// Records on an operation's owned span, even when a filter disabled it.
/// Falling back to `Span::current()` would overwrite the caller's outcome.
#[cfg_attr(
    target_arch = "wasm32",
    allow(unused_variables, reason = "WASM records no spans")
)]
pub(crate) fn outcome_on<T, E: ErrorKind>(
    span: &OperationSpan,
    result: Result<T, E>,
) -> Result<T, E> {
    #[cfg(not(target_arch = "wasm32"))]
    record_outcome(&span.span, result.as_ref().err().map(E::kind));
    result
}

/// Records a plain receipt result; failed APIs without receipts have no work totals.
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
pub(crate) fn receipt_on<T, E: ErrorKind>(
    span: &OperationSpan,
    result: Result<OperationReceipt<T>, E>,
) -> Result<OperationReceipt<T>, E> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Ok(receipt) = &result {
        record_work(&span.span, &receipt.work);
    }
    outcome_on(span, result)
}

/// Summary on an explicitly owned span: a filtered child cannot mutate its parent.
#[cfg_attr(
    target_arch = "wasm32",
    allow(unused_variables, reason = "WASM records no spans")
)]
pub(crate) fn measured_on<T, E: ErrorKind>(
    span: &OperationSpan,
    result: MeasuredResult<T, E>,
    work: impl FnOnce(&T) -> &WorkCounters,
) -> MeasuredResult<T, E> {
    #[cfg(not(target_arch = "wasm32"))]
    if !span.span.is_disabled() {
        let (work, kind) = match &result {
            Ok(value) => (work(value), None),
            Err(failure) => (&*failure.work, Some(failure.error.kind())),
        };
        record_outcome(&span.span, kind);
        record_work(&span.span, work);
    }
    result
}

#[cfg(not(target_arch = "wasm32"))]
fn record_work(span: &tracing::Span, work: &WorkCounters) {
    if !span.is_disabled() {
        let bytes = work
            .object_bytes_read
            .saturating_add(work.object_bytes_written)
            .saturating_add(work.authority_bytes_written);
        span.record("work.items", work.items_examined)
            .record("work.bytes", bytes)
            .record("work.durability", work.durability_operations);
    }
}

/// Closes a facade operation: records [`measured_on`] on its span and emits its
/// receipt through [`WorkCounters::emit`].
pub(crate) trait Observe {
    fn observe_on(self, span: &OperationSpan, op: &'static str) -> Self;
}

impl<T, E: ErrorKind> Observe for MeasuredResult<OperationReceipt<T>, E> {
    #[cfg_attr(
        target_arch = "wasm32",
        allow(unused_variables, reason = "WASM records no spans")
    )]
    fn observe_on(self, span: &OperationSpan, op: &'static str) -> Self {
        let result = measured_on(span, self, |receipt| &receipt.work);
        #[cfg(not(target_arch = "wasm32"))]
        scope(span, || match &result {
            Ok(receipt) => receipt.work.emit(&span.span, op, "ok"),
            Err(failure) => failure.work.emit(&span.span, op, "err"),
        });
        result
    }
}

/// An id field value: `bytes` as lowercase hex, formatted only when recorded.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn hex(bytes: &[u8]) -> tracing::field::DisplayValue<Hex<'_>> {
    tracing::field::display(Hex(bytes))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct Hex<'a>(&'a [u8]);

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Display for Hex<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0
            .iter()
            .try_for_each(|byte| write!(formatter, "{byte:02x}"))
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn record_outcome(span: &tracing::Span, kind: Option<&'static str>) {
    match kind {
        None => span.record("outcome", "ok"),
        Some(kind) => span.record("outcome", "err").record("error.kind", kind),
    };
}

#[cfg(all(
    test,
    feature = "native-mount",
    any(target_os = "linux", target_os = "macos")
))]
pub(crate) use worker_context_tests::assert_filtered_callback_isolation;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod worker_context_tests {
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::Duration;
    use tracing_subscriber::{layer::Context, prelude::*};

    #[derive(Default)]
    struct Evidence {
        records: Vec<(String, String, String)>,
        closes: Vec<String>,
        event_parent: Option<String>,
        events: Vec<(Option<String>, Vec<(String, String, String)>)>,
    }

    struct Capture {
        evidence: Arc<Mutex<Evidence>>,
        closed: mpsc::Sender<()>,
    }

    struct Fields<'a>(&'a mut Vec<(String, String, String)>, String);
    impl tracing::field::Visit for Fields<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .push((self.1.clone(), field.name().into(), format!("{value:?}")));
        }
    }

    impl<S> tracing_subscriber::Layer<S> for Capture
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        fn on_record(
            &self,
            id: &tracing::Id,
            record: &tracing::span::Record<'_>,
            ctx: Context<'_, S>,
        ) {
            let span = ctx.span(id).unwrap();
            let mut evidence = self.evidence.lock().unwrap();
            record.record(&mut Fields(&mut evidence.records, span.name().into()));
        }
        fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
            let parent = ctx.event_span(event).map(|span| span.name().into());
            let mut fields = Vec::new();
            event.record(&mut Fields(&mut fields, event.metadata().target().into()));
            let mut evidence = self.evidence.lock().unwrap();
            evidence.event_parent = parent.clone();
            evidence.events.push((parent, fields));
        }
        fn on_close(&self, id: tracing::Id, ctx: Context<'_, S>) {
            self.evidence
                .lock()
                .unwrap()
                .closes
                .push(ctx.span(&id).unwrap().name().into());
            if ctx.span(&id).unwrap().name() == "worker_operation" {
                self.closed.send(()).unwrap();
            }
        }
    }

    #[test]
    fn final_operation_and_filtered_context_drop_use_originating_dispatch() {
        for filtered in [false, true] {
            for unwind in [false, true] {
                let origin_seen = Arc::new(Mutex::new(Evidence::default()));
                let foreign_seen = Arc::new(Mutex::new(Evidence::default()));
                let (closed, _) = mpsc::channel();
                let origin = tracing::Dispatch::new(
                    tracing_subscriber::registry().with(
                        Capture {
                            evidence: Arc::clone(&origin_seen),
                            closed: closed.clone(),
                        }
                        .with_filter(
                            tracing_subscriber::filter::filter_fn(move |metadata| {
                                !filtered || metadata.name() != "drop_operation"
                            }),
                        ),
                    ),
                );
                let foreign =
                    tracing::Dispatch::new(tracing_subscriber::registry().with(Capture {
                        evidence: Arc::clone(&foreign_seen),
                        closed,
                    }));
                let operation = tracing::dispatcher::with_default(&origin, || {
                    let root = tracing::info_span!("drop_root");
                    let caller = tracing::info_span!(parent: &root, "drop_caller");
                    caller.in_scope(|| super::span!(INFO, "drop_operation"))
                    // Original root/caller handles die here. The operation's
                    // ancestry owns their last references until foreign drop.
                });
                assert!(
                    origin_seen.lock().unwrap().closes.is_empty(),
                    "operation lost retained ancestry"
                );
                let clone = operation.clone();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    tracing::dispatcher::with_default(&foreign, || {
                        drop(clone);
                        if unwind {
                            let _operation = operation;
                            panic!("adversarial unwind");
                        }
                        drop(operation);
                    });
                }));
                assert_eq!(result.is_err(), unwind);
                let seen = origin_seen.lock().unwrap();
                for name in ["drop_root", "drop_caller"] {
                    assert_eq!(
                        seen.closes.iter().filter(|closed| *closed == name).count(),
                        1
                    );
                }
                assert_eq!(
                    seen.closes
                        .iter()
                        .filter(|closed| *closed == "drop_operation")
                        .count(),
                    usize::from(!filtered)
                );
                let foreign = foreign_seen.lock().unwrap();
                assert!(
                    foreign.records.is_empty()
                        && foreign.closes.is_empty()
                        && foreign.events.is_empty()
                );
            }
        }
    }

    #[test]
    fn pending_instrumented_frame_drop_uses_originating_dispatch() {
        use std::future::Future as _;
        for filtered in [false, true] {
            let seen = Arc::new(Mutex::new(Evidence::default()));
            let (closed, _) = mpsc::channel();
            let origin = tracing::Dispatch::new(
                tracing_subscriber::registry().with(
                    Capture {
                        evidence: Arc::clone(&seen),
                        closed,
                    }
                    .with_filter(tracing_subscriber::filter::filter_fn(
                        move |metadata| !filtered || metadata.name() != "frame_operation",
                    )),
                ),
            );
            let foreign = tracing::Dispatch::new(tracing_subscriber::registry());
            let future = tracing::dispatcher::with_default(&origin, || {
                let root = tracing::info_span!("frame_root");
                let caller = tracing::info_span!(parent: &root, "frame_caller");
                let operation = caller.in_scope(|| super::span!(INFO, "frame_operation"));
                async move { super::in_span(&operation, std::future::pending::<()>()).await }
            });
            let mut future = Box::pin(future);
            tracing::dispatcher::with_default(&foreign, || {
                let mut task = std::task::Context::from_waker(std::task::Waker::noop());
                assert!(future.as_mut().poll(&mut task).is_pending());
                assert!(seen.lock().unwrap().closes.is_empty());
                drop(future);
            });
            let seen = seen.lock().unwrap();
            for name in ["frame_root", "frame_caller"] {
                assert_eq!(
                    seen.closes.iter().filter(|closed| *closed == name).count(),
                    1
                );
            }
            assert_eq!(
                seen.closes
                    .iter()
                    .filter(|closed| *closed == "frame_operation")
                    .count(),
                usize::from(!filtered)
            );
        }
    }

    #[cfg(all(
        feature = "native-mount",
        any(target_os = "linux", target_os = "macos")
    ))]
    pub(crate) fn assert_filtered_callback_isolation(
        body: impl Fn(&tracing::span::EnteredSpan) -> i32,
    ) {
        let _second = tracing::Dispatch::new(tracing_subscriber::registry());
        for visible in [false, true] {
            let evidence = Arc::new(Mutex::new(Evidence::default()));
            let (closed, _) = mpsc::channel();
            let dispatch = tracing::Dispatch::new(
                tracing_subscriber::registry()
                    .with(tracing_subscriber::filter::filter_fn(move |metadata| {
                        metadata.name() == "callback_parent"
                            || (visible && metadata.name() == "acyclic.fs.mount.filtered_callback")
                    }))
                    .with(Capture {
                        evidence: Arc::clone(&evidence),
                        closed,
                    }),
            );
            let errno = tracing::dispatcher::with_default(&dispatch, || {
                let parent = tracing::info_span!("callback_parent", errno = 77);
                assert!(!parent.is_disabled());
                let _entered = parent.enter();
                let child = tracing::trace_span!(
                    "acyclic.fs.mount.filtered_callback",
                    errno = tracing::field::Empty
                )
                .entered();
                assert_eq!(child.is_disabled(), !visible);
                if !visible {
                    assert_eq!(
                        tracing::Span::current()
                            .metadata()
                            .map(|metadata| metadata.name()),
                        Some("callback_parent")
                    );
                }
                body(&child)
            });
            let evidence = evidence.lock().unwrap();
            assert!(
                evidence
                    .records
                    .iter()
                    .all(|(name, _, _)| name != "callback_parent"),
                "callback overwrote parent"
            );
            assert_eq!(
                evidence
                    .records
                    .iter()
                    .filter(
                        |(name, field, _)| name == "acyclic.fs.mount.filtered_callback"
                            && field == "errno"
                    )
                    .map(|(_, _, value)| value.clone())
                    .collect::<Vec<_>>(),
                if visible {
                    vec![errno.to_string()]
                } else {
                    Vec::new()
                },
                "visible callback must record its own errno"
            );
        }
    }

    #[tokio::test]
    async fn filtered_facade_retains_one_parent_neutral_work_receipt()
    -> Result<(), Box<dyn std::error::Error>> {
        use tracing::{Instrument as _, instrument::WithSubscriber as _};
        let _second = tracing::Dispatch::new(tracing_subscriber::registry());
        let evidence = Arc::new(Mutex::new(Evidence::default()));
        let (closed, _) = mpsc::channel();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry().with(
                Capture {
                    evidence: Arc::clone(&evidence),
                    closed,
                }
                .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                    metadata.name() == "ledger_caller" || metadata.target() == "acyclic.work"
                })),
            ),
        );
        let receipt = async {
            let caller =
                tracing::info_span!("ledger_caller", outcome = "unchanged", work.items = 99);
            crate::Fs::memory()
                .create_volume(
                    crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral),
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )
                .instrument(caller)
                .await
        }
        .with_subscriber(dispatch)
        .await?;
        let evidence = evidence.lock().unwrap();
        assert!(
            evidence.records.is_empty(),
            "filtered operations mutated caller: {:?}",
            evidence.records
        );
        assert_eq!(evidence.events.len(), 1, "{:?}", evidence.events);
        let (parent, fields) = &evidence.events[0];
        assert_eq!(
            parent, &None,
            "filtered receipt must not borrow caller's span"
        );
        assert!(
            fields
                .iter()
                .any(|(_, field, value)| field == "op" && value == "\"create_volume\"")
        );
        assert!(
            fields
                .iter()
                .any(|(_, field, value)| field == "items_examined"
                    && value == &receipt.work.items_examined.to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn detached_worker_retains_scoped_subscriber_and_operation_until_work_finishes()
    -> Result<(), Box<dyn std::error::Error>> {
        use tracing::instrument::WithSubscriber as _;
        let evidence = Arc::new(Mutex::new(Evidence::default()));
        let (closed_tx, closed_rx) = mpsc::channel();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(Capture {
            evidence: Arc::clone(&evidence),
            closed: closed_tx,
        }));
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (caller, operation) = tracing::dispatcher::with_default(&dispatch, || {
            let caller = tracing::info_span!("worker_caller", outcome = tracing::field::Empty);
            let operation = tracing::info_span!(parent: &caller, "worker_operation", outcome = tracing::field::Empty, error.kind = tracing::field::Empty);
            (caller, super::OperationSpan::new(operation))
        });
        let unrelated = tracing::Dispatch::new(tracing_subscriber::registry());
        async {
            let worker_span = operation.clone();
            let job = super::in_span(&operation, async {
                tracing::info!("future reached worker boundary");
                tokio::task::spawn_blocking(super::in_context(worker_span.clone(), move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    tracing::info!("actual work completed");
                    #[derive(strum::IntoStaticStr)]
                    enum Failure {
                        Fault,
                    }
                    let _ = super::outcome_on::<(), _>(&worker_span, Err(Failure::Fault));
                }))
            })
            .await;
            started_rx.recv_timeout(Duration::from_secs(5))?;
            drop(job); // Tokio detaches: abandoning the caller cannot end actual work.
            drop(operation);
            drop(caller);
            assert!(matches!(
                closed_rx.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            release_tx.send(())?;
            closed_rx.recv_timeout(Duration::from_secs(5))?;
            Ok::<_, Box<dyn std::error::Error>>(())
        }
        .with_subscriber(unrelated)
        .await?;
        let evidence = evidence.lock().unwrap();
        assert_eq!(evidence.event_parent.as_deref(), Some("worker_operation"));
        assert_eq!(
            evidence.events.len(),
            2,
            "future and worker must use originating subscriber"
        );
        assert!(
            evidence
                .events
                .iter()
                .all(|(parent, _)| parent.as_deref() == Some("worker_operation"))
        );
        assert!(
            evidence
                .records
                .iter()
                .any(|(name, field, value)| name == "worker_operation"
                    && field == "outcome"
                    && value == "\"err\"")
        );
        assert!(
            evidence
                .records
                .iter()
                .any(|(name, field, value)| name == "worker_operation"
                    && field == "error.kind"
                    && value == "\"Fault\"")
        );
        assert!(
            evidence
                .records
                .iter()
                .all(|(name, _, _)| name != "worker_caller")
        );
        Ok(())
    }
}

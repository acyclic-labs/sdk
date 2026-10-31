use crate::*;
use acyclic_observability::{
    Export, ExportReceiver, ExportRecord, NativeTaskTerminal, Operations, Outcome,
};
use std::sync::{PoisonError, mpsc};
use tracing::{field::Visit, span::Record};
use tracing_subscriber::{Layer, layer::Context as LayerContext, prelude::*};

fn sample(receiver: &mut ExportReceiver) -> io::Result<ExportRecord> {
    let mut receive = std::pin::pin!(receiver.recv());
    match receive
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(Some(record)) => Ok(record),
        _ => Err(io::Error::other("original terminal sample absent")),
    }
}

fn task(
    work: impl FnOnce() -> io::Result<()> + Send + 'static,
) -> io::Result<NativeCompletion<()>> {
    crate::task_completion(work, None, None, None, &crate::workers::PERSISTENCE_WORKERS)
}

fn submit(workers: &NativeWorkers, task: &mut NativeCompletion<()>) -> Poll<io::Result<()>> {
    crate::workers::poll_submission_with(
        workers,
        &mut task.pending,
        &mut task.waiter,
        &Context::from_waker(Waker::noop()),
    )
}

#[derive(Default)]
struct Fields {
    queued: bool,
    terminal: bool,
    stage: bool,
}
impl Visit for Fields {
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.queued |= field.name() == "native.queued" && value;
    }
    fn record_str(&mut self, field: &tracing::field::Field, _: &str) {
        self.terminal |= field.name() == "native.terminal";
    }
    fn record_u64(&mut self, field: &tracing::field::Field, _: u64) {
        self.stage |= field.name() == "stage";
    }
    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

#[derive(Clone)]
struct Probe {
    admission: Arc<crate::sync::Mutex<crate::workers::AdmissionState>>,
    original: Arc<Mutex<Option<Arc<CompletionMutex<CompletionState<()>>>>>>,
    consumed: Arc<Mutex<Option<io::Result<()>>>>,
    sender: std::thread::ThreadId,
    delay: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
}
impl<S: tracing::Subscriber> Layer<S> for Probe {
    fn on_record(&self, _: &tracing::Id, record: &Record<'_>, _: LayerContext<'_, S>) {
        let mut fields = Fields::default();
        record.record(&mut fields);
        if fields.queued || fields.terminal || fields.stage {
            assert!(
                self.admission.try_lock().is_ok(),
                "callback under admission"
            );
        }
        if fields.queued && std::thread::current().id() == self.sender {
            let delay = self
                .delay
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(delay) = delay {
                // Hold the sender's callback until the original worker has
                // published and exported. The worker cannot need this callback.
                assert!(delay.recv().is_ok());
            }
        }
        if fields.terminal {
            let original = self
                .original
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(original) = original {
                let mut state = original
                    .try_lock()
                    .unwrap_or_else(|_| panic!("callback under original completion lock"));
                let Poll::Ready(result) = state.poll_result(Waker::noop()) else {
                    panic!("original terminal not published")
                };
                *self.consumed.lock().unwrap_or_else(PoisonError::into_inner) = Some(result);
            }
        }
    }
}

#[test]
fn original_queue_and_terminal_survive_detachment_racing_consumers_and_blocked_export()
-> io::Result<()> {
    let (sender, receive) = mpsc::sync_channel(1);
    let workers = NativeWorkers::single(sender, crate::workers::AdmissionState::new());
    let probe = Probe {
        admission: Arc::clone(&workers.admission),
        original: Arc::default(),
        consumed: Arc::default(),
        sender: std::thread::current().id(),
        delay: Arc::default(),
    };
    let credit = Arc::new(tokio::sync::Semaphore::new(1));
    let (export, mut output) = Export::channel(Arc::clone(&credit), true);
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(Operations(export.clone()))
            .with(probe.clone()),
    );
    let receive = crate::sync::Mutex::new(receive);
    tracing::dispatcher::with_default(&dispatch, || -> io::Result<()> {
        let waiting = obs::waiting("persistence-completion", 7, std::future::pending::<()>());
        let _entered = waiting.context().enter();
        let actual = std::fs::File::open(tempfile::tempdir()?.path().join("absent"))
            .err()
            .ok_or_else(|| io::Error::other("missing-file control succeeded"))?;
        let original_code = actual.raw_os_error();
        let mut first = task(move || Err(actual))?;
        *probe
            .original
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = first.state.clone();
        assert!(matches!(submit(&workers, &mut first), Poll::Ready(Ok(()))));
        // Full is the original primitive's rejection of this candidate, even
        // though the diagnostic stage-one marker has already been observed.
        let mut full = task(|| Ok(()))?;
        assert!(submit(&workers, &mut full).is_pending());
        assert!(matches!(
            full.pending.as_ref(),
            Some(NativeJob::Task { queued: false, .. })
        ));
        crate::workers::remove_capacity_waiter(&workers, full.waiter.take());
        drop(full);
        let rejected = sample(&mut output)?;
        assert_eq!(rejected.sample().outcome, Outcome::Incomplete);
        assert_eq!(
            rejected.sample().native_task.and_then(|task| task.queued),
            None
        );
        drop(rejected);
        drop(first); // Detach the observer before the accepted job runs.
        let job = crate::workers::receive_job(&receive, &workers.admission)
            .ok_or(io::ErrorKind::BrokenPipe)?;
        std::thread::spawn(move || {
            tracing::dispatcher::with_default(&tracing::Dispatch::none(), || job.run())
        })
        .join()
        .map_err(|_| io::Error::other("original worker panicked"))?;
        let consumed = probe
            .consumed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .ok_or_else(|| io::Error::other("original result not consumed"))?;
        assert_eq!(
            consumed.err().and_then(|error| error.raw_os_error()),
            original_code
        );
        let retained = sample(&mut output)?;
        let evidence = retained.sample();
        assert_eq!(evidence.outcome, Outcome::Err);
        assert_eq!(evidence.error_kind, Some("not_found"));
        assert_eq!(evidence.parent_id, waiting.context().id().as_ref());
        assert!(
            evidence.binding.is_none(),
            "native IDs supplied no authenticated admission"
        );
        let native = evidence.native_task.ok_or(io::ErrorKind::InvalidData)?;
        assert_eq!((native.queued, native.started), (Some(true), Some(true)));
        assert_eq!(native.terminal, Some(NativeTaskTerminal::IoError));
        assert_eq!(native.os_error, original_code);
        assert_eq!(credit.available_permits(), 0);

        // Original completion progresses while the one output credit is held.
        let mut blocked = task(|| Ok(()))?;
        assert!(matches!(
            submit(&workers, &mut blocked),
            Poll::Ready(Ok(()))
        ));
        let job = crate::workers::receive_job(&receive, &workers.admission)
            .ok_or(io::ErrorKind::BrokenPipe)?;
        let _ = job.run();
        let state = blocked.state.as_ref().ok_or(io::ErrorKind::InvalidData)?;
        let mut state = state.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(!state.task_pending);
        assert!(matches!(state.result.take(), Some(Ok(()))));
        drop(state);
        drop(blocked);
        assert_eq!(export.stats().full, Some(1));
        assert_eq!(export.stats().high_water, 1);
        drop(retained);
        assert_eq!(credit.available_permits(), 1);

        // Terminal may precede the sender's acceptance callback. Both edges
        // still name the very same task, with no callback under admission.
        let (done, finished) = mpsc::channel();
        *probe.delay.lock().unwrap_or_else(PoisonError::into_inner) = Some(finished);
        let mut racing = task(|| Ok(()))?;
        std::thread::scope(|scope| -> io::Result<()> {
            let receive = &receive;
            let admission = &workers.admission;
            let worker = scope.spawn(move || {
                if let Some(job) = crate::workers::receive_job(receive, admission) {
                    let _ = job.run();
                }
                let _ = done.send(());
            });
            assert!(matches!(submit(&workers, &mut racing), Poll::Ready(Ok(()))));
            worker
                .join()
                .map_err(|_| io::Error::other("racing worker panicked"))?;
            Ok(())
        })?;
        drop(racing);
        let terminal = sample(&mut output)?;
        assert_eq!(terminal.sample().outcome, Outcome::Ok);
        let native = terminal
            .sample()
            .native_task
            .ok_or(io::ErrorKind::InvalidData)?;
        assert_eq!((native.queued, native.started), (Some(true), Some(true)));
        assert_eq!(native.terminal, Some(NativeTaskTerminal::Ok));
        drop(terminal);

        // Accepted work without its producer is incomplete, not successful.
        let mut lost = task(|| Ok(()))?;
        assert!(matches!(submit(&workers, &mut lost), Poll::Ready(Ok(()))));
        drop(lost);
        drop(crate::workers::receive_job(&receive, &workers.admission));
        let lost = sample(&mut output)?;
        assert_eq!(lost.sample().outcome, Outcome::Incomplete);
        let native = lost
            .sample()
            .native_task
            .ok_or(io::ErrorKind::InvalidData)?;
        assert_eq!(native.queued, Some(true));
        assert_eq!((native.started, native.terminal), (None, None));
        drop(lost);

        // The same catch/publish owner retains panic independently of I/O.
        let mut panicked = task(|| panic!("original producer control"))?;
        assert!(matches!(
            submit(&workers, &mut panicked),
            Poll::Ready(Ok(()))
        ));
        let _ = crate::workers::receive_job(&receive, &workers.admission)
            .ok_or(io::ErrorKind::BrokenPipe)?
            .run();
        let panic = sample(&mut output)?;
        assert_eq!(panic.sample().outcome, Outcome::Err);
        assert_eq!(panic.sample().error_kind, Some("panic"));
        assert_eq!(
            panic.sample().native_task.and_then(|task| task.panicked),
            Some(true)
        );
        assert_eq!(
            panic.sample().native_task.and_then(|task| task.terminal),
            Some(NativeTaskTerminal::Panic)
        );
        assert!(
            panicked
                .state
                .as_ref()
                .ok_or(io::ErrorKind::InvalidData)?
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .panic
                .is_some()
        );
        drop(panic);
        drop(panicked);

        // Result and panic are independent original carriers. A panic after
        // staging an I/O result cannot erase its native cause or result.
        let actual = std::fs::File::open(tempfile::tempdir()?.path().join("absent"))
            .err()
            .ok_or_else(|| io::Error::other("missing-file control succeeded"))?;
        let original_code = actual.raw_os_error();
        let mut both = task(|| Ok(()))?;
        let original = both
            .state
            .as_ref()
            .ok_or(io::ErrorKind::InvalidData)?
            .clone();
        if let Some(NativeJob::Task { operation, .. }) = both.pending.as_mut() {
            *operation = Box::new(move || {
                original
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .result = Some(Err(actual));
                panic!("panic after original result publication control");
            });
        }
        assert!(matches!(submit(&workers, &mut both), Poll::Ready(Ok(()))));
        let _ = crate::workers::receive_job(&receive, &workers.admission)
            .ok_or(io::ErrorKind::BrokenPipe)?
            .run();
        let joined = sample(&mut output)?;
        assert_eq!(joined.sample().outcome, Outcome::Err);
        assert_eq!(joined.sample().error_kind, Some("not_found"));
        let evidence = joined
            .sample()
            .native_task
            .ok_or(io::ErrorKind::InvalidData)?;
        assert_eq!(evidence.terminal, Some(NativeTaskTerminal::IoError));
        assert_eq!(evidence.panicked, Some(true));
        assert_eq!(evidence.os_error, original_code);
        let original = both
            .state
            .as_ref()
            .ok_or(io::ErrorKind::InvalidData)?
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert_eq!(
            original
                .result
                .as_ref()
                .and_then(|result| result.as_ref().err())
                .and_then(io::Error::raw_os_error),
            original_code
        );
        assert!(original.panic.is_some());
        drop(original);
        drop(joined);
        drop(both);

        // Suppress the original result producer: the receipt must fail its
        // terminal obligation rather than certify accepted work as successful.
        let mut missing = task(|| Ok(()))?;
        if let Some(NativeJob::Task { operation, .. }) = missing.pending.as_mut() {
            *operation = Box::new(|| {});
        }
        assert!(matches!(
            submit(&workers, &mut missing),
            Poll::Ready(Ok(()))
        ));
        let _ = crate::workers::receive_job(&receive, &workers.admission)
            .ok_or(io::ErrorKind::BrokenPipe)?
            .run();
        let absent = sample(&mut output)?;
        assert_eq!(absent.sample().outcome, Outcome::Incomplete);
        assert_eq!(
            absent.sample().native_task.and_then(|task| task.terminal),
            Some(NativeTaskTerminal::Missing)
        );
        drop(absent);
        drop(missing);

        // Original channel closure is a typed refusal, not queue acceptance.
        drop(receive);
        let mut disconnected = task(|| Ok(()))?;
        assert!(
            matches!(submit(&workers, &mut disconnected), Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::BrokenPipe)
        );
        drop(disconnected);
        let refused = sample(&mut output)?;
        assert_eq!(refused.sample().outcome, Outcome::Incomplete);
        assert_eq!(
            refused.sample().native_task.and_then(|task| task.queued),
            None
        );
        drop(refused);
        assert_eq!(export.stats().retained, 0);
        Ok(())
    })?;
    // The same producer works inline with detail disabled. Its ordinary metric
    // outcome remains, without retaining a native field projection.
    let (minimal, mut minimal_output) = Export::channel(Arc::clone(&credit), false);
    let minimal_dispatch =
        tracing::Dispatch::new(tracing_subscriber::registry().with(Operations(minimal)));
    tracing::dispatcher::with_default(&minimal_dispatch, || -> io::Result<()> {
        let mut inline = task(|| Ok(()))?;
        let _ = inline
            .pending
            .take()
            .ok_or(io::ErrorKind::InvalidData)?
            .run();
        let record = sample(&mut minimal_output)?;
        assert_eq!(record.sample().outcome, Outcome::Ok);
        assert!(record.sample().native_task.is_none());
        Ok(())
    })?;
    // Disabled instrumentation adds no allocation to the original two-owner
    // construction (completion Arc and funded closure Box). The negative
    // control performs a real extra allocation in that same measurement.
    tracing::dispatcher::with_default(&tracing::Dispatch::none(), || {
        drop(task(|| Ok(()))); // Warm the same tracing callsite outside counting.
        let normal = acyclic_test_support::measure(|| {
            drop(task(|| Ok(())));
        });
        let extra = acyclic_test_support::measure(|| {
            drop(task(|| Ok(())));
            drop(std::hint::black_box(Box::new(1_u64)));
        });
        assert_eq!(normal.count_total, 2);
        assert_eq!(normal.count_current, 0);
        assert_eq!(extra.count_total, normal.count_total + 1);
        println!(
            "disabled task allocations={} bytes={} live={}; NativeJob bytes={}",
            normal.count_total,
            normal.bytes_total,
            normal.bytes_current,
            std::mem::size_of::<NativeJob>()
        );
    });
    Ok(())
}

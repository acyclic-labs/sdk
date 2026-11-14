//! An undecided handler failure among busy actors (GC-263, GC-264).
//!
//! Seeded busy actors hold every other delivery in flight while one actor's
//! handler fails undecided once, or persistently. Its input never idles: the
//! failure itself redelivers it at once, and an unchanged second failure
//! settles through the handler's failure transition. Neither path waits for
//! the busy actors, so its latency is independent of how busy the rest are.
use super::*;
use crate::guest::{CancellationToken, Delivery, GuestError};
use acyclic_native_runtime::testing::{
    Schedule, fill_seeded, seeded_runtime,
    sweep::{seeds, sweep},
};
use std::sync::atomic::AtomicUsize;

const FAILING: &str = "undecided-failing";

struct Busy {
    package: Package,
    failing: ActorReference,
    refusals: usize,
    attempts: AtomicUsize,
    failures: std::sync::Mutex<Vec<String>>,
    held: AtomicUsize,
    entered: tokio::sync::watch::Sender<usize>,
    gate: tokio::sync::Semaphore,
}

impl Handler for Busy {
    fn manifest(&self) -> &Manifest {
        self.package.manifest()
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            if delivery.actor == self.failing {
                if self.attempts.fetch_add(1, Ordering::SeqCst) < self.refusals {
                    return Err(GuestError::Pending("undecided storage outcome".into())
                        .with_context("context prepared, model not started"));
                }
            } else {
                let held = self.held.fetch_add(1, Ordering::SeqCst) + 1;
                let _attempt = ActiveAttempt(&self.held);
                self.entered.send_replace(held);
                self.gate
                    .acquire()
                    .await
                    .map_err(|_| GuestError::Handler("test gate closed".into()))?
                    .forget();
                return self
                    .package
                    .on_record(delivery, checkpoint, cancellation)
                    .await;
            }
            self.package
                .on_record(delivery, checkpoint, cancellation)
                .await
        })
    }

    fn on_failure<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        failure: String,
    ) -> GuestFuture<'a, Option<Transition>> {
        if let Ok(mut failures) = self.failures.lock() {
            failures.push(failure);
        }
        Box::pin(async move {
            self.package
                .on_record(delivery, checkpoint, CancellationToken::new())
                .await
                .map(Some)
        })
    }
}

fn failures(handler: &Busy) -> Vec<String> {
    handler
        .failures
        .lock()
        .map(|failures| failures.clone())
        .unwrap_or_default()
}

async fn history() -> TestResult {
    let mut choices = [0; 2];
    assert!(fill_seeded(&mut choices));
    let busy = usize::from(choices[0] % 4) + 1;
    let refusals = if choices[1] % 2 == 0 { 1 } else { usize::MAX };
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let (entered, mut observed) = tokio::sync::watch::channel(0);
    let handler = Arc::new(Busy {
        package: Package::new()?,
        failing: actor(FAILING)?,
        refusals,
        attempts: AtomicUsize::new(0),
        failures: std::sync::Mutex::default(),
        held: AtomicUsize::new(0),
        entered,
        gate: tokio::sync::Semaphore::new(0),
    });
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        store.clone(),
        handler.clone(),
        Arc::new(SystemClock),
    )
    .with_limits(RuntimeLimits::new(busy + 1, 64)?);
    let mut busy_actors = Vec::new();
    for index in 0..busy {
        busy_actors.push(spawn_with(&store, &format!("undecided-busy-{index}"), &[b"emit"]).await?);
    }
    let mut serving = Box::pin(runtime.run(|_, _| {}));
    observe_while_running(serving.as_mut(), async {
        observed.wait_for(|held| *held == busy).await?;
        Ok(())
    })
    .await?;
    // Every other delivery is in flight before the failing input exists.
    let failing = spawn_with(&store, FAILING, &[b"emit"]).await?;
    let settled = tokio::time::timeout(
        std::time::Duration::from_secs(3600),
        observe_while_running(
            serving.as_mut(),
            state_after(&store, &failing, |state| state.cursor("inbox") == Some(1)),
        ),
    )
    .await;
    if settled.is_err() {
        return Err(format!(
            "actor idled with its input while {busy} actors stayed busy \
             (undecided failures: {}, failure transitions: {})",
            handler.attempts.load(Ordering::SeqCst),
            failures(&handler).len()
        )
        .into());
    }
    settled??;
    assert_eq!(
        handler.held.load(Ordering::SeqCst),
        busy,
        "the failing actor's settlement waited for busy actors"
    );
    assert_eq!(handler.attempts.load(Ordering::SeqCst), 2);
    // A transient undecided failure settles by redelivery; a persistent one
    // fails visibly, as exactly the unchanged undecided error.
    let expected: Vec<String> = if refusals == 1 {
        Vec::new()
    } else {
        vec![
            crate::local::Error::UndecidedUnchanged(
                GuestError::Pending("undecided storage outcome".into())
                    .with_context("context prepared, model not started"),
            )
            .to_string(),
        ]
    };
    assert_eq!(failures(&handler), expected);
    assert_eq!(
        records(provider.as_ref(), &format!("out/{FAILING}")).await?,
        vec![Bytes::from_static(b"r0")]
    );
    handler.gate.add_permits(busy);
    for address in &busy_actors {
        observe_while_running(
            serving.as_mut(),
            state_after(&store, address, |state| state.cursor("inbox") == Some(1)),
        )
        .await?;
        assert_eq!(
            records(provider.as_ref(), &format!("out/{}", address.id())).await?,
            vec![Bytes::from_static(b"r0")]
        );
    }
    Ok(())
}

/// `DST_SEEDS=<a>..<b>` runs a deep sweep; `DST_SEED=<n>` replays one seed.
#[test]
fn seeded_undecided_failure_never_idles_behind_busy_actors() -> TestResult {
    sweep("actors-undecided", seeds(0..8), |seed| {
        seeded_runtime(seed)
            .map_err(|error| error.to_string())?
            .block_on(async {
                tokio::time::pause();
                let (schedule, mut boundaries) = Schedule::seeded(seed);
                drive(&schedule, &mut boundaries, history()).await
            })
            .map_err(|error| error.to_string())
    })?;
    Ok(())
}

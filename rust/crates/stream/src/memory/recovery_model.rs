use super::*;
use futures::StreamExt as _;
use loom::thread;
use std::cell::Cell;
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Fault {
    None,
    OmitAppendWake,
    RetainReplacedState,
    IgnoreHead,
    OmitPoisonPublication,
    HoldPublicationDuringMaintenance,
}

loom::thread_local! {
    static FAULT: Cell<Fault> = Cell::new(Fault::None);
}

pub(crate) fn set_fault(fault: Fault) {
    FAULT.with(|current| current.set(fault));
}

/// Omits this actor's append wake for adapter model negative controls.
pub fn omit_append_wake() {
    set_fault(Fault::OmitAppendWake);
}

pub(crate) fn fault_is(fault: Fault) -> bool {
    FAULT.with(|current| current.get() == fault)
}

fn expect_model_failure(run: impl FnOnce(), expected: &str) {
    let error = catch_unwind(AssertUnwindSafe(run)).expect_err("broken model must fail");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .expect("model failure message");
    assert!(message.contains(expected), "{message}");
}

impl UnixMillisClock for loom::sync::atomic::AtomicU64 {
    fn now_unix_millis(&self) -> u64 {
        self.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Owns the real async state machine on the heap, outside Loom's coroutine stack.
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

/// Applies the production recovery path to the existing provider object.
pub async fn recover(
    provider: &MemoryStream,
    replacement: &MemoryStream,
) -> std::result::Result<(), StreamError> {
    provider
        .install_state(&replacement.encode_state().await)
        .await
}

fn seed(provider: &MemoryStream) -> Result<(StreamPath, CommitId)> {
    let path = StreamPath::new("model")?;
    let outcome = block_on(provider.append(AppendRequest {
        path: path.clone(),
        records: vec![Bytes::from_static(b"anchor")],
        if_tail: Some(0),
        idempotency_key: None,
    }))?;
    let AppendOutcome::Committed(receipt) = outcome else {
        return Err("seed conflicted".into());
    };
    Ok((path, receipt.commit_id))
}

/// Checks schedules of bounded actor operations without a default preemption cap.
///
/// # Panics
/// Panics on a violated model invariant.
pub fn check(run: impl Fn() -> Result + Send + Sync + 'static) {
    check_with_bound(None, run);
}

/// Checks a model at exactly its explicit preemption bound. Environment settings
/// cannot change the search being certified; no permutation/time cap applies.
///
/// # Panics
/// Panics on a violated model invariant.
pub fn check_with_bound(
    preemptions: Option<usize>,
    run: impl Fn() -> Result + Send + Sync + 'static,
) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = preemptions;
    builder.max_permutations = None;
    builder.max_duration = None;
    builder.check(move || {
        let result = run();
        assert!(result.is_ok(), "{result:?}");
    });
}

/// Certifies the first failing preemption bound and rejects the fault at the suite bound.
///
/// # Panics
/// Panics if a smaller bound finds the fault, or either requested witness is absent.
pub fn assert_negative_control(
    minimum: usize,
    suite_bound: usize,
    expected: &str,
    run: impl Fn(Option<usize>),
) {
    assert!(
        minimum < suite_bound,
        "negative control requires search margin"
    );
    for bound in 0..minimum {
        run(Some(bound));
    }
    expect_model_failure(|| run(Some(minimum)), expected);
    expect_model_failure(|| run(Some(suite_bound)), expected);
}

/// Exhausts one producer and checked subscriber through the real provider.
///
/// # Panics
/// Panics on a violated model invariant.
fn subscription_and_append(omit_wake: bool) {
    check(move || {
        let provider = MemoryStream::default();
        let (path, anchor) = seed(&provider)?;
        let keeper = ManuallyDrop::new(provider.clone());
        let writer = thread::spawn({
            let provider = provider.clone();
            let path = path.clone();
            move || {
                if omit_wake {
                    set_fault(Fault::OmitAppendWake);
                }
                block_on(provider.append(AppendRequest {
                    path,
                    records: vec![Bytes::from_static(b"next")],
                    if_tail: Some(1),
                    idempotency_key: None,
                }))
            }
        });
        block_on(async {
            let mut stream = provider.follow_checked(path, 1, anchor).await?;
            let record = stream.next().await.ok_or("follow ended")??;
            assert_eq!(record.sequence, 1);
            assert_eq!(record.value.as_slice(), b"next");
            Ok(()) as Result
        })?;
        writer.join().map_err(|_| "writer panicked")??;
        drop(ManuallyDrop::into_inner(keeper));
        Ok(())
    });
}

/// Exhausts checked subscription against real state replacement.
///
/// # Panics
/// Panics on a violated model invariant.
fn subscription_and_recovery(retain_old: bool) {
    check(move || {
        let provider = MemoryStream::default();
        let (path, anchor) = seed(&provider)?;
        let keeper = ManuallyDrop::new(provider.clone());
        let empty = block_on(MemoryStream::default().encode_state());
        let recovery = thread::spawn({
            let provider = provider.clone();
            move || {
                if retain_old {
                    set_fault(Fault::RetainReplacedState);
                }
                block_on(provider.install_state(&empty))
            }
        });
        block_on(async {
            match provider.follow_checked(path, 1, anchor).await {
                Ok(mut stream) => assert!(matches!(
                    stream.next().await,
                    Some(Err(StreamError::NotFound
                        | StreamError::OutOfRange
                        | StreamError::PrefixNotRetained))
                )),
                Err(error) => assert!(matches!(
                    error,
                    StreamError::NotFound
                        | StreamError::OutOfRange
                        | StreamError::PrefixNotRetained
                )),
            }
        });
        recovery.join().map_err(|_| "recovery panicked")??;
        drop(ManuallyDrop::into_inner(keeper));
        Ok(())
    });
}

/// Exhausts Head publication against real state replacement.
///
/// # Panics
/// Panics on a violated model invariant.
fn head_fence_and_recovery(ignore_head: bool) {
    check(move || {
        let provider = MemoryStream::default();
        let (path, anchor) = seed(&provider)?;
        let replacement = MemoryStream::default();
        block_on(replacement.append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"replaced")],
            if_tail: Some(0),
            idempotency_key: None,
        }))?;
        let bytes = block_on(replacement.encode_state());
        let recovery = thread::spawn({
            let provider = provider.clone();
            move || block_on(provider.install_state(&bytes))
        });
        let request = CommitRequest {
            conditions: vec![CommitCondition::Head {
                path: path.clone(),
                expected: 1,
                commit_id: anchor,
            }],
            mutations: vec![CommitMutation::Append {
                path: path.clone(),
                records: vec![Bytes::from_static(b"obsolete")],
            }],
            idempotency_key: IdempotencyKey::new(Bytes::from_static(b"write"))?,
        };
        if ignore_head {
            set_fault(Fault::IgnoreHead);
        }
        block_on(provider.commit(request))?;
        recovery.join().map_err(|_| "recovery panicked")??;
        assert_eq!(
            block_on(provider.tail(path)),
            Ok(1),
            "obsolete generation published"
        );
        Ok(())
    });
}

/// Checks append delivery and detects removal of the production publication wake.
pub fn subscription_and_append_have_no_lost_wakeup() {
    subscription_and_append(false);
    expect_model_failure(|| subscription_and_append(true), "deadlock");
}

/// Checks recovery delivery and detects retaining an obsolete watch publisher.
pub fn subscription_and_recovery_never_wait_for_a_missing_anchor() {
    subscription_and_recovery(false);
    expect_model_failure(|| subscription_and_recovery(true), "deadlock");
}

/// Checks recovery fencing and detects ignoring the immutable head identity.
pub fn head_fence_and_recovery_cannot_publish_an_obsolete_generation() {
    head_fence_and_recovery(false);
    expect_model_failure(
        || head_fence_and_recovery(true),
        "obsolete generation published",
    );
}

/// Checks local visibility, journal sampling and push follow without a device acknowledgement.
pub fn local_accepted_queries_do_not_wait_for_durability() {
    use crate::local::query_model::accepted_queries_and_publication_have_no_lost_wakeup_or_durability_wait as run;
    run(false, Some(4));
    // Registration can park before the writer runs: no preemption is needed.
    assert_negative_control(0, 4, "deadlock", |bound| run(true, bound));
}

/// Checks that a terminal local failure wakes a subscriber already waiting for data.
pub fn local_poison_wakes_followers() {
    crate::local::query_model::poison_wakes_checked_followers(false);
    expect_model_failure(
        || crate::local::query_model::poison_wakes_checked_followers(true),
        "deadlock",
    );
}

/// Checks a parked accepted follow; the mutant omits its per-path wait registration.
pub fn local_persist_wakes_parked_followers(omit_wake: bool) {
    crate::local::query_model::persist_wakes_parked_followers(omit_wake);
}

/// Exhausts the real session registration adapter against accepted-history loss.
/// `omit_anchor` is the negative control: it bypasses only checked registration.
///
/// # Panics
/// Panics if a recovered history is delivered under the old snapshot's cut.
pub fn session_registration_and_recovery_never_splice(omit_anchor: bool) {
    loom::model(move || {
        let provider = MemoryStream::default();
        let (path, anchor) = seed(&provider).unwrap();
        let replacement = MemoryStream::default();
        block_on(replacement.append(AppendRequest {
            path: path.clone(),
            records: vec![
                Bytes::from_static(b"replacement"),
                Bytes::from_static(b"new tail"),
            ],
            if_tail: Some(0),
            idempotency_key: None,
        }))
        .unwrap();
        let recovery = thread::spawn({
            let provider = provider.clone();
            move || block_on(recover(&provider, &replacement))
        });
        block_on(async {
            let follow = wire::SessionFollow {
                path: path.as_str().into(),
                delivery: wire::SessionDelivery::All as i32,
                ..Default::default()
            };
            let cursor = wire_codec::session::record_cursor(1, anchor).unwrap();
            let registered = if omit_anchor {
                provider.follow(path, cursor.next).await
            } else {
                wire_codec::session::follow_records(&provider, &follow, &cursor).await
            };
            let error = match registered {
                Err(error) => error,
                Ok(mut records) => match records.next().await {
                    Some(Err(error)) => error,
                    other => panic!("spliced old snapshot into recovered history: {other:?}"),
                },
            };
            assert!(matches!(
                error,
                StreamError::NotFound | StreamError::OutOfRange | StreamError::PrefixNotRetained
            ));
        });
        recovery.join().unwrap().unwrap();
    });
}

/// Races the actual local checked-follow registration with its accepted publication.
pub fn local_path_registration(omit_wake: bool) {
    crate::local::follow_model::registration(omit_wake);
}
/// Races cancellation with publication to a surviving cursor on the same path.
pub fn local_path_cancellation(retain: bool) {
    crate::local::follow_model::cancellation(retain);
}

/// Checks that a failure-watch borrow never crosses local visibility acquisition.
pub fn local_failure_lock_order(hold_borrow: bool) {
    crate::local::follow_model::failure_lock_order(hold_borrow);
}

/// Checks unpolled record/child consumers against fair publication handoff.
pub fn local_follow_parking(retain_pending: bool, children: bool, memory: bool) {
    crate::local::parking_model::verify(retain_pending, children, memory);
}

/// Compares queued admission and cancellation with Tokio's fair lock.
pub fn lock_parity(negative: bool) {
    loom::model(move || {
        if negative {
            crate::query_sync::parity::negative();
        } else {
            crate::query_sync::parity::run();
        }
    });
}

/// Checks direct memory-provider streams against an unpolled state reservation.
pub fn memory_follow_parking(retain_pending: bool, children: bool, checked: bool) {
    crate::memory::parking_model::verify(retain_pending, children, checked);
}

/// Cancels real dispatched snapshot jobs while the writer and consumer race.
pub fn snapshot_executor_shutdown(
    discard: bool,
    children: bool,
    checked: bool,
    cancel: bool,
    polled: bool,
) {
    crate::memory::parking_model::shutdown(discard, children, checked, cancel, polled);
}

/// Every consumer departure from real dispatched snapshot jobs, on the memory
/// provider state lock and the local publication lock, under one mutant.
pub fn snapshot_consumer_departures(mutant: &str) {
    use crate::snapshot::model::{Departure, Mutant};
    let mutant = match mutant {
        "none" => Mutant::None,
        "leak-reservation" => Mutant::LeakReservation,
        "no-wake" => Mutant::NoWake,
        "armed-after-completion" => Mutant::ArmedAfterCompletion,
        other => panic!("unknown mutant {other}"),
    };
    for departure in [
        Departure::Keep,
        Departure::CancelRacesFirstPoll,
        Departure::CancelRacesRelease,
    ] {
        for children in [false, true] {
            crate::memory::parking_model::abandon(departure, children, mutant);
            crate::local::parking_model::abandon(departure, children, mutant);
        }
    }
}

/// Accepted readers and watermark queries must not wait for snapshot device I/O.
pub fn local_compaction_visibility(hold_publication: bool) {
    crate::local::compaction_model::verify(u8::from(hold_publication));
}

/// Detect releasing mutation serialization before the snapshot's device cut.
pub fn local_compaction_cut() {
    crate::local::compaction_model::verify(2);
}

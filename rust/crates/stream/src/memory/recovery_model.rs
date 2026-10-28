use super::*;
use futures::StreamExt as _;
use futures::TryFutureExt as _;
use loom::thread;
use std::cell::Cell;
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Fault {
    None,
    MixedReceiptCut,
    BarrierAsFlushed,
    OmitAppendWake,
    RetainReplacedState,
    IgnoreHead,
    IgnoreAbsent,
    RetainConflictKey,
    RootOnlyFork,
    IgnoreForkExclusions,
    IgnorePageRecordBytes,
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

/// Drives the real state machine with the same executor as snapshot workers.
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    // Waker reference counting belongs to the executor, not the provider model.
    // A single driver keeps every lock, registration and wake scheduled by Loom
    // without multiplying schedules by modeled Arc clones of its own waker.
    crate::snapshot::park(Box::pin(future))
}

/// Drives adapter models with the same parked-thread waker as snapshot workers.
pub use block_on as park;

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
        records: vec![Bytes::from_static(b"anchor").into()],
        if_tail: Some(0),
        idempotency_key: crate::IdempotencyKey::unique(),
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
                    records: vec![Bytes::from_static(b"next").into()],
                    if_tail: Some(1),
                    idempotency_key: crate::IdempotencyKey::unique(),
                }))
            }
        });
        block_on(async {
            let mut stream = provider
                .follow(crate::FollowRequest {
                    path: path,
                    from: 1,
                    preceding: Some(anchor),
                    filter: crate::FollowFilter::default(),
                    progress: false,
                })
                .map_ok(crate::records_only)
                .await?;
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
            match provider
                .follow(crate::FollowRequest {
                    path: path,
                    from: 1,
                    preceding: Some(anchor),
                    filter: crate::FollowFilter::default(),
                    progress: false,
                })
                .map_ok(crate::records_only)
                .await
            {
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
            records: vec![Bytes::from_static(b"replaced").into()],
            if_tail: Some(0),
            idempotency_key: crate::IdempotencyKey::unique(),
        }))?;
        let bytes = block_on(replacement.encode_state());
        let recovery = thread::spawn({
            let provider = provider.clone();
            move || block_on(provider.install_state(&bytes))
        });
        let request = CommitRequest {
            deadline_unix_millis: None,
            conditions: vec![CommitCondition::Head {
                path: path.clone(),
                expected: 1,
                commit_id: anchor,
            }],
            mutations: vec![CommitMutation::Append {
                path: path.clone(),
                records: vec![Bytes::from_static(b"obsolete").into()],
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
                Bytes::from_static(b"replacement").into(),
                Bytes::from_static(b"new tail").into(),
            ],
            if_tail: Some(0),
            idempotency_key: crate::IdempotencyKey::unique(),
        }))
        .unwrap();
        let recovery = thread::spawn({
            let provider = provider.clone();
            move || block_on(recover(&provider, &replacement))
        });
        block_on(async {
            let follow = wire::SessionFollow {
                resource: crate::wire_codec::session::stream_resource(path.as_str()),
                delivery: wire::SessionDelivery::All as i32,
                ..Default::default()
            };
            let cursor = wire_codec::session::record_cursor(1, anchor).unwrap();
            let registered = if omit_anchor {
                provider
                    .follow(crate::FollowRequest::new(path, cursor.next))
                    .map_ok(crate::records_only)
                    .await
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

/// Checks unpolled record consumers against fair publication handoff.
pub fn local_follow_parking(retain_pending: bool, memory: bool) {
    crate::local::parking_model::verify(retain_pending, memory);
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
pub fn memory_follow_parking(retain_pending: bool, checked: bool) {
    crate::memory::parking_model::verify(retain_pending, checked);
}

/// Cancels real dispatched snapshot jobs while the writer and consumer race.
pub fn snapshot_executor_shutdown(discard: bool, checked: bool, cancel: bool, polled: bool) {
    crate::memory::parking_model::shutdown(discard, checked, cancel, polled);
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
        crate::memory::parking_model::abandon(departure, mutant);
        crate::local::parking_model::abandon(departure, mutant);
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

/// Checks receipts against their own flushed cut while other calls advance it.
pub fn local_receipt_cuts(broken: bool) {
    crate::local::receipt_model::verify(broken);
}

/// Checks ordered and device-flushed progress on the production journal locks.
pub fn local_barrier_receipts(broken: bool) {
    crate::local::receipt_model::verify_barrier(broken);
}

pub(crate) struct Probe(pub(crate) loom::sync::atomic::AtomicUsize);
impl std::task::Wake for Probe {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, loom::sync::atomic::Ordering::SeqCst);
    }
}
pub(crate) fn probe() -> (std::sync::Arc<Probe>, std::task::Waker) {
    let probe = std::sync::Arc::new(Probe(loom::sync::atomic::AtomicUsize::new(0)));
    let wake = std::task::Waker::from(probe.clone());
    (probe, wake)
}

/// Explores one subtree fork racing a two-stream coordinated write.
/// Both inherited heads must come from one commit cut.
pub fn subtree_cut(root_only: bool) {
    check_with_bound(Some(2), move || {
        let provider = MemoryStream::default();
        for name in ["tree/a", "tree/b"] {
            block_on(provider.append(AppendRequest {
                path: StreamPath::new(name)?,
                records: vec![Bytes::from_static(b"seed").into()],
                if_tail: Some(0),
                idempotency_key: crate::IdempotencyKey::unique(),
            }))?;
        }
        let writing = thread::spawn({
            let provider = provider.clone();
            move || {
                block_on(
                    provider.commit(CommitRequest {
                        deadline_unix_millis: None,
                        conditions: ["tree/a", "tree/b"]
                            .map(|name| CommitCondition::Tail {
                                path: StreamPath::new(name).unwrap(),
                                expected: 1,
                            })
                            .into(),
                        mutations: ["tree/a", "tree/b"]
                            .map(|name| CommitMutation::Append {
                                path: StreamPath::new(name).unwrap(),
                                records: vec![Bytes::from_static(b"next").into()],
                            })
                            .into(),
                        idempotency_key: IdempotencyKey::new(b"writer".to_vec()).unwrap(),
                    }),
                )
                .unwrap();
            }
        });
        let forking = thread::spawn({
            let provider = provider.clone();
            move || {
                if root_only {
                    set_fault(Fault::RootOnlyFork);
                }
                block_on(provider.fork(ForkRequest {
                    source: StreamPath::new("tree").unwrap(),
                    destination: StreamPath::new("copy").unwrap(),
                    exclude: Vec::new(),
                    at_tail: Some(0),
                    idempotency_key: crate::IdempotencyKey::unique(),
                }))
                .unwrap();
            }
        });
        writing.join().unwrap();
        forking.join().unwrap();
        let a = block_on(provider.tail(StreamPath::new("copy/a")?)).ok();
        let b = block_on(provider.tail(StreamPath::new("copy/b")?)).ok();
        assert!(
            matches!((a, b), (Some(1), Some(1)) | (Some(2), Some(2))),
            "fork must inherit both descendants at one commit cut"
        );
        block_on(provider.append(AppendRequest {
            path: StreamPath::new("copy/a")?,
            records: vec![Bytes::from_static(b"private").into()],
            if_tail: a,
            idempotency_key: crate::IdempotencyKey::unique(),
        }))?;
        assert_eq!(block_on(provider.tail(StreamPath::new("tree/a")?))?, 2);
        assert_eq!(
            block_on(provider.tail(StreamPath::new("copy/b")?))?,
            b.unwrap()
        );
        Ok(())
    });
}

/// The old fork API has no exclusions: an existing sibling leaks into the child.
pub fn subtree_exclusions(ignore: bool) {
    check_with_bound(Some(1), move || {
        let provider = MemoryStream::default();
        block_on(provider.append(AppendRequest {
            path: StreamPath::new("tree/children/old/events")?,
            records: vec![Bytes::from_static(b"sibling").into()],
            if_tail: Some(0),
            idempotency_key: crate::IdempotencyKey::unique(),
        }))?;
        if ignore {
            set_fault(Fault::IgnoreForkExclusions);
        }
        block_on(provider.fork(ForkRequest {
            source: StreamPath::new("tree")?,
            destination: StreamPath::new("tree/children/new")?,
            exclude: vec![StreamPath::new("children")?],
            at_tail: Some(0),
            idempotency_key: crate::IdempotencyKey::unique(),
        }))?;
        assert_eq!(
            block_on(provider.tail(StreamPath::new("tree/children/new/children/old/events")?)),
            Err(StreamError::NotFound),
            "excluded sibling must not leak into the fork"
        );
        assert_eq!(
            block_on(provider.tail(StreamPath::new("tree/children/old/events")?))?,
            1
        );
        Ok(())
    });
}

/// Runs the public byte-bound oracle through actual projection accounting.
///
/// # Panics
/// Panics when projected record bytes bypass the page bound.
pub fn page_tail_bytes(ignore: bool) {
    check_with_bound(Some(0), move || {
        let provider = MemoryStream::default();
        if ignore {
            set_fault(Fault::IgnorePageRecordBytes);
        }
        assert!(
            block_on(crate::conformance::verify_page_tail_bytes(&provider)).is_ok(),
            "projected record headers and values must fit the page byte bound"
        );
        Ok(())
    });
}

/// Projection must keep both statuses from the same coordinated publication.
pub fn page_tail_cut(split_reads: bool) {
    check_with_bound(Some(2), move || {
        let provider = MemoryStream::default();
        for name in ["agents/a/status", "agents/b/status"] {
            block_on(provider.append(AppendRequest {
                path: StreamPath::new(name)?,
                records: vec![Bytes::from_static(b"old").into()],
                if_tail: Some(0),
                idempotency_key: crate::IdempotencyKey::unique(),
            }))?;
        }
        let writer = thread::spawn({
            let provider = provider.clone();
            move || {
                let outcome = block_on(
                    provider.commit(CommitRequest {
                        deadline_unix_millis: None,
                        conditions: ["agents/a/status", "agents/b/status"]
                            .map(|name| crate::CommitCondition::Tail {
                                path: StreamPath::new(name).unwrap(),
                                expected: 1,
                            })
                            .into(),
                        mutations: ["agents/a/status", "agents/b/status"]
                            .map(|name| CommitMutation::Append {
                                path: StreamPath::new(name).unwrap(),
                                records: vec![Bytes::from_static(b"new").into()],
                            })
                            .into(),
                        idempotency_key: IdempotencyKey::new(b"page-writer".to_vec()).unwrap(),
                    }),
                )
                .unwrap();
                assert!(matches!(outcome, crate::CommitOutcome::Committed(_)));
            }
        });
        let projection = thread::spawn({
            let provider = provider.clone();
            move || {
                let request = crate::ChildrenPageRequest {
                    parent: Some(StreamPath::new("agents").unwrap()),
                    after: None,
                    hierarchy_version: None,
                    limit: 2,
                    tail_stream: Some(StreamPath::new("status").unwrap()),
                };
                let mut page = block_on(provider.children_page(request.clone())).unwrap();
                if split_reads {
                    thread::yield_now();
                    page.children[1].tail_record =
                        block_on(provider.children_page(request)).unwrap().children[1]
                            .tail_record
                            .clone();
                }
                let first = page.children[0]
                    .tail_record
                    .as_ref()
                    .unwrap()
                    .value
                    .as_ref();
                let second = page.children[1]
                    .tail_record
                    .as_ref()
                    .unwrap()
                    .value
                    .as_ref();
                assert_eq!(first, second, "page statuses must share one namespace cut");
            }
        });
        writer.join().unwrap();
        projection.join().unwrap();
        Ok(())
    });
}

/// Runs one public agent-shaped history against the real memory provider with
/// an optional old or broken behaviour, which the history must reject.
fn agent_history(case: crate::conformance::wave1::Case, fault: Fault) {
    check_with_bound(Some(0), move || {
        set_fault(fault);
        let provider = MemoryStream::default();
        let namespace = StreamPath::new("agents")?;
        if let Err(failure) = block_on(crate::conformance::wave1::verify_case(
            &provider, &namespace, case,
        )) {
            panic!("agent history rejected the provider: {failure}");
        }
        Ok(())
    });
}

/// Spawn must leave the child's excluded status and children behind.
pub fn agent_spawn(ignore_exclusions: bool) {
    let fault = if ignore_exclusions {
        Fault::IgnoreForkExclusions
    } else {
        Fault::None
    };
    agent_history(crate::conformance::wave1::Case::Spawn, fault);
}

/// A closed inbox refuses sends only because `Absent` is enforced.
pub fn agent_closed_inbox(ignore_absent: bool) {
    let fault = if ignore_absent {
        Fault::IgnoreAbsent
    } else {
        Fault::None
    };
    agent_history(crate::conformance::wave1::Case::ClosedInbox, fault);
}

/// A root-only fork publication has no inherited descendants to follow.
pub fn agent_inherited_follow(root_only: bool) {
    let fault = if root_only {
        Fault::RootOnlyFork
    } else {
        Fault::None
    };
    agent_history(crate::conformance::wave1::Case::InheritedFollow, fault);
}

/// A root-only fork loses the conversation a grandchild must inherit.
pub fn agent_fork_lineage(root_only: bool) {
    let fault = if root_only {
        Fault::RootOnlyFork
    } else {
        Fault::None
    };
    agent_history(crate::conformance::wave1::Case::ForkLineage, fault);
}

/// A conditional commit racing an unconditional writer either commits under
/// its key or conflicts without spending it; one stable key then succeeds at
/// the observed tail and replays exactly.
pub fn stable_intent_key(retain_conflicts: bool) {
    check_with_bound(Some(2), move || {
        let provider = MemoryStream::default();
        let path = StreamPath::new("intent")?;
        let key = IdempotencyKey::new(b"intent".to_vec())?;
        let request = |expected| CommitRequest {
            deadline_unix_millis: None,
            conditions: vec![CommitCondition::Tail {
                path: path.clone(),
                expected,
            }],
            mutations: vec![CommitMutation::Append {
                path: path.clone(),
                records: vec![Bytes::from_static(b"intent").into()],
            }],
            idempotency_key: key.clone(),
        };
        let writer = thread::spawn({
            let provider = provider.clone();
            let path = path.clone();
            move || {
                block_on(provider.append(AppendRequest {
                    path,
                    records: vec![Bytes::from_static(b"other").into()],
                    if_tail: None,
                    idempotency_key: IdempotencyKey::new(b"other".to_vec()).unwrap(),
                }))
                .unwrap();
            }
        });
        if retain_conflicts {
            set_fault(Fault::RetainConflictKey);
        }
        let first = block_on(provider.commit(request(0)))?;
        writer.join().unwrap();
        let observed = block_on(provider.inspect_idempotency(key.clone()))?;
        let settled = match first {
            CommitOutcome::Committed(ref receipt) => {
                assert_eq!(
                    observed.map(|observation| observation.outcome),
                    Some(IdempotencyOutcome::Commit(block_on(
                        provider.read_commit(receipt.commit_id)
                    )?)),
                    "a committed intent must be recoverable"
                );
                first
            }
            CommitOutcome::Conflict(_) => {
                assert!(
                    observed.is_none(),
                    "a conflict must not spend the intent key"
                );
                let retried = block_on(provider.commit(request(1)))?;
                assert!(
                    matches!(retried, CommitOutcome::Committed(_)),
                    "the stable key must commit at the observed tail"
                );
                retried
            }
        };
        let CommitOutcome::Committed(envelope) = &settled else {
            unreachable!("settled above");
        };
        let expected = match &envelope.mutations[..] {
            [CommittedMutation::Append(append)] => append.start,
            _ => unreachable!("one append"),
        };
        assert_eq!(
            block_on(provider.commit(request(expected)))?,
            settled,
            "exact retry replays the receipt"
        );
        assert_eq!(
            block_on(provider.tail(path.clone()))?,
            2,
            "the intent committed exactly once"
        );
        Ok(())
    });
}

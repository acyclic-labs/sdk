use super::*;
use crate::client::CommandControl;
use crate::sync::owned::{Arc as GateArc, Mutex as GateMutex};
use acyclic_stream::{MemoryStream, StreamPath, verification::block_on};
use std::{
    cell::Cell,
    task::{Context, Waker},
};

thread_local! {
    static BROKEN_PARENT: Cell<bool> = const { Cell::new(false) };
    static UNCHECKED_ADMISSION: Cell<bool> = const { Cell::new(false) };
}
pub(super) fn broken_parent() -> bool {
    BROKEN_PARENT.with(Cell::get)
}
pub(super) fn unchecked_admission() -> bool {
    UNCHECKED_ADMISSION.with(Cell::get)
}
fn values() -> Value {
    json!({"model":null,"summary":null})
}
fn parent<'a>(
    volume: &'a TaskVolume<MemoryStream>,
    task: TaskId,
) -> BoxProviderFuture<'a, Result<Option<TaskId>>> {
    Box::pin(async move {
        cached_parent(volume, task, 1024)
            .await?
            .ok_or_else(|| Error::NotFound("modeled parent".into()))
    })
}
async fn matches(volume: &TaskVolume<MemoryStream>, task: TaskId, scope: TaskId) -> Result<bool> {
    has_ancestor(volume, Some(task), scope, 1024, |task| parent(volume, task)).await
}
fn assert_paused(volume: &TaskVolume<MemoryStream>, task: TaskId) {
    let mut waiting = Box::pin(CommandControl::wait_matching(
        volume,
        |scope| Box::pin(matches(volume, task, scope)),
        1024,
    ));
    assert!(
        std::future::Future::poll(waiting.as_mut(), &mut Context::from_waker(Waker::noop()))
            .is_pending(),
        "admitted task bypassed its parent's subtree pause"
    );
}

fn race(interrupted: u8, broken: bool) {
    BROKEN_PARENT.with(|flag| flag.set(false));
    UNCHECKED_ADMISSION.with(|flag| flag.set(false));
    let provider = Arc::new(MemoryStream::default());
    let path = StreamPath::new("parent-admission-model").unwrap();
    let volume = Arc::new(block_on(TaskVolume::open(provider.clone(), path.clone())).unwrap());
    let a = TaskId::from_bytes([1; 16]);
    let b = TaskId::from_bytes([2; 16]);
    let task = TaskId::from_bytes([3; 16]);
    // The two owner-attested roots have no parent. Matching needs their parent
    // facts only; omit unrelated model/context payloads from this small scenario.
    for root in [a, b] {
        block_on(claim_parent(&volume, root, None, 1024)).unwrap();
    }
    let admitted = Arc::new(loom::sync::Mutex::new((interrupted == 3).then_some(a)));
    if interrupted == 3 {
        block_on(CommandControl::change(
            &volume,
            Some(a),
            &[],
            Some(true),
            1024,
        ))
        .unwrap();
    }
    if matches!(interrupted, 1 | 2) {
        let gate = GateArc::new(GateMutex::new(()));
        let reached = loom::sync::atomic::AtomicBool::new(false);
        let preparation = Box::pin(prepare(
            &volume,
            task,
            Some(a),
            1024,
            Box::pin(async { Ok(None) }),
            async || {
                if interrupted == 1 {
                    reached.store(true, std::sync::atomic::Ordering::SeqCst);
                    futures::future::pending::<()>().await;
                }
                Ok(values())
            },
        ));
        let mut attempt = admit_prepared(
            &gate,
            preparation,
            Box::pin(async {
                reached.store(true, std::sync::atomic::Ordering::SeqCst);
                futures::future::pending::<Result<()>>().await
            }),
        );
        assert!(
            std::future::Future::poll(attempt.as_mut(), &mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert!(
            reached.load(std::sync::atomic::Ordering::SeqCst),
            "interruption did not reach the preparation/admission cut"
        );
        drop(attempt); // Cancellation drops the real admission guard.
    }
    // Reopen after cancellation, discarding the disposable index and owner gate.
    drop(volume);
    let volume =
        Arc::new(block_on(TaskVolume::open_existing(provider.clone(), path.clone())).unwrap());
    let gate = GateArc::new(GateMutex::new(()));
    // The owner boundary retains one immutable admission parent. It is a leaf
    // beneath the production admission gate, never held over volume/provider I/O.
    let peers: Vec<_> = [a, b]
        .into_iter()
        .map(|requested| {
            let gate = gate.clone();
            let volume = volume.clone();
            let admitted = admitted.clone();
            loom::thread::spawn(move || {
                BROKEN_PARENT.with(|flag| flag.set(broken && interrupted != 3));
                UNCHECKED_ADMISSION.with(|flag| flag.set(broken && interrupted == 3));
                let preparation = Box::pin(prepare(
                    &volume,
                    task,
                    Some(requested),
                    1024,
                    Box::pin(async { Ok(admitted.lock().unwrap().map(Some)) }),
                    async || Ok(values()),
                ));
                let result = block_on(admit_prepared(
                    &gate,
                    preparation,
                    Box::pin(async {
                        {
                            let mut retained = admitted.lock().unwrap();
                            if retained.is_some_and(|parent| parent != requested) {
                                return Err(Error::Conflict(
                                    "owner admission changed on retry".into(),
                                ));
                            }
                            *retained = Some(requested);
                        }
                        CommandControl::change(&volume, Some(requested), &[], Some(true), 1024)
                            .await?;
                        assert!(
                            matches(&volume, task, requested).await?,
                            "admitted task followed a different parent"
                        );
                        assert_paused(&volume, task);
                        Ok(())
                    }),
                ));
                assert!(
                    matches!(result, Ok(()) | Err(Error::Conflict(_))),
                    "{result:?}"
                );
            })
        })
        .collect();
    for peer in peers {
        if let Err(failure) = peer.join() {
            std::panic::resume_unwind(failure);
        }
    }
    let attested = admitted
        .lock()
        .unwrap()
        .expect("one parent must be admitted");
    drop(volume);
    let reopened = block_on(TaskVolume::open_existing(provider, path)).unwrap();
    assert_eq!(
        block_on(cached_parent(&reopened, task, 1024)).unwrap(),
        Some(Some(attested)),
        "admitted task followed a different parent"
    );
    assert!(block_on(matches(&reopened, task, attested)).unwrap());
    assert_paused(&reopened, task);
}
#[test]
fn competing_parent_preparations_admit_one_matching_lineage() {
    loom::model(|| race(0, false));
}
#[test]
fn competing_retry_after_a_partial_preparation_preserves_the_parent() {
    loom::model(|| race(1, false));
}
#[test]
fn competing_retry_after_a_complete_preparation_preserves_the_parent() {
    loom::model(|| race(2, false));
}
#[test]
#[should_panic(expected = "admitted task followed a different parent")]
fn negative_control_detects_a_broken_parent_after_partial_preparation() {
    loom::model(|| race(1, true));
}
#[test]
#[should_panic(expected = "admitted task followed a different parent")]
fn negative_control_detects_a_broken_parent_after_complete_preparation() {
    loom::model(|| race(2, true));
}

#[test]
fn competing_legacy_retries_validate_admission_before_publishing_inheritance() {
    loom::model(|| race(3, false));
}
#[test]
#[should_panic(expected = "admitted task followed a different parent")]
fn negative_control_detects_an_unchecked_legacy_admission() {
    loom::model(|| race(3, true));
}

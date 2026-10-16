use super::*;
use crate::sync::SyncMutex;
use acyclic_stream::verification::block_on;
use loom::sync::Arc;

// Atomic snapshots and conditional appends model the existing volume contract.
// This is a leaf lock: no protocol/provider call runs while it is held.
type State = SyncMutex<(u64, SummarySelection)>;
async fn read(state: &State) -> Result<(u64, Option<SummarySelection>)> {
    let state = state.lock().unwrap();
    Ok((state.0, Some(state.1.clone())))
}
async fn append(state: &State, expected: u64, value: SummarySelection) -> Result<bool> {
    let mut state = state.lock().unwrap();
    if state.0 != expected {
        return Ok(false);
    }
    state.0 += 1;
    state.1 = value;
    Ok(true)
}
async fn publish<const CHECK: bool>(state: &State, value: &SummaryCheckpoint, key: &FileKey) {
    select_with::<_, _, _, CHECK>(
        value,
        key,
        || read(state),
        |version, value| append(state, version, value),
    )
    .await
    .unwrap();
}
fn race<const CHECK: bool>(rewind: bool) {
    let cursor = crate::task_volume::Cursor {
        volume: [0; 32],
        epoch: uuid::Uuid::nil(),
        schema: crate::task_volume::SCHEMA,
        query: [0; 32],
        position: 1,
        accepted: 1,
        durable: 1,
        commit: [0; 32],
    };
    let a = crate::context::summary_source::tests::checkpoint(cursor);
    let task = a.boundary.task;
    let a_key = checkpoint_key(task, 1, OperationId::from_bytes([1; 16])).unwrap();
    let b_key = checkpoint_key(task, 2, OperationId::from_bytes([2; 16])).unwrap();
    let initial = SummarySelection {
        parent: None,
        checkpoint: a_key.clone(),
        selected_at: 1,
        through: None,
    };
    let state = Arc::new(State::new((0, initial.clone())));
    let mut b = a.clone();
    b.boundary.previous = Some(a_key.clone());
    b.boundary.captured_at = 2;
    let first = {
        let state = state.clone();
        let key = a_key.clone();
        loom::thread::spawn(move || {
            block_on(async {
                if rewind {
                    let mut selected = initial;
                    selected.selected_at = 2;
                    selected.through = Some(a.boundary.through);
                    loop {
                        let (version, _) = read(&state).await.unwrap();
                        if append(&state, version, selected.clone()).await.unwrap() {
                            break;
                        }
                    }
                } else {
                    publish::<CHECK>(&state, &a, &key).await;
                }
            })
        })
    };
    let second = {
        let state = state.clone();
        let key = b_key.clone();
        loom::thread::spawn(move || block_on(publish::<CHECK>(&state, &b, &key)))
    };
    first.join().unwrap();
    second.join().unwrap();
    assert_eq!(
        state.lock().unwrap().1.checkpoint,
        if rewind { a_key } else { b_key },
        "summary selection crossed its retained boundary"
    );
}
#[test]
fn publication_fences_stale_replay() {
    loom::model(|| race::<true>(false));
}
#[test]
fn publication_respects_a_concurrent_rewind_fence() {
    loom::model(|| race::<true>(true));
}
#[test]
#[should_panic(expected = "summary selection crossed its retained boundary")]
fn negative_control_detects_unfenced_publication() {
    loom::model(|| race::<false>(false));
}
#[test]
#[should_panic(expected = "summary selection crossed its retained boundary")]
fn negative_control_detects_an_omitted_rewind_fence() {
    loom::model(|| race::<false>(true));
}

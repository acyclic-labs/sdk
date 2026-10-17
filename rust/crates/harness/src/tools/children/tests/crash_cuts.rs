use super::*;
use crate::test_support::crash_cuts::{Cut, enumerate};
use acyclic_stream::LocalStream;

fn operation() -> OperationId {
    OperationId::from_bytes([9; 16])
}

fn input() -> WaitInput {
    WaitInput {
        agents: vec!["A".into(), "B".into()],
        mode: Some(WaitMode::Any),
    }
}

fn receipt() -> FileKey {
    super::super::selection::key(TaskId::from_bytes([2; 16]), operation()).unwrap()
}

async fn receipt_bytes(files: &TaskFiles<LocalStream>) -> Option<Vec<u8>> {
    files
        .inner
        .snapshot(&Query::File(receipt()), 1)
        .await
        .unwrap()
        .files
        .first()
        .map(|file| file.bytes.to_vec())
}

async fn setup(stream: Arc<LocalStream>) {
    let files = files_with(stream).await;
    for name in ["A", "B"] {
        publish(
            &files,
            &Component::new(name).unwrap(),
            ChildFile::Request,
            b"work",
            operation(),
        )
        .await
        .unwrap();
    }
    acknowledge_answer(
        &files,
        &Component::new("B").unwrap(),
        ChildState::Running,
        "answer B",
    )
    .await;
}

#[derive(Debug)]
struct Recovered {
    retained_before: bool,
    result: Option<Value>,
    receipt_kept: bool,
    replay_stable: bool,
    rebound_denied: bool,
}

async fn recover(stream: Arc<LocalStream>, fault: Fault) -> Recovered {
    let files = files_with(stream).await;
    // Observe before advancing A: the oracle neither creates nor repairs the
    // selected-answer receipt, and does not use selection::load.
    let before = receipt_bytes(&files).await;
    acknowledge_answer(
        &files,
        &Component::new("A").unwrap(),
        ChildState::Running,
        "answer A",
    )
    .await;
    super::super::selection::IGNORE
        .with(|flag| flag.set(matches!(fault, Fault::IgnoreReceipt) && before.is_some()));
    super::super::selection::DROP_METADATA
        .with(|flag| flag.set(matches!(fault, Fault::DropMetadata)));
    let result = wait(&files, input(), limits(), operation(), false)
        .await
        .unwrap();
    super::super::selection::IGNORE.with(|flag| flag.set(false));
    super::super::selection::DROP_METADATA.with(|flag| flag.set(false));
    let after = receipt_bytes(&files).await;
    let generation = files.inner.head().await.unwrap();
    let replay = wait(&files, input(), limits(), operation(), false)
        .await
        .unwrap();
    let replay_stable = replay == result && files.inner.head().await.unwrap() == generation;
    let rebound_denied = matches!(
        wait(
            &files,
            WaitInput {
                agents: vec!["B".into(), "A".into()],
                mode: Some(WaitMode::Any),
            },
            limits(),
            operation(),
            false,
        )
        .await,
        Err(Error::Conflict(_))
    );
    Recovered {
        retained_before: before.is_some(),
        result,
        receipt_kept: after.is_some() && before.is_none_or(|before| Some(before) == after),
        replay_stable,
        rebound_denied,
    }
}

type WaitResult = std::result::Result<Option<Value>, String>;

#[derive(Clone, Copy)]
enum Fault {
    None,
    IgnoreReceipt,
    SwallowSaveError,
    DropMetadata,
}

fn expected(name: &str) -> Value {
    json!({"agents":[{
        "reference":name, "state":"Running",
        "result":{
            "turn":1, "file":{"path":format!(".volumes/agents/{name}/results/1.json")},
            "outcome":{"Succeeded":{
                "text":format!("answer {name}"), "metadata":null, "steps":1, "attachments":[]
            }}
        }
    }]})
}

fn violation(cut: Option<Cut>, initial: &WaitResult, seen: &Recovered) -> Option<&'static str> {
    match initial {
        Ok(Some(value)) => {
            if !seen.retained_before {
                return Some("successful wait has no durable receipt");
            }
            if *value != expected("B") {
                return Some("initial wait changed the full selected answer");
            }
        }
        Ok(None) => return Some("initial wait lost its known ready answer"),
        Err(_) if cut.is_none() || cut == Some(Cut::BeforeWrite(1)) => {
            return Some("uninterrupted publication failed");
        }
        Err(_) => {}
    }
    let name = if seen.retained_before { "B" } else { "A" };
    if seen.result.as_ref() != Some(&expected(name)) {
        return Some("recovery changed the full selected answer");
    }
    if !seen.receipt_kept {
        return Some("recovery lost or replaced the receipt");
    }
    if !seen.replay_stable || !seen.rebound_denied {
        return Some("replay changed state or accepted a different selection");
    }
    None
}

async fn cuts(fault: Fault) -> Vec<(Option<Cut>, WaitResult, Recovered)> {
    let found = enumerate(
        setup,
        |stream| async move {
            let files = files_with(stream).await;
            super::super::selection::SWALLOW_SAVE_ERROR
                .with(|flag| flag.set(matches!(fault, Fault::SwallowSaveError)));
            let result = wait(&files, input(), limits(), operation(), false)
                .await
                .map_err(|error| error.to_string());
            super::super::selection::SWALLOW_SAVE_ERROR.with(|flag| flag.set(false));
            result
        },
        |stream| recover(stream, fault),
    )
    .await
    .expect("every cut models power loss faithfully");
    assert_eq!(
        found.writes, 1,
        "selected-answer durable boundaries changed"
    );
    assert_eq!(found.cuts.len(), found.cut_results.len());
    std::iter::once((None, found.clean_result, found.clean))
        .chain(found.cuts.into_iter().zip(found.cut_results).map(
            |((cut, seen), (result_cut, result))| {
                assert_eq!(cut, result_cut, "outcome paired with the wrong durable cut");
                (Some(cut), result, seen)
            },
        ))
        .collect()
}

#[tokio::test]
async fn every_durable_cut_preserves_the_wait_any_receipt() {
    for (cut, initial, seen) in cuts(Fault::None).await {
        assert_eq!(
            violation(cut, &initial, &seen),
            None,
            "{cut:?}: {initial:?}: {seen:?}"
        );
        assert_eq!(seen.retained_before, cut != Some(Cut::BeforeWrite(0)));
    }
}

#[tokio::test]
async fn negative_control_ignoring_the_receipt_changes_the_selected_answer() {
    let mut caught = Vec::new();
    for (cut, initial, seen) in cuts(Fault::IgnoreReceipt).await {
        if seen.retained_before {
            assert_eq!(
                violation(cut, &initial, &seen),
                Some("recovery changed the full selected answer")
            );
            assert_eq!(seen.result.as_ref().unwrap()["agents"][0]["reference"], "A");
            caught.push(cut);
        } else {
            assert_eq!(violation(cut, &initial, &seen), None, "{cut:?}: {seen:?}");
        }
    }
    assert_eq!(
        caught,
        [None, Some(Cut::LostAck(1)), Some(Cut::BeforeWrite(1))]
    );
}

#[tokio::test]
async fn negative_control_swallowing_a_failed_save_cannot_acknowledge_an_unretained_answer() {
    let mut caught = Vec::new();
    for (cut, initial, seen) in cuts(Fault::SwallowSaveError).await {
        if let Some(reason) = violation(cut, &initial, &seen) {
            assert_eq!(reason, "successful wait has no durable receipt");
            assert_eq!(initial, Ok(Some(expected("B"))));
            assert_eq!(seen.result, Some(expected("A")));
            caught.push(cut);
        }
    }
    assert_eq!(caught, [Some(Cut::BeforeWrite(0))]);
}

#[tokio::test]
async fn negative_control_dropping_metadata_changes_the_full_answer() {
    let mut caught = Vec::new();
    for (cut, initial, seen) in cuts(Fault::DropMetadata).await {
        if seen.retained_before {
            assert_eq!(
                violation(cut, &initial, &seen),
                Some("recovery changed the full selected answer")
            );
            assert_eq!(seen.result.as_ref().unwrap()["agents"][0]["reference"], "B");
            assert_eq!(
                seen.result.as_ref().unwrap()["agents"][0]["result"]["outcome"]["Succeeded"]["text"],
                "answer B"
            );
            caught.push(cut);
        } else {
            assert_eq!(violation(cut, &initial, &seen), None, "{cut:?}: {seen:?}");
        }
    }
    assert_eq!(
        caught,
        [None, Some(Cut::LostAck(1)), Some(Cut::BeforeWrite(1))]
    );
}

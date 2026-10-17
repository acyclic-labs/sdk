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

async fn recover(stream: Arc<LocalStream>, ignore: bool) -> Recovered {
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
    super::super::selection::IGNORE.with(|flag| flag.set(ignore && before.is_some()));
    let result = wait(&files, input(), limits(), operation(), false)
        .await
        .unwrap();
    super::super::selection::IGNORE.with(|flag| flag.set(false));
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

fn violation(seen: &Recovered) -> Option<&'static str> {
    let name = if seen.retained_before { "B" } else { "A" };
    let answer = format!("answer {name}");
    let Some(result) = &seen.result else {
        return Some("recovery returned no selected answer");
    };
    if result["agents"][0]["reference"] != name
        || result["agents"][0]["result"]["outcome"]["Succeeded"]["text"] != answer
    {
        return Some("recovery changed the durable selected answer");
    }
    if !seen.receipt_kept {
        return Some("recovery lost or replaced the receipt");
    }
    if !seen.replay_stable || !seen.rebound_denied {
        return Some("replay changed state or accepted a different selection");
    }
    None
}

async fn cuts(ignore: bool) -> Vec<(Option<Cut>, Recovered)> {
    let found = enumerate(
        setup,
        |stream| async move {
            let files = files_with(stream).await;
            let _ = wait(&files, input(), limits(), operation(), false).await;
        },
        |stream| recover(stream, ignore),
    )
    .await
    .expect("every cut models power loss faithfully");
    assert_eq!(
        found.writes, 1,
        "selected-answer durable boundaries changed"
    );
    std::iter::once((None, found.clean))
        .chain(found.cuts.into_iter().map(|(cut, seen)| (Some(cut), seen)))
        .collect()
}

#[tokio::test]
async fn every_durable_cut_preserves_the_wait_any_receipt() {
    for (cut, seen) in cuts(false).await {
        assert_eq!(violation(&seen), None, "{cut:?}: {seen:?}");
        assert_eq!(seen.retained_before, cut != Some(Cut::BeforeWrite(0)));
    }
}

#[tokio::test]
async fn negative_control_ignoring_the_receipt_changes_the_selected_answer() {
    let mut caught = Vec::new();
    for (cut, seen) in cuts(true).await {
        if seen.retained_before {
            assert_eq!(
                violation(&seen),
                Some("recovery changed the durable selected answer")
            );
            caught.push(cut);
        } else {
            assert_eq!(violation(&seen), None, "{cut:?}: {seen:?}");
        }
    }
    assert_eq!(
        caught,
        [None, Some(Cut::LostAck(1)), Some(Cut::BeforeWrite(1))]
    );
}

use super::*;

fn settlement_property(ready: bool, assert_property: bool) {
    if assert_property {
        assert!(ready, "retained history blocked a settled child");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn child_frontier_and_wait_selection_paths_round_trip(reference in "[a-z]{1,24}", operation in any::<[u8;16]>()) {
        let component = Component::new(reference).unwrap();
        for key in [FileKey::Child { reference: component.clone(),file: ChildFile::Delivered },
            FileKey::Task { task: component,file: crate::task_volume::TaskFile::ChildWait(Component::new(OperationId::from_bytes(operation).to_string()).unwrap()) }] {
            prop_assert_eq!(FileKey::parse(&key.path()).unwrap(),key);
        }
    }
}

#[tokio::test]
async fn retained_bodies_cannot_block_wait_control_or_archive() {
    let files = files().await;
    let reference = Component::new("worker").unwrap();
    let operation = OperationId::new();
    publish(
        &files,
        &reference,
        ChildFile::Request,
        &vec![b'p'; 3072],
        operation,
    )
    .await
    .unwrap();
    acknowledge_answer(&files, &reference, ChildState::Running, &"a".repeat(3072)).await;
    // Negative control: the former full-history query rejects this valid child.
    let broken = files
        .observe_files(
            Query::Child(reference.clone()),
            None,
            limits().attachments,
            limits().render_bytes as usize,
        )
        .await;
    assert!(matches!(
        broken,
        Err(crate::task_volume::Error::LimitExceeded)
    ));
    let broken_ready = broken.is_ok();
    std::panic::catch_unwind(|| settlement_property(broken_ready, true))
        .expect_err("the full-history query was not detected");
    settlement_property(false, false);
    let result = super::super::files::follow(&files, &reference, limits(), false, true)
        .await
        .unwrap()
        .unwrap();
    settlement_property(result["settled"] == true, true);
    assert_eq!(
        result["result"]["outcome"]["Succeeded"]["text"]
            .as_str()
            .unwrap()
            .len(),
        3072
    );
    let lifecycle = files.inner.child_lifecycle(Some(&reference)).await.unwrap();
    assert_eq!(lifecycle.files.len(), 2);
    assert!(lifecycle.files.iter().all(|file| matches!(
        file.key,
        FileKey::Child {
            file: ChildFile::Status | ChildFile::Result(_),
            ..
        }
    )));
    publish(
        &files,
        &reference,
        ChildFile::Control,
        Control::Archive.bytes(),
        operation,
    )
    .await
    .unwrap();
    acknowledge(&files, &reference, ChildState::Archived).await;
    assert_eq!(
        super::super::files::follow(&files, &reference, limits(), false, true)
            .await
            .unwrap()
            .unwrap()["state"],
        json!(ChildState::Archived)
    );
    let history = files
        .inner
        .snapshot(&Query::Child(reference), 16)
        .await
        .unwrap();
    assert!(history.files.iter().any(|file| file.bytes.len() == 3072));
}

#[tokio::test]
async fn lifecycle_selection_stays_constant_after_more_history_than_the_attachment_bound() {
    let files = files().await;
    let reference = Component::new("worker").unwrap();
    let operation = OperationId::new();
    let task = TaskId::from_bytes([7; 16]);
    publish(&files, &reference, ChildFile::Request, b"work", operation)
        .await
        .unwrap();
    for turn in 1..=32 {
        if turn > 1 {
            publish(
                &files,
                &reference,
                ChildFile::Inbox(Component::new(format!("input-{turn}")).unwrap()),
                b"more",
                operation,
            )
            .await
            .unwrap();
        }
        let status = ChildStatus {
            task,
            state: ChildState::Running,
            settled: false,
            inputs: turn,
        };
        for (kind, bytes) in [
            (ChildFile::Status, serde_json::to_vec(&status).unwrap()),
            (
                ChildFile::Result(turn),
                serde_json::to_vec(&crate::task_volume::ChildTurnResult {
                    task,
                    turn,
                    outcome: crate::Outcome::Cancelled,
                })
                .unwrap(),
            ),
            (
                ChildFile::Status,
                serde_json::to_vec(&ChildStatus {
                    settled: true,
                    ..status
                })
                .unwrap(),
            ),
        ] {
            let generation = files.inner.head().await.unwrap();
            files
                .inner
                .append(
                    generation,
                    &FileValue::new(
                        FileKey::Child {
                            reference: reference.clone(),
                            file: kind,
                        },
                        bytes,
                    ),
                )
                .await
                .unwrap();
        }
        let head = files.inner.child_lifecycle(Some(&reference)).await.unwrap();
        assert_eq!(head.files.len(), 2);
        let result = super::super::files::follow(&files, &reference, limits(), false, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result["result"]["turn"], turn);
    }
    assert!(matches!(
        files
            .observe_files(
                Query::Child(reference),
                None,
                limits().attachments,
                limits().render_bytes as usize
            )
            .await,
        Err(crate::task_volume::Error::LimitExceeded)
    ));
}

async fn recovery(ignore_selection: bool) -> (Value, Value) {
    let provider = Arc::new(MemoryStream::default());
    let files = files_with(provider.clone()).await;
    let workers = [Component::new("A").unwrap(), Component::new("B").unwrap()];
    let operation = OperationId::new();
    for worker in &workers {
        publish(&files, worker, ChildFile::Request, b"work", operation)
            .await
            .unwrap();
    }
    let input = || WaitInput {
        agents: vec!["A".into(), "B".into()],
        mode: Some(WaitMode::Any),
    };
    let mut pending = Box::pin(wait(&files, input(), limits(), operation, true));
    assert!(futures::poll!(&mut pending).is_pending());
    acknowledge(&files, &workers[1], ChildState::Running).await;
    let selected = pending.await.unwrap().unwrap();
    assert_eq!(selected["agents"][0]["reference"], "B");
    // Crash after the selected answer is retained, before its tool result is journaled.
    acknowledge(&files, &workers[0], ChildState::Running).await;
    drop(files);
    let reopened = files_with(provider).await;
    super::super::selection::IGNORE.with(|flag| flag.set(ignore_selection));
    let recovered = wait(&reopened, input(), limits(), operation, false)
        .await
        .unwrap()
        .unwrap();
    super::super::selection::IGNORE.with(|flag| flag.set(false));
    (selected, recovered)
}

fn selection_property((before, after): (Value, Value), assert_property: bool) {
    if assert_property {
        assert_eq!(
            before, after,
            "wait(any) changed its durable selection after a crash"
        );
    }
}

#[tokio::test]
async fn wait_any_retains_b_across_the_result_journal_crash_cut() {
    selection_property(recovery(false).await, true);
    let broken = recovery(true).await;
    std::panic::catch_unwind(|| selection_property(broken.clone(), true))
        .expect_err("ignoring the immutable selection was not detected");
    selection_property(broken, false);
}

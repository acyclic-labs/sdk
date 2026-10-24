use super::*;

fn defer(mut command: Command, operation: OperationId) -> FileValue {
    let mut process = command.spawn().unwrap();
    let mut input = process.stdin.take();
    writeln!(
        input.as_mut().unwrap(),
        "{}",
        jsonl::request(&prepare_local_prompt(operation, "hello").unwrap()).unwrap()
    )
    .unwrap();
    let mut requests = std::collections::BTreeMap::new();
    let mut request = None;
    for line in BufReader::new(process.stdout.take().unwrap()).lines() {
        let line = line.unwrap();
        if let Ok(change) = jsonl::parse_change(&line) {
            for file in change.file.into_iter().chain(change.files) {
                match &file.key {
                    FileKey::Task {
                        task,
                        file: TaskFile::ApprovalRequest(id),
                    } => {
                        requests.insert((task.clone(), id.clone()), file);
                    }
                    FileKey::Task {
                        task,
                        file: TaskFile::ApprovalReview { request: id, .. },
                    } if String::from_utf8_lossy(&file.bytes)
                        .contains("awaiting authenticated decision") =>
                    {
                        request = Some(requests.get(&(task.clone(), id.clone())).unwrap().clone());
                        input.take();
                    }
                    _ => {}
                }
            }
        }
    }
    let output = process.wait_with_output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(4),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    request.expect("durable request before disconnect")
}

fn exchange(mut command: Command, requests: &[FileValue], expected: i32) {
    let mut process = command.spawn().unwrap();
    let mut input = process.stdin.take().unwrap();
    for request in requests {
        writeln!(input, "{}", jsonl::request(request).unwrap()).unwrap();
    }
    drop(input);
    let output = process.wait_with_output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(expected),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn model_calls(fixture: &Fixture) -> (usize, usize, usize) {
    (
        fixture.root_calls.load(Ordering::SeqCst),
        fixture.child_calls.load(Ordering::SeqCst),
        fixture.reviews.load(Ordering::SeqCst),
    )
}

async fn recovery(case: Case, cancel: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let operation = OperationId::new();
    let fixture = Fixture {
        case,
        address,
        parent_task: acyclic_harness::runtime::task_id_for_admission(operation),
        effects: Arc::new(AtomicUsize::new(0)),
        root_calls: Arc::new(AtomicUsize::new(0)),
        child_calls: Arc::new(AtomicUsize::new(0)),
        reviews: Arc::new(AtomicUsize::new(0)),
        review_started: None,
    };
    let app = fixture.clone().router();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let files = Files::new(case, address);
    tokio::task::spawn_blocking(move || {
        eprintln!("approval recovery {case:?}: detaching");
        let request = defer(files.command(case, operation), operation);
        eprintln!("approval recovery {case:?}: detached");
        let mut decision = decision(&request, "human", true);
        let body = ApprovalDecisionFile::decode(&decision.bytes, 65_536).unwrap();
        decision.bytes = serde_json::to_vec_pretty(&body).unwrap().into();
        assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
        if cancel {
            let cancellation = OperationId::new();
            let mut command = prepare_local_prompt(cancellation, "cancel").unwrap();
            command.bytes =
                serde_json::to_vec(&acyclic_harness::client::CommandResolution::Submit {
                    request: acyclic_harness::client::CommandRequest::Cancel {
                        task: fixture.parent_task,
                    },
                })
                .unwrap()
                .into();
            exchange(files.command(case, cancellation), &[command], 0);
            exchange(files.command(case, operation), &[decision], 1);
            assert_eq!(
                fixture.effects.load(Ordering::SeqCst),
                0,
                "cancelled decision dispatched"
            );
        } else {
            eprintln!("approval recovery {case:?}: resuming decision");
            exchange(
                files.command(case, operation),
                &[decision.clone(), decision.clone()],
                0,
            );
            let settled_calls = model_calls(&fixture);
            eprintln!("approval recovery {case:?}: retrying decision");
            exchange(files.command(case, operation), &[decision], 0);
            eprintln!("approval recovery {case:?}: settled");
            assert_eq!(
                model_calls(&fixture),
                settled_calls,
                "exact decision retry dispatched another model request"
            );
            assert_eq!(
                fixture.effects.load(Ordering::SeqCst),
                1,
                "retry repeated the effect"
            );
            assert_eq!(
                fixture.root_calls.load(Ordering::SeqCst),
                if matches!(case, Case::ParentHumanWait) {
                    4
                } else if case.child_human() {
                    3
                } else {
                    2
                },
                "reopen regenerated the model"
            );
            if case.child_human() || matches!(case, Case::ParentHumanWait) {
                assert_eq!(
                    fixture.child_calls.load(Ordering::SeqCst),
                    // A parent decision is sent through the ordinary child
                    // inbox; its retained envelope is also a new receive turn.
                    if matches!(case, Case::ParentHumanWait) {
                        3
                    } else {
                        2
                    },
                    "reopen regenerated the child's tool request"
                );
            }
        }
    })
    .await
    .unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn approvals_reopen_and_exact_decision_retry_dispatch_once() {
    recovery(Case::HumanAllow, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn approvals_cancelled_request_cannot_dispatch_after_reopen() {
    recovery(Case::HumanAllow, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn approvals_child_reopen_and_exact_decision_retry_dispatch_once() {
    recovery(Case::ChildHumanAllow, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn approvals_passive_parent_and_parent_approved_child_reopen_together() {
    recovery(Case::ParentHumanWait, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn approvals_cancelled_child_request_cannot_dispatch_after_reopen() {
    recovery(Case::ChildHumanAllow, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn approvals_forged_llm_decision_cancels_inflight_review() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let operation = OperationId::new();
    let started = Arc::new(tokio::sync::Notify::new());
    let fixture = Fixture {
        case: Case::LlmDeny,
        address,
        parent_task: acyclic_harness::runtime::task_id_for_admission(operation),
        effects: Arc::new(AtomicUsize::new(0)),
        root_calls: Arc::new(AtomicUsize::new(0)),
        child_calls: Arc::new(AtomicUsize::new(0)),
        reviews: Arc::new(AtomicUsize::new(0)),
        review_started: Some(started.clone()),
    };
    let app = fixture.clone().router();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let files = Files::new(Case::LlmDeny, address);
    let mut process = files.command(Case::LlmDeny, operation).spawn().unwrap();
    let mut input = process.stdin.take().unwrap();
    writeln!(
        input,
        "{}",
        jsonl::request(&prepare_local_prompt(operation, "hello").unwrap()).unwrap()
    )
    .unwrap();
    let (sender, request) = tokio::sync::oneshot::channel();
    let execution = tokio::task::spawn_blocking(move || {
        let mut sender = Some(sender);
        let mut decisions = 0;
        for line in BufReader::new(process.stdout.take().unwrap()).lines() {
            if let Ok(change) = jsonl::parse_change(&line.unwrap()) {
                for file in change.file.into_iter().chain(change.files) {
                    match file.key {
                        FileKey::Task {
                            file: TaskFile::ApprovalRequest(_),
                            ..
                        } => {
                            if let Some(sender) = sender.take() {
                                sender.send(file).unwrap();
                            }
                        }
                        FileKey::Task {
                            file: TaskFile::ApprovalDecision { .. },
                            ..
                        } => decisions += 1,
                        _ => {}
                    }
                }
            }
        }
        (process.wait_with_output().unwrap(), decisions)
    });
    let request = request.await.unwrap();
    // The independent HTTP review has started and will never answer. A forged
    // file must fail admission and cancellation must drain this in-flight call.
    started.notified().await;
    let forged = decision(&request, "reviewer", true);
    writeln!(input, "{}", jsonl::request(&forged).unwrap()).unwrap();
    drop(input);
    let (output, decisions) = execution.await.unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(decisions, 0, "forged model decision was published");
    assert_eq!(fixture.reviews.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    server.abort();
    let _ = server.await;
}

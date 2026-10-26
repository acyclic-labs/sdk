//! Exercise decisive-review publication through the real local filesystem host.
use super::*;
use crate::test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing_subscriber::layer::SubscriberExt as _;

#[derive(Clone, Default)]
struct Flushes(Arc<AtomicU64>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Flushes {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Count<'a>(&'a AtomicU64);
        impl tracing::field::Visit for Count<'_> {
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                if field.name() == "fsyncs" {
                    self.0.fetch_add(value, Ordering::Relaxed);
                }
            }
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
        }
        event.record(&mut Count(&self.0));
    }
}

#[test]
fn decisive_reviews_share_settlement_durability_without_authorizing_early() -> Result<()> {
    let test = std::thread::current()
        .name()
        .ok_or_else(|| Error::Storage("review proof has no libtest identity".into()))?
        .to_owned();
    const CHILD: &str = "ACYCLIC_REVIEW_FLUSH_PROOF";
    if crate::native_environment::variable(CHILD).as_deref() != Some(std::ffi::OsStr::new(&test)) {
        let mut command = std::process::Command::new(std::env::current_exe().map_err(storage)?);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt as _;
            command.creation_flags(0x08000000);
        }
        let output = command
            .args([&test, "--exact", "--test-threads=1", "--nocapture"])
            .env(CHILD, &test)
            .output()
            .map_err(storage)?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(stdout.contains("1 passed; 0 failed"), "{stdout}\n{stderr}");
        assert_eq!(stderr.matches("APPROVALS_REVIEW_FLUSHES").count(), 2);
        eprint!("{stderr}");
        return Ok(());
    }
    let flushes = Flushes::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(flushes.clone()))
        .map_err(storage)?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?
        .block_on(async {
            for approved in [false, true] {
                let mut measured = Vec::new();
                for eager in [false, true] {
                    measured.push(trial(&flushes, approved, eager).await?);
                }
                assert_eq!(measured[0].0 + 1, measured[1].0, "{measured:?}");
                assert_eq!(measured[0].1 + 1, measured[1].1, "{measured:?}");
                eprintln!(
                    "APPROVALS_REVIEW_FLUSHES approved={approved} grouped={} former={}",
                    measured[0].0, measured[1].0
                );
                eprintln!(
                    "APPROVALS_ATTENTION_FLUSHES approved={approved} grouped={} former={}",
                    measured[0].1, measured[1].1
                );
            }
            Ok(())
        })
}

async fn trial(flushes: &Flushes, approved: bool, eager: bool) -> Result<(u64, u64)> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let provider = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(crate::model::ModelEvent::ToolCall(
                    crate::model::ToolCall::new(
                        "write-effect".into(),
                        "write".into(),
                        serde_json::json!({"path":"effect.txt", "text":"approved",
                            "media_type":"text/plain", "display_name":"effect"}),
                    ),
                )),
                ScriptedEvent::Event(crate::model::ModelEvent::Completed {
                    metadata: serde_json::Value::Null,
                }),
            ]),
            ScriptedTurn::Text("done".into()),
        ])
        .with_context_capacity(crate::context::ModelContextCapacity {
            context_tokens: 32_768,
            output_tokens: Some(4096),
        }),
    );
    let model = Model::new(
        "scripted".into(),
        "test".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(directory.path(), (model, provider.clone()))?
            .state_directory(state.path())
            .compaction(crate::context::CompactionPolicy::Disabled)
            .approval_policy(crate::approvals::ApprovalPolicy {
                approvers: vec![Approver::Rules {
                    principal: "rules".into(),
                    rules: Vec::new(),
                    fallback: if approved {
                        RuleVerdict::Allow
                    } else {
                        RuleVerdict::Deny
                    },
                }],
            }),
    )
    .await?;
    let crate::Admission::Accepted(RuntimeTask::Durable { task_id: task, .. }) = harness
        .submit(OperationId::new(), "write the effect")
        .await?
    else {
        return Err(Error::Conflict("expected durable admission".into()));
    };
    let commands = harness.commands()?;
    crate::package::harness::worker::advance_while(&harness, task, &commands, None, || async {
        Ok(false)
    })
    .await?;
    let files = harness.runtime.approval_files(task).await?;
    let snapshot = files
        .inner
        .pending_approvals(&Component::new(task.to_string())?)
        .await?;
    let observed: BTreeMap<_, _> = snapshot
        .files
        .into_iter()
        .map(|f| (f.key.clone(), f))
        .collect();
    let requests = pending(&observed);
    assert_eq!(requests.len(), 1);
    let request = requests[0];
    let attention_flushes =
        attention_trial(&harness, task, request, &files, flushes, eager).await?;
    let Some(Stage::Automatic(review)) = harness
        .approval_stage(task, request, &files, &observed, true)
        .await?
    else {
        return Err(Error::Conflict("expected decisive rule review".into()));
    };
    assert!(
        files
            .snapshot(&Query::File(review_key(request, &review.principal)?), 1)
            .await?
            .files
            .is_empty()
    );
    assert_eq!(provider.requests().len(), 1);
    assert!(!directory.path().join("effect.txt").exists());

    flushes.0.store(0, Ordering::Relaxed);
    if eager {
        // Execute the former independently durable review before settlement.
        publish_review(&files, review_key(request, &review.principal)?, &review).await?;
    }
    harness
        .settle_automatic_review(task, request, &files, review)
        .await?;
    let count = flushes.0.load(Ordering::Relaxed);
    assert!(count > 0, "native flushes were not observed");
    let snapshot = files
        .snapshot(&Query::Approvals(Component::new(task.to_string())?), 8)
        .await?;
    assert_eq!(snapshot.files.len(), 3);
    for file in &snapshot.files {
        assert!(
            files.inner.file_is_durable(file).await?,
            "settlement returned an unflushed audit"
        );
    }
    // Even a settled verdict cannot run the effect without the original
    // command resuming; the denied verdict must never run it at all.
    assert!(!directory.path().join("effect.txt").exists());
    let result = harness.resume(task).await;
    if approved {
        assert_eq!(result?.text, "done");
        assert_eq!(provider.requests().len(), 2);
        assert_eq!(
            std::fs::read(directory.path().join("effect.txt")).map_err(storage)?,
            b"approved"
        );
    } else {
        assert!(
            matches!(result, Err(Error::TaskFailed { task: failed, message })
            if failed == task && message == "declined")
        );
        assert_eq!(provider.requests().len(), 1);
        assert!(!directory.path().join("effect.txt").exists());
    }
    drop(commands);
    harness.close().await?;
    Ok((count, attention_flushes))
}

async fn attention_trial(
    harness: &LocalHarness,
    task: TaskId,
    request: &FileValue,
    files: &Arc<crate::task_volume::TaskFiles<LocalStream>>,
    flushes: &Flushes,
    eager: bool,
) -> Result<u64> {
    let inbox = Inbox {
        files: files.clone(),
        reference: Component::new("child-attention-proof")?,
        parent_done: acyclic_fs::CancellationToken::new(),
        parent_passive: acyclic_fs::CancellationToken::new(),
    };
    let stream = harness.runtime.task_host().workflow_stream();
    let other_directory = tempfile::tempdir().map_err(storage)?;
    let other = Arc::new(
        LocalStream::open(other_directory.path(), Default::default())
            .await
            .map_err(storage)?,
    );
    let before = files.inner.head().await?;
    assert!(matches!(
        parent::attention(&other, task, request, &inbox, "human").await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(files.inner.head().await?, before);
    let mirror = FileValue::new(
        FileKey::Child {
            reference: inbox.reference.clone(),
            file: crate::task_volume::ChildFile::ApprovalRequest(request_id(request)?.clone()),
        },
        request.bytes.clone(),
    );
    flushes.0.store(0, Ordering::Relaxed);
    if eager {
        // Actual former ordering: flush the mirror independently first.
        crate::task_volume::publish_durable(&files.inner, &mirror, None).await?;
    }
    parent::attention(&stream, task, request, &inbox, "human").await?;
    let count = flushes.0.load(Ordering::Relaxed);
    assert!(count > 0, "attention returned without a device barrier");
    let snapshot = files
        .snapshot(&Query::Child(inbox.reference.clone()), 2)
        .await?;
    assert_eq!(snapshot.files.len(), 2);
    for file in &snapshot.files {
        assert!(files.inner.file_is_durable(file).await?);
    }
    flushes.0.store(0, Ordering::Relaxed);
    parent::attention(&stream, task, request, &inbox, "human").await?;
    assert_eq!(
        flushes.0.load(Ordering::Relaxed),
        0,
        "unchanged attention was republished"
    );
    Ok(count)
}

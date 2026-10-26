use super::*;
use crate::executor::ModelPurpose;
use futures::{FutureExt, StreamExt};

type Journal = FilesystemExecutionJournal<
    acyclic_stream::LocalStream,
    Arc<acyclic_fs::StreamAuthorityStore<acyclic_stream::LocalStream>>,
    acyclic_fs::MemoryObjectBackend,
>;

async fn fixture(
    root: &std::path::Path,
    operation: OperationId,
    io: Option<&PreparationIo>,
) -> Result<(Journal, crate::core::AuthorityRoot)> {
    let stream = Arc::new(
        acyclic_stream::LocalStream::open(root.join("journal"), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );
    let authority = Arc::new(
        acyclic_stream::LocalStream::open(root.join("authority"), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let backend = acyclic_fs::Fs::new(
        Arc::new(acyclic_fs::StreamAuthorityStore::new(authority)),
        acyclic_fs::LogicalObjectStore::new(Arc::new(objects), bucket),
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    let fixture = crate::durable_host::tests::fixture::Fixture::new(
        stream.clone(),
        backend,
        serde_json::json!({"type":"integer"}),
        Some(crate::scheduler::SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 0,
            model_steps: 2,
        }),
        &[],
    )
    .await?;
    let owner = fixture.running(80).await?;
    let request = phase_request()?;
    let (task, fence) = owner.task_binding();
    use crate::runtime::DurableTaskHost;
    fixture
        .host
        .claim_model_dispatch(task, operation, 0, request.request_digest(), fence.clone())
        .await?;
    let owner = if io.is_some() {
        fixture.host.journal_owner(task, fence, io).await?
    } else {
        owner
    };
    let journal = fixture.execution(owner, operation)?;
    let files = crate::task_volume::TaskFiles::open(
        stream,
        &fixture.storage,
        &fixture.issuer.verifier(&mut None)?,
        &journal.scope,
        65_536,
    )
    .await?;
    let journal = journal.with_task_files(Arc::new(files))?;
    journal
        .append(
            operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    let reference =
        crate::executor::stage_json(&journal, operation, "request", request.request(), None)
            .await?;
    journal
        .append(
            operation,
            "model:0:started",
            &ExecutionEvent::ModelStarted {
                step: 0,
                purpose: ModelPurpose::Response,
                request_digest: request.request_digest(),
                request: reference,
            },
            None,
        )?
        .await?;
    Ok((journal, fixture.issuer))
}

fn phase_request() -> Result<crate::model::PreparedModelRequest> {
    crate::model::PreparedModelRequest::prepare(
        crate::model::ModelRequest {
            model: crate::model::Model::new(
                "test".into(),
                "phase".into(),
                "1".into(),
                serde_json::Value::Null,
            )?,
            messages: vec![crate::model::ModelMessage {
                role: crate::model::ModelRole::User,
                content: ModelContent::Text("input".into()),
            }],
            tools: Vec::new(),
            max_output_tokens: None,
        },
        Limits::default(),
        None,
    )
}

#[tokio::test]
async fn phase_keeps_deltas_live_and_seals_successful_and_failed_prefixes() -> Result<()> {
    for mode in 0..3 {
        for defer in [false, true] {
            for fail in [false, true] {
                check_phase(mode, defer, fail).await?;
            }
        }
    }
    Ok(())
}

async fn check_phase(mode: u8, defer: bool, fail: bool) -> Result<()> {
    use crate::task_volume::Query;
    let io = phase_io(mode)?;
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let operation = OperationId::new();
    let (journal, runtime) = fixture(directory.path(), operation, io.as_ref()).await?;
    // Journal admission ports are weak. The test's runtime owns signing custody
    // until its final phase barrier and live-publication checks have completed.
    let _runtime = runtime;
    assert!(journal.supports_deferred_durability());
    let path = journal.path(operation)?.path().clone();
    let baseline = journal.stream.watermarks(path.clone()).await?;
    let mut observer = journal
        .task_files
        .as_ref()
        .ok_or_else(|| Error::Invalid("phase fixture lacks task files".into()))?
        .inner
        .observe(Query::Root, None, 16)
        .await?;
    assert!(observer.changes.next().now_or_never().is_none());
    let future = async {
        crate::executor::append_model_event(
            &journal,
            operation,
            "model:0:0",
            (0, ModelPurpose::Response),
            &ModelEvent::Content {
                delta: "visible".into(),
            },
            io.as_ref(),
        )
        .await?;
        let watermarks = journal.stream.watermarks(path.clone()).await?;
        assert_eq!(watermarks.accepted, baseline.accepted + 1);
        // Negative control executes the former, individually durable append.
        assert_eq!(
            watermarks.durable,
            if defer {
                baseline.durable
            } else {
                watermarks.accepted
            }
        );
        let mut visible = false;
        while let Some(Some(change)) = observer.changes.next().now_or_never() {
            let change = change?;
            visible |= change
                .file
                .into_iter()
                .chain(change.files)
                .any(|file| file.bytes.as_ref() == b"visible");
        }
        assert!(visible, "phase buffered live publication");
        if fail {
            Err(Error::Conflict("fixture stream failure".into()))
        } else {
            Ok(())
        }
    };
    let result = if defer {
        crate::executor::phase::run(&journal, io.as_ref(), future).await
    } else {
        future.await
    };
    assert_eq!(
        result.is_err(),
        fail,
        "phase result mode={mode} defer={defer} fail={fail}: {result:?}"
    );
    assert_eq!(
        journal.stream.watermarks(path.clone()).await?.durable,
        baseline.accepted + 1
    );
    Ok(())
}

fn phase_io(mode: u8) -> Result<Option<PreparationIo>> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(16 * 1024 * 1024, 4 * 1024 * 1024);
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    Ok(match mode {
        0 => None,
        1 => Some(PreparationIo::from_budget(budget, &cancellation)?),
        _ => Some(PreparationIo::from_native_work(&work)?),
    })
}

#[tokio::test]
async fn standalone_phase_cancellation_cannot_revoke_accepted_durability() -> Result<()> {
    check_cancelled_phase(1).await
}

#[tokio::test]
async fn native_phase_cancellation_cannot_revoke_accepted_durability() -> Result<()> {
    check_cancelled_phase(2).await
}

async fn check_cancelled_phase(mode: u8) -> Result<()> {
    for late_admission in [true, false] {
        for exit in 0..3 {
            let fail = exit != 0;
            let io = phase_io(mode)?.ok_or_else(|| Error::Invalid("phase IO missing".into()))?;
            let directory =
                tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
            let operation = OperationId::new();
            let (journal, _runtime) = fixture(directory.path(), operation, Some(&io)).await?;
            let path = journal.path(operation)?.path().clone();
            let before = journal.stream.watermarks(path.clone()).await?;
            let before_work = io.receipt();
            let future = async {
                crate::executor::append_model_event(
                    &journal,
                    operation,
                    "model:0:0",
                    (0, ModelPurpose::Response),
                    &ModelEvent::Content {
                        delta: "accepted before cancellation".into(),
                    },
                    Some(&io),
                )
                .await?;
                let accepted = journal.stream.watermarks(path.clone()).await?;
                assert_eq!(accepted.accepted, before.accepted + 1);
                assert_eq!(accepted.durable, before.durable);
                io.cancellation().cancel();
                match exit {
                    0 => Ok(()),
                    1 => io.check_cancellation(),
                    _ => std::future::pending::<Result<()>>().await,
                }
            };
            // As in the model dispatch entry point, completion must outlive
            // the loser of the cancellation race as well as an ordinary error.
            let future = async {
                let cancellation = io.cancellation().cancelled();
                futures::pin_mut!(cancellation, future);
                match futures::future::select(cancellation, future).await {
                    futures::future::Either::Left(_) => Err(Error::InteractionRejected(
                        crate::InteractionRejection::Cancelled,
                    )),
                    futures::future::Either::Right((result, _)) => result,
                }
            };
            let result = if late_admission {
                // Execute the old ordering as a negative control: constructing
                // completion after cancellation strands the accepted prefix.
                let result = acyclic_stream::deferring_durability(future).await;
                match journal.durability_barrier(Some(&io)) {
                    Ok(barrier) => barrier.await.and(result),
                    Err(error) => Err(error),
                }
            } else {
                crate::executor::phase::run(&journal, Some(&io), future).await
            };
            assert_eq!(
                result.is_err(),
                late_admission || fail,
                "mode={mode} late={late_admission} fail={fail}: {result:?}"
            );
            let after = journal.stream.watermarks(path).await?;
            assert_eq!(
                after.durable == after.accepted,
                !late_admission,
                "cancellation durability oracle mode={mode} late={late_admission}"
            );
            if !late_admission {
                assert!(
                    io.receipt().durability_operations > before_work.durability_operations,
                    "completion escaped the original ledger"
                );
            }
        }
    }
    // A cancelled caller cannot start a phase; the reservation is its single
    // admission boundary, before the deferred body can accept anything.
    let io = phase_io(mode)?.ok_or_else(|| Error::Invalid("phase IO missing".into()))?;
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (journal, _runtime) = fixture(directory.path(), OperationId::new(), Some(&io)).await?;
    io.cancellation().cancel();
    let polled = std::sync::atomic::AtomicBool::new(false);
    assert!(
        crate::executor::phase::run(&journal, Some(&io), async {
            polled.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
        .await
        .is_err()
    );
    assert!(!polled.load(std::sync::atomic::Ordering::SeqCst));
    Ok(())
}

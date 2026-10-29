use super::*;
use crate::local::provider_tests::{enrollment, options};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitMutation, CommitOutcome, CommitRequest,
    StreamProviderExt as _,
};
use futures::FutureExt;

#[tokio::test]
async fn lost_admission_reply_keeps_original_incarnation_closed()
-> Result<(), Box<dyn std::error::Error>> {
    for destroy in [false, true] {
        let machine = MachineId::new();
        let directory = tempfile::tempdir()?;
        let streams = Arc::new(
            acyclic_stream::LocalStream::open(
                directory.path(),
                acyclic_stream::LocalStreamLimits::default(),
            )
            .await?,
        );
        let root = StreamPath::new(format!("machine-tests/{machine}"))?;
        let path = StreamPath::new(format!("{root}/machines/{machine}"))?;
        let backend = LocalBackend {
            machine: LocalMachine::new(machine, options())?,
            generation: std::sync::Mutex::new(Generation::new()?),
        };
        let local =
            AdmittedMachine::open(backend, enrollment(machine), streams.clone(), root).await?;
        let key = IdempotencyKey::new();
        let (schedule, mut boundaries) = acyclic_native_runtime::testing::Schedule::new();
        let mut submitted = Box::pin(schedule.scope(async {
            if destroy {
                local.destroy_machine(machine, key).await
            } else {
                local.suspend(machine, key).await
            }
        }));
        let admission = loop {
            let boundary = tokio::select! {
                result = &mut submitted => panic!("original admission completed before releasing its physical commit: {result:?}"),
                boundary = boundaries.recv() => boundary.unwrap(),
            };
            if boundary.name == "native:submitted" && boundary.detail.starts_with("commit ") {
                break boundary;
            }
            boundary.release();
        };
        // The actual durable provider retains the already submitted mutation
        // when its original observer disappears. No fake receipt participates.
        drop(submitted);
        admission.release();
        let drained = schedule.drained();
        tokio::pin!(drained);
        loop {
            tokio::select! {
                biased;
                () = &mut drained => break,
                boundary = boundaries.recv() => boundary.unwrap().release(),
            }
        }
        let admitted = streams.head(path.clone()).await?;
        assert_eq!(admitted.tail, 2);
        assert_eq!(
            local.inspect_machine(machine).await?.state,
            if destroy {
                MachineState::Destroying
            } else {
                MachineState::Suspending
            }
        );
        assert!(matches!(
            local
                .exec(
                    machine,
                    ExecCommand::new(vec!["must-not-spawn".into()], 5_000)?
                )
                .await,
            Err(ProviderError::Conflict(_))
        ));
        let later = IdempotencyKey::new();
        assert!(matches!(local.wake(machine, later).await,
            Err(ProviderError::Indeterminate(original)) if original == later));
        assert_eq!(streams.head(path).await?, admitted);
        assert!(matches!(local.recover(key).await,
            Err(ProviderError::Indeterminate(original)) if original == key));
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn file_deadlines_include_waiting_for_original_lifecycle_owner()
-> Result<(), Box<dyn std::error::Error>> {
    let machine = MachineId::new();
    let backend = LocalBackend {
        machine: LocalMachine::new(machine, options())?,
        generation: std::sync::Mutex::new(Generation::new()?),
    };
    let mut physical = backend.execution()?;
    let local = AdmittedMachine::open(
        backend,
        enrollment(machine),
        Arc::new(acyclic_stream::MemoryStream::default()),
        StreamPath::new(format!("machine-tests/{machine}"))?,
    )
    .await?;
    let mut stopping = Box::pin(local.suspend(machine, IdempotencyKey::new()));
    assert!(futures::poll!(&mut stopping).is_pending());
    let path = std::env::temp_dir().join(format!("acyclic-unentered-file-{machine}"));
    let expiry = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64
        + 5_000;
    let access = FileAccess::new(path.to_str().unwrap().into(), 0, expiry)?;
    let mut read = Box::pin(local.read_file(machine, access.clone(), 1));
    let mut write = Box::pin(local.write_file(machine, access, vec![1], true));
    assert!(futures::poll!(&mut read).is_pending());
    assert!(futures::poll!(&mut write).is_pending());
    tokio::time::advance(Duration::from_secs(5)).await;
    // Poll once: the former ordering fails here instead of hanging the test.
    assert!(matches!(
        futures::poll!(&mut read),
        std::task::Poll::Ready(Err(ProviderError::Rejected(_)))
    ));
    assert!(matches!(
        futures::poll!(&mut write),
        std::task::Poll::Ready(Err(ProviderError::Rejected(_)))
    ));
    physical.retired(true);
    assert_eq!(stopping.await?, MutationOutcome::Suspended(machine));
    assert!(!path.exists());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn queued_spawn_observation_obeys_original_deadline_and_shutdown()
-> Result<(), Box<dyn std::error::Error>> {
    for expire in [true, false] {
        let machine = MachineId::new();
        let marker = std::env::temp_dir().join(format!("acyclic-queued-spawn-{machine}"));
        let mut configuration = options();
        configuration.environment.insert(
            "ACYCLIC_TEST_MACHINE_MARKER".into(),
            marker.to_str().unwrap().into(),
        );
        let backend = LocalBackend {
            machine: LocalMachine::new(machine, configuration)?,
            generation: std::sync::Mutex::new(Generation::new()?),
        };
        let live = backend.generation.lock().unwrap().live.clone();
        let local = AdmittedMachine::open(
            backend,
            enrollment(machine),
            Arc::new(acyclic_stream::MemoryStream::default()),
            StreamPath::new(format!("machine-tests/{machine}"))?,
        )
        .await?;
        let command = ExecCommand::new(
            vec![
                std::env::current_exe()?.to_str().unwrap().into(),
                "--exact".into(),
                "local::tests::child".into(),
                "--nocapture".into(),
            ],
            5_000,
        )?;
        let (schedule, mut boundaries) = acyclic_native_runtime::testing::Schedule::new();
        let mut submitted = Box::pin(schedule.scope(local.exec(machine, command)));
        let queued = tokio::select! {
            result = &mut submitted => panic!("spawn completed before its original native submission was released: {}", result.is_ok()),
            boundary = boundaries.recv() => boundary.unwrap(),
        };
        assert_eq!(queued.name, "native:submitted");
        assert!(live.clone().try_write_owned().is_err());
        let key = IdempotencyKey::new();
        if expire {
            tokio::time::advance(Duration::from_secs(5)).await;
            assert!(
                matches!(submitted.await, Err(ProviderError::CommandIndeterminate(id)) if id == machine)
            );
            assert!(
                matches!(local.suspend(machine, key).await, Err(ProviderError::Indeterminate(original)) if original == key)
            );
        } else {
            // The original command's stop event must run even while its spawn
            // is queued. Holding the observation lock makes this join hang.
            let (stopped, command) = futures::join!(local.suspend(machine, key), submitted);
            assert!(
                matches!(stopped, Err(ProviderError::Indeterminate(original)) if original == key)
            );
            let mut command = command?;
            assert!(
                matches!(command.next().await, Some(Err(ProviderError::CommandIndeterminate(id))) if id == machine)
            );
            assert!(command.next().await.is_none());
        }
        assert!(live.clone().try_write_owned().is_err());
        queued.release();
        // Drain that same real worker, including its completion publication.
        // Observation expiry did not cancel or substitute its physical join.
        let drain = schedule.drained();
        tokio::pin!(drain);
        loop {
            tokio::select! {
                biased;
                () = &mut drain => break,
                boundary = boundaries.recv() => boundary.unwrap().release(),
            }
        }
        let _joined = live.write_owned().await;
        assert!(
            !marker.exists(),
            "expired/stopped queued work must never spawn its child"
        );
    }
    Ok(())
}

#[tokio::test]
async fn failed_terminal_publication_preserves_pending_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let machine = MachineId::new();
    let backend = LocalBackend {
        machine: LocalMachine::new(machine, options())?,
        generation: std::sync::Mutex::new(Generation::new()?),
    };
    let mut physical = backend.execution()?;
    let streams = Arc::new(acyclic_stream::MemoryStream::default());
    let root = StreamPath::new(format!("machine-tests/{machine}"))?;
    let path = StreamPath::new(format!("{root}/machines/{machine}"))?;
    let local = AdmittedMachine::open(backend, enrollment(machine), streams.clone(), root).await?;
    let key = IdempotencyKey::new();
    let mut stopping = Box::pin(local.suspend(machine, key));
    assert!(futures::poll!(&mut stopping).is_pending());
    let head = streams.head(path.clone()).await?;
    let record = head.last.unwrap();
    // Invalidate the original publication condition without changing its
    // pending state. The real Streams authority must reject settlement.
    assert!(matches!(
        streams
            .commit(CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: acyclic_stream::IdempotencyKey::new(
                    machine.to_string().into_bytes()
                )?,
                conditions: vec![CommitCondition::Head {
                    path: path.clone(),
                    expected: head.tail,
                    commit_id: record.commit_id
                }],
                mutations: vec![CommitMutation::Append {
                    path: path.clone(),
                    records: vec![AppendRecord {
                        value: record.value,
                        headers: record.headers
                    }]
                }],
            })
            .await?,
        CommitOutcome::Committed(_)
    ));
    physical.retired(true);
    assert!(
        matches!(stopping.await, Err(ProviderError::Indeterminate(original)) if original == key)
    );
    assert_eq!(
        local.inspect_machine(machine).await?.state,
        MachineState::Suspending
    );
    let retained = streams.head(path.clone()).await?;
    let wake = IdempotencyKey::new();
    assert!(
        matches!(local.wake(machine, wake).await, Err(ProviderError::Indeterminate(original)) if original == wake)
    );
    assert_eq!(streams.head(path).await?, retained);
    assert!(
        matches!(local.recover(key).await, Err(ProviderError::Indeterminate(original)) if original == key)
    );
    Ok(())
}

#[tokio::test]
async fn pending_watch_follows_original_publication_and_ends_on_producer_loss()
-> Result<(), Box<dyn std::error::Error>> {
    for complete in [true, false] {
        let machine = MachineId::new();
        let backend = LocalBackend {
            machine: LocalMachine::new(machine, options())?,
            generation: std::sync::Mutex::new(Generation::new()?),
        };
        // Retain the actual native execution lease that stop must join. No
        // fake completion source, timer or provider implementation participates.
        let mut physical = backend.execution()?;
        let local = AdmittedMachine::open(
            backend,
            enrollment(machine),
            Arc::new(acyclic_stream::MemoryStream::default()),
            StreamPath::new(format!("machine-tests/{machine}"))?,
        )
        .await?;
        let key = IdempotencyKey::new();
        let mut stopping = Box::pin(local.suspend(machine, key));
        // One admission turn reaches the real held lease. Subsequent progress
        // comes only from its release or the original producer's destruction.
        assert!(futures::poll!(&mut stopping).is_pending());
        let operation = local.recover_operation(key).await?;
        let mut watched = local.watch_operation(operation).await?;
        assert_eq!(
            watched.next().await.unwrap()?.phase,
            OperationPhase::Pending
        );
        assert!(watched.next().now_or_never().is_none());
        if complete {
            physical.retired(true);
            let (settled, observed) = futures::join!(stopping, watched.next());
            assert_eq!(settled?, MutationOutcome::Suspended(machine));
            assert_eq!(observed.unwrap()?.phase, OperationPhase::Succeeded);
            assert!(watched.next().await.is_none());
        } else {
            // An abandoned accepted mutation does not supply a terminal event.
            // Its original loss signal must end this existing watch promptly.
            drop(stopping);
            assert!(matches!(watched.next().await,
                Some(Err(ProviderError::Indeterminate(original))) if original == key));
            assert!(watched.next().await.is_none());
            assert!(matches!(local.watch_operation(operation).await,
                Err(ProviderError::Indeterminate(original)) if original == key));
            assert!(matches!(local.recover(key).await,
                Err(ProviderError::Indeterminate(original)) if original == key));
            physical.retired(true);
        }
    }
    Ok(())
}

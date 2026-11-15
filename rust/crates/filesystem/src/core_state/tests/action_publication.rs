use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const CHILD: &str = "core_state::tests::action_publication::action_write_child";
const OPERATION: OperationId = OperationId::from_bytes([0x99; 16]);

#[tokio::test]
async fn cancelled_directory_reset_retains_original_journal_until_native_completion() -> TestResult
{
    use crate::materializer::{JournaledMaterializer, MaterializationBackend};
    use acyclic_native_runtime::testing::Schedule;

    crate::initialize_test_native_io();
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("checkout");
    let operation = directory.path().join("operation");
    std::fs::create_dir(&root)?;
    std::fs::write(root.join("authored"), b"keep")?;
    std::fs::create_dir_all(operation.join("target/stale"))?;
    std::fs::create_dir(operation.join("backup"))?;
    std::fs::write(operation.join("target/stale/file"), b"partial staging")?;
    let cancellation = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(native::finite_budget(), &cancellation)
        .map_err(|(error, _)| error)?;
    let (root, mut allocation) = work.path(&root, None)?;
    let (operation, mut operation_allocation) = work.path(&operation, None)?;
    allocation.absorb(&mut operation_allocation)?;
    let admission = crate::NativeTreeMaterializationBackend::admission().await?;
    let backend = crate::NativeTreeMaterializationBackend::new(
        root.clone(),
        operation,
        allocation,
        work.clone(),
        admission,
    )?;
    let store = LocalCoreStateStore::new(directory.path().join("state")).materialization(work);
    let mut journal = materialization_journal(OPERATION);
    journal.root_identity = backend.allocation_identity();
    journal.preparation_identity = backend.preparation_identity();
    journal.phase = crate::MaterializationPhase::Preparing;
    journal
        .plan
        .edits
        .push(crate::MaterializationEdit::Install {
            path: "file.txt".into(),
            image: vec![1],
        });
    assert!(
        MaterializationJournalStore::compare_and_swap(&store, OPERATION, 0, journal)
            .await?
            .0
    );
    let journal = MaterializationJournalStore::load(&store, OPERATION)
        .await?
        .ok_or("missing original journal")?;
    let revision = journal.revision;
    let lock_path = {
        let namespace = store.namespace()?;
        namespace
            .record(MATERIALIZATION_FAMILY, &OPERATION.into_bytes())?
            .lock
    };
    let materializer = JournaledMaterializer::new(store, backend);
    let (schedule, mut events) = Schedule::seeded(73);
    let mut original = Box::pin(schedule.scope(materializer.reset_preparation_target(journal)));
    let mut resetting = false;
    let submitted = loop {
        tokio::select! {
            result = &mut original => panic!("reset completed before its accepted effect: {result:?}"),
            event = events.recv() => {
                let event = event.ok_or("lost native boundary")?;
                if event.name == "materialization:reset" {
                    resetting = true;
                } else if resetting && event.name == "native:submitted" {
                    break event;
                }
                event.release();
            }
        }
    };
    // Lose the handler's observation while its original native effect is
    // accepted but has not run. Redelivery must not acquire the same record.
    drop(original);
    let lock = OpenOptions::new().read(true).write(true).open(&lock_path)?;
    let replay = FileLock::try_acquire(lock, FileLockMode::Exclusive);
    let excluded = matches!(&replay, Err(error) if error.kind() == std::io::ErrorKind::WouldBlock);
    drop(replay);
    submitted.release();
    loop {
        tokio::select! {
            () = schedule.drained() => break,
            event = events.recv() => event.ok_or("lost native completion")?.release(),
        }
    }
    assert!(
        excluded,
        "redelivery acquired the journal before the original effect completed"
    );
    let journal = MaterializationJournalStore::load(materializer.store(), OPERATION)
        .await?
        .ok_or("lost original journal")?;
    assert_eq!(
        journal.revision, revision,
        "cancelled observer acknowledged the effect"
    );
    let (_, completed) = materializer.reset_preparation_target(journal).await?;
    assert_eq!(completed.revision, revision + 1);
    assert_eq!(std::fs::read(root.join("authored"))?, b"keep");
    assert_eq!(
        std::fs::read_dir(
            materializer
                .backend()
                .operation_directory_path()
                .join("target")
        )?
        .count(),
        0
    );
    Ok(())
}

async fn seed(root: &Path) -> TestResult {
    std::fs::create_dir_all(root)?;
    let cancellation = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(native::finite_budget(), &cancellation)
        .map_err(|(error, _)| error)?;
    let held = crate::native_host::HostRoot::open(root, &mut &work)?;
    let mut journal = materialization_journal(OPERATION);
    journal.root_identity = Some(held.identity().to_bytes());
    journal.phase = crate::MaterializationPhase::Preparing;
    journal
        .plan
        .edits
        .push(crate::MaterializationEdit::Install {
            path: "file.txt".into(),
            image: vec![1],
        });
    assert!(journal.is_valid_for(OPERATION));
    let store = LocalCoreStateStore::new(root.join("state"));
    assert!(
        MaterializationJournalStore::compare_and_swap(&store, OPERATION, 0, journal)
            .await?
            .0
    );
    Ok(())
}

fn spawn_write(root: &Path, cut: u64) -> std::io::Result<std::process::Child> {
    std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", CHILD, "--nocapture"])
        .env("ACYCLIC_ACTION_CRASH_ROOT", root)
        .env("ACYCLIC_ACTION_CRASH_CUT", cut.to_string())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
}

#[tokio::test]
async fn every_prepared_action_write_recovers_and_retries() -> TestResult {
    crate::initialize_test_native_io();
    let directory = tempfile::tempdir()?;
    let baseline = directory.path().join("baseline");
    seed(&baseline).await?;
    let output = spawn_write(&baseline, 0)?.wait_with_output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let writes: u64 = std::fs::read_to_string(baseline.join("writes"))?.parse()?;
    assert!(
        writes >= 4,
        "temporary creation, image write, flush and replacement must be covered"
    );
    let jobs = (1..=writes + 1)
        .map(|cut| {
            let root = directory.path().join(cut.to_string());
            move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime
                    .block_on(async {
                        seed(&root).await?;
                        let output = spawn_write(&root, cut)?.wait_with_output()?;
                        assert_eq!(
                            output.status.code(),
                            Some(if cut <= writes { 86 } else { 0 }),
                            "cut {cut}: {}\n{}",
                            String::from_utf8_lossy(&output.stdout),
                            String::from_utf8_lossy(&output.stderr)
                        );
                        let cancellation = crate::CancellationToken::new();
                        let work = crate::NativeMaterializationWork::new(
                            native::finite_budget(),
                            &cancellation,
                        )
                        .map_err(|(error, _)| error)?;
                        let store =
                            LocalCoreStateStore::new(root.join("state")).materialization(work);
                        let recovered = MaterializationJournalStore::load(&store, OPERATION)
                            .await?
                            .ok_or("lost acknowledged journal")?;
                        assert!(matches!(recovered.revision, 1 | 2));
                        assert!(recovered.is_valid_for(OPERATION));
                        let revision = recovered.revision;
                        let retried = store.prepare_action(recovered).await?.await?;
                        assert_eq!(retried.revision, revision + 1);
                        assert_eq!(
                            MaterializationJournalStore::load(&store, OPERATION).await?,
                            Some(retried)
                        );
                        Ok::<_, Box<dyn std::error::Error>>(())
                    })
                    .unwrap();
                usize::try_from(cut).unwrap()
            }
        })
        .collect();
    let completed = crate::test_support::finish_physical_cuts(jobs);
    assert_eq!(
        completed,
        (1..=writes + 1)
            .map(|cut| usize::try_from(cut).unwrap())
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn action_write_child() -> TestResult {
    let Some(root) = std::env::var_os("ACYCLIC_ACTION_CRASH_ROOT") else {
        return Ok(());
    };
    crate::initialize_test_native_io();
    let root = PathBuf::from(root);
    let cut: u64 = std::env::var("ACYCLIC_ACTION_CRASH_CUT")?.parse()?;
    let cancellation = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(native::finite_budget(), &cancellation)
        .map_err(|(error, _)| error)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let store = LocalCoreStateStore::new(root.join("state")).materialization(work.clone());
        let journal = MaterializationJournalStore::load(&store, OPERATION)
            .await?
            .ok_or("missing journal")?;
        work.state.lock().write_probe = Some(crate::demand::WriteProbe {
            writes: 0,
            stop_before: (cut != 0).then_some(cut),
        });
        let completion = store.prepare_action(journal).await?;
        completion.await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    let writes = work
        .state
        .lock()
        .write_probe
        .take()
        .ok_or("missing write probe")?
        .writes;
    std::fs::write(root.join("writes"), writes.to_string())?;
    // Include the completed-publication crash: never unwind the accepted action.
    std::process::exit(0);
}

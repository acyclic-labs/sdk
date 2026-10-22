use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const CHILD: &str = "core_state::tests::action_publication::action_write_child";
const OPERATION: OperationId = OperationId::from_bytes([0x99; 16]);

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
    for cut in 1..=writes + 1 {
        let root = directory.path().join(cut.to_string());
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
        let work = crate::NativeMaterializationWork::new(native::finite_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        let store = LocalCoreStateStore::new(root.join("state")).materialization(work);
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
    }
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

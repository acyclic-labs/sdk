use super::*;

#[test]
fn publishes_fresh_and_existing_records_with_one_image_write()
-> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_test_native_io();
    for existing in [false, true] {
        let directory = tempfile::tempdir()?;
        let store = LocalCoreStateStore::new(directory.path());
        if existing {
            let namespace = store.namespace()?;
            let mut paths = namespace.record("publication-one-image", &[0x96; 16])?;
            with_lock(&mut paths, |paths| write_journaled(paths, &7_u64))?;
        }
        let value = 42_u64;
        let length = serde_json::to_vec(&value)?.len() as u64;
        let budget = crate::WorkBudget {
            object_bytes_written: length,
            ..crate::performance::work_budget(100_000_000, 100_000_000)
        };
        let cancellation = crate::CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let store = store.materialization(work.clone());
        let namespace = store.namespace()?;
        let mut paths = namespace.record("publication-one-image", &[0x96; 16])?;
        with_lock(&mut paths, |paths| write_journaled(paths, &value))?;
        assert_eq!(work.receipt().object_bytes_written, length);
        let recovered = read_recoverable::<u64>(&paths)?.ok_or("publication disappeared")?;
        assert_eq!(recovered.0, value);
        assert!(!paths.temporary.exists());
        assert!(!paths.previous.exists());
        work.receipt().verify(budget)?;
    }
    Ok(())
}

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const CHILD: &str = "core_state::records::windows_publication_tests::record_write_child";

fn spawn_write(root: &Path, cut: u64) -> std::io::Result<std::process::Child> {
    std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", CHILD, "--nocapture"])
        .env("ACYCLIC_RECORD_CRASH_ROOT", root)
        .env("ACYCLIC_RECORD_CRASH_CUT", cut.to_string())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
}

fn seed(root: &Path, existing: bool) -> TestResult {
    let store = LocalCoreStateStore::new(root);
    let namespace = store.namespace()?;
    let mut paths = namespace.record("publication-cuts", &[0x97; 16])?;
    with_lock(&mut paths, |paths| {
        if existing {
            write_journaled(paths, &7_u64)?;
        }
        Ok(())
    })?;
    Ok(())
}

fn recover_and_retry(root: &Path, existing: bool) -> TestResult {
    let cancellation = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(
        crate::performance::work_budget(100_000_000, 100_000_000),
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let store = LocalCoreStateStore::new(root).materialization(work);
    let namespace = store.namespace()?;
    let mut paths = namespace.record("publication-cuts", &[0x97; 16])?;
    with_lock(&mut paths, |paths| {
        let value = read_recoverable::<u64>(paths)?.map(|(value, _)| value);
        if existing {
            assert!(
                matches!(value, Some(7 | 42)),
                "lost acknowledged old record: {value:?}"
            );
        } else {
            assert!(
                matches!(value, None | Some(42)),
                "torn fresh record: {value:?}"
            );
        }
        write_journaled(paths, &42_u64)?;
        assert_eq!(
            read_recoverable::<u64>(paths)?.map(|(value, _)| value),
            Some(42)
        );
        Ok(())
    })?;
    Ok(())
}

#[test]
fn every_physical_record_write_recovers_and_retries() -> TestResult {
    crate::initialize_test_native_io();
    let directory = tempfile::tempdir()?;
    for existing in [false, true] {
        let baseline = directory.path().join(format!("baseline-{existing}"));
        seed(&baseline, existing)?;
        let output = spawn_write(&baseline, 0)?.wait_with_output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let writes: u64 = std::fs::read_to_string(baseline.join("writes"))?.parse()?;
        assert!(
            writes > 4,
            "write and durability boundaries were not exercised"
        );
        for cut in 1..=writes + 1 {
            let root = directory.path().join(format!("{existing}-{cut}"));
            seed(&root, existing)?;
            let output = spawn_write(&root, cut)?.wait_with_output()?;
            assert_eq!(
                output.status.code(),
                Some(if cut <= writes { 86 } else { 0 }),
                "cut {cut}: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            recover_and_retry(&root, existing)?;
        }
    }
    Ok(())
}

#[test]
fn record_write_child() -> TestResult {
    let Some(root) = std::env::var_os("ACYCLIC_RECORD_CRASH_ROOT") else {
        return Ok(());
    };
    crate::initialize_test_native_io();
    let root = PathBuf::from(root);
    let cut: u64 = std::env::var("ACYCLIC_RECORD_CRASH_CUT")?.parse()?;
    let cancellation = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(
        crate::performance::work_budget(100_000_000, 100_000_000),
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let store = LocalCoreStateStore::new(&root).materialization(work.clone());
    let namespace = store.namespace()?;
    let mut paths = namespace.record("publication-cuts", &[0x97; 16])?;
    work.state.lock().write_probe = Some(crate::demand::WriteProbe {
        writes: 0,
        stop_before: (cut != 0).then_some(cut),
    });
    with_lock(&mut paths, |paths| write_journaled(paths, &42_u64))?;
    let writes = work
        .state
        .lock()
        .write_probe
        .take()
        .ok_or("missing write probe")?
        .writes;
    std::fs::write(root.join("writes"), writes.to_string())?;
    // Do not unwind: this also covers the completed-publication cut.
    std::process::exit(0);
}

#[test]
fn ordinary_record_faults_recover_and_retry() -> TestResult {
    use acyclic_native_runtime::testing::{DurableFault, Schedule};
    crate::initialize_test_native_io();
    let directory = tempfile::tempdir()?;
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    for existing in [false, true] {
        let baseline = directory.path().join(format!("ordinary-{existing}"));
        seed(&baseline, existing)?;
        let store = LocalCoreStateStore::new(&baseline);
        let namespace = store.namespace()?;
        let mut paths = namespace.record("publication-cuts", &[0x97; 16])?;
        let (schedule, _) = Schedule::seeded(97);
        runtime.block_on(
            schedule
                .scope(async { with_lock(&mut paths, |paths| write_journaled(paths, &42_u64)) }),
        )?;
        let count = schedule.durable_writes().len();
        assert!(
            count >= 4,
            "write, file flush, rename and directory flush must be traced"
        );
        for cut in 0..count {
            for fault in [
                DurableFault::Before,
                DurableFault::After,
                DurableFault::Failed,
                DurableFault::DiskFull,
                DurableFault::Torn(1),
            ] {
                let root = directory
                    .path()
                    .join(format!("ordinary-{existing}-{cut}-{fault:?}"));
                seed(&root, existing)?;
                let store = LocalCoreStateStore::new(&root);
                let namespace = store.namespace()?;
                let mut paths = namespace.record("publication-cuts", &[0x97; 16])?;
                let (schedule, _) = Schedule::seeded(97);
                schedule.cut_at(cut, fault);
                let result = runtime.block_on(schedule.scope(async {
                    with_lock(&mut paths, |paths| write_journaled(paths, &42_u64))
                }));
                assert!(
                    result.is_err(),
                    "fault was not reached: {existing} {cut} {fault:?}"
                );
                let injected = schedule
                    .storage_fault()
                    .ok_or("missing original injected fault")?;
                assert_eq!((injected.ordinal, injected.fault), (cut, fault));
                recover_and_retry(&root, existing)?;
            }
        }
    }
    Ok(())
}

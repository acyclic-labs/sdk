use super::*;
use std::process::{Child, Command, Stdio};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const CHILD: &str = "materializer::directory_preparation::crash_tests::publication_write_child";
const OPERATION: OperationId = OperationId::from_bytes([0xa7; 16]);

fn spawn(root: &Path, cut: Option<u64>) -> std::io::Result<Child> {
    Command::new(std::env::current_exe()?)
        .args(["--exact", CHILD, "--nocapture"])
        .env("ACYCLIC_PUBLICATION_CRASH_ROOT", root)
        .env(
            "ACYCLIC_PUBLICATION_CRASH_CUT",
            cut.unwrap_or(0).to_string(),
        )
        .env_remove("RUST_MIN_STACK")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
}

fn succeeds(child: Child) -> TestResult {
    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(format!(
            "publication child failed: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(())
}

acyclic_test_support::partitioned_tests! {
    #[test] fn -> TestResult;
    WRITE_PARTITIONS, 8, publication_writes;
    0 => every_native_publication_write_recovers_without_stale_capture,
    1 => every_native_publication_write_recovers_without_stale_capture_part_1,
    2 => every_native_publication_write_recovers_without_stale_capture_part_2,
    3 => every_native_publication_write_recovers_without_stale_capture_part_3,
    4 => every_native_publication_write_recovers_without_stale_capture_part_4,
    5 => every_native_publication_write_recovers_without_stale_capture_part_5,
    6 => every_native_publication_write_recovers_without_stale_capture_part_6,
    7 => every_native_publication_write_recovers_without_stale_capture_part_7,
}

fn publication_writes(selected: Vec<usize>) -> TestResult {
    let [partition]: [usize; 1] = selected
        .try_into()
        .map_err(|_| "one registered publication partition must be selected")?;
    let root = tempfile::tempdir()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let seed = root.path().join("seed");
    runtime.block_on(publish(&seed, None, true))?;
    assert_eq!(
        std::fs::read(seed.join("checkout/file"))?,
        b"original generation",
        "setup must not perform the measured transition"
    );
    assert!(!seed.join("checkout/added").exists());
    let seed = acyclic_native_runtime::testing::snapshot::Snapshot::capture(&seed)?;
    let baseline = root.path().join("baseline");
    seed.restore(&baseline)?;
    let writes = runtime.block_on(publish(&baseline, None, false))?;
    assert!(
        writes > 3,
        "the probe did not cover journal, mkdir, and publication writes"
    );
    // The shared checked registry assigns every measured write and sync,
    // including the initial cut and completed publication, exactly once.
    // Repeating those witnesses in every partition adds no recovery coverage.
    // Each cut and recovery still owns its original child process.
    let total = usize::try_from(writes.checked_add(1).ok_or("write count overflow")?)?;
    let assigned =
        acyclic_test_support::partitions::indices(total, WRITE_PARTITIONS, partition, |_| false)?;
    // A cut spends its time waiting on its own children, so independent cuts
    // share the bounded supervisors instead of occupying the caller one at a
    // time. Every cut still restores its own fixture and owns both processes.
    let jobs = assigned
        .iter()
        .map(|&index| {
            let seed = &seed;
            let root = &root;
            move || {
                let cut = u64::try_from(index).unwrap() + 1;
                let path = root.path().join(cut.to_string());
                // Only completed generation setup is shared as immutable bytes.
                // Every transition and recovery still runs against independent
                // files with the original physical write/sync probe and durable
                // semantics.
                seed.restore(&path).unwrap();
                let output = spawn(&path, Some(cut)).unwrap().wait_with_output().unwrap();
                assert_eq!(
                    output.status.code(),
                    Some(if cut <= writes { 86 } else { 0 }),
                    "cut {cut}: {}\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                succeeds(spawn(&path, None).unwrap())
                    .unwrap_or_else(|error| panic!("recovery cut {cut}: {error}"));
                assert_eq!(
                    std::fs::read(path.join("checkout/file")).unwrap(),
                    b"rebased generation"
                );
                assert_eq!(
                    std::fs::read(path.join("checkout/added")).unwrap(),
                    b"new file"
                );
                index
            }
        })
        .collect();
    assert_eq!(crate::test_support::finish_physical_cuts(jobs), assigned);
    Ok(())
}

#[test]
fn publication_write_child() -> TestResult {
    let Some(root) = std::env::var_os("ACYCLIC_PUBLICATION_CRASH_ROOT") else {
        return Ok(());
    };
    let root = PathBuf::from(root);
    let cut: u64 = std::env::var("ACYCLIC_PUBLICATION_CRASH_CUT")?.parse()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime
        .block_on(publish(&root, (cut != 0).then_some(cut), false))
        .map(|_| ())
}

async fn publish(root: &Path, cut: Option<u64>, prepare: bool) -> TestResult<u64> {
    let cancellation = crate::CancellationToken::new();
    let fs = crate::Fs::local(crate::LocalOptions::new(root.join("fs"))).await?;
    let manifest = root.join("generations");
    let workspace = if manifest.exists() {
        fs.open_workspace("publication", crate::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value
    } else {
        assert!(prepare, "a publication cut must restore its captured seed");
        let workspace = fs
            .create_workspace("publication", crate::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value;
        workspace.write_text("/file", "original generation").await?;
        let from = workspace
            .head(crate::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value
            .id();
        workspace.write_text("/file", "rebased generation").await?;
        workspace.write_text("/added", "new file").await?;
        let to = workspace
            .head(crate::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value
            .id();
        std::fs::create_dir(root.join("checkout"))?;
        std::fs::write(root.join("checkout/file"), "original generation")?;
        let mut ids = [0; 64];
        ids[..32].copy_from_slice(from.digest().as_bytes());
        ids[32..].copy_from_slice(to.digest().as_bytes());
        std::fs::write(&manifest, ids)?;
        workspace
    };
    if prepare {
        return Ok(0);
    }
    let ids = std::fs::read(&manifest)?;
    let from = GenerationId::new(crate::Digest::from_bytes(ids[..32].try_into()?));
    let to = GenerationId::new(crate::Digest::from_bytes(ids[32..].try_into()?));
    let from = workspace
        .generation(from, crate::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|failure| failure.error)?
        .value;
    let to = workspace
        .generation(to, crate::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|failure| failure.error)?
        .value;
    let state = crate::LocalCoreStateStore::new(root.join("state"));
    let work = NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let checkout = root.join("checkout");
    let operation = root.join("staging/operation");
    let identity = work.root_identity(&checkout)?;
    work.state.lock().write_probe = Some(crate::ledger::WriteProbe {
        writes: 0,
        stop_before: cut,
    });
    let result = publish_native_generation_transition(
        &from,
        &to,
        &state,
        NativeWorkspacePublication {
            root: &checkout,
            source_root_identity: identity,
            operation_directory: &operation,
            operation_id: OPERATION,
            from: from.id(),
            to: to.id(),
            excluded_names: &[],
            options: &crate::MaterializeOptions::native(operation.join("target")),
            budget: crate::WorkBudget::UNBOUNDED,
            cancellation: &cancellation,
        },
        work.clone(),
        || std::future::ready(Ok(())),
    )
    .await
    .map_err(|failure| failure.error)?;
    let writes = work
        .state
        .lock()
        .write_probe
        .as_ref()
        .ok_or("missing write probe")?
        .writes;
    work.state.lock().write_probe = None;
    assert_eq!(result.value.phase, MaterializationPhase::Applied);
    assert_eq!(std::fs::read(checkout.join("file"))?, b"rebased generation");
    assert_eq!(std::fs::read(checkout.join("added"))?, b"new file");
    assert_eq!(
        workspace
            .head(crate::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value
            .id(),
        to.id(),
        "recovery changed the logical head"
    );
    Ok(writes)
}

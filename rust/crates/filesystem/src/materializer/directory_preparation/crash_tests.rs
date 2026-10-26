use super::*;
use std::process::{Child, Command, Stdio};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const CHILD: &str = "materializer::directory_preparation::crash_tests::publication_write_child";
const OPERATION: OperationId = OperationId::from_bytes([0xa7; 16]);

fn spawn(root: &Path, cut: Option<u64>, prepare: bool) -> std::io::Result<Child> {
    Command::new(std::env::current_exe()?)
        .args(["--exact", CHILD, "--nocapture"])
        .env("ACYCLIC_PUBLICATION_CRASH_ROOT", root)
        .env(
            "ACYCLIC_PUBLICATION_CRASH_CUT",
            cut.unwrap_or(0).to_string(),
        )
        .env(
            "ACYCLIC_PUBLICATION_PREPARE_ONLY",
            u8::from(prepare).to_string(),
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
    WRITE_PARTITIONS, 64, publication_writes;
    0 => every_native_publication_write_recovers_without_stale_capture,
    1 => every_native_publication_write_recovers_without_stale_capture_part_1,
    2 => every_native_publication_write_recovers_without_stale_capture_part_2,
    3 => every_native_publication_write_recovers_without_stale_capture_part_3,
    4 => every_native_publication_write_recovers_without_stale_capture_part_4,
    5 => every_native_publication_write_recovers_without_stale_capture_part_5,
    6 => every_native_publication_write_recovers_without_stale_capture_part_6,
    7 => every_native_publication_write_recovers_without_stale_capture_part_7,
    8 => every_native_publication_write_recovers_without_stale_capture_part_8,
    9 => every_native_publication_write_recovers_without_stale_capture_part_9,
    10 => every_native_publication_write_recovers_without_stale_capture_part_10,
    11 => every_native_publication_write_recovers_without_stale_capture_part_11,
    12 => every_native_publication_write_recovers_without_stale_capture_part_12,
    13 => every_native_publication_write_recovers_without_stale_capture_part_13,
    14 => every_native_publication_write_recovers_without_stale_capture_part_14,
    15 => every_native_publication_write_recovers_without_stale_capture_part_15,
    16 => every_native_publication_write_recovers_without_stale_capture_part_16,
    17 => every_native_publication_write_recovers_without_stale_capture_part_17,
    18 => every_native_publication_write_recovers_without_stale_capture_part_18,
    19 => every_native_publication_write_recovers_without_stale_capture_part_19,
    20 => every_native_publication_write_recovers_without_stale_capture_part_20,
    21 => every_native_publication_write_recovers_without_stale_capture_part_21,
    22 => every_native_publication_write_recovers_without_stale_capture_part_22,
    23 => every_native_publication_write_recovers_without_stale_capture_part_23,
    24 => every_native_publication_write_recovers_without_stale_capture_part_24,
    25 => every_native_publication_write_recovers_without_stale_capture_part_25,
    26 => every_native_publication_write_recovers_without_stale_capture_part_26,
    27 => every_native_publication_write_recovers_without_stale_capture_part_27,
    28 => every_native_publication_write_recovers_without_stale_capture_part_28,
    29 => every_native_publication_write_recovers_without_stale_capture_part_29,
    30 => every_native_publication_write_recovers_without_stale_capture_part_30,
    31 => every_native_publication_write_recovers_without_stale_capture_part_31,
    32 => every_native_publication_write_recovers_without_stale_capture_part_32,
    33 => every_native_publication_write_recovers_without_stale_capture_part_33,
    34 => every_native_publication_write_recovers_without_stale_capture_part_34,
    35 => every_native_publication_write_recovers_without_stale_capture_part_35,
    36 => every_native_publication_write_recovers_without_stale_capture_part_36,
    37 => every_native_publication_write_recovers_without_stale_capture_part_37,
    38 => every_native_publication_write_recovers_without_stale_capture_part_38,
    39 => every_native_publication_write_recovers_without_stale_capture_part_39,
    40 => every_native_publication_write_recovers_without_stale_capture_part_40,
    41 => every_native_publication_write_recovers_without_stale_capture_part_41,
    42 => every_native_publication_write_recovers_without_stale_capture_part_42,
    43 => every_native_publication_write_recovers_without_stale_capture_part_43,
    44 => every_native_publication_write_recovers_without_stale_capture_part_44,
    45 => every_native_publication_write_recovers_without_stale_capture_part_45,
    46 => every_native_publication_write_recovers_without_stale_capture_part_46,
    47 => every_native_publication_write_recovers_without_stale_capture_part_47,
    48 => every_native_publication_write_recovers_without_stale_capture_part_48,
    49 => every_native_publication_write_recovers_without_stale_capture_part_49,
    50 => every_native_publication_write_recovers_without_stale_capture_part_50,
    51 => every_native_publication_write_recovers_without_stale_capture_part_51,
    52 => every_native_publication_write_recovers_without_stale_capture_part_52,
    53 => every_native_publication_write_recovers_without_stale_capture_part_53,
    54 => every_native_publication_write_recovers_without_stale_capture_part_54,
    55 => every_native_publication_write_recovers_without_stale_capture_part_55,
    56 => every_native_publication_write_recovers_without_stale_capture_part_56,
    57 => every_native_publication_write_recovers_without_stale_capture_part_57,
    58 => every_native_publication_write_recovers_without_stale_capture_part_58,
    59 => every_native_publication_write_recovers_without_stale_capture_part_59,
    60 => every_native_publication_write_recovers_without_stale_capture_part_60,
    61 => every_native_publication_write_recovers_without_stale_capture_part_61,
    62 => every_native_publication_write_recovers_without_stale_capture_part_62,
    63 => every_native_publication_write_recovers_without_stale_capture_part_63,
}

fn publication_writes(selected: Vec<usize>) -> TestResult {
    let [partition]: [usize; 1] = selected
        .try_into()
        .map_err(|_| "one registered publication partition must be selected")?;
    let root = tempfile::tempdir()?;
    let seed = root.path().join("seed");
    succeeds(spawn(&seed, None, true)?)?;
    assert_eq!(
        std::fs::read(seed.join("checkout/file"))?,
        b"original generation",
        "setup must not perform the measured transition"
    );
    assert!(!seed.join("checkout/added").exists());
    let seed = acyclic_native_runtime::testing::snapshot::Snapshot::capture(&seed)?;
    let baseline = root.path().join("baseline");
    seed.restore(&baseline)?;
    succeeds(spawn(&baseline, None, false)?)?;
    let writes: u64 = std::fs::read_to_string(baseline.join("writes"))?.parse()?;
    assert!(
        writes > 3,
        "the probe did not cover journal, mkdir, and publication writes"
    );
    // The shared checked registry assigns every measured write and sync, not
    // a hand-written phase list. Each partition also proves the initial cut
    // and completed publication. One cut/recovery owns the child allowance;
    // neither wave can fan out processes, descriptors or filesystem flushes.
    let total = usize::try_from(writes.checked_add(1).ok_or("write count overflow")?)?;
    let assigned =
        acyclic_test_support::partitions::indices(total, WRITE_PARTITIONS, partition, |index| {
            index == 0 || index + 1 == total
        })?;
    // The measured transition has 299 crash positions. Four partitions still
    // admitted 76 durable cut/recovery pairs each. Keep the complete registry,
    // including both witnesses, while bounding one fixture to seven pairs.
    assert!(
        assigned.len() <= 7,
        "publication fixture exceeds its cut allowance"
    );
    eprintln!("publication partition {partition}: {assigned:?} of {total} measured cuts");
    for index in assigned {
        let started = std::time::Instant::now();
        let cut = u64::try_from(index)?.checked_add(1).ok_or("cut overflow")?;
        let path = root.path().join(cut.to_string());
        // Only generation setup is shared, captured after its writer exits.
        // Every transition and recovery still runs against independent files
        // with the original physical write/sync probe and durable semantics.
        seed.restore(&path)?;
        let output = spawn(&path, Some(cut), false)?.wait_with_output()?;
        assert_eq!(
            output.status.code(),
            Some(if cut <= writes { 86 } else { 0 }),
            "cut {cut}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        succeeds(spawn(&path, None, false)?)
            .map_err(|error| format!("recovery cut {cut}: {error}"))?;
        assert_eq!(
            std::fs::read(path.join("checkout/file"))?,
            b"rebased generation"
        );
        assert_eq!(std::fs::read(path.join("checkout/added"))?, b"new file");
        eprintln!("publication cut {cut} recovered in {:?}", started.elapsed());
    }
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
    runtime.block_on(publish(&root, (cut != 0).then_some(cut)))
}

async fn publish(root: &Path, cut: Option<u64>) -> TestResult {
    let cancellation = crate::CancellationToken::new();
    let fs = crate::Fs::local(crate::LocalOptions::new(root.join("fs"))).await?;
    let manifest = root.join("generations");
    let prepare = std::env::var("ACYCLIC_PUBLICATION_PREPARE_ONLY")? == "1";
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
        return Ok(());
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
    work.state.lock().write_probe = Some(crate::demand::WriteProbe {
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
    .map_err(|(error, _)| error)?;
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
    std::fs::write(root.join("writes"), writes.to_string())?;
    Ok(())
}

use super::*;
use std::process::{Child, Command, Stdio};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const CHILD: &str = "materializer::directory_preparation::crash_tests::publication_write_child";
const OPERATION: OperationId = OperationId::from_bytes([0xa7; 16]);

fn spawn(root: &Path, cut: Option<u64>, broken: bool) -> std::io::Result<Child> {
    Command::new(std::env::current_exe()?)
        .args(["--exact", CHILD, "--nocapture"])
        .env("ACYCLIC_PUBLICATION_CRASH_ROOT", root)
        .env(
            "ACYCLIC_PUBLICATION_CRASH_CUT",
            cut.unwrap_or(0).to_string(),
        )
        .env(
            "ACYCLIC_PUBLICATION_CRASH_BROKEN",
            if broken { "1" } else { "0" },
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

#[test]
fn every_native_publication_write_recovers_without_stale_capture() -> TestResult {
    let root = tempfile::tempdir()?;
    let baseline = root.path().join("baseline");
    succeeds(spawn(&baseline, None, false)?)?;
    let writes: u64 = std::fs::read_to_string(baseline.join("writes"))?.parse()?;
    assert!(
        writes > 3,
        "the probe did not cover journal, mkdir, and publication writes"
    );
    // Every dispatched write and sync is a cut; nothing maintains a parallel
    // hand-written list of phases. The last cut includes completed publication.
    let children = (1..=writes + 1)
        .map(|cut| {
            let path = root.path().join(cut.to_string());
            spawn(&path, Some(cut), false).map(|child| (cut, path, child))
        })
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut recovery = Vec::new();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut negative_control = false;
    for (cut, path, child) in children {
        let output = child.wait_with_output()?;
        assert_eq!(
            output.status.code(),
            Some(if cut <= writes { 86 } else { 0 }),
            "cut {cut}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if !negative_control {
            let store = crate::LocalCoreStateStore::new(path.join("state"));
            let journal = runtime.block_on(store.load(OPERATION))?;
            if journal.as_ref().is_some_and(|journal| {
                journal
                    .preparation_progress
                    .is_some_and(|progress| progress.pending)
            }) {
                let broken = spawn(&path, None, true)?.wait_with_output()?;
                assert!(
                    !broken.status.success(),
                    "broken pending recovery passed cut {cut}"
                );
                negative_control = true;
            }
        }
        recovery.push((cut, path.clone(), spawn(&path, None, false)?));
    }
    for (cut, path, child) in recovery {
        succeeds(child).map_err(|error| format!("recovery cut {cut}: {error}"))?;
        assert_eq!(
            std::fs::read(path.join("checkout/file"))?,
            b"rebased generation"
        );
        assert_eq!(std::fs::read(path.join("checkout/added"))?, b"new file");
    }
    assert!(
        negative_control,
        "no durable pending preparation was exercised"
    );
    Ok(())
}

#[test]
fn publication_write_child() -> TestResult {
    let Some(root) = std::env::var_os("ACYCLIC_PUBLICATION_CRASH_ROOT") else {
        return Ok(());
    };
    let root = PathBuf::from(root);
    let cut: u64 = std::env::var("ACYCLIC_PUBLICATION_CRASH_CUT")?.parse()?;
    let broken = std::env::var("ACYCLIC_PUBLICATION_CRASH_BROKEN")? == "1";
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(publish(&root, (cut != 0).then_some(cut), broken))
}

async fn publish(root: &Path, cut: Option<u64>, broken: bool) -> TestResult {
    let cancellation = crate::CancellationToken::new();
    let fs = crate::Fs::local(crate::LocalOptions::new(root.join("fs"))).await?;
    let manifest = root.join("generations");
    let workspace = if manifest.exists() {
        fs.open_workspace("publication", crate::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value
    } else {
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
    if broken
        && state.load(OPERATION).await?.is_some_and(|journal| {
            journal
                .preparation_progress
                .is_some_and(|progress| progress.pending)
        })
    {
        return Err(std::io::Error::other("negative control rejects pending preparation").into());
    }
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

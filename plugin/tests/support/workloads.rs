//! Local, real-mount toolchain qualification. This deliberately stays out of CI:
//! compiler caches, toolchain installation, and workstation load affect timing.

use super::{command, test_tempdir, try_output_with_timeout};
use acyclic_fs::kernel::FileMetadata;
use acyclic_fs::{
    Fs, IdempotencyKey, LocalOptions, MountOptions, MountPublication, TransactionCommit,
};
use bytes::Bytes;
use serde_json::json;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

const TOOL_TIMEOUT: Duration = Duration::from_secs(90);

#[tokio::test]
#[ignore = "local-only real mount, Cargo, and Lean/Lake performance qualification"]
async fn native_and_mounted_cargo_and_lake_workloads() {
    let native_first = match std::env::var("ACYCLIC_WORKLOAD_ORDER").as_deref() {
        Ok("mounted-first") => false,
        Ok("native-first") | Err(_) => true,
        Ok(other) => panic!("invalid workload order: {other}"),
    };
    let temporary = test_tempdir("toolchain-workloads-");
    let native = temporary.path().join("native");
    let mounted = temporary.path().join("mounted");
    fs::create_dir(&native).expect("native project directory");
    fs::create_dir(&mounted).expect("empty mount point");
    let files = workload_files();
    for (relative, contents) in &files {
        let destination = native.join(relative);
        fs::create_dir_all(destination.parent().expect("fixture parent"))
            .expect("native fixture directory");
        fs::write(destination, contents).expect("native fixture file");
    }

    let engine = Fs::local(LocalOptions::new(temporary.path().join("sdk-state")))
        .await
        .expect("local SDK engine");
    let workspace = engine
        .create_workspace("toolchain-workloads")
        .await
        .expect("SDK workload workspace");
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new())
        .await
        .expect("fixture transaction");
    transaction
        .create_dir_all("/src")
        .await
        .expect("fixture source directory");
    for (relative, contents) in files {
        transaction
            .create_file(
                &format!("/{relative}"),
                Bytes::from(contents),
                FileMetadata::default(),
            )
            .await
            .expect("fixture file in SDK workspace");
    }
    assert!(matches!(
        transaction.commit().await.expect("commit fixture"),
        TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_)
    ));
    let mount = workspace
        .mount(
            &mounted,
            MountOptions::read_write().publication(MountPublication::Manual),
        )
        .await
        .expect("real writable native mount");

    let qualification = run_workloads(&native, &mounted, native_first).await;
    let synchronized = mount.sync().await;
    let unmounted = mount.unmount().await;
    assert!(synchronized.is_ok(), "mount sync: {synchronized:?}");
    assert!(unmounted.is_ok(), "mount cleanup: {unmounted:?}");
    let timings = qualification.expect("native and mounted toolchain workflows");
    let receipt = json!({
        "schema": "acyclic-toolchain-workload-v1",
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "cargo_cold_order": if native_first { "native-first" } else { "mounted-first" },
        "lake_cold_order": if native_first { "mounted-first" } else { "native-first" },
        "cargo_test_ms": timings.cargo,
        "cargo_external_target_ms": timings.cargo_external_target,
        "lake_build_ms": timings.lake,
        "lean_check_ms": timings.lean_check,
    });
    if let Ok(path) = std::env::var("ACYCLIC_WORKLOAD_RECEIPT") {
        fs::write(
            path,
            serde_json::to_vec_pretty(&receipt).expect("receipt JSON"),
        )
        .expect("write requested workload receipt");
    }
    println!("{receipt}");
    for (name, native_ms, mounted_ms) in [
        ("Cargo cold", timings.cargo[0], timings.cargo[1]),
        ("Cargo warm", timings.cargo[2], timings.cargo[3]),
        (
            "Cargo external target",
            timings.cargo_external_target[0],
            timings.cargo_external_target[1],
        ),
        ("Lake cold", timings.lake[0], timings.lake[1]),
        ("Lake warm", timings.lake[2], timings.lake[3]),
        ("Lean check", timings.lean_check[0], timings.lean_check[1]),
    ] {
        // A small scheduling allowance is needed for sub-second commands;
        // 500 ms hid fivefold warm-build regressions in this fixture.
        let allowed_ms = native_ms.saturating_mul(105) / 100 + 25;
        assert!(
            mounted_ms <= allowed_ms,
            "{name} mount exceeded the 5% native limit plus 25ms scheduling allowance: native={native_ms}ms mounted={mounted_ms}ms allowed={allowed_ms}ms"
        );
    }
}

struct Timings {
    // Native cold, mounted cold, native warm, mounted warm.
    cargo: [u128; 4],
    // Native and mounted cold builds with output placed outside either checkout.
    cargo_external_target: [u128; 2],
    lake: [u128; 4],
    lean_check: [u128; 2],
}

async fn run_workloads(
    native: &Path,
    mounted: &Path,
    native_first: bool,
) -> Result<Timings, String> {
    let cargo_cold = run_pair(
        native,
        mounted,
        "cargo",
        &["test", "--offline"],
        native_first,
    )
    .await?;
    let cargo_warm = run_pair(
        native,
        mounted,
        "cargo",
        &["test", "--offline"],
        !native_first,
    )
    .await?;
    let cargo = [cargo_cold[0], cargo_cold[1], cargo_warm[0], cargo_warm[1]];
    let targets = native.parent().ok_or("workload parent directory")?;
    let native_target = targets.join("native-target");
    let mounted_target = targets.join("mounted-target");
    let cargo_external_target = if native_first {
        [
            run_cargo_external_target(native, &native_target).await?,
            run_cargo_external_target(mounted, &mounted_target).await?,
        ]
    } else {
        let mounted_ms = run_cargo_external_target(mounted, &mounted_target).await?;
        [
            run_cargo_external_target(native, &native_target).await?,
            mounted_ms,
        ]
    };
    let lake_cold = run_pair(native, mounted, "lake", &["build"], !native_first).await?;
    let lake_warm = run_pair(native, mounted, "lake", &["build"], native_first).await?;
    let lake = [lake_cold[0], lake_cold[1], lake_warm[0], lake_warm[1]];
    let lean_check = run_pair(
        native,
        mounted,
        "lake",
        &["env", "lean", "AcyclicPerf.lean"],
        native_first,
    )
    .await?;
    Ok(Timings {
        cargo,
        cargo_external_target,
        lake,
        lean_check,
    })
}

async fn run_pair(
    native: &Path,
    mounted: &Path,
    program: &'static str,
    args: &'static [&'static str],
    native_first: bool,
) -> Result<[u128; 2], String> {
    if native_first {
        Ok([
            run_tool(native, program, args).await?,
            run_tool(mounted, program, args).await?,
        ])
    } else {
        let mounted_ms = run_tool(mounted, program, args).await?;
        Ok([run_tool(native, program, args).await?, mounted_ms])
    }
}

async fn run_cargo_external_target(root: &Path, target: &Path) -> Result<u128, String> {
    let root = root.to_path_buf();
    let target = target.to_path_buf();
    tokio::task::spawn_blocking(move || {
        run_tool_blocking(&root, "cargo", &["test", "--offline"], Some(&target))
    })
    .await
    .map_err(|error| format!("external-target Cargo supervisor failed: {error}"))?
}

async fn run_tool(
    root: &Path,
    program: &'static str,
    args: &'static [&'static str],
) -> Result<u128, String> {
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || run_tool_blocking(&root, program, args, None))
        .await
        .map_err(|error| format!("{program} supervisor failed: {error}"))?
}

fn run_tool_blocking(
    root: &Path,
    program: &str,
    args: &[&str],
    target: Option<&Path>,
) -> Result<u128, String> {
    let started = Instant::now();
    let mut process = command(program);
    process.current_dir(root).args(args);
    if let Some(target) = target {
        process.env("CARGO_TARGET_DIR", target);
    }
    let bounded = try_output_with_timeout(&mut process, TOOL_TIMEOUT)
        .map_err(|error| format!("{program} could not start in {}: {error}", root.display()))?;
    let elapsed = started.elapsed().as_millis();
    let success = bounded.output.status.success() && !bounded.expired;
    let stderr = String::from_utf8_lossy(&bounded.output.stderr);
    let stdout = String::from_utf8_lossy(&bounded.output.stdout);
    drop(bounded.process_tree);
    if !success {
        return Err(format!(
            "{program} {args:?} failed in {} after {elapsed}ms (expired={}): stdout={} stderr={}",
            root.display(),
            bounded.expired,
            stdout.chars().take(2_000).collect::<String>(),
            stderr.chars().take(2_000).collect::<String>()
        ));
    }
    println!(
        "workload {} {:?} {}ms in {} target={:?}",
        program,
        args,
        elapsed,
        root.display(),
        target
    );
    Ok(elapsed)
}

fn workload_files() -> Vec<(String, Vec<u8>)> {
    let mut files = vec![
        (
            "Cargo.toml".to_owned(),
            b"[package]\nname = \"acyclic_workload\"\nversion = \"0.1.0\"\nedition = \"2021\"\n"
                .to_vec(),
        ),
        (
            "lakefile.toml".to_owned(),
            b"name = \"acyclic_workload\"\nversion = \"0.1.0\"\ndefaultTargets = [\"AcyclicPerf\"]\n[[lean_lib]]\nname = \"AcyclicPerf\"\n"
                .to_vec(),
        ),
    ];
    if let Some(toolchain) = installed_lean_toolchain() {
        files.push((
            "lean-toolchain".to_owned(),
            format!("{toolchain}\n").into_bytes(),
        ));
    }
    let mut rust = String::new();
    let mut lean = String::from("import Lean\nnamespace AcyclicPerf\n");
    for index in 0..96 {
        rust.push_str(&format!("mod unit_{index};\n"));
        files.push((
            format!("src/unit_{index}.rs"),
            format!("pub fn value() -> u64 {{ {index} }}\n").into_bytes(),
        ));
    }
    rust.push_str("pub fn checksum() -> u64 {\n");
    for index in 0..96 {
        rust.push_str(&format!("    unit_{index}::value() +\n"));
    }
    rust.push_str("    0\n}\n#[test]\nfn result_is_stable() { assert_eq!(checksum(), 4560); }\n");
    for index in 0..256 {
        lean.push_str(&format!(
            "def value{index} : Nat := {index}\ntheorem value{index}_correct : value{index} = {index} := rfl\n"
        ));
    }
    lean.push_str("end AcyclicPerf\n");
    files.push(("src/lib.rs".to_owned(), rust.into_bytes()));
    files.push(("AcyclicPerf.lean".to_owned(), lean.into_bytes()));
    files
}

fn installed_lean_toolchain() -> Option<String> {
    let output = command("elan").args(["toolchain", "list"]).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()?
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .find(|name| name.starts_with("leanprover/lean4:"))
        .map(str::to_owned)
}

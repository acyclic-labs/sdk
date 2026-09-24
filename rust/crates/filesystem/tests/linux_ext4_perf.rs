//! Local-only Linux qualification for exact native working-set costs.

#![cfg(target_os = "linux")]

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use std::time::Instant;

use acyclic_fs::demand::{DemandSource, native::NativeDemandSource};
use acyclic_fs::kernel::NamespacePath;
use acyclic_fs::model::{FilesystemProfile, VolumeLimits};
use acyclic_fs::path::PortablePath;
use acyclic_fs::{
    CancellationToken, Fs, IdempotencyKey, LazyWorkspace, LocalFs, LocalOptions,
    MaterializeOptions, MemoryLazyWorkspaceStore, MountOptions, MountPath, PublicationPermit,
    TransactionCommit, WorkBudget,
};
use serde_json::json;

fn component(value: &str) -> Vec<u8> {
    value.as_bytes().to_vec()
}

fn source_tree(
    directories: usize,
    files_per_directory: usize,
) -> Result<tempfile::TempDir, Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    let hot = source.path().join("hot");
    std::fs::create_dir(&hot)?;
    for directory in 0..directories {
        let target = hot.join(format!("d{directory:03}"));
        std::fs::create_dir(&target)?;
        for file in 0..files_per_directory {
            std::fs::write(
                target.join(format!("f{file:03}.txt")),
                format!("payload-{directory:03}-{file:03}\n"),
            )?;
        }
    }
    Ok(source)
}

fn run_workflow(
    root: &Path,
    program: &str,
    args: &[&str],
) -> Result<u128, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let output = Command::new(program)
        .args(args)
        .current_dir(root)
        .env_remove("CARGO_TARGET_DIR")
        .output()?;
    let elapsed_ms = started.elapsed().as_millis();
    if !output.status.success() {
        return Err(format!(
            "{program} {args:?} in {} failed ({}):\n{}\n{}",
            root.display(),
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(elapsed_ms)
}

async fn mounted_workflow(
    fs: &LocalFs,
    source: &Path,
    name: &str,
    program: &str,
    args: &[&str],
) -> Result<(u128, u128, u128, u128), Box<dyn std::error::Error>> {
    let demand = Arc::new(
        NativeDemandSource::open(source, FilesystemProfile::Portable, VolumeLimits::default())
            .await?,
    );
    let lazy = LazyWorkspace::attach(fs, name, demand, MemoryLazyWorkspaceStore::default()).await?;
    let view = tempfile::tempdir()?;
    let mount = lazy.mount(view.path(), MountOptions::read_write()).await?;
    let mounted = (|| {
        if program == "cargo" {
            let pristine = view.path().join("promoted-pristine.txt");
            std::fs::write(&pristine, b"authored")
                .map_err(|error| format!("write pristine source file: {error}"))?;
            let directory = view.path().join("target/debug/incremental/probe-working");
            std::fs::create_dir_all(&directory)
                .map_err(|error| format!("create nested directory: {error}"))?;
            let closed = directory.join("closed.bin");
            std::fs::write(&closed, b"closed")
                .map_err(|error| format!("write closed file: {error}"))?;
            assert_eq!(std::fs::read(&closed)?, b"closed");
            std::fs::remove_file(&closed)
                .map_err(|error| format!("remove closed authored file: {error}"))?;
            let open = directory.join("open.bin");
            std::fs::write(&open, b"open").map_err(|error| format!("write open file: {error}"))?;
            let handle = std::fs::File::open(&open)
                .map_err(|error| format!("open authored file: {error}"))?;
            std::fs::remove_file(&open)
                .map_err(|error| format!("remove open authored file: {error}"))?;
            drop(handle);
            std::fs::remove_file(view.path().join("preexisting.txt"))
                .map_err(|error| format!("remove pre-existing source file: {error}"))?;
            let promoted = view.path().join("promoted.txt");
            assert_eq!(
                std::fs::read(&promoted)
                    .map_err(|error| format!("read source file before promotion: {error}"))?,
                b"source"
            );
            let mut writer = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&promoted)
                .map_err(|error| format!("open source file for truncating write: {error}"))?;
            use std::io::Write as _;
            writer
                .write_all(b"authored")
                .map_err(|error| format!("write promoted source file: {error}"))?;
            drop(writer);
            std::fs::remove_file(&promoted)
                .map_err(|error| format!("remove promoted source file: {error}"))?;
        }
        Ok::<_, Box<dyn std::error::Error>>((
            run_workflow(view.path(), program, args)?,
            run_workflow(view.path(), program, args)?,
        ))
    })();
    let unmounted = mount.unmount().await;
    let (mounted_cold_ms, mounted_warm_ms) = mounted?;
    unmounted?;
    let native_cold_ms = run_workflow(source, program, args)?;
    let native_warm_ms = run_workflow(source, program, args)?;
    Ok((
        mounted_cold_ms,
        mounted_warm_ms,
        native_cold_ms,
        native_warm_ms,
    ))
}

fn check_open_hardlinks(view: &Path) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{Read as _, Seek as _, Write as _};
    use std::os::unix::fs::MetadataExt as _;

    let shared = view.join("shared.txt");
    let alias = view.join("alias.txt");
    let mut reader = std::fs::File::open(&shared)?;
    let mut alias_reader = std::fs::File::open(&alias)?;
    let mut writer = std::fs::OpenOptions::new().write(true).open(&shared)?;
    writer.write_all(b"update")?;
    writer.flush()?;
    reader.rewind()?;
    alias_reader.rewind()?;
    let mut contents = Vec::new();
    let mut alias_contents = Vec::new();
    reader.read_to_end(&mut contents)?;
    alias_reader.read_to_end(&mut alias_contents)?;
    assert_eq!(contents, b"update");
    assert_eq!(alias_contents, b"update");
    assert_eq!(std::fs::read(&shared)?, b"update");
    assert_eq!(std::fs::read(&alias)?, b"update");
    let shared_metadata = std::fs::metadata(&shared)?;
    let alias_metadata = std::fs::metadata(&alias)?;
    assert!(shared_metadata.is_file() && alias_metadata.is_file());
    assert_eq!(shared_metadata.ino(), alias_metadata.ino());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "local-only Linux FUSE source promotion and dirty checkout regression"]
async fn qualify_linux_lazy_source_promotion() -> Result<(), Box<dyn std::error::Error>> {
    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("source.txt"), b"source")?;
    std::fs::write(source.path().join("shared.txt"), b"shared")?;
    std::fs::hard_link(
        source.path().join("shared.txt"),
        source.path().join("alias.txt"),
    )?;
    std::fs::write(source.path().join("remove.txt"), b"remove")?;
    std::fs::write(source.path().join("direct-remove.txt"), b"remove")?;
    let demand = Arc::new(
        NativeDemandSource::open(
            source.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?,
    );
    let lazy = LazyWorkspace::attach(
        &fs,
        "linux-source-promotion",
        demand,
        MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    let view = tempfile::tempdir()?;
    let mount = lazy
        .mount(view.path(), MountOptions::read_write())
        .await
        .map_err(|error| format!("mount lazy source: {error}"))?;
    let result = async {
        use std::io::Write as _;
        check_open_hardlinks(view.path())?;

        let source_file = view.path().join("source.txt");
        assert_eq!(
            std::fs::read(&source_file)
                .map_err(|error| format!("read source before O_TRUNC: {error}"))?,
            b"source"
        );
        let mut writer = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&source_file)
            .map_err(|error| format!("open source file with O_TRUNC: {error}"))?;
        writer
            .write_all(b"changed")
            .map_err(|error| format!("write promoted source: {error}"))?;
        drop(writer);
        assert_eq!(
            std::fs::read(&source_file)
                .map_err(|error| format!("read promoted source: {error}"))?,
            b"changed"
        );
        let lookup = lazy.lookup("/direct-remove.txt").await?;
        assert!(matches!(lookup, acyclic_fs::LazyLookup::Source(_)));
        let before = lazy.workspace().head().await?.id();
        lazy.remove_if("/direct-remove.txt", None)
            .await
            .map_err(|error| format!("direct lazy source remove: {error}"))?;
        let after = lazy.workspace().head().await?.id();
        assert_eq!(before, after, "source-only tombstone must not advance HEAD");
        std::fs::remove_file(view.path().join("remove.txt"))
            .map_err(|error| format!("remove source file: {error}"))?;
        std::fs::write(view.path().join("new.txt"), b"new")
            .map_err(|error| format!("write new file: {error}"))?;
        assert_eq!(
            std::fs::read(view.path().join("new.txt"))
                .map_err(|error| format!("read new file: {error}"))?,
            b"new"
        );
        let nested = view.path().join("target/debug/incremental/probe-working");
        std::fs::create_dir_all(&nested)
            .map_err(|error| format!("create nested authored directory: {error}"))?;
        let closed = nested.join("closed.bin");
        std::fs::write(&closed, b"closed")
            .map_err(|error| format!("write nested authored file: {error}"))?;
        assert_eq!(
            std::fs::read(&closed)
                .map_err(|error| format!("read nested authored file: {error}"))?,
            b"closed"
        );
        let names = std::fs::read_dir(&nested)
            .map_err(|error| format!("open nested authored directory: {error}"))?
            .map(|entry| {
                entry
                    .map(|entry| entry.file_name())
                    .map_err(|error| format!("read nested authored directory entry: {error}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        assert!(names.iter().any(|name| name == "closed.bin"));
        std::fs::remove_file(&closed)
            .map_err(|error| format!("remove nested authored file: {error}"))?;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    let unmounted = mount.unmount().await;
    result?;
    unmounted.map_err(|error| format!("unmount lazy source: {error}"))?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "local-only Linux FUSE write, unlink, and lookup visibility regression"]
async fn qualify_linux_write_unlink_visibility() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{Read as _, Seek as _, Write as _};
    use std::os::unix::fs::MetadataExt as _;

    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("victim.txt"), b"source")?;
    let demand = Arc::new(
        NativeDemandSource::open(
            source.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?,
    );
    let lazy = LazyWorkspace::attach(
        &fs,
        "linux-write-unlink",
        demand,
        MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    let view = tempfile::tempdir()?;
    let mount = lazy.mount(view.path(), MountOptions::read_write()).await?;
    let result = async {
        (|| -> Result<(), Box<dyn std::error::Error>> {
            let path = view.path().join("victim.txt");
            let mut survivor = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|error| format!("open surviving handle: {error}"))?;
            let original_inode = survivor
                .metadata()
                .map_err(|error| format!("stat surviving handle before unlink: {error}"))?
                .ino();
            std::fs::write(&path, b"update")
                .map_err(|error| format!("write source before unlink: {error}"))?;
            std::fs::remove_file(&path)
                .map_err(|error| format!("unlink written source file: {error}"))?;
            let names = std::fs::read_dir(view.path())
                .map_err(|error| format!("open root directory after unlink: {error}"))?
                .map(|entry| entry.map(|entry| entry.file_name()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| format!("readdir after unlink: {error}"))?;
            let listed = names.iter().any(|name| name == "victim.txt");
            let reopened = std::fs::File::open(&path);
            survivor
                .rewind()
                .map_err(|error| format!("seek old handle after unlink: {error}"))?;
            survivor
                .write_all(b"second")
                .map_err(|error| format!("write through unlinked old handle: {error}"))?;
            survivor
                .flush()
                .map_err(|error| format!("flush old handle after unlink: {error}"))?;
            survivor
                .rewind()
                .map_err(|error| format!("seek written old handle after unlink: {error}"))?;
            let mut contents = Vec::new();
            survivor
                .read_to_end(&mut contents)
                .map_err(|error| format!("read written old handle after unlink: {error}"))?;
            assert_eq!(
                survivor
                    .metadata()
                    .map_err(|error| format!("stat old handle after unlink: {error}"))?
                    .ino(),
                original_inode
            );
            assert!(!listed, "unlinked source still appears in readdir");
            assert!(
                matches!(reopened, Err(ref error) if error.kind() == std::io::ErrorKind::NotFound),
                "fresh open of unlinked path must return ENOENT"
            );
            assert_eq!(contents, b"second");
            Ok(())
        })()?;
        mount.sync().await?;
        let names = std::fs::read_dir(view.path())?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<Result<Vec<_>, _>>()?;
        assert!(!names.iter().any(|name| name == "victim.txt"));
        assert!(matches!(
            std::fs::File::open(view.path().join("victim.txt")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound
        ));

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    let unmounted = mount.unmount().await;
    result?;
    unmounted.map_err(|error| format!("unmount after unlink case: {error}"))?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "local-only Linux FUSE open hard-link alias after unlink"]
async fn qualify_linux_open_hardlink_unlink() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{Seek as _, Write as _};
    use std::os::unix::fs::MetadataExt as _;

    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("a"), b"source")?;
    std::fs::hard_link(source.path().join("a"), source.path().join("b"))?;
    let demand = Arc::new(
        NativeDemandSource::open(
            source.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?,
    );
    let lazy = LazyWorkspace::attach(
        &fs,
        "linux-open-hardlink-unlink",
        demand,
        MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    let view = tempfile::tempdir()?;
    let mount = lazy.mount(view.path(), MountOptions::read_write()).await?;
    let result = async {
        let a = view.path().join("a");
        let b = view.path().join("b");
        let verify = |expected: &[u8]| -> Result<(), Box<dyn std::error::Error>> {
            assert!(matches!(
                std::fs::File::open(&a),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound
            ));
            let names = std::fs::read_dir(view.path())?
                .map(|entry| entry.map(|entry| entry.file_name()))
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(names.len(), 1);
            assert_eq!(
                names.first().map(|name| name.as_os_str()),
                Some(std::ffi::OsStr::new("b"))
            );
            assert_eq!(std::fs::read(&b)?, expected);
            Ok(())
        };
        let mut old_a = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&a)?;
        let inode = old_a.metadata()?.ino();
        assert_eq!(std::fs::metadata(&b)?.ino(), inode);
        std::fs::remove_file(&a)?;
        verify(b"source")?;
        old_a.rewind()?;
        old_a.write_all(b"update")?;
        old_a.flush()?;
        assert_eq!(old_a.metadata()?.ino(), inode);
        verify(b"update")?;
        mount.sync().await?;
        verify(b"update")?;
        assert!(matches!(
            lazy.lookup("/a").await,
            Err(acyclic_fs::LazyWorkspaceError::NotFound)
        ));
        assert!(matches!(
            lazy.lookup("/b").await,
            Ok(acyclic_fs::LazyLookup::Shadow { .. })
        ));
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    let unmounted = mount.unmount().await;
    result?;
    unmounted?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "local-only Linux FUSE retained directory after ancestor tombstone"]
async fn qualify_linux_ancestor_whiteout() -> Result<(), Box<dyn std::error::Error>> {
    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let source = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("dir"))?;
    std::fs::write(source.path().join("dir/sub"), b"source")?;
    let demand = Arc::new(
        NativeDemandSource::open(
            source.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?,
    );
    let lazy = LazyWorkspace::attach(
        &fs,
        "linux-ancestor-whiteout",
        demand,
        MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    let view = tempfile::tempdir()?;
    let mount = lazy.mount(view.path(), MountOptions::read_write()).await?;
    let result = async {
        let retained = cap_std::fs::Dir::open_ambient_dir(
            view.path().join("dir"),
            cap_std::ambient_authority(),
        )?;
        assert_eq!(std::fs::read(view.path().join("dir/sub"))?, b"source");
        std::fs::remove_file(view.path().join("dir/sub"))
            .map_err(|error| format!("unlink mounted source child: {error}"))?;
        std::fs::remove_dir(view.path().join("dir"))
            .map_err(|error| format!("rmdir mounted source directory: {error}"))?;
        let retained_error = retained.open("sub").err().map(|error| error.kind());
        let path_error = std::fs::File::open(view.path().join("dir/sub"))
            .err()
            .map(|error| error.kind());
        assert!(matches!(
            retained_error,
            Some(std::io::ErrorKind::NotFound | std::io::ErrorKind::StaleNetworkFileHandle)
        ));
        assert!(matches!(
            path_error,
            Some(std::io::ErrorKind::NotFound | std::io::ErrorKind::StaleNetworkFileHandle)
        ));
        std::fs::create_dir(view.path().join("dir"))
            .map_err(|error| format!("create authored replacement directory: {error}"))?;
        std::fs::write(view.path().join("dir/sub"), b"replacement")
            .map_err(|error| format!("write authored replacement child: {error}"))?;
        assert_eq!(std::fs::read(view.path().join("dir/sub"))?, b"replacement");
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    let unmounted = mount.unmount().await;
    result?;
    unmounted?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "local-only Linux FUSE source rename and replacement visibility regression"]
async fn qualify_linux_source_rename_visibility() -> Result<(), Box<dyn std::error::Error>> {
    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("old.txt"), b"old")?;
    std::fs::write(source.path().join("replace.txt"), b"replace")?;
    let demand = Arc::new(
        NativeDemandSource::open(
            source.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?,
    );
    let lazy = LazyWorkspace::attach(
        &fs,
        "linux-source-rename",
        demand,
        MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    let view = tempfile::tempdir()?;
    let mount = lazy.mount(view.path(), MountOptions::read_write()).await?;
    let result = async {
        let old = view.path().join("old.txt");
        let new = view.path().join("new.txt");
        let replace = view.path().join("replace.txt");
        let visible = |phase: &str,
                       absent: &Path,
                       present: &Path|
         -> Result<(), Box<dyn std::error::Error>> {
            assert_eq!(std::fs::read(present)?, b"old");
            let absent_open = std::fs::File::open(absent);
            let names = std::fs::read_dir(view.path())?
                .map(|entry| entry.map(|entry| entry.file_name()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| format!("{phase}: readdir after rename: {error}"))?;
            assert!(
                matches!(absent_open, Err(ref error) if error.kind() == std::io::ErrorKind::NotFound),
                "{phase}: old path must return ENOENT"
            );
            assert!(!names
                .iter()
                .any(|name| Some(name.as_os_str()) == absent.file_name()));
            assert!(names
                .iter()
                .any(|name| Some(name.as_os_str()) == present.file_name()));
            Ok(())
        };

        std::fs::rename(&old, &new).map_err(|error| format!("rename source to new: {error}"))?;
        visible("before first sync", &old, &new)?;
        mount.sync().await?;
        visible("after first sync", &old, &new)?;

        std::fs::rename(&new, &replace)
            .map_err(|error| format!("replace source with renamed file: {error}"))?;
        visible("before replacement sync", &new, &replace)?;
        mount.sync().await?;
        visible("after replacement sync", &new, &replace)?;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    let unmounted = mount.unmount().await;
    result?;
    unmounted?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "local-only real Cargo and Lake workflows through a Linux FUSE mount"]
async fn report_linux_compiler_workflow_costs() -> Result<(), Box<dyn std::error::Error>> {
    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;

    let cargo = tempfile::tempdir()?;
    std::fs::create_dir(cargo.path().join("src"))?;
    std::fs::write(
        cargo.path().join("Cargo.toml"),
        "[package]\nname = \"acyclic_profile\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )?;
    std::fs::write(
        cargo.path().join("src/lib.rs"),
        "pub fn triangular(n: u64) -> u64 { (0..=n).sum() }\n#[cfg(test)] mod tests { #[test] fn triangular_ten() { assert_eq!(super::triangular(10), 55); } }\n",
    )?;
    std::fs::write(cargo.path().join("preexisting.txt"), b"source")?;
    std::fs::write(cargo.path().join("promoted.txt"), b"source")?;
    std::fs::write(cargo.path().join("promoted-pristine.txt"), b"source")?;
    let cargo_costs = mounted_workflow(
        &fs,
        cargo.path(),
        "linux-cargo-workflow-costs",
        "cargo",
        &["test", "--offline", "--quiet"],
    )
    .await?;

    let lake = tempfile::tempdir()?;
    std::fs::create_dir(lake.path().join("AcyclicProfile"))?;
    std::fs::write(
        lake.path().join("lakefile.toml"),
        "name = \"AcyclicProfile\"\nversion = \"0.1.0\"\ndefaultTargets = [\"AcyclicProfile\"]\n[[lean_lib]]\nname = \"AcyclicProfile\"\n",
    )?;
    std::fs::write(
        lake.path().join("AcyclicProfile.lean"),
        "import AcyclicProfile.Core\ntheorem triangular_ten : triangular 10 = 55 := by decide\n",
    )?;
    std::fs::write(
        lake.path().join("AcyclicProfile/Core.lean"),
        "def triangular (n : Nat) : Nat := (List.range (n + 1)).foldl (· + ·) 0\n",
    )?;
    let lake_costs = mounted_workflow(
        &fs,
        lake.path(),
        "linux-lake-workflow-costs",
        "lake",
        &["build"],
    )
    .await?;
    println!(
        "{}",
        json!({
            "schema": "acyclic-linux-compiler-workflow-cost-v1",
            "cargo_ms": cargo_costs,
            "lake_ms": lake_costs,
        })
    );
    Ok(())
}

#[tokio::test]
#[ignore = "local-only live Linux FUSE source-backed mount profile"]
#[allow(
    clippy::too_many_lines,
    reason = "one local-only source-view comparison"
)]
async fn report_linux_lazy_mount_read_costs() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{Read as _, Seek as _, SeekFrom};

    let source = source_tree(10, 100)?;
    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let demand = Arc::new(
        NativeDemandSource::open(
            source.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?,
    );
    let profile_source = Arc::clone(&demand);
    let started = Instant::now();
    let lazy = LazyWorkspace::attach(
        &fs,
        "linux-lazy-mount-costs",
        demand,
        MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    let attach_us = started.elapsed().as_micros();
    let view = tempfile::tempdir()?;
    let started = Instant::now();
    let mount = lazy.mount(view.path(), MountOptions::read_write()).await?;
    let mount_us = started.elapsed().as_micros();
    let read_tree = |root: &std::path::Path| -> Result<usize, Box<dyn std::error::Error>> {
        let mut bytes = 0;
        for index in 0..100 {
            bytes += std::fs::read(root.join(format!("hot/d000/f{index:03}.txt")))?.len();
        }
        Ok(bytes)
    };
    let parallel_read_tree = |root: &std::path::Path| -> Result<usize, Box<dyn std::error::Error>> {
        let workers = (0..8)
            .map(|worker| {
                let root = root.to_path_buf();
                std::thread::spawn(move || -> std::io::Result<usize> {
                    let mut bytes = 0;
                    for index in (worker..100).step_by(8) {
                        bytes +=
                            std::fs::read(root.join(format!("hot/d000/f{index:03}.txt")))?.len();
                    }
                    Ok(bytes)
                })
            })
            .collect::<Vec<_>>();
        let mut bytes = 0;
        for worker in workers {
            bytes += worker.join().map_err(|_| "parallel reader panicked")??;
        }
        Ok(bytes)
    };
    let started = Instant::now();
    let native_bytes = read_tree(source.path())?;
    let native_read_us = started.elapsed().as_micros();
    let started = Instant::now();
    let cold_bytes = read_tree(view.path())?;
    let cold_read_us = started.elapsed().as_micros();
    let started = Instant::now();
    let warm_bytes = read_tree(view.path())?;
    let warm_read_us = started.elapsed().as_micros();
    let repeated_read = |root: &std::path::Path| -> Result<u128, Box<dyn std::error::Error>> {
        let mut file = std::fs::File::open(root.join("hot/d000/f000.txt"))?;
        let mut byte = [0_u8; 1];
        let started = Instant::now();
        for _ in 0..128 {
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut byte)?;
            assert_eq!(byte[0], b'p');
        }
        Ok(started.elapsed().as_micros())
    };
    let native_repeated_read_us = repeated_read(source.path())?;
    let mounted_repeated_read_us = repeated_read(view.path())?;
    let profile_open_read =
        |root: &std::path::Path| -> Result<[u128; 3], Box<dyn std::error::Error>> {
            let mut elapsed = [0_u128; 3];
            for index in 0..100 {
                let path = root.join(format!("hot/d000/f{index:03}.txt"));
                let started = Instant::now();
                let metadata = std::fs::metadata(&path)?;
                elapsed[0] += started.elapsed().as_micros();
                let started = Instant::now();
                let mut file = std::fs::File::open(&path)?;
                elapsed[1] += started.elapsed().as_micros();
                let started = Instant::now();
                let mut contents = Vec::new();
                file.read_to_end(&mut contents)?;
                elapsed[2] += started.elapsed().as_micros();
                assert_eq!(contents.len() as u64, metadata.len());
            }
            Ok(elapsed)
        };
    let native_open_read_us = profile_open_read(source.path())?;
    let mounted_open_read_us = profile_open_read(view.path())?;
    let started = Instant::now();
    let native_parallel_bytes = parallel_read_tree(source.path())?;
    let native_parallel_read_us = started.elapsed().as_micros();
    let started = Instant::now();
    let mounted_parallel_bytes = parallel_read_tree(view.path())?;
    let mounted_parallel_read_us = started.elapsed().as_micros();
    assert_eq!(native_bytes, cold_bytes);
    assert_eq!(native_bytes, warm_bytes);
    assert_eq!(native_bytes, native_parallel_bytes);
    assert_eq!(native_bytes, mounted_parallel_bytes);
    mount.unmount().await?;
    let source_reference = profile_source.reference();
    let mut observed_files = Vec::new();
    let started = Instant::now();
    for index in 0..100 {
        let path = PortablePath::parse(
            &format!("/hot/d000/f{index:03}.txt"),
            VolumeLimits::default(),
        )?;
        let path = NamespacePath::from_portable(&path, VolumeLimits::default())?;
        let node = profile_source
            .lookup(source_reference, &path, &CancellationToken::new())
            .await?
            .value
            .ok_or("native source lookup missed a fixture file")?;
        observed_files.push((path, node.version));
    }
    let demand_lookup_us = started.elapsed().as_micros();
    let started = Instant::now();
    let mut demand_bytes = 0;
    for (path, version) in &observed_files {
        demand_bytes += profile_source
            .read_range(
                source_reference,
                path,
                *version,
                0,
                128,
                &CancellationToken::new(),
            )
            .await?
            .value
            .len();
    }
    let demand_read_us = started.elapsed().as_micros();
    assert_eq!(native_bytes, demand_bytes);
    let started = Instant::now();
    let mut observed = 0;
    for directory in std::fs::read_dir(source.path().join("hot"))? {
        for file in std::fs::read_dir(directory?.path())? {
            file?.metadata()?;
            observed += 1;
        }
    }
    assert_eq!(observed, 1_000);
    let native_scan_us = started.elapsed().as_micros();
    let copied = tempfile::tempdir()?;
    let started = Instant::now();
    let status = std::process::Command::new("cp")
        .arg("-a")
        .arg(source.path().join("hot"))
        .arg(copied.path())
        .status()?;
    assert!(status.success());
    let native_copy_us = started.elapsed().as_micros();
    for directory in 0..10 {
        let target = copied.path().join(format!("hot/d{directory:03}"));
        for file in 0..100 {
            std::fs::File::open(target.join(format!("f{file:03}.txt")))?.sync_all()?;
        }
        std::fs::File::open(target)?.sync_all()?;
    }
    std::fs::File::open(copied.path().join("hot"))?.sync_all()?;
    std::fs::File::open(copied.path())?.sync_all()?;
    let native_copy_and_sync_us = started.elapsed().as_micros();
    println!(
        "{}",
        json!({
            "schema": "acyclic-linux-lazy-mount-read-cost-v1",
            "source_files": 1_000,
            "read_files": 100,
            "attach_us": attach_us,
            "mount_us": mount_us,
            "native_read_us": native_read_us,
            "cold_read_us": cold_read_us,
            "warm_read_us": warm_read_us,
            "native_repeated_read_us": native_repeated_read_us,
            "mounted_repeated_read_us": mounted_repeated_read_us,
            "native_open_read_us": native_open_read_us,
            "mounted_open_read_us": mounted_open_read_us,
            "native_parallel_read_us": native_parallel_read_us,
            "mounted_parallel_read_us": mounted_parallel_read_us,
            "demand_lookup_us": demand_lookup_us,
            "demand_read_us": demand_read_us,
            "native_scan_us": native_scan_us,
            "native_copy_us": native_copy_us,
            "native_copy_and_sync_us": native_copy_and_sync_us,
        })
    );
    Ok(())
}

#[tokio::test]
#[ignore = "local-only Linux native working-set performance qualification"]
async fn report_linux_working_set_costs() -> Result<(), Box<dyn std::error::Error>> {
    const DIRECTORIES: usize = 10;
    const CHANGES: usize = 100;
    let files_per_directory = std::env::var("ACYCLIC_BENCH_FILES_PER_DIRECTORY")
        .ok()
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(100);
    assert!(files_per_directory >= CHANGES / DIRECTORIES);

    let source = source_tree(DIRECTORIES, files_per_directory)?;

    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let started = Instant::now();
    let demand = Arc::new(
        NativeDemandSource::open(
            source.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?,
    );
    let lazy = LazyWorkspace::attach(
        &fs,
        "linux-native-costs",
        demand,
        MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    let attach_us = started.elapsed().as_micros();

    let view = tempfile::tempdir()?;
    let started = Instant::now();
    let working_set = lazy
        .prepare_native_working_set(
            "/hot",
            &MaterializeOptions::native(view.path()),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            PublicationPermit::Unrestricted,
        )
        .await?;
    let prepare_us = started.elapsed().as_micros();
    let started = Instant::now();
    working_set.validate_for_presentation().await?;
    let activation_us = started.elapsed().as_micros();

    let changed = write_changed_working_set_files(view.path(), DIRECTORIES, CHANGES)?;
    let started = Instant::now();
    working_set.capture_host_paths(&changed).await?;
    let capture_us = started.elapsed().as_micros();
    let started = Instant::now();
    working_set
        .sync_with_permit(PublicationPermit::Unrestricted)
        .await?;
    let sync_us = started.elapsed().as_micros();
    let published = lazy.workspace().head().await?;
    let started = Instant::now();
    working_set
        .sync_with_permit(PublicationPermit::Unrestricted)
        .await?;
    let noop_sync_us = started.elapsed().as_micros();
    let noop_head = lazy.workspace().head().await?;
    assert_eq!(noop_head.id(), published.id());
    let started = Instant::now();
    working_set.capture_host_subtree(&MountPath::root()).await?;
    let noop_capture_us = started.elapsed().as_micros();
    let started = Instant::now();
    working_set
        .sync_with_permit(PublicationPermit::Unrestricted)
        .await?;
    let second_noop_sync_us = started.elapsed().as_micros();
    let second_noop_head = lazy.workspace().head().await?;
    assert_eq!(second_noop_head.id(), published.id());

    println!(
        "{}",
        json!({
            "schema": "acyclic-linux-native-working-set-cost-v1",
            "files": DIRECTORIES * files_per_directory,
            "changed_paths": CHANGES,
            "attach_us": attach_us,
            "prepare_us": prepare_us,
            "activation_us": activation_us,
            "capture_us": capture_us,
            "sync_us": sync_us,
            "noop_sync_us": noop_sync_us,
            "noop_capture_us": noop_capture_us,
            "second_noop_sync_us": second_noop_sync_us,
        })
    );
    Ok(())
}

fn write_changed_working_set_files(
    view: &Path,
    directories: usize,
    changes: usize,
) -> Result<Vec<MountPath>, Box<dyn std::error::Error>> {
    let mut changed = Vec::with_capacity(changes);
    for index in 0..changes {
        let directory = index % directories;
        let file = index / directories;
        std::fs::write(
            view.join("hot")
                .join(format!("d{directory:03}/f{file:03}.txt")),
            format!("changed-{index}\n"),
        )?;
        changed.push(
            MountPath::root()
                .child(component(&format!("d{directory:03}")))
                .child(component(&format!("f{file:03}.txt"))),
        );
    }
    Ok(changed)
}

#[tokio::test]
#[ignore = "local-only small exact-generation materializer profile"]
#[allow(
    clippy::too_many_lines,
    reason = "keep one local-only profiling sample self-contained"
)]
async fn report_linux_exact_materializer_costs() -> Result<(), Box<dyn std::error::Error>> {
    fn counters() -> Result<(u64, u64, u64), Box<dyn std::error::Error>> {
        let mut cpu = 0_u64;
        for entry in std::fs::read_dir("/proc/self/task")? {
            let path = entry?.path().join("schedstat");
            let value = match std::fs::read_to_string(path) {
                Ok(value) => value,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            let runtime = value
                .split_whitespace()
                .next()
                .ok_or("missing thread CPU counter")?
                .parse::<u64>()?;
            cpu = cpu
                .checked_add(runtime)
                .ok_or("process CPU counter overflow")?;
        }
        let io = std::fs::read_to_string("/proc/self/io")?;
        let counter = |name: &str| -> Result<u64, Box<dyn std::error::Error>> {
            io.lines()
                .find_map(|line| line.strip_prefix(name))
                .ok_or("missing process I/O counter")?
                .trim()
                .parse()
                .map_err(Into::into)
        };
        Ok((cpu, counter("read_bytes:")?, counter("write_bytes:")?))
    }

    let source = source_tree(1, 100)?;
    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let workspace = fs
        .create_workspace("linux-exact-materializer-costs")
        .await?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.create_dir_all("/hot/d000").await?;
    for index in 0..100 {
        transaction
            .write_text(
                &format!("/hot/d000/f{index:03}.txt"),
                &format!("payload-000-{index:03}\n"),
            )
            .await?;
    }
    let TransactionCommit::Committed(exact) = transaction.commit().await? else {
        return Err("materializer fixture did not commit".into());
    };

    let mut materialize_us = Vec::new();
    let mut native_copy_us = Vec::new();
    let mut native_copy_and_sync_us = Vec::new();
    let mut cpu_ns = Vec::new();
    let mut read_bytes = Vec::new();
    let mut write_bytes = Vec::new();
    let mut largest_tick_gap_us = Vec::new();
    for _ in 0..5 {
        let destination = tempfile::tempdir()?;
        let running = Arc::new(AtomicBool::new(true));
        let largest_gap = Arc::new(AtomicU64::new(0));
        let tick_running = Arc::clone(&running);
        let tick_gap = Arc::clone(&largest_gap);
        let ticker = tokio::spawn(async move {
            let mut previous = Instant::now();
            while tick_running.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(1)).await;
                let now = Instant::now();
                tick_gap.fetch_max(
                    u64::try_from(now.duration_since(previous).as_micros()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                previous = now;
            }
        });
        let before = counters()?;
        let started = Instant::now();
        let receipt = exact
            .materialize_path(
                "/hot",
                &MaterializeOptions::native(destination.path()),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?;
        materialize_us.push(u64::try_from(started.elapsed().as_micros())?);
        let after = counters()?;
        running.store(false, Ordering::Relaxed);
        ticker.await?;
        cpu_ns.push(after.0.saturating_sub(before.0));
        read_bytes.push(after.1.saturating_sub(before.1));
        write_bytes.push(after.2.saturating_sub(before.2));
        largest_tick_gap_us.push(largest_gap.load(Ordering::Relaxed));
        assert_eq!(receipt.value.files, 100);

        let native_destination = tempfile::tempdir()?;
        let started = Instant::now();
        let status = std::process::Command::new("cp")
            .arg("-a")
            .arg(source.path().join("hot"))
            .arg(native_destination.path())
            .status()?;
        assert!(status.success());
        native_copy_us.push(u64::try_from(started.elapsed().as_micros())?);
        let copied = native_destination.path().join("hot");
        for index in 0..100 {
            std::fs::File::open(copied.join(format!("d000/f{index:03}.txt")))?.sync_all()?;
        }
        std::fs::File::open(copied.join("d000"))?.sync_all()?;
        std::fs::File::open(&copied)?.sync_all()?;
        std::fs::File::open(native_destination.path())?.sync_all()?;
        native_copy_and_sync_us.push(u64::try_from(started.elapsed().as_micros())?);
    }
    materialize_us.sort_unstable();
    native_copy_us.sort_unstable();
    native_copy_and_sync_us.sort_unstable();
    println!(
        "{}",
        json!({
            "schema": "acyclic-linux-exact-materializer-cost-v1",
            "files": 100,
            "samples": 5,
            "materialize_us": materialize_us,
            "native_copy_us": native_copy_us,
            "native_copy_and_sync_us": native_copy_and_sync_us,
            "cpu_ns": cpu_ns,
            "read_bytes": read_bytes,
            "write_bytes": write_bytes,
            "largest_tick_gap_us": largest_tick_gap_us,
        })
    );
    Ok(())
}

//! Local-only Linux qualification for exact native working-set costs.

#![cfg(all(target_os = "linux", not(target_arch = "wasm32")))]

#[path = "support/native_access.rs"]
mod native_access;

use std::path::Path;

use acyclic_fs::kernel::KernelAccounting;
use acyclic_fs::{
    CancellationToken, Fs, LocalOptions, MaterializeOptions, MountOptions, MountPath,
    PublicationPermit, WorkBudget,
};

#[path = "support/portable_source.rs"]
mod portable_source;
use portable_source::{attach, open_source, work};
fn component(value: &str) -> Vec<u8> {
    value.as_bytes().to_vec()
}

/// Serializes real FUSE mounts across the test runner's per-case processes.
struct MountSerial(std::fs::File);

impl MountSerial {
    fn acquire() -> Result<Self, Box<dyn std::error::Error>> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open("/tmp/acyclic-linux-mount-qualification.lock")?;
        file.lock()?;
        Ok(Self(file))
    }
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
async fn qualify_linux_lazy_source_promotion() -> Result<(), Box<dyn std::error::Error>> {
    let _serial = MountSerial::acquire()?;
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
    let demand = open_source(source.path()).await?;
    let lazy = attach(&fs, "linux-source-promotion", demand).await?;
    let view = tempfile::tempdir()?;
    let mount = lazy
        .mount(
            view.path(),
            native_access::options(lazy.workspace(), MountOptions::read_write()).await?,
        )
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
        let cancel = CancellationToken::new();
        let work = work(&cancel)?;
        let mut ctx = KernelAccounting::native(&work, &cancel);
        let lookup = lazy.lookup("/direct-remove.txt", &mut ctx).await?;
        assert!(matches!(&lookup.0, acyclic_fs::LazyLookup::Source { .. }));
        lazy.retire_lookup(lookup, &mut ctx)?;
        let before = lazy.workspace().head(&work, &cancel).await?.id();
        lazy.remove("/direct-remove.txt", None, &mut ctx)
            .await
            .map_err(|error| format!("direct lazy source remove: {error}"))?;
        let after = lazy.workspace().head(&work, &cancel).await?.id();
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
async fn qualify_linux_write_unlink_visibility() -> Result<(), Box<dyn std::error::Error>> {
    let _serial = MountSerial::acquire()?;
    use std::io::{Read as _, Seek as _, Write as _};
    use std::os::unix::fs::MetadataExt as _;

    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("victim.txt"), b"source")?;
    let demand = open_source(source.path()).await?;
    let lazy = attach(&fs, "linux-write-unlink", demand).await?;
    let view = tempfile::tempdir()?;
    let mount = lazy
        .mount(
            view.path(),
            native_access::options(lazy.workspace(), MountOptions::read_write()).await?,
        )
        .await?;
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
async fn qualify_linux_open_hardlink_unlink() -> Result<(), Box<dyn std::error::Error>> {
    let _serial = MountSerial::acquire()?;
    use std::io::{Seek as _, Write as _};
    use std::os::unix::fs::MetadataExt as _;

    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("a"), b"source")?;
    std::fs::hard_link(source.path().join("a"), source.path().join("b"))?;
    let demand = open_source(source.path()).await?;
    let lazy = attach(&fs, "linux-open-hardlink-unlink", demand).await?;
    let view = tempfile::tempdir()?;
    let mount = lazy
        .mount(
            view.path(),
            native_access::options(lazy.workspace(), MountOptions::read_write()).await?,
        )
        .await?;
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
        let cancel = CancellationToken::new();
        let work = work(&cancel)?;
        let mut ctx = KernelAccounting::native(&work, &cancel);
        assert!(matches!(
            lazy.lookup("/a", &mut ctx).await,
            Err(acyclic_fs::LazyWorkspaceError::NotFound)
        ));
        let lookup = lazy.lookup("/b", &mut ctx).await?;
        assert!(matches!(&lookup.0, acyclic_fs::LazyLookup::Shadow { .. }));
        lazy.retire_lookup(lookup, &mut ctx)?;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    let unmounted = mount.unmount().await;
    result?;
    unmounted?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn qualify_linux_ancestor_whiteout() -> Result<(), Box<dyn std::error::Error>> {
    let _serial = MountSerial::acquire()?;
    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let source = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("dir"))?;
    std::fs::write(source.path().join("dir/sub"), b"source")?;
    let demand = open_source(source.path()).await?;
    let lazy = attach(&fs, "linux-ancestor-whiteout", demand).await?;
    let view = tempfile::tempdir()?;
    let mount = lazy
        .mount(
            view.path(),
            native_access::options(lazy.workspace(), MountOptions::read_write()).await?,
        )
        .await?;
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
async fn qualify_linux_source_rename_visibility() -> Result<(), Box<dyn std::error::Error>> {
    let _serial = MountSerial::acquire()?;
    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("old.txt"), b"old")?;
    std::fs::write(source.path().join("replace.txt"), b"replace")?;
    let demand = open_source(source.path()).await?;
    let lazy = attach(&fs, "linux-source-rename", demand).await?;
    let view = tempfile::tempdir()?;
    let mount = lazy
        .mount(
            view.path(),
            native_access::options(lazy.workspace(), MountOptions::read_write()).await?,
        )
        .await?;
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

/// Asserts the deterministic work ledger of a native working set: preparation
/// stays lazy, capture visits exactly the changed entries, publication appends
/// one record per change plus its generation, and a no-op sync appends nothing.
#[tokio::test]
async fn linux_working_set_cost_ledger_is_bounded() -> Result<(), Box<dyn std::error::Error>> {
    const DIRECTORIES: usize = 2;
    const FILES_PER_DIRECTORY: usize = 4;
    const CHANGES: usize = 4;
    let source = source_tree(DIRECTORIES, FILES_PER_DIRECTORY)?;
    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let demand = open_source(source.path()).await?;
    let lazy = attach(&fs, "linux-working-set-ledger", demand).await?;
    let view = tempfile::tempdir()?;
    let metadata = tempfile::tempdir()?;
    let store = acyclic_fs::LocalCoreStateStore::new(metadata.path());
    let mut materialization = MaterializeOptions::native(view.path());
    materialization.materialized_view = Some(acyclic_fs::NativeViewBaseline::recording(&store));
    let cancel = CancellationToken::new();
    let work = work(&cancel)?;
    let working_set = lazy
        .prepare_native_working_set(
            native_access::options(
                lazy.workspace(),
                MountOptions::read_write()
                    .publication(acyclic_fs::MountPublication::Manual)
                    .subdirectory("/hot"),
            )
            .await?,
            &materialization,
            WorkBudget::UNBOUNDED,
            &cancel,
            PublicationPermit::Unrestricted,
        )
        .await?;
    let prepared = work.receipt();
    assert_eq!(prepared.materializations, 0, "preparation must stay lazy");
    assert_eq!(prepared.source_entries_visited, 0);
    assert_eq!(prepared.source_bytes_read, 0);
    assert_eq!(prepared.authority_records_appended, 0);
    working_set.validate_for_presentation(&work).await?;
    let changed = write_changed_working_set_files(view.path(), DIRECTORIES, CHANGES)?;
    let before_capture = work.receipt();
    working_set.capture_host_paths(&changed, &work).await?;
    let captured = work.receipt().checked_sub_cumulative(&before_capture)?;
    assert_eq!(captured.source_entries_visited, CHANGES as u64);
    assert_eq!(captured.materializations, 0);
    assert_eq!(captured.authority_records_appended, 0);
    let before_sync = work.receipt();
    working_set
        .sync_with_permit(PublicationPermit::Unrestricted, &work, |_| {
            std::future::ready(Ok::<(), acyclic_fs::MountLifecycleError>(()))
        })
        .await?;
    let synced = work.receipt().checked_sub_cumulative(&before_sync)?;
    assert_eq!(
        synced.source_entries_visited,
        (DIRECTORIES * (FILES_PER_DIRECTORY + 1)) as u64
    );
    assert_eq!(synced.authority_records_appended, CHANGES as u64 + 1);
    assert_eq!(synced.materializations, 0);
    let published = lazy.workspace().head(&work, &cancel).await?;
    let before_noop = work.receipt();
    working_set
        .sync_with_permit(PublicationPermit::Unrestricted, &work, |_| {
            std::future::ready(Ok::<(), acyclic_fs::MountLifecycleError>(()))
        })
        .await?;
    let noop = work.receipt().checked_sub_cumulative(&before_noop)?;
    assert_eq!(noop.authority_records_appended, 0);
    assert_eq!(noop.materializations, 0);
    assert_eq!(
        lazy.workspace().head(&work, &cancel).await?.id(),
        published.id()
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

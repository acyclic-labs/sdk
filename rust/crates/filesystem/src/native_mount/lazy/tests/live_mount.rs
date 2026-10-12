use super::*;
#[tokio::test(flavor = "multi_thread")]
#[ignore = "mounts a live native session; requires the host's native mount capability"]
async fn live_mount_shows_changes_made_to_its_source() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let (source, mount) = (root.path().join("source"), root.path().join("mount"));
    std::fs::create_dir_all(source.join("d"))?;
    std::fs::write(source.join("a"), b"short")?;
    let demand = native_source(
        &source,
        crate::VolumeConfig::native(crate::Lifecycle::Ephemeral).profile,
    )
    .await?;
    let live = live_lazy_mount_of(demand, &mount, "live-source-changes").await?;
    // Let the kernel cache every fact the changes below supersede.
    for _ in 0..2 {
        assert_eq!(std::fs::metadata(mount.join("a"))?.len(), 5);
        assert_eq!(std::fs::read(mount.join("a"))?, b"short");
        assert!(sorted_names(&mount.join("d"))?.is_empty());
        assert!(is_absent(&mount.join("d").join("x")));
        assert!(is_absent(&mount.join("gone")));
    }

    std::fs::write(source.join("a"), b"much longer")?;
    std::fs::write(source.join("d").join("x"), b"x")?;
    std::fs::create_dir(source.join("gone"))?;
    std::fs::write(source.join("gone").join("x"), b"xy")?;
    live.revalidate()?;
    assert_eq!(std::fs::metadata(mount.join("a"))?.len(), 11);
    assert_eq!(std::fs::read(mount.join("a"))?, b"much longer");
    assert_eq!(sorted_names(&mount.join("d"))?, ["x"]);
    assert_eq!(std::fs::metadata(mount.join("d").join("x"))?.len(), 1);
    assert_eq!(std::fs::read(mount.join("gone").join("x"))?, b"xy");

    std::fs::remove_file(source.join("a"))?;
    std::fs::rename(source.join("gone"), source.join("moved"))?;
    live.revalidate()?;
    assert!(is_absent(&mount.join("a")));
    assert!(is_absent(&mount.join("gone")));
    assert_eq!(std::fs::read(mount.join("moved").join("x"))?, b"xy");

    // Without revalidation, the source's report alone shows a change.
    std::fs::write(source.join("d").join("x"), b"reported")?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::fs::metadata(mount.join("d").join("x"))?.len() != 8 {
        assert!(
            std::time::Instant::now() < deadline,
            "a reported change never became visible"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert_eq!(std::fs::read(mount.join("d").join("x"))?, b"reported");
    live.unmount().await?;
    Ok(())
}
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "mounts a live native session; requires the host's native mount capability"]
async fn live_mount_keeps_a_reused_identity_apart() -> Result<(), Box<dyn std::error::Error>> {
    for watched in [true, false] {
        keeps_a_reused_identity_apart(watched).await?;
    }
    Ok(())
}
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "mounts a live native session; requires the host's native mount capability"]
async fn live_mount_keeps_hard_links_one_file() -> Result<(), Box<dyn std::error::Error>> {
    for watched in [true, false] {
        keeps_hard_links_one_file(watched).await?;
    }
    Ok(())
}
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "mounts a live native session; requires the host's native mount capability"]
async fn live_mount_shows_writes_through_a_name_outside_its_source()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let (source, mount, outside) = (
        root.path().join("source"),
        root.path().join("mount"),
        root.path().join("outside"),
    );
    std::fs::create_dir_all(&source)?;
    std::fs::create_dir_all(&outside)?;
    std::fs::write(source.join("linked"), b"before")?;
    std::fs::hard_link(source.join("linked"), outside.join("alias"))?;
    let demand = native_source(
        &source,
        crate::VolumeConfig::native(crate::Lifecycle::Ephemeral).profile,
    )
    .await?;
    let live = live_lazy_mount_of(demand, &mount, "live-linked-outside").await?;
    for _ in 0..3 {
        assert_eq!(std::fs::read(mount.join("linked"))?, b"before");
        assert_eq!(std::fs::metadata(mount.join("linked"))?.len(), 6);
    }
    // Written in place through the name outside the source: unreported.
    std::fs::OpenOptions::new()
        .write(true)
        .open(outside.join("alias"))
        .and_then(|mut file| std::io::Write::write_all(&mut file, b"AFTER!, longer"))?;
    assert_eq!(std::fs::metadata(mount.join("linked"))?.len(), 14);
    assert_eq!(std::fs::read(mount.join("linked"))?, b"AFTER!, longer");
    live.unmount().await?;
    Ok(())
}
#[cfg(not(target_os = "macos"))]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "mounts a live native session; requires the host's native mount capability"]
async fn live_mount_follows_concurrent_changes_to_its_source()
-> Result<(), Box<dyn std::error::Error>> {
    const FILES: usize = 24;
    const WRITERS: usize = 4;
    const READERS: usize = 4;
    const CHANGES: usize = 150;
    fn version(name: usize, generation: usize) -> Vec<u8> {
        let mut bytes = format!("{name}:{generation}:").into_bytes();
        let length = 16 + (name * 7 + generation * 13) % 200;
        bytes.resize(length, b'.');
        let tail = format!(":{length}");
        bytes.truncate(length - tail.len());
        bytes.extend_from_slice(tail.as_bytes());
        bytes
    }
    fn whole(bytes: &[u8]) -> bool {
        std::str::from_utf8(bytes).is_ok_and(|text| {
            text.rsplit_once(':')
                .and_then(|(_, length)| length.parse::<usize>().ok())
                == Some(bytes.len())
        })
    }

    let root = tempfile::tempdir()?;
    let (source, mount) = (root.path().join("source"), root.path().join("mount"));
    std::fs::create_dir_all(&source)?;
    for name in 0..FILES {
        std::fs::write(source.join(format!("f{name}")), version(name, 0))?;
        std::fs::write(source.join(format!("g{name}")), version(name, 0))?;
    }
    let demand = native_source(
        &source,
        crate::VolumeConfig::native(crate::Lifecycle::Ephemeral).profile,
    )
    .await?;
    let live = live_lazy_mount_of(demand, &mount, "live-concurrent-changes").await?;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let torn = Arc::new(Mutex::new(Vec::new()));
    let readers = (0..READERS)
        .map(|reader| {
            let (mount, stop, torn) = (mount.clone(), Arc::clone(&stop), Arc::clone(&torn));
            std::thread::spawn(move || {
                let mut turn = reader;
                while !stop.load(Ordering::Acquire) {
                    turn += 1;
                    let replaced = mount.join(format!("f{}", turn % FILES));
                    let rewritten = mount.join(format!("g{}", turn % FILES));
                    let _ = std::fs::metadata(&replaced);
                    let _ = std::fs::metadata(&rewritten);
                    let _ = std::fs::read_dir(&mount).map(Iterator::count);
                    // A file rewritten in place may be read mid-write,
                    // natively too.
                    let _ = std::fs::read(&rewritten);
                    // A read may fail while its file is replaced; one
                    // that succeeds returns one whole version.
                    if let Ok(bytes) = std::fs::read(&replaced)
                        && !whole(&bytes)
                        && let Ok(mut torn) = torn.lock()
                    {
                        torn.push(String::from_utf8_lossy(&bytes).into_owned());
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    let writers = (0..WRITERS)
        .map(|writer| {
            let source = source.clone();
            std::thread::spawn(move || -> Result<(), String> {
                let failed = |step: &str, path: &std::path::Path, error: std::io::Error| {
                    format!("{step} {}: {error}", path.display())
                };
                for change in 0..CHANGES {
                    let name = (writer + change * WRITERS) % FILES;
                    let path = source.join(format!("f{name}"));
                    match change % 3 {
                        // Replaced whole, as an editor saves.
                        0 | 2 => {
                            let staged = source.join(format!(".f{name}-{writer}"));
                            std::fs::write(&staged, version(name, change + 1))
                                .map_err(|error| failed("staging", &staged, error))?;
                            std::fs::rename(&staged, &path)
                                .map_err(|error| failed("renaming onto", &path, error))?;
                        }
                        _ => match std::fs::remove_file(&path) {
                            Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                                return Err(failed("removing", &path, error));
                            }
                            _ => {}
                        },
                    }
                    let rewritten = source.join(format!("g{name}"));
                    std::fs::write(&rewritten, version(name, change + 1))
                        .map_err(|error| failed("rewriting", &rewritten, error))?;
                }
                Ok(())
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().map_err(|_| "writer panicked")??;
    }
    stop.store(true, Ordering::Release);
    for reader in readers {
        reader.join().map_err(|_| "reader panicked")?;
    }
    let torn = torn.lock().map_err(|_| "torn reads poisoned")?.clone();
    assert!(torn.is_empty(), "reads returned no whole version: {torn:?}");

    live.revalidate()?;
    assert_eq!(sorted_names(&mount)?, sorted_names(&source)?);
    for name in sorted_names(&source)? {
        let (native, mounted) = (source.join(&name), mount.join(&name));
        assert_eq!(
            std::fs::metadata(&mounted)?.len(),
            std::fs::metadata(&native)?.len(),
            "{name:?} differs in length"
        );
        assert_eq!(
            std::fs::read(&mounted)?,
            std::fs::read(&native)?,
            "{name:?} differs"
        );
    }
    live.unmount().await?;
    Ok(())
}

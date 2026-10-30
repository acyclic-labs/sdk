use super::*;

pub(super) fn io_child(mount: &Path) -> Result<(), Failure> {
    verify_projected_seed(mount)?;
    fs::create_dir_all(mount.join("nested/deeper"))?;
    let created = mount.join("nested/created.txt");
    fs::write(&created, b"created-v1")?;
    fs::write(&created, b"created-v2")?;
    if fs::read(&created)? != b"created-v2" {
        return Err("create/read/write mismatch".into());
    }

    let atomic = mount.join("nested/atomic.txt");
    fs::write(&atomic, b"atomic-old")?;
    let temporary = mount.join("nested/.atomic.txt.save");
    let mut saved = File::create(&temporary)?;
    saved.write_all(b"atomic-new")?;
    saved.sync_all()?;
    drop(saved);
    replace(&temporary, &atomic)?;

    let rename_source = mount.join("nested/rename-source.txt");
    fs::write(&rename_source, b"rename-me")?;
    fs::rename(&rename_source, mount.join("nested/renamed.txt"))?;
    let deleted = mount.join("nested/delete-me.txt");
    fs::write(&deleted, b"delete-me")?;
    fs::remove_file(&deleted)?;

    let mut first = OpenOptions::new().read(true).write(true).open(&created)?;
    let mut second = OpenOptions::new().read(true).open(&created)?;
    first.seek(SeekFrom::End(0))?;
    first.write_all(b"-concurrent")?;
    first.sync_all()?;
    let mut observed = Vec::new();
    second.seek(SeekFrom::Start(0))?;
    second.read_to_end(&mut observed)?;
    if observed != b"created-v2-concurrent" {
        return Err("an already-open handle did not observe the concurrent write".into());
    }
    drop(first);
    drop(second);
    fs::write(&created, b"created-v2")?;

    let hard_source = mount.join("nested/hard-source.txt");
    fs::write(&hard_source, b"linked")?;
    fs::hard_link(&hard_source, mount.join("nested/hard-link.txt"))?;
    create_symlink(Path::new("created.txt"), &mount.join("nested/symbolic.txt"))?;
    let metadata = mount.join("nested/metadata.txt");
    create_metadata_file(&metadata)?;
    if std::env::var("ACYCLIC_EXPECTED_NATIVE_KIND").as_deref() == Ok("macos-nfs") {
        macos_nfs_xattrs_and_toolchain(mount, &metadata)?;
        let entries = fs::read_dir(mount.join("large-tree"))?.collect::<Result<Vec<_>, _>>()?;
        if entries.len() != MACOS_NFS_LARGE_TREE_ENTRIES {
            return Err(format!(
                "large-tree readdir returned {} of {} entries",
                entries.len(),
                MACOS_NFS_LARGE_TREE_ENTRIES
            )
            .into());
        }
        for index in [
            0,
            MACOS_NFS_LARGE_TREE_ENTRIES / 2,
            MACOS_NFS_LARGE_TREE_ENTRIES - 1,
        ] {
            let path = mount.join(format!("large-tree/entry-{index:04}.txt"));
            if fs::metadata(&path)?.len() != 10 || fs::read(path)? != b"large-tree" {
                return Err(format!("large-tree entry {index} was incoherent").into());
            }
        }
    }
    #[cfg(windows)]
    create_timestamp_file(&mount.join("nested/timestamps.txt"))?;
    case_behavior(mount)?;

    let outside = mount
        .parent()
        .ok_or("mount has no parent")?
        .join("outside-guard");
    if fs::read(&outside)? != b"outside-safe" {
        return Err("escape guard changed".into());
    }
    let traversal = mount.join("nested/../../outside-guard");
    if fs::read(traversal)? != b"outside-safe" {
        return Err("normalized traversal did not remain host-controlled".into());
    }
    let traversal_write = mount.join("nested/../../escape-attempt.txt");
    fs::write(traversal_write, b"host-only")?;
    fs::write(mount.join("nested/watch-sentinel.txt"), b"watch-me")?;
    println!("native mount I/O matrix passed");
    Ok(())
}

fn verify_projected_seed(mount: &Path) -> Result<(), Failure> {
    let renamed_before_read = mount.join("seed/nested/rename-before-read.txt");
    let renamed_before_read_destination = mount.join("seed/nested/renamed-before-read.txt");
    fs::rename(&renamed_before_read, &renamed_before_read_destination)?;
    if fs::read(&renamed_before_read_destination)? != b"renamed-before-hydration" {
        return Err("rename-before-hydration returned stale or missing content".into());
    }
    if fs::read(mount.join("seed/nested/original.txt"))? != b"seed" {
        return Err("projected regular file differs through real mount".into());
    }
    if fs::read(mount.join("seed/nested/original-hard.txt"))? != b"seed" {
        return Err("projected hard link differs through real mount".into());
    }
    if std::env::var("ACYCLIC_EXPECTED_NATIVE_KIND")? == "windows-projfs" {
        verify_large_windows_directory(mount)?;
    }
    let projected_link = fs::read_link(mount.join("seed/nested/original-link.txt"))?;
    if projected_link != Path::new("original.txt") {
        return Err(format!("projected symbolic link target differs: {projected_link:?}").into());
    }
    #[cfg(windows)]
    mutate_default_metadata(&mount.join("seed/nested/default-metadata.txt"))?;
    #[cfg(windows)]
    if fs::read(mount.join("seed/nested/default-read.txt"))? != b"read-only-observation" {
        return Err("default-metadata read fixture differs".into());
    }
    Ok(())
}

const PUBLISHED: &[u8] = b"acyclic-parallel-published\n";
const RELEASE: u8 = b'!';

pub(super) async fn run_parallel_io_children(mount: &Path, count: usize) -> Result<(), Failure> {
    let executable = std::env::current_exe()?;
    let mut children = Vec::with_capacity(count);
    let result = async {
        for index in 0..count {
            children.push(Some(CapturedChild::spawn(
                Command::new(&executable)
                    .arg("--parallel-io-child")
                    .arg(mount)
                    .arg(index.to_string())
                    .arg(count.to_string()),
            )?));
        }
        for child in children.iter_mut().flatten() {
            child.receipt(PUBLISHED).await?;
        }
        // Each receipt follows its own completed rename. No child lists until
        // every producer has published; the pipe also reports parent loss.
        for child in children.iter_mut().flatten() {
            child.release(&[RELEASE])?;
        }
        for (index, child) in children.iter_mut().enumerate() {
            let output = child
                .take()
                .ok_or("parallel child disappeared")?
                .finish()
                .await?;
            if !output.status.success() {
                return Err(format!(
                    "parallel I/O child {index} failed ({}): {}{}",
                    output.status,
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                )
                .into());
            }
        }
        Ok::<_, Failure>(())
    }
    .await;
    if let Err(error) = result {
        let mut failures = vec![error.to_string()];
        for (index, child) in children.into_iter().enumerate() {
            if let Some(child) = child
                && let Err(cleanup) = child.stop().await
            {
                failures.push(format!("parallel child {index} cleanup failed: {cleanup}"));
            }
        }
        return Err(failures.join("; ").into());
    }
    Ok(())
}

pub(super) fn parallel_io_child(mount: &Path, index: usize, count: usize) -> Result<(), Failure> {
    if count == 0 || index >= count {
        return Err("invalid parallel client identity".into());
    }
    let directory = mount.join("parallel-clients");
    let staging = mount.join("parallel-staging");
    fs::create_dir_all(&directory)
        .map_err(|error| format!("parallel client {index} create directory: {error}"))?;
    fs::create_dir_all(&staging)
        .map_err(|error| format!("parallel client {index} create staging: {error}"))?;
    let body = format!("client-{index}");
    let staged = staging.join(format!("client-{index}.tmp"));
    fs::write(&staged, body.as_bytes())
        .map_err(|error| format!("parallel client {index} write own file: {error}"))?;
    fs::rename(&staged, directory.join(format!("client-{index}.txt")))
        .map_err(|error| format!("parallel client {index} publish own file: {error}"))?;
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(PUBLISHED)?;
    stdout.flush()?;
    drop(stdout);
    let mut release = [0];
    std::io::stdin()
        .lock()
        .read_exact(&mut release)
        .map_err(|error| {
            std::io::Error::new(
                error.kind(),
                format!("parallel client {index} publication release failed: {error}"),
            )
        })?;
    if release != [RELEASE] {
        return Err("parallel client received an invalid publication release".into());
    }
    let entries = fs::read_dir(&directory)
        .and_then(Iterator::collect::<Result<Vec<_>, _>>)
        .map_err(|error| format!("parallel client {index} list directory: {error}"))?;
    if entries.len() != count {
        return Err(format!(
            "parallel client {index} observed {} of {count} published peers",
            entries.len(),
        )
        .into());
    }
    for peer in 0..count {
        let path = directory.join(format!("client-{peer}.txt"));
        let expected = format!("client-{peer}");
        if fs::read(&path)
            .map_err(|error| format!("parallel client {index} read peer {peer}: {error}"))?
            != expected.as_bytes()
        {
            return Err(format!("parallel client {index} saw incoherent peer {peer}").into());
        }
    }
    Ok(())
}

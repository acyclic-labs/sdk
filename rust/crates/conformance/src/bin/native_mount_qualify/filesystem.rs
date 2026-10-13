use super::*;

pub(super) fn reject_output_inside_checkout(
    output: Option<&Path>,
    checkout_root: Option<&Path>,
) -> Result<(), Failure> {
    let (Some(output), Some(checkout_root)) = (output, checkout_root) else {
        return Ok(());
    };
    let checkout_root = checkout_root.canonicalize()?;
    let output = if output.is_absolute() {
        output.to_path_buf()
    } else {
        std::env::current_dir()?.join(output)
    };
    let output = resolve_existing_ancestor(&output)?;
    if output.starts_with(&checkout_root) {
        return Err("qualification receipt must be written outside the checkout".into());
    }
    let git_dir = resolve_git_dir(&checkout_root)?;
    if output.starts_with(git_dir) {
        return Err(
            "qualification receipt must be written outside Git administrative state".into(),
        );
    }
    Ok(())
}

fn resolve_existing_ancestor(path: &Path) -> Result<PathBuf, Failure> {
    let mut cursor = path;
    let mut missing = Vec::new();
    while !cursor.exists() {
        let name = cursor
            .file_name()
            .ok_or("output path has no existing ancestor")?;
        missing.push(name.to_owned());
        cursor = cursor
            .parent()
            .ok_or("output path has no existing ancestor")?;
    }
    let mut resolved = cursor.canonicalize()?;
    for component in missing.iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

#[cfg(windows)]
pub(super) fn create_timestamp_file(path: &Path) -> Result<(), Failure> {
    fs::write(path, b"timestamp")?;
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    let modified = std::time::UNIX_EPOCH
        .checked_add(Duration::from_nanos(u64::try_from(
            QUALIFICATION_MODIFIED_NS,
        )?))
        .ok_or("qualification timestamp overflowed")?;
    file.set_times(fs::FileTimes::new().set_modified(modified))?;
    Ok(())
}

#[cfg(windows)]
pub(super) fn mutate_default_metadata(path: &Path) -> Result<(), Failure> {
    let editor = OpenOptions::new().read(true).write(true).open(path)?;
    let observer = OpenOptions::new().read(true).open(path)?;
    let modified = std::time::UNIX_EPOCH
        .checked_add(Duration::from_nanos(u64::try_from(
            QUALIFICATION_MODIFIED_NS,
        )?))
        .ok_or("qualification timestamp overflowed")?;
    editor.set_times(fs::FileTimes::new().set_modified(modified))?;
    // Close the earlier, modified handle first. This exercises per-handle
    // metadata baselines when another handle to the same path remains open.
    drop(editor);
    drop(observer);
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_readonly(true);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

pub(super) fn replace(source: &Path, destination: &Path) -> Result<(), Failure> {
    acyclic_native_runtime::durable_rename(
        source,
        destination,
        acyclic_native_runtime::RenameMode::Replace,
        None,
    )?;
    Ok(())
}

#[cfg(unix)]
pub(super) fn create_symlink(target: &Path, link: &Path) -> Result<(), Failure> {
    std::os::unix::fs::symlink(target, link)?;
    Ok(())
}

#[cfg(windows)]
pub(super) fn create_symlink(target: &Path, link: &Path) -> Result<(), Failure> {
    std::os::windows::fs::symlink_file(target, link)?;
    Ok(())
}

#[cfg(unix)]
pub(super) fn create_metadata_file(path: &Path) -> Result<(), Failure> {
    use std::os::unix::fs::PermissionsExt as _;
    fs::write(path, b"metadata")?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o640))?;
    if fs::metadata(path)?.permissions().mode() & 0o777 != 0o640 {
        return Err("POSIX mode did not round-trip".into());
    }
    Ok(())
}

#[cfg(windows)]
pub(super) fn create_metadata_file(path: &Path) -> Result<(), Failure> {
    fs::write(path, b"metadata")?;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_readonly(true);
    fs::set_permissions(path, permissions)?;
    if !fs::metadata(path)?.permissions().readonly() {
        return Err("Windows read-only attribute did not round-trip".into());
    }
    Ok(())
}

pub(super) fn case_behavior(mount: &Path) -> Result<(), Failure> {
    let upper = mount.join("nested/CaseProbe.txt");
    let lower = mount.join("nested/caseprobe.txt");
    fs::write(&upper, b"upper")?;
    let expected = std::env::var("ACYCLIC_EXPECTED_NATIVE_KIND")?;
    if expected == "windows-projfs" {
        if fs::read(&lower)? != b"upper" {
            return Err("Windows mount did not preserve case-insensitive lookup".into());
        }
    } else {
        fs::write(&lower, b"lower")?;
        if fs::read(&upper)? != b"upper" || fs::read(&lower)? != b"lower" {
            return Err("case-distinct names were not preserved".into());
        }
    }
    Ok(())
}

#[cfg(unix)]
pub(super) fn native_link_bytes(target: &str) -> Bytes {
    Bytes::copy_from_slice(target.as_bytes())
}

#[cfg(windows)]
pub(super) fn native_link_bytes(target: &str) -> Bytes {
    Bytes::from(
        target
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>(),
    )
}

pub(super) fn snapshot_checkout(root: &Path) -> Result<CheckoutSnapshot, Failure> {
    let root = root.canonicalize()?;
    let head = git_output(&root, &["rev-parse", "HEAD"])?;
    let status = git_output(
        &root,
        &["status", "--porcelain=v1", "--untracked-files=all"],
    )?;
    let git_dir = resolve_git_dir(&root)?;
    Ok(CheckoutSnapshot {
        head,
        status,
        checkout: tree_digest(&root, &[OsStr::new(".git"), OsStr::new("target")])?,
        git_admin: tree_digest(&git_dir, &[])?,
    })
}

fn resolve_git_dir(root: &Path) -> Result<PathBuf, Failure> {
    let git_dir =
        PathBuf::from(String::from_utf8(git_output(root, &["rev-parse", "--git-dir"])?)?.trim());
    let git_dir = if git_dir.is_absolute() {
        git_dir
    } else {
        root.join(git_dir)
    };
    Ok(git_dir.canonicalize()?)
}

fn git_output(root: &Path, args: &[&str]) -> Result<Vec<u8>, Failure> {
    let output = tool_output(Command::new("git").args(args).current_dir(root))?;
    if !output.status.success() {
        return Err(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(output.stdout)
}

fn tree_digest(root: &Path, excluded_root_names: &[&OsStr]) -> Result<blake3::Hash, Failure> {
    let mut paths = Vec::new();
    collect_paths(root, root, excluded_root_names, &mut paths)?;
    paths.sort();
    let mut digest = blake3::Hasher::new();
    for relative in paths {
        let path = root.join(&relative);
        let metadata = fs::symlink_metadata(&path)?;
        digest.update(relative.to_string_lossy().as_bytes());
        update_stable_metadata(&mut digest, &metadata);
        if metadata.file_type().is_symlink() {
            digest.update(b"L");
            digest.update(fs::read_link(path)?.to_string_lossy().as_bytes());
        } else if metadata.is_dir() {
            digest.update(b"D");
        } else if metadata.is_file() {
            digest.update(b"F");
            digest.update(&fs::read(path)?);
        }
    }
    Ok(digest.finalize())
}

#[cfg(unix)]
fn update_stable_metadata(digest: &mut blake3::Hasher, metadata: &fs::Metadata) {
    use std::os::unix::fs::MetadataExt as _;
    digest.update(&metadata.mode().to_le_bytes());
    digest.update(&metadata.uid().to_le_bytes());
    digest.update(&metadata.gid().to_le_bytes());
    digest.update(&metadata.mtime().to_le_bytes());
    digest.update(&metadata.mtime_nsec().to_le_bytes());
    digest.update(&metadata.ctime().to_le_bytes());
    digest.update(&metadata.ctime_nsec().to_le_bytes());
}

#[cfg(windows)]
fn update_stable_metadata(digest: &mut blake3::Hasher, metadata: &fs::Metadata) {
    use std::os::windows::fs::MetadataExt as _;
    digest.update(&metadata.file_attributes().to_le_bytes());
    digest.update(&metadata.creation_time().to_le_bytes());
    digest.update(&metadata.last_write_time().to_le_bytes());
}

fn collect_paths(
    root: &Path,
    directory: &Path,
    excluded_root_names: &[&OsStr],
    paths: &mut Vec<PathBuf>,
) -> Result<(), Failure> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if directory == root
            && excluded_root_names
                .iter()
                .any(|excluded| *excluded == entry.file_name().as_os_str())
        {
            continue;
        }
        let path = entry.path();
        let relative = path.strip_prefix(root)?.to_path_buf();
        paths.push(relative);
        if entry.file_type()?.is_dir() {
            collect_paths(root, &path, excluded_root_names, paths)?;
        }
    }
    Ok(())
}

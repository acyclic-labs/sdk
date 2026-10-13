use super::*;

pub(super) fn fixture(args: &[String]) -> Result<(), Failure> {
    let root = PathBuf::from(args.first().ok_or("fixture: missing <dir>")?);
    let with_fifo = args.iter().any(|a| a == "--with-fifo");
    fs::create_dir_all(root.join("src/nested"))?;
    fs::create_dir_all(root.join("node_modules/.bin"))?;
    fs::create_dir_all(root.join(".git/objects"))?;
    fs::create_dir_all(root.join("empty-dir"))?;

    fs::write(root.join("README.md"), b"fixture repo\n")?;
    fs::write(root.join(".gitignore"), b"generated/\n.env\n")?;
    fs::write(root.join(".env"), b"SECRET=hunter2\n")?;
    fs::write(root.join("src/main.rs"), b"fn main() {}\n")?;
    fs::write(root.join("src/nested/mod.rs"), b"// nested\n")?;
    fs::write(root.join(".git/objects/pack-data"), b"\x00\x01\x02git\n")?;
    fs::write(root.join("uni-\u{00e9}\u{4e2d}.txt"), b"unicode name\n")?;

    // Deterministic 8 MiB binary file (multi-chunk content).
    let mut big = Vec::with_capacity(8 * 1024 * 1024);
    let mut state: u32 = 0x9e37_79b9;
    while big.len() < 8 * 1024 * 1024 {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        big.extend_from_slice(&state.to_le_bytes());
    }
    fs::write(root.join("assets.bin"), &big)?;

    // Executable script (mode-bit round-trip).
    let script = root.join("tool.sh");
    fs::write(&script, b"#!/bin/sh\necho ok\n")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755))?;
    }

    // Symlinks: valid relative (the node_modules/.bin shape) and dangling.
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        symlink("../../tool.sh", root.join("node_modules/.bin/tool"))?;
        symlink("no-such-target", root.join("dangling-link"))?;
    }

    // Hard-link pair.
    fs::write(root.join("hardlink-a"), b"same inode\n")?;
    fs::hard_link(root.join("hardlink-a"), root.join("hardlink-b"))?;

    if with_fifo {
        let status = acyclic_native_runtime::process_status(
            std::process::Command::new("mkfifo").arg(root.join("pipe.fifo")),
            std::time::Duration::from_secs(120),
        )?;
        if !status.success() {
            return Err("mkfifo failed".into());
        }
    }

    println!("fixture written to {}", root.display());
    Ok(())
}

// ---------------------------------------------------------------------------
// restore-gen: materialize one generation from an existing store
// ---------------------------------------------------------------------------

pub(super) fn corpus(args: &[String]) -> Result<(), Failure> {
    let root = PathBuf::from(args.first().ok_or("corpus: missing <dir>")?);
    let files: u64 = args.get(1).ok_or("corpus: missing <files>")?.parse()?;
    let total_mb: u64 = args.get(2).ok_or("corpus: missing <mb>")?.parse()?;
    let bytes_per_file = (total_mb * 1024 * 1024) / files.max(1);
    let started = Instant::now();
    write_corpus(&root, files, bytes_per_file)?;
    println!(
        "corpus: {files} files x {bytes_per_file} bytes in {:?}",
        started.elapsed()
    );
    Ok(())
}

pub(super) fn write_corpus(root: &Path, files: u64, bytes_per_file: u64) -> Result<(), Failure> {
    let payload = corpus_payload(bytes_per_file);
    for index in 0..files {
        if index % 100 == 0 {
            fs::create_dir_all(root.join(corpus_directory(index / 100)))?;
        }
        fs::write(root.join(corpus_file(index)), &payload)?;
    }
    Ok(())
}

pub(super) fn corpus_payload(bytes: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(bytes as usize);
    let mut state: u32 = 0x1234_5678;
    while (payload.len() as u64) < bytes {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        payload.extend_from_slice(&state.to_le_bytes());
    }
    payload.truncate(bytes as usize);
    payload
}

pub(super) fn corpus_directory(directory: u64) -> String {
    format!("dir-{directory:05}")
}

pub(super) fn corpus_file(index: u64) -> PathBuf {
    Path::new(&corpus_directory(index / 100)).join(format!("file-{index:07}.dat"))
}

// ---------------------------------------------------------------------------
// mount-bench: the plugin's lazy mount of a physical root versus the same
// operations on the root itself
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
pub(super) enum EntryKind {
    File,
    Dir,
    Symlink,
    Other,
}

pub(super) fn walk(root: &Path) -> Result<BTreeMap<PathBuf, EntryKind>, Failure> {
    let mut entries = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            let meta = fs::symlink_metadata(&path)?;
            let relative = path.strip_prefix(root)?.to_path_buf();
            let kind = if meta.file_type().is_symlink() {
                EntryKind::Symlink
            } else if meta.is_dir() {
                stack.push(path.clone());
                EntryKind::Dir
            } else if meta.is_file() {
                EntryKind::File
            } else {
                EntryKind::Other
            };
            entries.insert(relative, kind);
        }
    }
    Ok(entries)
}

pub(super) fn compare_trees(source: &Path, restored: &Path) -> Result<Vec<String>, Failure> {
    let left = walk(source)?;
    let right = walk(restored)?;
    let mut mismatches = Vec::new();

    for (path, kind) in &left {
        match right.get(path) {
            None => mismatches.push(format!("{}: missing in restore", path.display())),
            Some(other) if other != kind => {
                mismatches.push(format!("{}: kind {kind:?} vs {other:?}", path.display()));
            }
            Some(_) => {
                let a = source.join(path);
                let b = restored.join(path);
                match kind {
                    EntryKind::Symlink => {
                        let ta = fs::read_link(&a)?;
                        let tb = fs::read_link(&b)?;
                        if ta != tb {
                            mismatches.push(format!(
                                "{}: symlink target {:?} vs {:?}",
                                path.display(),
                                ta,
                                tb
                            ));
                        }
                    }
                    EntryKind::File => {
                        if !files_equal(&a, &b)? {
                            mismatches.push(format!("{}: content differs", path.display()));
                        }
                        if let Some(m) = mode_mismatch(&a, &b, path)? {
                            mismatches.push(m);
                        }
                    }
                    EntryKind::Dir => {
                        if let Some(m) = mode_mismatch(&a, &b, path)? {
                            mismatches.push(m);
                        }
                    }
                    EntryKind::Other => {}
                }
            }
        }
    }
    for path in right.keys() {
        if !left.contains_key(path) {
            mismatches.push(format!("{}: extra in restore", path.display()));
        }
    }
    Ok(mismatches)
}

pub(super) fn files_equal(a: &Path, b: &Path) -> Result<bool, Failure> {
    let (ma, mb) = (fs::metadata(a)?, fs::metadata(b)?);
    if ma.len() != mb.len() {
        return Ok(false);
    }
    let (mut fa, mut fb) = (fs::File::open(a)?, fs::File::open(b)?);
    let mut ba = vec![0u8; 1024 * 1024];
    let mut bb = vec![0u8; 1024 * 1024];
    loop {
        let na = fa.read(&mut ba)?;
        let nb = fb.read(&mut bb)?;
        if na != nb || ba[..na] != bb[..nb] {
            return Ok(false);
        }
        if na == 0 {
            return Ok(true);
        }
    }
}

#[cfg(unix)]
pub(super) fn mode_mismatch(
    a: &Path,
    b: &Path,
    relative: &Path,
) -> Result<Option<String>, Failure> {
    use std::os::unix::fs::MetadataExt;
    let ma = fs::metadata(a)?.mode() & 0o7777;
    let mb = fs::metadata(b)?.mode() & 0o7777;
    if ma == mb {
        Ok(None)
    } else {
        Ok(Some(format!(
            "{}: mode {ma:o} vs {mb:o}",
            relative.display()
        )))
    }
}

#[cfg(not(unix))]
pub(super) fn mode_mismatch(
    _a: &Path,
    _b: &Path,
    _relative: &Path,
) -> Result<Option<String>, Failure> {
    Ok(None)
}

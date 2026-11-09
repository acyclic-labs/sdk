//! Runtime repository inputs for source policy tests, without Cargo dependencies
//! on the files being inspected. Cross-built tests supply their source snapshot.
use std::{
    collections::BTreeSet,
    fs, io,
    path::{Path, PathBuf},
};

const ROOTS: [&str; 7] = [
    "rust/crates",
    "rust/fuzz",
    "rust/verification",
    "plugin",
    "examples",
    "compatibility",
    "fuzz",
];

fn excluded(name: &std::ffi::OsStr) -> bool {
    matches!(
        name.to_str(),
        Some("target" | "node_modules" | "vendor" | ".git")
    )
}

fn rust_source_subtree(root: &Path, path: &Path) -> bool {
    let mut parts = path.strip_prefix(root).expect("repository path").iter();
    parts.next() == Some(std::ffi::OsStr::new("rust"))
        && parts.next() == Some(std::ffi::OsStr::new("crates"))
        && parts.next().is_some()
        && parts.next() == Some(std::ffi::OsStr::new("src"))
}

fn collect(
    root: &Path,
    directory: &Path,
    active: &mut BTreeSet<PathBuf>,
    files: &mut Vec<PathBuf>,
) -> io::Result<()> {
    let real = directory.canonicalize()?;
    if !real.starts_with(root) || !active.insert(real.clone()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "source directory escapes or cycles",
        ));
    }
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        // Harness policies cover entire Rust src trees, including modules
        // whose directory names happen to match build/dependency caches.
        // Inventory their union; never drop a caller's source-policy input.
        if excluded(&entry.file_name()) && !rust_source_subtree(root, &path) {
            continue;
        }
        let metadata = fs::metadata(&path)?;
        if metadata.is_dir() {
            collect(root, &path, active, files)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            if !path.canonicalize()?.starts_with(root) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "source file escapes repository",
                ));
            }
            files.push(path);
        }
    }
    assert!(active.remove(&real));
    Ok(())
}

/// Reads every Rust source under the seven repository source roots, ordered by
/// repository-relative slash-separated path. Includes tests and build scripts.
/// Build/dependency caches are excluded outside Rust crate src trees; inside
/// src, every directory remains visible to its crate's source policies.
///
/// # Errors
/// Returns an error if any root or source cannot be read, a path is not UTF-8,
/// or a symlink escapes the repository or forms a directory cycle. Never returns
/// a partial inventory.
pub fn repository_rust_sources(root: &Path) -> io::Result<Vec<(String, String)>> {
    let root = root.canonicalize()?;
    let mut files = Vec::new();
    let mut active = BTreeSet::new();
    for name in ROOTS {
        collect(&root, &root.join(name), &mut active, &mut files)?;
    }
    let mut sources = files
        .into_iter()
        .map(|path| {
            let name = path
                .strip_prefix(&root)
                .expect("held source root")
                .to_str()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "non-UTF-8 Rust source path")
                })?
                .replace('\\', "/");
            Ok((name, fs::read_to_string(path)?))
        })
        .collect::<io::Result<Vec<_>>>()?;
    sources.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(sources)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_preserves_all_roots_additions_and_exclusions() -> io::Result<()> {
        let fixture = tempfile::tempdir()?;
        let root = fixture.path().canonicalize()?;
        let mut expected = Vec::new();
        for name in ROOTS {
            let directory = root.join(name);
            fs::create_dir_all(directory.join("child"))?;
            for file in ["normal.rs", "target.rs", "child/nested.rs"] {
                let path = directory.join(file);
                fs::write(&path, "// fixture source")?;
                expected.push((format!("{name}/{file}"), "// fixture source".to_owned()));
            }
            fs::write(directory.join("data.txt"), "non-Rust fixture")?;
            for excluded in ["target", "node_modules", "vendor", ".git"] {
                let ignored = directory.join(excluded);
                fs::create_dir(&ignored)?;
                fs::write(ignored.join("excluded.rs"), "// excluded source")?;
            }
        }
        for name in ["harness", "filesystem"] {
            for component in ["target", "node_modules", "vendor", ".git"] {
                let relative = format!("rust/crates/{name}/src/{component}/kept.rs");
                let path = root.join(&relative);
                fs::create_dir_all(path.parent().expect("source parent"))?;
                fs::write(&path, "// Rust module, not a dependency cache")?;
                expected.push((
                    relative,
                    "// Rust module, not a dependency cache".to_owned(),
                ));
            }
        }
        let check = |expected: &mut Vec<(String, String)>| -> io::Result<()> {
            expected.sort_by(|left, right| left.0.cmp(&right.0));
            assert_eq!(repository_rust_sources(&root)?, *expected);
            Ok(())
        };
        check(&mut expected)?;
        let added = root.join("plugin/child/added.rs");
        fs::write(&added, "// newly added source")?;
        expected.push((
            "plugin/child/added.rs".into(),
            "// newly added source".into(),
        ));
        check(&mut expected)?;
        fs::write(root.join("plugin/normal.rs"), "// edited source")?;
        expected
            .iter_mut()
            .find(|(path, _)| path == "plugin/normal.rs")
            .unwrap()
            .1 = "// edited source".into();
        check(&mut expected)?;
        // A lost source root must fail admission, never yield a partial catalog.
        fs::remove_dir_all(root.join("compatibility"))?;
        assert!(repository_rust_sources(&root).is_err());
        Ok(())
    }
}

//! The build host owns repository enumeration; target tests borrow its inputs.
use std::{
    collections::BTreeSet,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

pub const ROOTS: [&str; 7] = [
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
    // Tracking the directory also invalidates Cargo's catalog when a new source
    // is added. Each include_str! below tracks changes to existing source bytes.
    println!("cargo:rerun-if-changed={}", directory.display());
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

pub fn generate(root: &Path, output: &Path) -> io::Result<()> {
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
            Ok((name, path))
        })
        .collect::<io::Result<Vec<_>>>()?;
    sources.sort_by(|left, right| left.0.cmp(&right.0));
    let mut generated = fs::File::create(output.join("repository-rust-sources.rs"))?;
    writeln!(generated, "&[")?;
    for (name, path) in sources {
        writeln!(generated, "({name:?}, include_str!({path:?})),")?;
    }
    writeln!(generated, "]")
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture(PathBuf);
    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).expect("remove catalog fixture");
        }
    }

    #[test]
    fn inventory_preserves_all_roots_additions_and_exclusions() -> io::Result<()> {
        let directory = std::env::temp_dir().join(format!("source-catalog-{}", std::process::id()));
        fs::create_dir(&directory)?;
        let fixture = Fixture(directory);
        let root = fixture.0.canonicalize()?;
        let output = root.join("output");
        fs::create_dir(&output)?;
        let mut expected = Vec::new();
        for name in ROOTS {
            let directory = root.join(name);
            fs::create_dir_all(directory.join("child"))?;
            for file in ["normal.rs", "target.rs", "child/nested.rs"] {
                let path = directory.join(file);
                fs::write(&path, "// fixture source")?;
                expected.push((format!("{name}/{file}"), path));
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
                expected.push((relative, path));
            }
        }
        let check = |expected: &mut Vec<(String, PathBuf)>| -> io::Result<()> {
            generate(&root, &output)?;
            expected.sort_by(|left, right| left.0.cmp(&right.0));
            let mut contents = String::from("&[\n");
            for (name, path) in expected {
                contents.push_str(&format!("({name:?}, include_str!({path:?})),\n"));
            }
            contents.push_str("]\n");
            assert_eq!(
                fs::read_to_string(output.join("repository-rust-sources.rs"))?,
                contents
            );
            Ok(())
        };
        check(&mut expected)?;
        let added = root.join("plugin/child/added.rs");
        fs::write(&added, "// newly added source")?;
        expected.push(("plugin/child/added.rs".into(), added));
        check(&mut expected)?;
        // A lost source root must fail admission, never yield a partial catalog.
        fs::remove_dir_all(root.join("compatibility"))?;
        assert!(generate(&root, &output).is_err());
        Ok(())
    }
}

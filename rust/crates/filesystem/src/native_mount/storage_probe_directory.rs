//! One owner for temporary native storage probes and their cleanup policy.

use std::io;
use std::path::{Path, PathBuf};

type Cleanup = fn(&Path, bool) -> io::Result<()>;

pub(super) struct ProbeDirectory<F: Fn(&Path, bool) -> io::Result<()> = Cleanup> {
    path: PathBuf,
    cleanup: Option<F>,
}

impl ProbeDirectory {
    pub(super) fn create(root: &Path) -> io::Result<Self> {
        let nonce = crate::native_mount::clock::Clock::capture()
            .now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos();
        let path = root.join(format!(
            ".acyclic-fs-storage-acceleration-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir(&path)?;
        #[cfg(target_os = "macos")]
        let cleanup: Cleanup = remove_known_files;
        #[cfg(not(target_os = "macos"))]
        let cleanup: Cleanup = |path, _| std::fs::remove_dir_all(path);
        Ok(Self {
            path,
            cleanup: Some(cleanup),
        })
    }
}

impl<F: Fn(&Path, bool) -> io::Result<()>> ProbeDirectory<F> {
    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn remove(mut self) -> io::Result<()> {
        if let Some(cleanup) = &self.cleanup {
            cleanup(&self.path, true)?;
        }
        self.cleanup = None;
        Ok(())
    }
}

impl<F: Fn(&Path, bool) -> io::Result<()>> Drop for ProbeDirectory<F> {
    fn drop(&mut self) {
        if let Some(cleanup) = &self.cleanup {
            let _ = cleanup(&self.path, false);
        }
    }
}

#[cfg(any(target_os = "macos", test))]
fn remove_known_files(directory: &Path, fail_fast: bool) -> io::Result<()> {
    for name in ["sparse.bin", "clone-source.bin", "clone-target.bin"] {
        let path = directory.join(name);
        if !fail_fast || path.exists() {
            let result = std::fs::remove_file(path);
            if fail_fast {
                result?;
            }
        }
    }
    std::fs::remove_dir(directory)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Released(Arc<AtomicUsize>);
    impl Drop for Released {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn cleanup_releases_every_owned_field_exactly_once() -> io::Result<()> {
        for mode in 0..3 {
            for populated in [false, true] {
                let root = tempfile::tempdir()?;
                let path = root.path().join("probe");
                std::fs::create_dir(&path)?;
                if populated {
                    std::fs::write(path.join("sparse.bin"), b"probe")?;
                }
                let released = Arc::new(AtomicUsize::new(0));
                let marker = Released(Arc::clone(&released));
                let calls = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
                let observed = Arc::clone(&calls);
                let directory = ProbeDirectory {
                    path: path.clone(),
                    cleanup: Some(move |path: &Path, fail_fast| {
                        let _ = &marker;
                        observed[usize::from(fail_fast)].fetch_add(1, Ordering::SeqCst);
                        if mode == 1 && fail_fast {
                            Err(io::ErrorKind::PermissionDenied.into())
                        } else {
                            std::fs::remove_dir_all(path)
                        }
                    }),
                };
                assert_eq!(released.load(Ordering::SeqCst), 0);
                match mode {
                    0 => directory.remove()?,
                    1 => assert!(directory.remove().is_err()),
                    _ => drop(directory),
                }
                assert!(!path.exists());
                assert_eq!(released.load(Ordering::SeqCst), 1);
                assert_eq!(calls[1].load(Ordering::SeqCst), usize::from(mode != 2));
                assert_eq!(calls[0].load(Ordering::SeqCst), usize::from(mode != 0));
            }
        }
        Ok(())
    }

    #[test]
    fn known_file_cleanup_never_removes_unrecognized_entries() -> io::Result<()> {
        for mask in 0..8 {
            for foreign in [false, true] {
                for fail_fast in [false, true] {
                    let root = tempfile::tempdir()?;
                    let path = root.path().join("probe");
                    std::fs::create_dir(&path)?;
                    for (bit, name) in ["sparse.bin", "clone-source.bin", "clone-target.bin"]
                        .iter()
                        .enumerate()
                    {
                        if mask & (1 << bit) != 0 {
                            std::fs::write(path.join(name), b"probe")?;
                        }
                    }
                    if foreign {
                        std::fs::write(path.join("foreign"), b"keep")?;
                    }
                    assert_eq!(remove_known_files(&path, fail_fast).is_err(), foreign);
                    assert_eq!(path.exists(), foreign);
                    if foreign {
                        assert_eq!(std::fs::read(path.join("foreign"))?, b"keep");
                        assert_eq!(std::fs::read_dir(&path)?.count(), 1);
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn drop_cleanup_continues_after_each_known_file_failure() -> io::Result<()> {
        let names = ["sparse.bin", "clone-source.bin", "clone-target.bin"];
        for blocked in 0..names.len() {
            for fail_fast in [false, true] {
                let root = tempfile::tempdir()?;
                let path = root.path().join("probe");
                std::fs::create_dir(&path)?;
                for (index, name) in names.iter().enumerate() {
                    if index == blocked {
                        std::fs::create_dir(path.join(name))?;
                    } else {
                        std::fs::write(path.join(name), b"probe")?;
                    }
                }
                assert!(remove_known_files(&path, fail_fast).is_err());
                for (index, name) in names.iter().enumerate() {
                    assert_eq!(
                        path.join(name).exists(),
                        index == blocked || (fail_fast && index > blocked)
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn created_probe_has_one_owner_on_explicit_and_drop_cleanup() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        for explicit in [false, true] {
            let directory = ProbeDirectory::create(root.path())?;
            let path = directory.path().to_owned();
            std::fs::write(path.join("sparse.bin"), b"probe")?;
            if explicit {
                directory.remove()?;
            } else {
                drop(directory);
            }
            assert!(!path.exists());
        }
        Ok(())
    }
}

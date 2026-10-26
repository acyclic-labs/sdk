use super::*;
use std::os::windows::ffi::{OsStrExt as _, OsStringExt as _};
use std::path::PathBuf;

fn decoded(path: &Path, meter: Option<&Arc<dyn NativeReadMeter>>) -> io::Result<PathBuf> {
    let (units, allocation) = crate::namespace::namespace_path(path, meter)?;
    let path_units = units.strip_suffix(&[0]).ok_or(io::ErrorKind::InvalidData)?;
    let result = PathBuf::from(std::ffi::OsString::from_wide(path_units));
    drop(units);
    drop(allocation);
    Ok(result)
}

#[test]
fn namespace_paths_normalize_dos_paths_once_and_preserve_explicit_namespaces() -> io::Result<()> {
    for (input, expected) in [
        (
            r"C:\parent\child\..\name & %.txt",
            r"\\?\C:\parent\name & %.txt",
        ),
        ("C:/parent/./name", r"\\?\C:\parent\name"),
        (
            r"\\server\share\parent\..\name",
            r"\\?\UNC\server\share\name",
        ),
        (r"\\?\C:\literal\..\name", r"\\?\C:\literal\..\name"),
        (r"\\?\UNC\server\share\name", r"\\?\UNC\server\share\name"),
        (r"\\.\C:\name", r"\\.\C:\name"),
        (r"C:\parent\NUL", r"\\.\NUL"),
        (r"C:\parent\name. ", r"\\?\C:\parent\name"),
    ] {
        assert_eq!(decoded(Path::new(input), None)?, Path::new(expected));
    }
    assert!(crate::namespace::namespace_path(Path::new("invalid\0name"), None).is_err());
    Ok(())
}

proptest::proptest! {
    #[test]
    fn normalized_literal_paths_are_absolute_and_idempotent(
        components in proptest::collection::vec("[a-zA-Z0-9 &_%-]{1,20}", 1..6),
    ) {
        // Append a non-space unit so these are literal names, not DOS names
        // whose terminal spaces GetFullPathNameW intentionally removes.
        let suffix = components.iter().map(|part| format!("{part}x")).collect::<Vec<_>>().join("\\");
        let path = PathBuf::from(format!("C:\\{suffix}"));
        let normalized = decoded(&path, None).unwrap();
        proptest::prop_assert_eq!(&normalized, &PathBuf::from(format!(r"\\?\C:\{suffix}")));
        proptest::prop_assert_eq!(decoded(&normalized, None).unwrap(), normalized);
    }
}

#[test]
fn every_namespace_publication_accepts_long_paths_and_keeps_collision_semantics() -> io::Result<()>
{
    let root = tempfile::tempdir()?;
    let directory = root
        .path()
        .join("a".repeat(90))
        .join("b".repeat(90))
        .join("c".repeat(90));
    std::fs::create_dir_all(&directory)?;
    assert!(directory.as_os_str().encode_wide().count() > 260);
    for verbatim in [false, true] {
        let directory = if verbatim {
            directory.canonicalize()?
        } else {
            directory.clone()
        };
        let from = directory.join("source & %.txt");
        let to = directory.join("renamed & %.txt");
        let linked = directory.join("linked & %.txt");
        std::fs::write(&from, b"original")?;
        durable_rename(&from, &to, RenameMode::NoReplace, None)?;
        assert!(!from.exists());
        publish_new(&to, &linked, None)?;
        assert_eq!(std::fs::read(&linked)?, b"original");
        std::fs::write(&from, b"replacement")?;
        for result in [
            durable_rename(&from, &to, RenameMode::NoReplace, None),
            publish_new(&from, &linked, None),
        ] {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::AlreadyExists);
        }
        durable_rename(&from, &to, RenameMode::Replace, None)?;
        assert_eq!(std::fs::read(&to)?, b"replacement");
        assert_eq!(std::fs::read(&linked)?, b"original");
        std::fs::remove_file(to)?;
        std::fs::remove_file(linked)?;
    }
    Ok(())
}

#[test]
fn normalization_keeps_exact_live_buffer_custody_and_releases_every_denied_cut() -> io::Result<()> {
    let path = Path::new(r"C:\parent\name & %.txt");
    for allocations in 0..=2 {
        let mut budget = ReadBudgetMeter::new(u64::MAX, u64::MAX);
        budget.allocation_limit = allocations;
        let observer = Arc::new(budget);
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let result = crate::namespace::namespace_path(path, Some(&meter));
        assert_eq!(result.is_ok(), allocations == 2);
        if let Ok((units, custody)) = result {
            let custody = custody.expect("original allocation owner");
            assert_eq!(custody.bytes(), (units.capacity() * 2) as u64);
            assert_eq!(observer.memory.load(Ordering::Acquire), custody.bytes());
            drop(units);
            drop(custody);
        }
        assert_eq!(observer.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}

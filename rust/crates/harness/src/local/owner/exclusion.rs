//! Exclusions belong to SDK-owned directories, never the repository's Git metadata.
use super::*;
use acyclic_fs::{DemandError, HeldRoot};
use std::{collections::BTreeMap, io::Read as _};

pub(in crate::local) struct Exclusion {
    directories: Vec<Directory>,
}

struct Directory {
    root: HeldRoot,
    patterns: Vec<Vec<u8>>,
}

fn missing(error: &DemandError) -> bool {
    matches!(error, DemandError::Io(error) if error.kind() == std::io::ErrorKind::NotFound)
}

fn read_only(error: &DemandError) -> bool {
    matches!(error, DemandError::Io(error) if matches!(
        error.kind(),
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::ReadOnlyFilesystem
    ))
}

fn settle(root: &HeldRoot, file: &fs::File) -> Result<()> {
    match root.sync_file(file) {
        Ok(()) => {}
        // Windows cannot flush through a read-only handle. If no writer can
        // open the owned record, there is no pending publisher to settle here.
        Err(error) if read_only(&error) => match root.open_file(Path::new(".gitignore"), true) {
            Ok(writable) => root.sync_file(&writable).map_err(storage)?,
            Err(error) if read_only(&error) => return Ok(()),
            Err(error) => return Err(storage(error)),
        },
        Err(error) => return Err(storage(error)),
    }
    root.sync_directory().map_err(storage)
}

fn contains(mut file: &fs::File, pattern: &[u8]) -> std::io::Result<(bool, bool)> {
    let length = file.metadata()?.len();
    let mut remaining = length;
    let mut buffer = [0; 8192];
    let (mut index, mut valid, mut carriage_return) = (0, true, false);
    let (mut start, mut negation, mut effective) = (true, false, false);
    let mut last = None;
    while remaining != 0 {
        let count = (&mut file).take(remaining).read(&mut buffer)?;
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Git exclusions changed during read",
            ));
        }
        remaining -= u64::try_from(count).map_err(std::io::Error::other)?;
        for byte in buffer.iter().take(count) {
            last = Some(*byte);
            if *byte == b'\n' {
                effective = settled(effective, negation, valid && index == pattern.len());
                (index, valid, carriage_return) = (0, true, false);
                (start, negation) = (true, false);
                continue;
            }
            if start {
                (start, negation) = (false, *byte == b'!');
            }
            if valid {
                if !carriage_return && pattern.get(index) == Some(byte) {
                    index += 1;
                } else if !carriage_return && index == pattern.len() && *byte == b'\r' {
                    carriage_return = true;
                } else {
                    valid = false;
                }
            }
        }
    }
    if file.metadata()?.len() != length {
        return Err(std::io::Error::other("Git exclusions changed during read"));
    }
    Ok((
        settled(effective, negation, valid && index == pattern.len()),
        last.is_none_or(|byte| byte == b'\n'),
    ))
}

/// The rule's effect after one complete line.
fn settled(effective: bool, negation: bool, rule: bool) -> bool {
    !negation && (effective || rule)
}

impl Exclusion {
    pub(super) fn capture(root: &HeldRoot) -> Result<Self> {
        Self::capture_paths(
            root,
            &[Path::new(
                super::super::workspace::volumes::INCARNATION_PATH,
            )],
        )
    }

    /// Reserve only the supplied SDK-owned names. The parent of a nested name
    /// is SDK metadata; a top-level metadata directory owns all its contents.
    /// Local ignore files outrank ancestor negations without touching .git or
    /// the project's .gitignore. Authored siblings remain visible to Git.
    pub(in crate::local) fn capture_paths(root: &HeldRoot, paths: &[&Path]) -> Result<Self> {
        let mut groups = BTreeMap::<PathBuf, Vec<Vec<u8>>>::new();
        for path in paths {
            if path
                .components()
                .any(|part| !matches!(part, std::path::Component::Normal(_)))
            {
                return Err(Error::Invalid("checkout exclusion must be relative".into()));
            }
            let parent = path
                .parent()
                .ok_or_else(|| Error::Invalid("checkout exclusion has no parent".into()))?;
            if parent.as_os_str().is_empty() {
                groups
                    .entry(path.to_path_buf())
                    .or_default()
                    .push(b"*".to_vec());
            } else {
                let name = path
                    .file_name()
                    .ok_or_else(|| Error::Invalid("checkout exclusion has no name".into()))?;
                groups
                    .entry(parent.to_path_buf())
                    .or_default()
                    .push(pattern(Path::new(name))?);
            }
        }
        let directories = groups
            .into_iter()
            .map(|(relative, mut patterns)| {
                patterns.push(b"/.gitignore".to_vec());
                patterns.sort();
                patterns.dedup();
                Ok(Directory {
                    root: super::super::workspace::volumes::held_directories(root, &relative)?,
                    patterns,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { directories })
    }

    pub(in crate::local) fn publish(&self) -> Result<()> {
        for directory in &self.directories {
            directory.publish()?;
        }
        Ok(())
    }
}

impl Directory {
    fn effective(&self) -> Result<bool> {
        let file = match self.root.open_file(Path::new(".gitignore"), false) {
            Ok(file) => file,
            Err(error) if missing(&error) => return Ok(false),
            Err(error) => return Err(storage(error)),
        };
        let file = acyclic_native_runtime::FileLock::acquire(
            file,
            acyclic_native_runtime::FileLockMode::Exclusive,
        )
        .map_err(storage)?;
        for pattern in &self.patterns {
            use std::io::Seek as _;
            (&*file).rewind().map_err(storage)?;
            if !contains(&file, pattern).map_err(storage)?.0 {
                return Ok(false);
            }
        }
        settle(&self.root, &file)?;
        Ok(true)
    }

    fn publish(&self) -> Result<()> {
        if self.effective()? {
            return Ok(());
        }
        let file = acyclic_native_runtime::FileLock::acquire(
            self.root
                .open_file(Path::new(".gitignore"), true)
                .map_err(storage)?,
            acyclic_native_runtime::FileLockMode::Exclusive,
        )
        .map_err(storage)?;
        for pattern in &self.patterns {
            use std::io::Seek as _;
            (&*file).rewind().map_err(storage)?;
            let (present, line_ended) = contains(&file, pattern).map_err(storage)?;
            if present {
                continue;
            }
            let mut appended = Vec::new();
            if !line_ended {
                appended.push(b'\n');
            }
            appended.extend_from_slice(pattern);
            appended.push(b'\n');
            acyclic_native_runtime::write_file_all(&mut &*file, &appended).map_err(storage)?;
        }
        self.root.sync_file(&file).map_err(storage)?;
        self.root.sync_directory().map_err(storage)
    }
}

pub(in crate::local) fn pattern(path: &Path) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    for part in path.components() {
        let std::path::Component::Normal(part) = part else {
            return Err(Error::Invalid("checkout exclusion must be relative".into()));
        };
        #[cfg(unix)]
        let name = Some(std::os::unix::ffi::OsStrExt::as_bytes(part));
        #[cfg(not(unix))]
        let name = part.to_str().map(str::as_bytes);
        // A rule is one line, so a name with a line break has no exact rule.
        let name = name
            .filter(|name| !name.iter().any(|byte| matches!(byte, b'\r' | b'\n' | 0)))
            .ok_or_else(|| {
                Error::Unsupported("checkout path cannot be represented in Git exclusions".into())
            })?;
        result.push(b'/');
        for &byte in name {
            if matches!(byte, b'*' | b'?' | b'[' | b'\\' | b'!' | b'#')
                || byte.is_ascii_whitespace()
            {
                result.push(b'\\');
            }
            result.push(byte);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::io::{Seek as _, Write as _};

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn streamed_matching_preserves_exact_opaque_line_semantics(bytes in prop::collection::vec(any::<u8>(), 0..20_000), present in any::<bool>(), negated in any::<bool>()) {
            let pattern = b"/.incarnation";
            let mut bytes = bytes;
            if present { bytes.extend_from_slice(b"\n/.incarnation\r\n"); }
            if negated { bytes.extend_from_slice(b"!/.incarnation/"); }
            let lines = bytes.split(|byte| *byte == b'\n').collect::<Vec<_>>();
            let rule = lines.iter().rposition(|line| line.strip_suffix(b"\r").unwrap_or(line) == pattern);
            let negation = lines.iter().rposition(|line| line.first() == Some(&b'!'));
            let expected = rule.is_some() && rule > negation;
            let mut file = tempfile::tempfile().unwrap();
            file.write_all(&bytes).unwrap();
            file.rewind().unwrap();
            prop_assert_eq!(contains(&file, pattern).unwrap().0, expected);
        }
    }

    #[test]
    fn owned_exclusion_publication_stays_with_the_held_checkout() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("checkout");
        fs::create_dir(&root).unwrap();
        let held = acyclic_fs::hold_root(&root).unwrap();
        let exclusion = Exclusion::capture_paths(
            &held,
            &[
                Path::new(".volumes/home"),
                Path::new(".volumes/.incarnation"),
            ],
        )
        .unwrap();
        let moved = temporary.path().join("moved");
        fs::rename(&root, &moved).unwrap();
        fs::create_dir(&root).unwrap();
        exclusion.publish().unwrap();
        assert!(!root.join(".volumes").exists());
        let record = moved.join(".volumes/.gitignore");
        let published = fs::read(&record).unwrap();
        exclusion.publish().unwrap();
        assert_eq!(fs::read(&record).unwrap(), published);
    }
}

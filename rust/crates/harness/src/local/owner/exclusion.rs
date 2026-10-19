//! Git metadata is captured once, then every publication uses held directories.
use super::*;
use acyclic_fs::{DemandError, HeldRoot};

pub(super) struct Exclusion {
    common: HeldRoot,
    pattern: String,
}

fn missing(error: &DemandError) -> bool {
    matches!(error, DemandError::Io(error) if error.kind() == std::io::ErrorKind::NotFound)
}

fn read(root: &HeldRoot, leaf: &str) -> std::result::Result<String, DemandError> {
    // Pointer records follow the existing local owner-record bound. An opaque
    // exclude file is streamed separately and never needs a size-based buffer.
    const MAXIMUM_METADATA_BYTES: u64 = 65_536;
    let file = root.open_file(Path::new(leaf), false)?;
    if file.metadata()?.len() > MAXIMUM_METADATA_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Git metadata pointer exceeds the local metadata bound",
        )
        .into());
    }
    let mut text = String::new();
    file.take(MAXIMUM_METADATA_BYTES + 1)
        .read_to_string(&mut text)?;
    if text.len() as u64 > MAXIMUM_METADATA_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Git metadata pointer grew beyond its admitted bound",
        )
        .into());
    }
    Ok(text)
}

fn contains(file: &mut fs::File, pattern: &[u8]) -> std::io::Result<(bool, bool)> {
    let length = file.metadata()?.len();
    let mut remaining = length;
    let mut buffer = [0; 8192];
    let (mut index, mut valid, mut carriage_return) = (0, true, false);
    let mut last = None;
    while remaining != 0 {
        let count = (&mut *file).take(remaining).read(&mut buffer)?;
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
                if valid && index == pattern.len() {
                    return Ok((true, true));
                }
                (index, valid, carriage_return) = (0, true, false);
            } else if valid {
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
        valid && index == pattern.len(),
        last.is_none_or(|byte| byte == b'\n'),
    ))
}

fn pointed_directory(root: &HeldRoot, pointer: &str) -> Result<HeldRoot> {
    let pointer = Path::new(pointer);
    if pointer.is_absolute() {
        // Absolute metadata pointers explicitly name their own authority.
        return acyclic_fs::hold_root(pointer).map_err(storage);
    }
    let mut target = root.path().to_path_buf();
    for component in pointer.components() {
        match component {
            std::path::Component::Normal(name) => target.push(name),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir if target.pop() => {}
            _ => {
                return Err(Error::Invalid(
                    "invalid relative Git metadata pointer".into(),
                ));
            }
        }
    }
    let ancestor = root
        .path()
        .ancestors()
        .find(|ancestor| target.starts_with(ancestor))
        .ok_or_else(|| Error::Invalid("Git metadata pointer has no common ancestor".into()))?;
    let held = if ancestor == root.path() {
        root.try_clone().map_err(storage)?
    } else {
        let held = acyclic_fs::hold_root(ancestor).map_err(storage)?;
        let relative = root.path().strip_prefix(ancestor).map_err(storage)?;
        if held.open_directory(relative).map_err(storage)?.identity() != root.identity() {
            return Err(Error::Unauthorized("Git metadata root was replaced".into()));
        }
        held
    };
    held.open_directory(target.strip_prefix(ancestor).map_err(storage)?)
        .map_err(storage)
}

fn pointer_line(text: &str) -> Result<&str> {
    let line = text.strip_suffix('\n').unwrap_or(text);
    let line = line.strip_suffix('\r').unwrap_or(line);
    if line.is_empty() || line.contains(['\n', '\r', '\0']) {
        return Err(Error::Invalid("invalid Git metadata pointer record".into()));
    }
    Ok(line)
}

fn metadata(root: &HeldRoot) -> Result<Option<HeldRoot>> {
    match root.open_directory(Path::new(".git")) {
        Ok(directory) => Ok(Some(directory)),
        Err(error) if missing(&error) => Ok(None),
        Err(directory_error) => {
            // A linked worktree stores a regular .git pointer instead.
            let text = read(root, ".git").map_err(|_| storage(directory_error))?;
            let pointer = pointer_line(&text)?
                .strip_prefix("gitdir: ")
                .ok_or_else(|| Error::Invalid("checkout .git file has no gitdir pointer".into()))?;
            pointed_directory(root, pointer).map(Some)
        }
    }
}

impl Exclusion {
    pub(super) fn capture(root: &HeldRoot) -> Result<Option<Self>> {
        let mut top = root.path();
        let mut held = root.try_clone().map_err(storage)?;
        loop {
            if let Some(git) = metadata(&held)? {
                let common = match read(&git, "commondir") {
                    Ok(pointer) => pointed_directory(&git, pointer_line(&pointer)?)?,
                    Err(error) if missing(&error) => git,
                    Err(error) => return Err(storage(error)),
                };
                let relative = root.path().strip_prefix(top).map_err(storage)?;
                let pattern =
                    pattern(&relative.join(super::super::workspace::volumes::INCARNATION_PATH))?;
                return Ok(Some(Self { common, pattern }));
            }
            let Some(parent) = top.parent() else {
                return Ok(None);
            };
            let ancestor = acyclic_fs::hold_root(parent).map_err(storage)?;
            // Named ancestor discovery is only accepted if its held descendant
            // is the captured checkout. A replaced path cannot redirect it.
            let relative = root.path().strip_prefix(parent).map_err(storage)?;
            if ancestor
                .open_directory(relative)
                .map_err(storage)?
                .identity()
                != root.identity()
            {
                return Err(Error::Unauthorized(
                    "checkout changed during Git metadata capture".into(),
                ));
            }
            held = ancestor;
            top = parent;
        }
    }

    pub(super) fn publish(&self) -> Result<()> {
        let info = self
            .common
            .create_directories(Path::new("info"))
            .map_err(storage)?;
        let mut file = info
            .open_file(Path::new("exclude"), true)
            .map_err(storage)?;
        // Serialize the read/append pair across processes; retain opaque user
        // bytes and avoid duplicate records on reopen.
        file.lock().map_err(storage)?;
        let (present, line_ended) =
            contains(&mut file, self.pattern.as_bytes()).map_err(storage)?;
        if !present {
            let mut appended = String::new();
            if !line_ended {
                appended.push('\n');
            }
            appended.push_str("# acyclic harness checkout incarnation\n");
            appended.push_str(&self.pattern);
            appended.push('\n');
            acyclic_native_runtime::write_file_all(&mut file, appended.as_bytes()).map_err(storage)?;
            info.sync_file(&file).map_err(storage)?;
        }
        info.sync_directory().map_err(storage)
    }
}

fn pattern(path: &Path) -> Result<String> {
    let mut result = String::new();
    for part in path.components() {
        let std::path::Component::Normal(part) = part else {
            return Err(Error::Invalid("checkout exclusion must be relative".into()));
        };
        let name = part
            .to_str()
            .filter(|name| !name.contains(['\r', '\n', '\0']))
            .ok_or_else(|| {
                Error::Unsupported("checkout path cannot be represented in Git exclusions".into())
            })?;
        result.push('/');
        for character in name.chars() {
            if matches!(character, '*' | '?' | '[' | '\\' | '!' | '#') || character.is_whitespace()
            {
                result.push('\\');
            }
            result.push(character);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::io::{Seek as _, Write as _};

    #[test]
    fn pointer_records_preserve_path_spaces_and_reject_extra_records() {
        assert_eq!(
            pointer_line("dir with trailing space \n").unwrap(),
            "dir with trailing space "
        );
        assert_eq!(pointer_line("dir\r\n").unwrap(), "dir");
        for malformed in ["", "\n", "dir\nother", "dir\0", "dir\rname"] {
            assert!(pointer_line(malformed).is_err(), "{malformed:?}");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn streamed_matching_preserves_exact_opaque_line_semantics(bytes in prop::collection::vec(any::<u8>(), 0..20_000), present in any::<bool>()) {
            let pattern = b"/.volumes/.incarnation";
            let mut bytes = bytes;
            if present { bytes.extend_from_slice(b"\n/.volumes/.incarnation\r\n"); }
            let expected = bytes.split(|byte| *byte == b'\n').any(|line| line.strip_suffix(b"\r").unwrap_or(line) == pattern);
            let mut file = tempfile::tempfile().unwrap();
            file.write_all(&bytes).unwrap();
            file.rewind().unwrap();
            prop_assert_eq!(contains(&mut file, pattern).unwrap().0, expected);
        }
    }

    #[test]
    fn oversized_pointer_records_are_rejected_before_publication() {
        let temporary = tempfile::tempdir().unwrap();
        let mut file = fs::File::create(temporary.path().join(".git")).unwrap();
        file.write_all(b"gitdir: ").unwrap();
        file.set_len(65_537).unwrap();
        let held = acyclic_fs::hold_root(temporary.path()).unwrap();
        assert!(read(&held, ".git").is_err());
        assert!(Exclusion::capture(&held).is_err());
        assert!(!temporary.path().join(".volumes").exists());
    }

    fn git(root: &Path, arguments: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .args(arguments)
            .current_dir(root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("HOME", root)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        String::from_utf8(output.stdout).unwrap()
    }

    #[test]
    fn held_metadata_excludes_only_owned_tokens_and_preserves_opaque_user_bytes() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        git(root, &["init", "-q"]);
        let exclude = root.join(".git/info/exclude");
        let user = b"# user\xff\n/keep";
        fs::write(&exclude, user).unwrap();
        let nested = root.join("nested space");
        fs::create_dir(&nested).unwrap();
        for path in [root, nested.as_path()] {
            let held = acyclic_fs::hold_root(path).unwrap();
            let exclusion = Exclusion::capture(&held).unwrap().unwrap();
            exclusion.publish().unwrap();
            exclusion.publish().unwrap();
            let token = path.join(".volumes/.incarnation");
            fs::create_dir_all(&token).unwrap();
            fs::write(token.join("token"), "owned").unwrap();
        }
        fs::create_dir_all(root.join(".volumes/user-owned")).unwrap();
        fs::write(root.join(".volumes/user-owned/value"), "user").unwrap();
        let bytes = fs::read(&exclude).unwrap();
        assert!(bytes.starts_with(user));
        assert_eq!(
            bytes
                .windows(b"# acyclic harness checkout incarnation".len())
                .filter(|part| *part == b"# acyclic harness checkout incarnation")
                .count(),
            2
        );
        let status = git(root, &["status", "--porcelain", "--untracked-files=all"]);
        assert!(status.contains(".volumes/user-owned/value"), "{status}");
        assert!(!status.contains(".incarnation"), "{status}");
        // Negative control: the same SDK files are visible without the fix.
        fs::write(&exclude, user).unwrap();
        assert!(
            git(root, &["status", "--porcelain", "--untracked-files=all"])
                .contains(".incarnation/token")
        );
    }

    #[test]
    fn checkout_replacement_cannot_redirect_captured_metadata_publication() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("checkout");
        fs::create_dir(&root).unwrap();
        git(&root, &["init", "-q"]);
        let held = acyclic_fs::hold_root(&root).unwrap();
        let exclusion = Exclusion::capture(&held).unwrap().unwrap();
        let moved = temporary.path().join("original");
        fs::rename(&root, &moved).unwrap();
        fs::create_dir(&root).unwrap();
        git(&root, &["init", "-q"]);
        let replacement = root.join(".git/info/exclude");
        fs::write(&replacement, "replacement user bytes").unwrap();
        exclusion.publish().unwrap();
        assert_eq!(fs::read(&replacement).unwrap(), b"replacement user bytes");
        assert!(
            fs::read_to_string(moved.join(".git/info/exclude"))
                .unwrap()
                .contains("/.volumes/.incarnation")
        );
        // A path-only publisher would select the replacement's metadata.
        let named = acyclic_fs::hold_root(&root).unwrap();
        assert_ne!(named.identity(), held.identity());
        let redirected = Exclusion::capture(&named).unwrap().unwrap();
        redirected.publish().unwrap();
        assert_ne!(fs::read(&replacement).unwrap(), b"replacement user bytes");
    }

    #[test]
    fn linked_worktrees_publish_in_the_held_common_directory() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("repo");
        fs::create_dir(&root).unwrap();
        git(&root, &["init", "-q"]);
        git(
            &root,
            &[
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@t",
                "commit",
                "-q",
                "--allow-empty",
                "-m",
                "initial",
            ],
        );
        let linked = temporary.path().join("linked");
        git(&root, &["worktree", "add", "-q", linked.to_str().unwrap()]);
        let held = acyclic_fs::hold_root(&linked).unwrap();
        Exclusion::capture(&held)
            .unwrap()
            .unwrap()
            .publish()
            .unwrap();
        fs::create_dir_all(linked.join(".volumes/.incarnation")).unwrap();
        fs::write(linked.join(".volumes/.incarnation/token"), "owned").unwrap();
        assert_eq!(git(&linked, &["status", "--porcelain"]), "");
        assert!(
            fs::read_to_string(root.join(".git/info/exclude"))
                .unwrap()
                .contains("/.volumes/.incarnation")
        );
    }
}

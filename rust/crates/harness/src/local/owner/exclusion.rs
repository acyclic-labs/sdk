//! Git metadata is captured once, then every publication uses held directories.
use super::*;
use acyclic_fs::{DemandError, HeldRoot, SearchRoot};

pub(super) struct Exclusion {
    common: HeldRoot,
    pattern: Vec<u8>,
}

fn missing(error: &DemandError) -> bool {
    matches!(error, DemandError::Io(error) if error.kind() == std::io::ErrorKind::NotFound)
}

fn read(root: &SearchRoot, leaf: &str) -> std::result::Result<Vec<u8>, DemandError> {
    bounded(root.open_file(Path::new(leaf))?)
}

fn bounded(file: fs::File) -> std::result::Result<Vec<u8>, DemandError> {
    // Pointer records follow the existing local owner-record bound. An opaque
    // exclude file is streamed separately and never needs a size-based buffer.
    const MAXIMUM_METADATA_BYTES: u64 = 65_536;
    if file.metadata()?.len() > MAXIMUM_METADATA_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Git metadata pointer exceeds the local metadata bound",
        )
        .into());
    }
    // Git writes pointer paths as raw filename bytes, which need not be UTF-8.
    let mut text = Vec::new();
    file.take(MAXIMUM_METADATA_BYTES + 1)
        .read_to_end(&mut text)?;
    if text.len() as u64 > MAXIMUM_METADATA_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Git metadata pointer grew beyond its admitted bound",
        )
        .into());
    }
    Ok(text)
}

/// Whether `pattern` is an exact rule line that no later negation follows, and
/// whether the file ends at a line boundary. Git applies the last matching
/// rule and only a negation (`!`) can re-include a path, so a rule followed by
/// any negation may be undone and is reasserted after it.
fn contains(file: &mut fs::File, pattern: &[u8]) -> std::io::Result<(bool, bool)> {
    let length = file.metadata()?.len();
    let mut remaining = length;
    let mut buffer = [0; 8192];
    let (mut index, mut valid, mut carriage_return) = (0, true, false);
    let (mut start, mut negation, mut effective) = (true, false, false);
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

/// The directory a Git metadata pointer names. A relative pointer is resolved
/// below the nearest common ancestor, held for search only, and accepted only
/// if that ancestor's held descendant is `root`.
fn pointed_directory(root: &SearchRoot, pointer: &Path) -> Result<HeldRoot> {
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
    let relative = target.strip_prefix(ancestor).map_err(storage)?;
    if ancestor == root.path() {
        return root.open_directory(relative).map_err(storage);
    }
    let held = acyclic_fs::hold_search_root(ancestor).map_err(storage)?;
    let descendant = root.path().strip_prefix(ancestor).map_err(storage)?;
    if held
        .open_search_directory(descendant)
        .map_err(storage)?
        .identity()
        != root.identity()
    {
        return Err(Error::Unauthorized("Git metadata root was replaced".into()));
    }
    held.open_directory(relative).map_err(storage)
}

fn pointer_line(text: &[u8]) -> Result<&[u8]> {
    let line = text.strip_suffix(b"\n").unwrap_or(text);
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    if line.is_empty() || line.iter().any(|byte| matches!(byte, b'\n' | b'\r' | 0)) {
        return Err(Error::Invalid("invalid Git metadata pointer record".into()));
    }
    Ok(line)
}

/// The path a pointer record names: raw filename bytes on Unix, and UTF-8 on
/// Windows, where Git records paths as UTF-8.
fn pointer_path(line: &[u8]) -> Result<PathBuf> {
    #[cfg(unix)]
    let path = Some(PathBuf::from(
        <std::ffi::OsStr as std::os::unix::ffi::OsStrExt>::from_bytes(line),
    ));
    #[cfg(not(unix))]
    let path = std::str::from_utf8(line).ok().map(PathBuf::from);
    path.ok_or_else(|| Error::Invalid("Git metadata pointer is not a path".into()))
}

fn metadata(root: &SearchRoot) -> Result<Option<HeldRoot>> {
    match root.open_directory(Path::new(".git")) {
        Ok(directory) => Ok(Some(directory)),
        Err(error) if missing(&error) => Ok(None),
        // A linked worktree stores a regular .git pointer instead.
        Err(_) => match read(root, ".git") {
            Ok(text) => gitfile(root, &text).map(Some),
            Err(_) => linked(root).map(Some),
        },
    }
}

/// The Git directory a gitfile names. Git resolves a relative pointer from
/// the directory holding `.git`, even when `.git` itself is a link.
fn gitfile(root: &SearchRoot, text: &[u8]) -> Result<HeldRoot> {
    let pointer = pointer_line(text)?
        .strip_prefix(b"gitdir: ")
        .ok_or_else(|| Error::Invalid("checkout .git file has no gitdir pointer".into()))?;
    pointed_directory(root, &pointer_path(pointer)?)
}

/// Git follows a link at `.git` to a Git directory or to a gitfile. The
/// link's target is named by path, then held without following links, and
/// accepted only if it is the object the held checkout's `.git` resolves to.
fn linked(root: &SearchRoot) -> Result<HeldRoot> {
    let name = Path::new(".git");
    let resolved = root.resolved_identity(name).map_err(storage)?;
    let target = fs::canonicalize(root.path().join(name)).map_err(storage)?;
    let (Some(parent), Some(leaf)) = (target.parent(), target.file_name()) else {
        return Err(Error::Invalid("checkout .git link has no target".into()));
    };
    let (parent, leaf) = (
        acyclic_fs::hold_search_root(parent).map_err(storage)?,
        Path::new(leaf),
    );
    let replaced = || Error::Unauthorized("checkout .git link changed during capture".into());
    match parent.open_directory(leaf) {
        Ok(git) if git.identity() == resolved => Ok(git),
        Ok(_) => Err(replaced()),
        Err(_) => {
            let file = parent.open_file(leaf).map_err(storage)?;
            if acyclic_fs::file_identity(&file).map_err(storage)? != resolved {
                return Err(replaced());
            }
            gitfile(root, &bounded(file).map_err(storage)?)
        }
    }
}

impl Exclusion {
    pub(super) fn capture(root: &HeldRoot) -> Result<Option<Self>> {
        let mut top = root.path();
        let mut held = root.search().map_err(storage)?;
        loop {
            if let Some(git) = metadata(&held)? {
                let search = git.search().map_err(storage)?;
                let common = match read(&search, "commondir") {
                    Ok(pointer) => {
                        pointed_directory(&search, &pointer_path(pointer_line(&pointer)?)?)?
                    }
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
            // Ancestors are held for search only: a checkout below a
            // directory the process may cross but not list still opens.
            let ancestor = acyclic_fs::hold_search_root(parent).map_err(storage)?;
            // Named ancestor discovery is only accepted if its held descendant
            // is the captured checkout. A replaced path cannot redirect it.
            let relative = root.path().strip_prefix(parent).map_err(storage)?;
            if ancestor
                .open_search_directory(relative)
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
        let (present, line_ended) = contains(&mut file, &self.pattern).map_err(storage)?;
        if !present {
            let mut appended = Vec::new();
            if !line_ended {
                appended.push(b'\n');
            }
            appended.extend_from_slice(b"# acyclic harness checkout incarnation\n");
            appended.extend_from_slice(&self.pattern);
            appended.push(b'\n');
            acyclic_native_runtime::write_file_all(&mut file, &appended).map_err(storage)?;
        }
        // A previous publisher may have died after writing the complete rule
        // but before flushing it. The reader must settle those same bytes too.
        info.sync_file(&file).map_err(storage)?;
        info.sync_directory().map_err(storage)
    }
}

/// The anchored Git exclusion rule for `path`, as raw filename bytes: Git
/// matches paths as bytes on Unix, and records them as UTF-8 on Windows.
fn pattern(path: &Path) -> Result<Vec<u8>> {
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

    #[test]
    fn pointer_records_preserve_path_spaces_and_reject_extra_records() {
        assert_eq!(
            pointer_line(b"dir with trailing space \n").unwrap(),
            b"dir with trailing space "
        );
        assert_eq!(pointer_line(b"dir\r\n").unwrap(), b"dir");
        for malformed in [&b""[..], b"\n", b"dir\nother", b"dir\0", b"dir\rname"] {
            assert!(pointer_line(malformed).is_err(), "{malformed:?}");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn streamed_matching_preserves_exact_opaque_line_semantics(bytes in prop::collection::vec(any::<u8>(), 0..20_000), present in any::<bool>(), negated in any::<bool>()) {
            let pattern = b"/.volumes/.incarnation";
            let mut bytes = bytes;
            if present { bytes.extend_from_slice(b"\n/.volumes/.incarnation\r\n"); }
            if negated { bytes.extend_from_slice(b"!/.volumes/.incarnation/"); }
            // Model: the last exact rule line follows every negation line.
            let lines = bytes.split(|byte| *byte == b'\n').collect::<Vec<_>>();
            let rule = lines.iter().rposition(|line| line.strip_suffix(b"\r").unwrap_or(line) == pattern);
            let negation = lines.iter().rposition(|line| line.first() == Some(&b'!'));
            let expected = rule.is_some() && rule > negation;
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
        assert!(read(&held.search().unwrap(), ".git").is_err());
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

    #[cfg(unix)]
    #[test]
    fn non_utf8_checkout_directories_publish_their_exact_rule() {
        use std::os::unix::ffi::OsStrExt as _;
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        git(root, &["init", "-q"]);
        let nested = root.join(std::ffi::OsStr::from_bytes(b"nested\xff *"));
        fs::create_dir(&nested).unwrap();
        let held = acyclic_fs::hold_root(&nested).unwrap();
        Exclusion::capture(&held)
            .unwrap()
            .unwrap()
            .publish()
            .unwrap();
        fs::create_dir_all(nested.join(".volumes/.incarnation")).unwrap();
        fs::write(nested.join(".volumes/.incarnation/token"), "owned").unwrap();
        fs::write(nested.join("user"), "user").unwrap();
        let status = git(root, &["status", "--porcelain", "--untracked-files=all"]);
        assert!(!status.contains(".incarnation"), "{status}");
        // Negative control: a sibling user file in the same directory stays visible.
        assert!(status.contains("user"), "{status}");
        assert!(
            fs::read(root.join(".git/info/exclude"))
                .unwrap()
                .ends_with(b"/nested\xff\\ \\*/.volumes/.incarnation\n")
        );
    }

    #[test]
    fn later_user_negations_cannot_reinclude_the_token() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        git(root, &["init", "-q"]);
        let exclude = root.join(".git/info/exclude");
        let user: &[u8] =
            b"/.volumes/.incarnation\n!/.volumes/.incarnation/\n!/.volumes/.incarnation/**\n";
        fs::write(&exclude, user).unwrap();
        fs::create_dir_all(root.join(".volumes/.incarnation")).unwrap();
        fs::write(root.join(".volumes/.incarnation/token"), "owned").unwrap();
        let status = || git(root, &["status", "--porcelain", "--untracked-files=all"]);
        // Negative control: the exact rule is present but undone, so a
        // publisher that only looks for the rule leaves the token visible.
        assert!(status().contains(".incarnation/token"));
        let held = acyclic_fs::hold_root(root).unwrap();
        let exclusion = Exclusion::capture(&held).unwrap().unwrap();
        exclusion.publish().unwrap();
        exclusion.publish().unwrap();
        assert!(!status().contains(".incarnation"), "{}", status());
        let mut expected = user.to_vec();
        expected
            .extend_from_slice(b"# acyclic harness checkout incarnation\n/.volumes/.incarnation\n");
        assert_eq!(fs::read(&exclude).unwrap(), expected);
    }

    #[cfg(unix)]
    #[test]
    fn linked_worktrees_with_non_utf8_names_publish_through_their_pointer_bytes() {
        use std::os::unix::ffi::OsStrExt as _;
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
        let linked = temporary
            .path()
            .join(std::ffi::OsStr::from_bytes(b"linked\xff"));
        let output = std::process::Command::new("git")
            .args(["worktree", "add", "-q"])
            .arg(&linked)
            .current_dir(&root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("HOME", &root)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        // Negative control: the pointer is not UTF-8, so a text reader fails.
        assert!(std::str::from_utf8(&fs::read(linked.join(".git")).unwrap()).is_err());
        let held = acyclic_fs::hold_root(&linked).unwrap();
        Exclusion::capture(&held)
            .unwrap()
            .unwrap()
            .publish()
            .unwrap();
        fs::create_dir_all(linked.join(".volumes/.incarnation")).unwrap();
        fs::write(linked.join(".volumes/.incarnation/token"), "owned").unwrap();
        assert_eq!(git(&linked, &["status", "--porcelain"]), "");
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
        let renamed = fs::rename(&root, &moved);
        // Windows refuses to rename a directory below held handles, so the
        // captured checkout cannot be replaced at all while it is held.
        if cfg!(windows) {
            assert_eq!(
                renamed.unwrap_err().kind(),
                std::io::ErrorKind::PermissionDenied
            );
            exclusion.publish().unwrap();
            assert!(
                fs::read_to_string(root.join(".git/info/exclude"))
                    .unwrap()
                    .contains("/.volumes/.incarnation")
            );
            return;
        }
        renamed.unwrap();
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

    fn commit(root: &Path) {
        git(
            root,
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
    }

    /// Denies listing a directory while keeping traversal, and restores
    /// listing on drop so the temporary tree can be removed.
    struct Unlisted(PathBuf);

    /// Every user, so the deny entry applies to the test process itself.
    #[cfg(windows)]
    const EVERYONE_LIST: &str = "*S-1-1-0:(RD)";

    impl Unlisted {
        fn new(path: &Path) -> Self {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                fs::set_permissions(path, fs::Permissions::from_mode(0o311)).unwrap();
            }
            #[cfg(windows)]
            assert!(icacls(path, &["/deny", EVERYONE_LIST]));
            Self(path.to_path_buf())
        }
    }

    impl Drop for Unlisted {
        fn drop(&mut self) {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                let _ = fs::set_permissions(&self.0, fs::Permissions::from_mode(0o755));
            }
            #[cfg(windows)]
            let _ = icacls(&self.0, &["/remove:d", "*S-1-1-0"]);
        }
    }

    #[cfg(windows)]
    fn icacls(path: &Path, arguments: &[&str]) -> bool {
        std::process::Command::new("icacls")
            .arg(path)
            .args(arguments)
            .output()
            .is_ok_and(|output| output.status.success())
    }

    #[test]
    fn ancestors_that_can_be_searched_but_not_listed_still_lead_to_metadata() {
        let temporary = tempfile::tempdir().unwrap();
        let repo = temporary.path().join("repo");
        let locked = repo.join("locked");
        let checkout = locked.join("checkout");
        fs::create_dir_all(&checkout).unwrap();
        git(&repo, &["init", "-q"]);
        commit(&repo);
        let linked = locked.join("linked");
        git(&repo, &["worktree", "add", "-q", linked.to_str().unwrap()]);
        // Git can record a relative pointer; resolving it crosses `locked`.
        fs::write(linked.join(".git"), "gitdir: ../../.git/worktrees/linked\n").unwrap();
        let _restore = Unlisted::new(&locked);
        // The process may cross `locked` but not list it. A process that
        // overrides access checks could list it, exercising nothing.
        assert!(
            fs::read_dir(&locked).is_err(),
            "test must not run with an access-check override"
        );
        for path in [&checkout, &linked] {
            let held = acyclic_fs::hold_root(path).unwrap();
            Exclusion::capture(&held)
                .unwrap()
                .unwrap()
                .publish()
                .unwrap();
            assert_eq!(
                git(path, &["check-ignore", ".volumes/.incarnation/token"]),
                ".volumes/.incarnation/token\n"
            );
        }
        let exclude = fs::read_to_string(repo.join(".git/info/exclude")).unwrap();
        assert!(
            exclude.contains("\n/locked/checkout/.volumes/.incarnation\n"),
            "{exclude}"
        );
        assert!(exclude.contains("\n/.volumes/.incarnation\n"), "{exclude}");
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_git_metadata_publishes_in_the_git_directory_it_names() {
        let temporary = tempfile::tempdir().unwrap();
        let store = temporary.path().join("store");
        fs::create_dir(&store).unwrap();
        git(&store, &["init", "-q"]);
        commit(&store);
        // `.git` is a link to a Git directory.
        let checkout = temporary.path().join("checkout");
        fs::create_dir(&checkout).unwrap();
        std::os::unix::fs::symlink("../store/.git", checkout.join(".git")).unwrap();
        // `.git` is a link to a linked worktree's gitfile.
        let linked = temporary.path().join("linked");
        git(&store, &["worktree", "add", "-q", linked.to_str().unwrap()]);
        let gitfile = temporary.path().join("linked.gitfile");
        fs::rename(linked.join(".git"), &gitfile).unwrap();
        std::os::unix::fs::symlink(&gitfile, linked.join(".git")).unwrap();
        for path in [&checkout, &linked] {
            let held = acyclic_fs::hold_root(path).unwrap();
            Exclusion::capture(&held)
                .unwrap()
                .unwrap()
                .publish()
                .unwrap();
            fs::create_dir_all(path.join(".volumes/.incarnation")).unwrap();
            fs::write(path.join(".volumes/.incarnation/token"), "owned").unwrap();
            fs::write(path.join("user"), "user").unwrap();
            let status = git(path, &["status", "--porcelain", "--untracked-files=all"]);
            assert!(!status.contains(".incarnation"), "{status}");
            // Negative control: a user file beside the token stays visible.
            assert!(status.contains("user"), "{status}");
        }
        // Both publish in the real common directory, never through the link.
        assert!(
            fs::symlink_metadata(checkout.join(".git"))
                .unwrap()
                .is_symlink()
        );
        assert!(
            fs::read_to_string(store.join(".git/info/exclude"))
                .unwrap()
                .contains("\n/.volumes/.incarnation\n")
        );
    }
}

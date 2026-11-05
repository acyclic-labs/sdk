use super::*;

pub(super) async fn resolved_git_regular_files<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &crate::PinnedReader<A, O>,
    host: &GitIgnorePolicy,
    root_display: String,
    root: NamespacePath,
    limits: VolumeLimits,
    maximum_entries: Option<usize>,
    cancellation: &CancellationToken,
) -> Result<(Vec<(String, ResolvedFile<A, O>)>, bool), WorkspaceError> {
    let mut pending = vec![(root_display, root)];
    let mut regular = Vec::new();
    let mut visited = 0_usize;
    let mut truncated = false;
    'walk: while let Some((directory_display, directory)) = pending.pop() {
        let mut after = None;
        loop {
            let page = reader
                .resolve_directory_page(
                    &directory,
                    after.as_ref(),
                    1_024,
                    WorkBudget::UNBOUNDED,
                    cancellation,
                )
                .await
                .map_err(|failure| WorkspaceError::from(failure.error))?
                .value;
            let has_more = page.has_more;
            after = page.entries.last().map(|entry| entry.name.clone());
            for entry in page.entries {
                if maximum_entries.is_some_and(|maximum| visited >= maximum) {
                    truncated = true;
                    break 'walk;
                }
                visited = visited.saturating_add(1);
                let name = entry
                    .name
                    .unicode_text(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new(),
                    )
                    .ok()
                    .map(|receipt| receipt.value)
                    .ok_or_else(|| WorkspaceError::Path("non-portable Git path".to_owned()))?;
                let path = if directory_display.is_empty() {
                    name.to_string()
                } else {
                    format!("{directory_display}/{name}")
                };
                if host.reserves(&path) {
                    continue;
                }
                if entry.file.description().kind == FileKind::Directory {
                    let mut components = directory.components().to_vec();
                    components.push(entry.name);
                    let child =
                        NamespacePath::new(components, limits).map_err(WorkspaceError::from)?;
                    pending.push((path, child));
                } else if entry.file.description().kind == FileKind::Regular {
                    regular.push((path, entry.file));
                }
            }
            if !has_more {
                break;
            }
        }
    }
    regular.sort_by(|left, right| left.0.cmp(&right.0));
    Ok((regular, truncated))
}

pub(super) async fn resolved_git_grep_files<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &crate::PinnedReader<A, O>,
    host: &GitIgnorePolicy,
    root_display: String,
    root: NamespacePath,
    limits: VolumeLimits,
    maximum_entries: Option<usize>,
    cancellation: &CancellationToken,
) -> Result<(Vec<(String, ResolvedFile<A, O>)>, bool), WorkspaceError> {
    if root_display.is_empty() {
        return resolved_git_regular_files(
            reader,
            host,
            root_display,
            root,
            limits,
            maximum_entries,
            cancellation,
        )
        .await;
    }
    let mut roots = reader
        .resolve_files(
            std::slice::from_ref(&root),
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(|failure| WorkspaceError::from(failure.error))?
        .value;
    let root_file = roots.pop().flatten().ok_or(WorkspaceError::NotFound)?;
    match root_file.description().kind {
        FileKind::Directory => {
            resolved_git_regular_files(
                reader,
                host,
                root_display,
                root,
                limits,
                maximum_entries,
                cancellation,
            )
            .await
        }
        FileKind::Regular => Ok((vec![(root_display, root_file)], false)),
        _ => Ok((Vec::new(), false)),
    }
}

/// Portable namespace fact used by Git-shaped inspection commands.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitTreeEntry {
    /// Slash-separated path relative to the compatibility root.
    pub path: String,
    /// Stable textual filesystem kind.
    pub kind: String,
}

/// One literal grep match in an authenticated generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitGrepMatch {
    /// Slash-separated path relative to the compatibility root.
    pub path: String,
    /// One-based logical line number.
    pub line: u64,
    /// Complete UTF-8 line without its terminator.
    pub text: String,
}

/// Machine-readable grep result with optional caller-selected truncation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitGrepResult {
    /// Literal matching lines in stable path/line order.
    pub matches: Vec<GitGrepMatch>,
    /// More paths, bytes, or matches existed beyond the supplied bounds.
    pub truncated: bool,
}

/// One line attributed across explicit compatibility history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitBlameLine {
    /// Compatibility commit that last changed this line position.
    pub commit: GitCommitId,
    /// One-based line number in the selected version.
    pub line: u64,
    /// Complete UTF-8 line without its terminator.
    pub text: String,
}

/// Walks an authenticated tree in stable portable path order.
///
/// Paths in the host's namespaces are outside the repository and never listed.
pub async fn walk_git_tree<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    generation: &Generation<A, O>,
    host: &GitIgnorePolicy,
    root: Option<&str>,
    maximum_entries: impl Into<Option<u32>>,
) -> Result<Vec<GitTreeEntry>, WorkspaceError> {
    let maximum_entries = maximum_entries.into();
    let root = root.unwrap_or("").trim_matches('/');
    if maximum_entries == Some(0) || host.reserves(root) {
        return Ok(Vec::new());
    }
    let start = if root.is_empty() {
        "/".to_owned()
    } else {
        format!("/{root}")
    };
    if !root.is_empty() {
        let stat = generation
            .stat(&start, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        if stat.kind != FileKind::Directory {
            return Ok(vec![GitTreeEntry {
                path: root.to_owned(),
                kind: git_kind(stat.kind).to_owned(),
            }]);
        }
    }
    let maximum = maximum_entries.map(|maximum| maximum as usize);
    let mut entries = Vec::new();
    let mut pending = vec![start];
    while let Some(directory) = pending.pop() {
        let mut after = None;
        loop {
            let page = generation
                .list_directory(
                    &directory,
                    after.as_ref(),
                    1_024,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value.0)
                .map_err(|failure| failure.error)?;
            for entry in &page.entries {
                let name = entry
                    .name
                    .unicode_text(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new(),
                    )
                    .ok()
                    .map(|receipt| receipt.value)
                    .ok_or_else(|| WorkspaceError::Path("non-portable Git path".to_owned()))?;
                let parent = directory.trim_start_matches('/');
                let path = if parent.is_empty() {
                    name.to_string()
                } else {
                    format!("{parent}/{name}")
                };
                if host.reserves(&path) {
                    continue;
                }
                entries.push(GitTreeEntry {
                    path: path.clone(),
                    kind: git_kind(entry.kind).to_owned(),
                });
                if maximum.is_some_and(|maximum| entries.len() >= maximum) {
                    entries.sort_by(|left, right| left.path.cmp(&right.path));
                    return Ok(entries);
                }
                if entry.kind == FileKind::Directory {
                    pending.push(format!("/{path}"));
                }
            }
            if !page.has_more {
                break;
            }
            after = page.entries.last().map(|entry| entry.name.clone());
        }
    }
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(entries)
}

/// Searches UTF-8 regular files without consulting the host filesystem.
///
/// Paths in the host's namespaces are outside the repository and never read.
pub async fn grep_git_generation<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
>(
    generation: &Generation<A, O>,
    host: &GitIgnorePolicy,
    pattern: &str,
    root: Option<&str>,
    maximum_entries: impl Into<Option<u32>>,
    maximum_file_bytes: impl Into<Option<u64>>,
    maximum_matches: impl Into<Option<u32>>,
) -> Result<GitGrepResult, WorkspaceError> {
    let maximum_entries = maximum_entries.into();
    let maximum_file_bytes = maximum_file_bytes.into();
    let maximum_matches = maximum_matches.into();
    if root.is_some_and(|root| host.reserves(root)) {
        return Ok(GitGrepResult {
            matches: Vec::new(),
            truncated: false,
        });
    }
    if maximum_entries == Some(0) {
        return Ok(GitGrepResult {
            matches: Vec::new(),
            truncated: true,
        });
    }
    let cancellation = CancellationToken::new();
    let checkout = generation
        .workspace
        .checkout(
            GenerationSelector::Exact(generation.id()),
            CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let reader = checkout.pinned_reader().map_err(WorkspaceError::from)?;
    let limits = checkout.volume_config().limits;
    let root = root.unwrap_or("/");
    let root_path = customer_path(
        &format!("/{}", root.trim_start_matches('/')),
        checkout.volume_config(),
    )?;
    let root_display = root.trim_matches('/').to_owned();
    let maximum_entries = maximum_entries.map(|maximum| maximum as usize);
    let (regular, mut truncated) = resolved_git_grep_files(
        &reader,
        host,
        root_display,
        root_path,
        limits,
        maximum_entries,
        &cancellation,
    )
    .await?;
    let mut matches = Vec::new();
    let maximum_matches = maximum_matches.map(|maximum| maximum as usize);
    for batch in regular.chunks(GREP_READ_CONCURRENCY) {
        let mut scans: Vec<_> = batch.iter().map(|_| GrepScan::default()).collect();
        let allowance = maximum_matches.map(|maximum| maximum.saturating_sub(matches.len()));
        loop {
            let mut requests = Vec::with_capacity(batch.len());
            let mut selected = Vec::with_capacity(batch.len());
            for (index, (path, file)) in batch.iter().enumerate() {
                let scan = &mut scans[index];
                if maximum_file_bytes
                    .is_some_and(|maximum| file.description().logical_bytes > maximum)
                {
                    truncated = true;
                    continue;
                }
                let remaining = file.description().logical_bytes - scan.offset;
                if scan.invalid || remaining == 0 {
                    continue;
                }
                requests.push(ResolvedFileRangeReadRequest {
                    file,
                    range: ByteRange {
                        offset: scan.offset,
                        length: remaining.min(64 * 1024),
                    },
                });
                selected.push((index, path, remaining.min(64 * 1024)));
            }
            if requests.is_empty() {
                break;
            }
            let reads = reader
                .read_resolved_ranges(
                    &requests,
                    GREP_READ_CONCURRENCY,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map_err(|failure| WorkspaceError::from(failure.error))?
                .value;
            for ((index, path, length), read) in selected.into_iter().zip(reads) {
                if read.bytes.len() as u64 != length {
                    return Err(WorkspaceError::Engine(
                        "incomplete Git file range".to_owned(),
                    ));
                }
                let scan = &mut scans[index];
                scan.offset += read.bytes.len() as u64;
                scan.push(path, pattern, &read.bytes, allowance);
                if scan.offset == batch[index].1.description().logical_bytes
                    && !scan.pending.is_empty()
                {
                    scan.line(path, pattern, &[], false, allowance);
                }
            }
        }
        for scan in scans {
            for found in scan.matches {
                if maximum_matches.is_some_and(|maximum| matches.len() >= maximum) {
                    return Ok(GitGrepResult {
                        matches,
                        truncated: true,
                    });
                }
                matches.push(found);
            }
        }
    }

    Ok(GitGrepResult { matches, truncated })
}

// One unfinished line and the result, rather than a complete file body. A
// later invalid UTF-8 line discards earlier matches from this binary file.
#[derive(Default)]
struct GrepScan {
    offset: u64,
    number: u64,
    pending: Vec<u8>,
    matches: Vec<GitGrepMatch>,
    invalid: bool,
}

impl GrepScan {
    fn push(&mut self, path: &str, pattern: &str, mut bytes: &[u8], allowance: Option<usize>) {
        while let Some(newline) = bytes.iter().position(|byte| *byte == b'\n') {
            self.line(path, pattern, &bytes[..newline], true, allowance);
            if self.invalid {
                return;
            }
            bytes = &bytes[newline + 1..];
        }
        self.pending.extend_from_slice(bytes);
    }

    fn line(
        &mut self,
        path: &str,
        pattern: &str,
        bytes: &[u8],
        terminated: bool,
        allowance: Option<usize>,
    ) {
        let bytes = if self.pending.is_empty() {
            bytes
        } else {
            self.pending.extend_from_slice(bytes);
            &self.pending
        };
        let bytes = if terminated {
            bytes.strip_suffix(b"\r").unwrap_or(bytes)
        } else {
            bytes
        };
        let Ok(text) = std::str::from_utf8(bytes) else {
            self.invalid = true;
            self.matches.clear();
            self.pending.clear();
            return;
        };
        self.number += 1;
        // A finite result needs one extra match to distinguish truncation.
        // Continue validating the file before exposing any of its matches.
        if text.contains(pattern) && allowance.is_none_or(|maximum| self.matches.len() <= maximum) {
            self.matches.push(GitGrepMatch {
                path: path.to_owned(),
                line: self.number,
                text: text.to_owned(),
            });
        }
        self.pending.clear();
    }
}

/// Attributes UTF-8 lines across explicit compatibility history.
pub async fn blame_git_generations<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
>(
    history_newest_first: &[(GitCommit, Generation<A, O>)],
    host: &GitIgnorePolicy,
    path: &str,
    maximum_file_bytes: impl Into<Option<u64>>,
) -> Result<Vec<GitBlameLine>, WorkspaceError> {
    let maximum_file_bytes = maximum_file_bytes.into();
    let relative = path.trim_start_matches('/');
    let absolute = format!("/{relative}");
    // A commit recorded before its path became host-owned still never shows it.
    let in_repository = !host.reserves(relative);
    let mut current = Vec::<String>::new();
    let mut owners = Vec::<GitCommitId>::new();
    for (commit, generation) in history_newest_first.iter().rev() {
        let next = if in_repository && commit.tracked_paths.contains(relative) {
            let bytes = generation
                .read(
                    &absolute,
                    maximum_file_bytes,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)?;
            std::str::from_utf8(&bytes)
                .map_err(|_| WorkspaceError::Path("Git blame requires UTF-8 text".to_owned()))?
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let mut next_owners = Vec::with_capacity(next.len());
        for (index, line) in next.iter().enumerate() {
            next_owners.push(if current.get(index) == Some(line) {
                owners.get(index).copied().unwrap_or(commit.id)
            } else {
                commit.id
            });
        }
        current = next;
        owners = next_owners;
    }
    Ok(current
        .into_iter()
        .zip(owners)
        .enumerate()
        .map(|(index, (text, commit))| GitBlameLine {
            commit,
            line: u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1),
            text,
        })
        .collect())
}

pub(super) const fn git_kind(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Regular => "regular",
        FileKind::Directory => "directory",
        FileKind::SymbolicLink => "symbolic-link",
        FileKind::Fifo => "fifo",
        FileKind::Socket => "socket",
        FileKind::CharacterDevice => "character-device",
        FileKind::BlockDevice => "block-device",
        FileKind::ReparsePoint => "windows-reparse-point",
        FileKind::MountBoundary => "mount-boundary",
    }
}

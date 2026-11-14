use super::*;

/// Fail-closed unified-patch application error.
#[derive(Debug, Error)]
pub enum GitPatchError {
    /// Workspace access or publication failed.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// Patch bytes are not UTF-8 unified text.
    #[error("Git patch is not UTF-8 unified text")]
    NonText,
    /// Patch syntax is incomplete or unsupported.
    #[error("invalid Git unified patch: {0}")]
    Invalid(String),
    /// A hunk does not match the authenticated working generation.
    #[error("Git patch hunk does not apply to '{path}' at source line {line}")]
    Conflict {
        /// Portable path whose preimage did not match.
        path: String,
        /// One-based source line from the hunk header.
        line: usize,
    },
}

struct UnifiedPatch<'a> {
    old_path: Option<&'a str>,
    new_path: Option<&'a str>,
    hunks: Vec<UnifiedHunk<'a>>,
}

struct UnifiedHunk<'a> {
    old_start: usize,
    old_lines: usize,
    lines: Vec<&'a str>,
}

/// Applies a UTF-8 patch under one writer permit.
pub async fn apply_git_patch<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    workspace: &Workspace<A, O>,
    patch: &[u8],
    idempotency_key: IdempotencyKey,
    permit: crate::PublicationPermit,
) -> Result<TransactionCommit<A, O>, GitPatchError> {
    if let Some(generation) = workspace
        .operation_generation(
            idempotency_key,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?
    {
        return Ok(TransactionCommit::AlreadyCommitted(generation));
    }
    let head = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    apply_git_patch_at(workspace, patch, &head, idempotency_key, permit).await
}

// The preimage generation is also the transaction base. A concurrent writer
// between head acquisition and checkout must enter the normal rebase fence.
pub(super) async fn apply_git_patch_at<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
>(
    workspace: &Workspace<A, O>,
    patch: &[u8],
    head: &Generation<A, O>,
    idempotency_key: IdempotencyKey,
    permit: crate::PublicationPermit,
) -> Result<TransactionCommit<A, O>, GitPatchError> {
    let patches = parse_unified_patch(patch)?;
    let mut transaction = workspace
        .begin_transaction_at(
            head,
            idempotency_key,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    for patch in patches {
        let path = patch
            .new_path
            .or(patch.old_path)
            .ok_or_else(|| GitPatchError::Invalid("patch has no path".to_owned()))?;
        let original = if patch.old_path.is_none() {
            acyclic_stream::OwnedBytes::from_static(b"")
        } else {
            let length = head
                .stat(
                    &format!("/{path}"),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| failure.error)?
                .value
                .logical_bytes
                .ok_or_else(|| GitPatchError::Invalid("patch preimage is not a file".to_owned()))?;
            head.read(
                &format!("/{path}"),
                length,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?
        };
        let original = std::str::from_utf8(&original).map_err(|_| GitPatchError::NonText)?;
        let replacement = apply_unified_hunks(path, original, &patch.hunks)?;
        if patch.new_path.is_none() {
            if !replacement.is_empty() {
                return Err(GitPatchError::Invalid(
                    "deleted-file patch leaves content".to_owned(),
                ));
            }
            transaction
                .remove(
                    &format!("/{path}"),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)?;
            continue;
        }
        if let Some((parent, _)) = path.rsplit_once('/') {
            transaction
                .create_dir_all(
                    &format!("/{parent}"),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)?;
        }
        transaction
            .write_text(&format!("/{path}"), &replacement)
            .await?;
    }
    transaction
        .commit(
            None,
            permit,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            |_| {
                std::future::ready(Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(
                    crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    },
                ))
            },
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .map_err(Into::into)
}

fn parse_unified_patch(bytes: &[u8]) -> Result<Vec<UnifiedPatch<'_>>, GitPatchError> {
    let text = std::str::from_utf8(bytes).map_err(|_| GitPatchError::NonText)?;
    if text.contains("GIT binary patch") {
        return Err(GitPatchError::Invalid(
            "binary patches require a typed binary mutation".to_owned(),
        ));
    }
    let mut lines = text.split_inclusive('\n').peekable();
    let mut patches = Vec::new();
    while let Some(line) = lines.next() {
        if !line.starts_with("--- ") {
            continue;
        }
        let old_path = parse_patch_path(line.trim_end_matches(['\r', '\n']), "--- ")?;
        let Some(next) = lines.next() else {
            return Err(GitPatchError::Invalid("missing +++ header".to_owned()));
        };
        let new_path = parse_patch_path(next.trim_end_matches(['\r', '\n']), "+++ ")?;
        let mut hunks = Vec::new();
        while let Some(line) = lines.next_if(|line| !line.starts_with("--- ")) {
            if !line.starts_with("@@ ") {
                continue;
            }
            let (old_start, old_lines, new_lines) = parse_hunk_header(line)?;
            let (mut old_left, mut new_left) = (old_lines, new_lines);
            let mut hunk_lines: Vec<&str> = Vec::new();
            while old_left > 0 || new_left > 0 {
                let line = lines.next().ok_or_else(|| {
                    GitPatchError::Invalid("hunk is shorter than its header".to_owned())
                })?;
                let (old, new) = match line.as_bytes().first() {
                    Some(b'\\') => {
                        strip_final_newline(&mut hunk_lines)?;
                        continue;
                    }
                    Some(b' ') => (1, 1),
                    Some(b'-') => (1, 0),
                    Some(b'+') => (0, 1),
                    _ => {
                        return Err(GitPatchError::Invalid(
                            "hunk line lacks a unified-diff prefix".to_owned(),
                        ));
                    }
                };
                (old_left, new_left) = old_left
                    .checked_sub(old)
                    .zip(new_left.checked_sub(new))
                    .ok_or_else(|| {
                        GitPatchError::Invalid("hunk lines contradict its header".to_owned())
                    })?;
                hunk_lines.push(line);
            }
            if lines.next_if(|line| line.starts_with('\\')).is_some() {
                strip_final_newline(&mut hunk_lines)?;
            }
            // Git drops body lines past the counts, which would lose edits
            // without an error. `-- ` is the mail signature.
            if lines.peek().is_some_and(|line| {
                line.starts_with([' ', '+'])
                    || (line.starts_with('-')
                        && !line.starts_with("--- ")
                        && line.trim_end() != "--")
            }) {
                return Err(GitPatchError::Invalid(
                    "hunk is longer than its header".to_owned(),
                ));
            }
            hunks.push(UnifiedHunk {
                old_start,
                old_lines,
                lines: hunk_lines,
            });
        }
        if hunks.is_empty() {
            return Err(GitPatchError::Invalid("patch contains no hunks".to_owned()));
        }
        patches.push(UnifiedPatch {
            old_path,
            new_path,
            hunks,
        });
    }
    if patches.is_empty() {
        return Err(GitPatchError::Invalid("patch contains no files".to_owned()));
    }
    Ok(patches)
}

fn parse_patch_path<'a>(line: &'a str, prefix: &str) -> Result<Option<&'a str>, GitPatchError> {
    let value = line
        .strip_prefix(prefix)
        .ok_or_else(|| GitPatchError::Invalid(format!("missing {prefix}header")))?
        .split('\t')
        .next()
        .unwrap_or_default();
    if value == "/dev/null" {
        return Ok(None);
    }
    let value = value
        .strip_prefix("a/")
        .or_else(|| value.strip_prefix("b/"))
        .unwrap_or(value)
        .trim_matches('/');
    if value.contains(':') || !crate::path::is_canonical_relative(value) {
        return Err(GitPatchError::Invalid(
            "patch path escapes its root".to_owned(),
        ));
    }
    Ok(Some(value))
}

fn strip_final_newline(lines: &mut [&str]) -> Result<(), GitPatchError> {
    let previous = lines.last_mut().ok_or_else(|| {
        GitPatchError::Invalid("no-newline marker does not follow a hunk line".to_owned())
    })?;
    if let Some(line) = previous.strip_suffix('\n') {
        *previous = line.strip_suffix('\r').unwrap_or(line);
    }
    Ok(())
}

// `@@ -start[,lines] +start[,lines] @@`: an omitted count is one line.
fn parse_hunk_header(line: &str) -> Result<(usize, usize, usize), GitPatchError> {
    fn range(text: &str) -> Option<(usize, usize)> {
        match text.split_once(',') {
            Some((start, lines)) => Some((start.parse().ok()?, lines.parse().ok()?)),
            None => Some((text.parse().ok()?, 1)),
        }
    }
    let mut ranges = line
        .strip_prefix("@@ -")
        .ok_or_else(|| GitPatchError::Invalid("malformed hunk header".to_owned()))?
        .split_whitespace();
    let old = ranges.next().and_then(range);
    let new = ranges
        .next()
        .and_then(|text| text.strip_prefix('+'))
        .and_then(range);
    match (old, new) {
        (Some((old_start, old_lines)), Some((_, new_lines))) => {
            Ok((old_start, old_lines, new_lines))
        }
        _ => Err(GitPatchError::Invalid(
            "malformed hunk line range".to_owned(),
        )),
    }
}

fn apply_unified_hunks(
    path: &str,
    original: &str,
    hunks: &[UnifiedHunk<'_>],
) -> Result<String, GitPatchError> {
    let source: Vec<_> = original.split_inclusive('\n').collect();
    let mut output = String::new();
    let mut cursor = 0_usize;
    for hunk in hunks {
        // An empty source range names the line it follows, not its first line.
        let start = if hunk.old_lines == 0 {
            hunk.old_start
        } else {
            hunk.old_start.saturating_sub(1)
        };
        if start < cursor || start > source.len() {
            return Err(GitPatchError::Conflict {
                path: path.to_owned(),
                line: hunk.old_start,
            });
        }
        for line in source.get(cursor..start).unwrap_or_default() {
            output.push_str(line);
        }
        cursor = start;
        for line in &hunk.lines {
            let Some(prefix) = line.chars().next() else {
                continue;
            };
            let payload = line.get(prefix.len_utf8()..).unwrap_or_default();
            match prefix {
                ' ' | '-' => {
                    if source.get(cursor).copied() != Some(payload) {
                        return Err(GitPatchError::Conflict {
                            path: path.to_owned(),
                            line: cursor.saturating_add(1),
                        });
                    }
                    if prefix == ' ' {
                        output.push_str(payload);
                    }
                    cursor = cursor.saturating_add(1);
                }
                '+' => output.push_str(payload),
                _ => {
                    return Err(GitPatchError::Invalid(
                        "unsupported hunk line prefix".to_owned(),
                    ));
                }
            }
        }
    }
    for line in source.get(cursor..).unwrap_or_default() {
        output.push_str(line);
    }
    Ok(output)
}

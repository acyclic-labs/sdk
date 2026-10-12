use super::*;

/// Git-compatible ignore policy used by commit and parent-join capture.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitIgnorePolicy {
    rules: Vec<GitIgnoreRule>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct GitIgnoreRule {
    pattern: String,
    negated: bool,
    directory_only: bool,
    anchored: bool,
}

impl GitIgnorePolicy {
    /// Identifies changed, newly ignored paths whose child-side binding wins
    pub async fn newly_ignored_paths_at<A, O>(
        &self,
        generation: &Generation<A, O>,
        changes: &[crate::ChangedPath],
        already_tracked: &BTreeSet<String>,
    ) -> Result<BTreeSet<String>, GitCaptureError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let mut paths = BTreeSet::new();
        let mut nested_policies = BTreeMap::new();
        for change in changes {
            let path = portable_changed_path(&change.path)?;
            let is_directory = change
                .after
                .or(change.before)
                .is_some_and(|record| record.kind == FileKind::Directory);
            // A directory binding can contain tracked or independently changed
            // descendants. Child-wins publication is pathwise for newly ignored
            // files, never a replacement of the enclosing subtree.
            if is_directory {
                continue;
            }
            if !git_path_eligible_with_root_at(
                generation,
                &mut nested_policies,
                self,
                &path,
                is_directory,
                already_tracked,
            )
            .await?
            {
                paths.insert(format!("/{path}"));
            }
        }
        Ok(paths)
    }

    /// Parses Gitignore-shaped UTF-8 lines.
    #[must_use]
    pub fn parse(contents: &str) -> Self {
        let rules = contents
            .lines()
            .filter_map(|line| {
                let mut line = line.trim_end_matches('\r');
                if line.is_empty() || line.starts_with('#') {
                    return None;
                }
                let negated = line.starts_with('!');
                if negated {
                    line = line.strip_prefix('!').unwrap_or(line);
                }
                let anchored = line.starts_with('/');
                if anchored {
                    line = line.strip_prefix('/').unwrap_or(line);
                }
                let directory_only = line.ends_with('/');
                if directory_only {
                    line = line.trim_end_matches('/');
                }
                if line.is_empty() {
                    return None;
                }
                Some(GitIgnoreRule {
                    pattern: line.to_owned(),
                    negated,
                    directory_only,
                    anchored,
                })
            })
            .collect();
        Self { rules }
    }

    /// Returns whether a path participates in compatibility commit/join state.
    #[must_use]
    pub fn eligible(&self, path: &str, is_directory: bool, already_tracked: bool) -> bool {
        if already_tracked {
            return true;
        }
        let path = path.trim_start_matches('/');
        let mut ignored = false;
        for rule in &self.rules {
            if ignore_matches(rule, path, is_directory) {
                ignored = !rule.negated;
            }
        }
        !ignored
    }

    fn apply(&self, path: &str, is_directory: bool, ignored: &mut bool) {
        for rule in &self.rules {
            if ignore_matches(rule, path, is_directory) {
                *ignored = !rule.negated;
            }
        }
    }
}

fn wildcard_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let (mut pattern_index, mut value_index, mut star, mut checkpoint) = (0, 0, None, 0);
    while value_index < value.len() {
        if pattern.get(pattern_index).is_some_and(|byte| {
            *byte == b'?' || value.get(value_index).is_some_and(|value| value == byte)
        }) {
            pattern_index += 1;
            value_index += 1;
        } else if pattern.get(pattern_index) == Some(&b'*') {
            star = Some(pattern_index);
            pattern_index += 1;
            checkpoint = value_index;
        } else if let Some(star_index) = star {
            pattern_index = star_index + 1;
            checkpoint += 1;
            value_index = checkpoint;
        } else {
            return false;
        }
    }
    while pattern.get(pattern_index) == Some(&b'*') {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

pub(super) fn git_path_eligible(
    root: &GitIgnorePolicy,
    nested: &BTreeMap<String, GitIgnorePolicy>,
    path: &str,
    is_directory: bool,
    already_tracked: bool,
) -> bool {
    if already_tracked {
        return true;
    }
    let path = path.trim_start_matches('/');
    let mut ignored = false;
    root.apply(path, is_directory, &mut ignored);
    // Matching prefixes are ordered parent before child.
    for (directory, policy) in nested {
        if let Some(relative) = path
            .strip_prefix(directory)
            .and_then(|suffix| suffix.strip_prefix('/'))
        {
            policy.apply(relative, is_directory, &mut ignored);
        }
    }
    !ignored
}

fn ignore_matches(rule: &GitIgnoreRule, path: &str, is_directory: bool) -> bool {
    let matches = |candidate: &str| {
        if rule.anchored || rule.pattern.contains('/') {
            wildcard_matches(&rule.pattern, candidate)
        } else {
            candidate
                .split('/')
                .any(|component| wildcard_matches(&rule.pattern, component))
        }
    };
    if rule.directory_only {
        path.match_indices('/')
            .map(|(end, _)| &path[..end])
            .chain(is_directory.then_some(path))
            .any(matches)
    } else {
        matches(path)
    }
}

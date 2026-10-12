use super::*;

/// Git-compatible ignore policy used by commit and parent-join capture.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitIgnorePolicy {
    pub(super) rules: Vec<GitIgnoreRule>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct GitIgnoreRule {
    pub(super) pattern: String,
    pub(super) negated: bool,
    pub(super) directory_only: bool,
    pub(super) anchored: bool,
}

impl GitIgnorePolicy {
    /// Identifies changed, newly ignored paths whose child binding wins.
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
            let is_directory = change.after.or(change.before).is_some_and(|record| record.kind == FileKind::Directory);
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
                Some(GitIgnoreRule { pattern: line.to_owned(), negated, directory_only, anchored })
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
        self.apply(path, is_directory, &mut ignored);
        !ignored
    }

    pub(super) fn apply(&self, path: &str, is_directory: bool, ignored: &mut bool) {
        for rule in &self.rules {
            if ignore_matches(rule, path, is_directory) {
                *ignored = !rule.negated;
            }
        }
    }
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
    let mut applicable = nested
        .iter()
        .filter_map(|(directory, policy)| {
            path.strip_prefix(directory)
                .and_then(|suffix| suffix.strip_prefix('/'))
                .map(|relative| (directory.split('/').count(), relative, policy))
        })
        .collect::<Vec<_>>();
    applicable.sort_by_key(|(depth, _, _)| *depth);
    for (_, relative, policy) in applicable {
        policy.apply(relative, is_directory, &mut ignored);
    }
    !ignored
}

pub(super) fn ignore_matches(rule: &GitIgnoreRule, path: &str, is_directory: bool) -> bool {
    if rule.directory_only {
        let components: Vec<_> = path.split('/').collect();
        let directory_count = if is_directory { components.len() } else { components.len().saturating_sub(1) };
        return (1..=directory_count).any(|end| {
            let prefix = components.get(..end).unwrap_or_default();
            let directory = prefix.join("/");
            if rule.anchored || rule.pattern.contains('/') {
                wildcard_matches(&rule.pattern, &directory)
            } else {
                prefix.iter().any(|component| wildcard_matches(&rule.pattern, component))
            }
        });
    }
    if rule.anchored || rule.pattern.contains('/') {
        wildcard_matches(&rule.pattern, path)
    } else {
        path.split('/').any(|component| wildcard_matches(&rule.pattern, component))
    }
}

pub(super) fn wildcard_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let (mut pattern_index, mut value_index, mut star, mut checkpoint) = (0, 0, None, 0);
    while value_index < value.len() {
        if pattern
            .get(pattern_index)
            .is_some_and(|byte| *byte == b'?' || value.get(value_index).is_some_and(|value| value == byte))
        {
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

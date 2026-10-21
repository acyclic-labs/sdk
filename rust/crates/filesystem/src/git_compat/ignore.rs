use super::*;

/// Git-compatible ignore policy used by commit and parent-join capture.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitIgnorePolicy {
    rules: Vec<GitIgnoreRule>,
    /// Exact repository-relative paths owned by the host, never Git content.
    reserved: BTreeSet<String>,
    /// Root names the host owns; they are outside the repository entirely,
    /// like reserved paths, but joins bind them by their ignore rules.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    host_namespaces: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct GitIgnoreRule {
    pattern: String,
    negated: bool,
    directory_only: bool,
    anchored: bool,
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
        // Joins bind host namespaces by their ignore rules alone, exactly as
        // before they were kept out of the repository view: the
        // `.acyclic-sdk/` and `.git/` rules make the child win there.
        let rules = Self {
            rules: self.rules.clone(),
            reserved: self.reserved.clone(),
            host_namespaces: BTreeSet::new(),
        };
        let mut paths = BTreeSet::new();
        let mut nested_policies = BTreeMap::new();
        for change in changes {
            let path = portable_changed_path(&change.path)?;
            let is_directory = change
                .after
                .or(change.before)
                .is_some_and(|record| record.kind == FileKind::Directory);
            // Directory bindings can contain tracked descendants.
            if is_directory {
                continue;
            }
            if !git_path_eligible_with_root_at(
                generation,
                &mut nested_policies,
                &rules,
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
        Self {
            rules,
            reserved: BTreeSet::new(),
            host_namespaces: BTreeSet::new(),
        }
    }

    /// Reserves exact repository-relative paths, and everything beneath
    /// them, for the host. A reserved path is never Git content: no ignore
    /// negation, nested `.gitignore`, or earlier tracking re-admits it, and
    /// its name is matched byte for byte rather than as a pattern.
    #[must_use]
    pub fn reserving<I, S>(mut self, paths: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.reserved.extend(
            paths
                .into_iter()
                .map(|path| path.as_ref().trim_matches('/').to_owned())
                .filter(|path| !path.is_empty()),
        );
        self
    }

    /// Returns this policy followed by a later, higher-precedence one, as
    /// Git layers a root `.gitignore` over `info/exclude`. Reservations of
    /// both are kept.
    #[must_use]
    pub fn followed_by(&self, later: &Self) -> Self {
        Self {
            rules: self.rules.iter().chain(&later.rules).cloned().collect(),
            reserved: self.reserved.union(&later.reserved).cloned().collect(),
            host_namespaces: self
                .host_namespaces
                .union(&later.host_namespaces)
                .cloned()
                .collect(),
        }
    }

    /// The exact reserved host paths, without leading or trailing slashes.
    pub fn reserved_paths(&self) -> impl Iterator<Item = &str> {
        self.reserved.iter().map(String::as_str)
    }

    /// Places root namespaces the host owns outside the repository.
    ///
    /// Like a reserved path, no negation, nested policy, or tracked path can
    /// bring a host-owned path into the repository view.
    #[must_use]
    pub fn with_host_namespaces<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.host_namespaces
            .extend(names.into_iter().map(Into::into));
        self
    }

    /// Returns whether a tracked set still names a reserved or host-owned
    /// path.
    pub(super) fn tracks_reserved(&self, tracked: &BTreeSet<String>) -> bool {
        self.reserved
            .iter()
            .chain(&self.host_namespaces)
            .any(|name| {
                let prefix = format!("{name}/");
                tracked.contains(name)
                    || tracked
                        .range(prefix.clone()..)
                        .next()
                        .is_some_and(|path| path.starts_with(&prefix))
            })
    }

    /// Returns whether a path is, or lies beneath, a reserved host path or a
    /// host-owned root namespace.
    #[must_use]
    pub fn reserves(&self, path: &str) -> bool {
        let path = path.trim_start_matches('/');
        let root = path.split_once('/').map_or(path, |(root, _)| root);
        self.host_namespaces.contains(root)
            || self.reserved.iter().any(|reserved| {
                path.strip_prefix(reserved.as_str())
                    .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
            })
    }

    /// Returns whether a path participates in compatibility commit/join state.
    #[must_use]
    pub fn eligible(&self, path: &str, is_directory: bool, already_tracked: bool) -> bool {
        if self.reserves(path) {
            return false;
        }
        if already_tracked {
            return true;
        }
        let path = path.trim_start_matches('/');
        let mut ignored = false;
        self.apply(path, is_directory, &mut ignored);
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
    if root.reserves(path) {
        return false;
    }
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
        let mut candidate = if is_directory {
            Some(path)
        } else {
            path.rsplit_once('/').map(|(parent, _)| parent)
        };
        while let Some(parent) = candidate {
            if matches(parent) {
                return true;
            }
            candidate = parent.rsplit_once('/').map(|(parent, _)| parent);
        }
        false
    } else {
        matches(path)
    }
}

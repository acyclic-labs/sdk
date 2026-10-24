use super::*;
mod wildmatch;

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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GitIgnoreRule {
    pattern: String,
    negated: bool,
    directory_only: bool,
    anchored: bool,
    #[serde(skip)]
    compiled: std::sync::OnceLock<Vec<wildmatch::Token>>,
}

impl PartialEq for GitIgnoreRule {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern
            && self.negated == other.negated
            && self.directory_only == other.directory_only
            && self.anchored == other.anchored
    }
}
impl Eq for GitIgnoreRule {}

impl GitIgnorePolicy {
    /// Search policies at the volume root, from lowest to highest precedence.
    /// Repository metadata is read only through the selected volume capability.
    pub const SEARCH_ROOT_FILES: [&'static str; 3] = [".git/info/exclude", ".gitignore", ".ignore"];

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
                // Git discards unescaped trailing spaces. A space preceded by
                // an odd run of backslashes belongs to the pattern.
                while line.ends_with(' ') {
                    let escapes = line.as_bytes()[..line.len() - 1]
                        .iter()
                        .rev()
                        .take_while(|byte| **byte == b'\\')
                        .count();
                    if escapes % 2 == 1 {
                        break;
                    }
                    line = &line[..line.len() - 1];
                }
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
                    compiled: std::sync::OnceLock::new(),
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
        let mut policy = self.clone();
        policy.extend(later.clone());
        policy
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

    /// Adds a later policy at the same directory, preserving rule precedence.
    pub fn extend(&mut self, policy: Self) {
        self.rules.extend(policy.rules);
        self.reserved.extend(policy.reserved);
        self.host_namespaces.extend(policy.host_namespaces);
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
        // A child cannot be re-included while one of its parents is ignored.
        // This matters for explicit subtree search roots as well as walking.
        for end in path
            .match_indices('/')
            .map(|(end, _)| end)
            .chain([path.len()])
        {
            let mut ignored = false;
            self.apply(
                &path[..end],
                end != path.len() || is_directory,
                &mut ignored,
            );
            if ignored {
                return false;
            }
        }
        true
    }

    /// Applies directory-local policies in ancestor order. Each policy's
    /// patterns are relative to the directory containing its ignore file.
    #[must_use]
    pub fn eligible_with_nested(
        &self,
        nested: &BTreeMap<String, Self>,
        path: &str,
        is_directory: bool,
    ) -> bool {
        git_path_eligible(self, nested, path, is_directory, false)
    }

    pub(crate) fn eligible_with_lookup<'a>(
        &self,
        lookup: impl Fn(&str) -> Option<&'a Self>,
        path: &str,
        is_directory: bool,
    ) -> bool {
        eligible_with_lookup(self, lookup, path, is_directory)
    }

    fn apply(&self, path: &str, is_directory: bool, ignored: &mut bool) {
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
    if root.reserves(path) {
        return false;
    }
    if already_tracked {
        return true;
    }
    eligible_with_lookup(root, |key| nested.get(key), path, is_directory)
}

fn eligible_with_lookup<'a>(
    root: &GitIgnorePolicy,
    lookup: impl Fn(&str) -> Option<&'a GitIgnorePolicy>,
    path: &str,
    is_directory: bool,
) -> bool {
    let path = path.trim_start_matches('/');
    for end in path
        .match_indices('/')
        .map(|(end, _)| end)
        .chain([path.len()])
    {
        let candidate = &path[..end];
        let directory = end != path.len() || is_directory;
        let mut ignored = false;
        root.apply(candidate, directory, &mut ignored);
        // Look up just this path's ancestors. Scanning every previously read
        // ignore directory for every file makes a large walk quadratic.
        for (split, _) in candidate.match_indices('/') {
            if let Some(policy) = lookup(&candidate[..split]) {
                policy.apply(&candidate[split + 1..], directory, &mut ignored);
            }
        }
        if ignored {
            return false;
        }
    }
    true
}

fn ignore_matches(rule: &GitIgnoreRule, path: &str, is_directory: bool) -> bool {
    let compiled = rule
        .compiled
        .get_or_init(|| wildmatch::tokens(rule.pattern.as_bytes()));
    let matches = |candidate: &str| {
        if rule.anchored || rule.pattern.contains('/') {
            wildmatch::matches(compiled, candidate)
        } else {
            candidate
                .split('/')
                .any(|component| wildmatch::matches(compiled, component))
        }
    };
    {
        let mut candidate = if !rule.directory_only || is_directory {
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
    }
}

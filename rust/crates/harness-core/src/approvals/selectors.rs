use super::*;

/// A typed predicate on the exact admitted argument object. These selectors
/// restrict approval; they never grant filesystem or workload authority.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApprovalSelector {
    /// Exact value at a JSON pointer, for example the declared volume identity.
    Equal {
        /// RFC 6901 pointer into the admitted arguments.
        pointer: String,
        /// Required value; missing fields never match, even when this is null.
        #[cfg_attr(feature = "wasm", tsify(type = "unknown"))]
        value: Value,
    },
    /// Anchored string pattern, for example a shell command. Only `*` is a
    /// wildcard; every other character is literal. It never executes a shell.
    Pattern {
        /// RFC 6901 pointer into the admitted arguments.
        pointer: String,
        /// Full-string pattern, with zero or more `*` wildcards.
        pattern: String,
    },
    /// Same path or a descendant, using the filesystem's canonical portable
    /// path grammar. Relative paths are anchored at the volume root.
    PathWithin {
        /// RFC 6901 pointer into the admitted arguments.
        pointer: String,
        /// Canonical logical root; traversal and native separators are refused.
        root: String,
    },
}

impl ApprovalSelector {
    pub(super) fn validate(&self) -> Result<()> {
        let pointer = match self {
            Self::Equal { pointer, .. }
            | Self::Pattern { pointer, .. }
            | Self::PathWithin { pointer, .. } => pointer,
        };
        if (!pointer.is_empty() && !pointer.starts_with('/'))
            || pointer
                .split('~')
                .skip(1)
                .any(|suffix| !suffix.starts_with(['0', '1']))
        {
            return Err(Error::Invalid(
                "approval selector requires an RFC 6901 pointer".into(),
            ));
        }
        if let Self::PathWithin { root, .. } = self {
            portable_path(root)
                .ok_or_else(|| Error::Invalid("approval path root is not canonical".into()))?;
        }
        Ok(())
    }

    pub(super) fn matches(&self, arguments: &Value) -> bool {
        match self {
            Self::Equal { pointer, value } => arguments.pointer(pointer) == Some(value),
            Self::Pattern { pointer, pattern } => arguments
                .pointer(pointer)
                .and_then(Value::as_str)
                .is_some_and(|text| pattern_matches(pattern, text)),
            Self::PathWithin { pointer, root } => {
                let path = arguments
                    .pointer(pointer)
                    .and_then(Value::as_str)
                    .and_then(portable_path);
                path.zip(portable_path(root))
                    .is_some_and(|(path, root)| path.is_within(&root))
            }
        }
    }
}

fn portable_path(path: &str) -> Option<acyclic_fs::path::PortablePath> {
    if path.is_empty() || path.contains('\\') {
        return None;
    }
    let absolute = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };
    acyclic_fs::path::PortablePath::parse(&absolute, acyclic_fs::VolumeLimits::default()).ok()
}

// Each literal search consumes its matched prefix. Unlike regex backtracking,
// later stars never reconsider already-consumed input or expand an execution tree.
fn pattern_matches(pattern: &str, mut text: &str) -> bool {
    let mut parts = pattern.split('*').peekable();
    let Some(first) = parts.next() else {
        return text.is_empty();
    };
    if parts.peek().is_none() {
        return first == text;
    }
    let Some(rest) = text.strip_prefix(first) else {
        return false;
    };
    text = rest;
    while let Some(literal) = parts.next() {
        if parts.peek().is_none() {
            return text.ends_with(literal);
        }
        let Some((_, rest)) = text.split_once(literal) else {
            return false;
        };
        text = rest;
    }
    false
}

#[cfg(test)]
mod tests;

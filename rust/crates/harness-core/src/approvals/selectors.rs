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

    #[cfg(test)]
    pub(super) fn matches(&self, arguments: &Value) -> bool {
        self.matches_with_io(arguments, None).unwrap_or(false)
    }

    pub(super) fn matches_with_io(
        &self,
        arguments: &Value,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<bool> {
        super::io::step(io, 1)?;
        Ok(match self {
            Self::Equal { pointer, value } => selected(arguments, pointer, io)? == Some(value),
            Self::Pattern { pointer, pattern } => selected(arguments, pointer, io)?
                .and_then(Value::as_str)
                .is_some_and(|text| pattern_matches(pattern, text)),
            Self::PathWithin { pointer, root } => {
                let path = match selected(arguments, pointer, io)?.and_then(Value::as_str) {
                    Some(path) => portable_path_with_io(path, io)?,
                    None => None,
                };
                path.zip(portable_path_with_io(root, io)?)
                    .is_some_and(|(path, root)| path.is_within(&root))
            }
        })
    }
}

// The canonical serde pointer implementation decodes each segment through two
// string replacements. Fund those original temporary allocations before entry;
// keep its one grammar and object/array traversal implementation.
fn selected<'a>(
    arguments: &'a Value,
    pointer: &str,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Option<&'a Value>> {
    crate::executor::with_history_io(io, |work| {
        if let Some(work) = work.as_deref_mut() {
            work.check_cancellation()?;
            work.codec_charge(crate::executor::json::JsonCodecCharge::Work {
                bytes: pointer.len() as u64,
                items: 1,
            })?;
            for token in pointer.split('/').skip(1) {
                let bytes = token.len() as u64;
                if bytes != 0 {
                    for _ in 0..2 {
                        work.retain_owned(bytes)?;
                        work.allocation_attempt(bytes)?;
                        work.copy_bytes(bytes)?;
                    }
                }
            }
        }
        Ok::<_, Error>(())
    })?;
    Ok(arguments.pointer(pointer))
}

fn portable_path(path: &str) -> Option<acyclic_fs::path::PortablePath> {
    portable_path_with_io(path, None).ok().flatten()
}

fn portable_path_with_io(
    path: &str,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Option<acyclic_fs::path::PortablePath>> {
    super::io::step(io, path.len() as u64)?;
    if path.is_empty() || path.contains('\\') {
        return Ok(None);
    }
    let absolute = if path.starts_with('/') {
        super::io::copy(path, io)?
    } else {
        super::io::text(format_args!("/{path}"), io)?
    };
    // PortablePath validates borrowed text then copies that text once.
    crate::executor::with_history_io(io, |work| {
        if let Some(work) = work.as_deref_mut() {
            work.retain_owned(absolute.len() as u64)?;
            work.allocation_attempt(absolute.len() as u64)?;
            work.copy_bytes(absolute.len() as u64)?;
        }
        Ok::<_, Error>(())
    })?;
    Ok(acyclic_fs::path::PortablePath::parse(&absolute, acyclic_fs::VolumeLimits::default()).ok())
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

//! Host-independent identity of a documentation input by its path.
//!
//! A rustdoc receipt records the paths of the checkout that produced it. Replaying that receipt on
//! another host must name the same source, so identity is derived from path components rather than
//! from the host's own path rules.

/// Formats a path lossily with forward slashes, without resolving or validating it.
pub fn normalize_path(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// Splits a path into its root marker and components, or `None` when it cannot be replayed.
///
/// Compare path components explicitly so Windows Rustdoc paths can be replayed on any host. Device
/// namespaces other than verbatim drive/UNC remain distinct.
pub fn components(path: &str, windows: bool) -> Option<Vec<String>> {
    let mut path = if windows {
        path.replace('\\', "/")
    } else {
        path.to_owned()
    };
    if windows {
        if let Some(rest) = path.strip_prefix("//?/") {
            if rest
                .get(..4)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("UNC/"))
            {
                if let Some(tail) = rest.get(4..) {
                    path = format!("//{tail}");
                }
            } else if rest.as_bytes().get(1) == Some(&b':') {
                path = rest.to_owned();
            }
        }
    }
    let root = if windows && path.starts_with("//") {
        "unc"
    } else if windows && path.as_bytes().get(1) == Some(&b':') {
        if path.as_bytes().get(2) == Some(&b'/') {
            "drive-root"
        } else {
            "drive-relative"
        }
    } else if path.starts_with('/') {
        "root"
    } else {
        "relative"
    };
    let mut parts = vec![root.to_owned()];
    for part in path
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
    {
        if part == ".." {
            let protected = match root {
                "unc" => 3,
                "drive-root" => 2,
                _ => 1,
            };
            if parts.len() <= protected {
                return None;
            }
            parts.pop();
            continue;
        }
        if windows
            && part.contains(':')
            && !(parts.len() == 1
                && part.len() == 2
                && part.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
                && matches!(root, "drive-root" | "drive-relative"))
        {
            return None;
        }
        parts.push(part.to_owned());
    }
    Some(parts)
}

/// Names a recorded filename by its identity under `root`, independent of the reading host.
pub fn stable_filename(filename: &str, root: &str) -> String {
    // The checkout selects the path flavor. A backslash is a legal POSIX
    // filename character, even when a receipt is read on a Windows host.
    let windows =
        root.as_bytes().get(1) == Some(&b':') || root.starts_with("\\\\") || root.starts_with("//");
    let Some(parts) = components(filename, windows) else {
        return filename.to_owned();
    };
    if matches!(
        parts.first().map(String::as_str),
        Some("relative" | "drive-relative")
    ) {
        return filename.to_owned();
    }
    if let Some(rebased) = rebase(filename, root, "/<root>", windows) {
        if let Some(suffix) = rebased.strip_prefix("/<root>") {
            return format!("<root>/{}", suffix.strip_prefix('/').unwrap_or(suffix));
        }
    }
    let leaf = parts.last().map_or("generated-source", String::as_str);
    format!("<external>/{leaf}")
}

/// Relocates `path` from `old` to `new`, or `None` when it is not owned by `old`.
pub fn rebase(path: &str, old: &str, new: &str, windows: bool) -> Option<String> {
    let parts = components(path, windows)?;
    let prefix = components(old, windows)?;
    let destination = components(new, windows)?;
    let prefix_root = prefix.first()?;
    let destination_root = destination.first()?;
    if [prefix_root.as_str(), destination_root.as_str()]
        .iter()
        .any(|root| matches!(*root, "relative" | "drive-relative"))
    {
        return None;
    }
    if parts.len() < prefix.len()
        || !parts.iter().zip(&prefix).all(|(a, b)| {
            if windows {
                a.eq_ignore_ascii_case(b)
            } else {
                a == b
            }
        })
    {
        return None;
    }
    let destination = if windows {
        new.replace('\\', "/")
    } else {
        new.to_owned()
    };
    let suffix = parts.get(prefix.len()..)?;
    Some(if suffix.is_empty() {
        destination
    } else {
        format!("{}/{}", destination.trim_end_matches('/'), suffix.join("/"))
    })
}

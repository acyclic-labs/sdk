use std::path::Path;

pub(super) fn matches(key: &str, configured: &Path, canonical: &Path) -> bool {
    [configured, canonical].into_iter().any(|path| {
        path.to_str()
            .is_some_and(|path| equivalent(key, path, cfg!(windows)))
    })
}

fn separator(byte: u8, windows: bool) -> bool {
    byte == b'/' || windows && byte == b'\\'
}

fn drive(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

// Keep roots distinct: a drive-relative path is never its absolute root, and
// a rooted local path is never a UNC path. Strip only recognized verbatim roots.
fn spelling(path: &str, windows: bool) -> (bool, bool, &str) {
    let bytes = path.as_bytes();
    if windows
        && bytes.len() >= 4
        && separator(bytes[0], true)
        && separator(bytes[1], true)
        && bytes[2] == b'?'
        && separator(bytes[3], true)
    {
        let rest = &path[4..];
        if rest
            .get(..3)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("UNC"))
            && rest
                .as_bytes()
                .get(3)
                .is_some_and(|byte| separator(*byte, true))
        {
            return (true, true, &rest[4..]);
        }
        if drive(rest)
            && rest
                .as_bytes()
                .get(2)
                .is_some_and(|byte| separator(*byte, true))
        {
            return (false, true, rest);
        }
    }
    let rooted = bytes.first().is_some_and(|byte| separator(*byte, windows))
        || windows && drive(path) && bytes.get(2).is_some_and(|byte| separator(*byte, true));
    let unc = windows && bytes.len() >= 2 && separator(bytes[0], true) && separator(bytes[1], true);
    (unc, rooted, path)
}

/// Compare configured spellings without allocating keys or resolving paths again.
pub(super) fn equivalent(left: &str, right: &str, windows: bool) -> bool {
    let (left_unc, left_rooted, left) = spelling(left, windows);
    let (right_unc, right_rooted, right) = spelling(right, windows);
    if (left_unc, left_rooted, left.is_empty()) != (right_unc, right_rooted, right.is_empty()) {
        return false;
    }
    let mut right = components(right, windows, right_unc);
    for left in components(left, windows, left_unc) {
        let Some(right) = right.next() else {
            return false;
        };
        if !(left == right
            || windows
                && (left.eq_ignore_ascii_case(right)
                    || left.chars().map(fold_case).eq(right.chars().map(fold_case))))
        {
            return false;
        }
    }
    right.next().is_none()
}

fn fold_case(character: char) -> char {
    let mut upper = character.to_uppercase();
    let first = upper.next().unwrap_or(character);
    // Native path case comparison does not expand one character into several.
    if upper.next().is_none() {
        first
    } else {
        character
    }
}

fn components(path: &str, windows: bool, unc: bool) -> impl Iterator<Item = &str> {
    path.split(move |character| character == '/' || windows && character == '\\')
        .filter(|component| !component.is_empty())
        .enumerate()
        .filter_map(move |(index, component)| {
            (component != "." || unc && index < 2).then_some(component)
        })
}

#[cfg(test)]
mod tests;

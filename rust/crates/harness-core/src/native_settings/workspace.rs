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
    matches!(path.as_bytes(), [letter, b':', ..] if letter.is_ascii_alphabetic())
}

// Keep roots distinct: a drive-relative path is never its absolute root, and
// a rooted local path is never a UNC path. Strip only recognized verbatim roots.
fn spelling(path: &str, windows: bool) -> (bool, bool, &str) {
    let bytes = path.as_bytes();
    if windows
        && let Some((prefix, rest)) = path.split_at_checked(4)
        && matches!(prefix.as_bytes(), [first, second, b'?', last]
            if separator(*first, true) && separator(*second, true) && separator(*last, true))
    {
        if let Some((prefix, tail)) = rest.split_at_checked(4)
            && prefix
                .get(..3)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("UNC"))
            && prefix
                .as_bytes()
                .last()
                .is_some_and(|byte| separator(*byte, true))
        {
            return (true, true, tail);
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
    let unc = windows
        && matches!(bytes, [first, second, ..] if separator(*first, true) && separator(*second, true));
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

#[cfg(all(test, windows))]
mod tests;

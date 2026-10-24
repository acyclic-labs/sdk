//! Bounded-state wildmatch: stars never recurse or cross a path separator
//! unless the pattern contains a complete `**` path component.
#[derive(Clone, Debug)]
pub(super) enum Token {
    Byte(u8),
    Any,
    Star(bool),
    Directories,
    Class {
        negated: bool,
        ranges: Vec<(u8, u8)>,
    },
}

pub(super) fn tokens(pattern: &[u8]) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut at = 0;
    while at < pattern.len() {
        match pattern[at] {
            b'\\' if at + 1 < pattern.len() => {
                at += 1;
                tokens.push(Token::Byte(pattern[at]));
            }
            b'?' => tokens.push(Token::Any),
            b'*' => {
                let start = at;
                while pattern.get(at + 1) == Some(&b'*') {
                    at += 1;
                }
                let component = at > start
                    && (start == 0 || pattern[start - 1] == b'/')
                    && pattern.get(at + 1).is_none_or(|byte| *byte == b'/');
                if component && pattern.get(at + 1) == Some(&b'/') {
                    tokens.push(Token::Directories);
                    at += 1;
                } else {
                    tokens.push(Token::Star(component));
                }
            }
            b'[' => {
                if let Some((token, end)) = class(pattern, at + 1) {
                    tokens.push(token);
                    at = end;
                } else {
                    tokens.push(Token::Byte(b'['));
                }
            }
            byte => tokens.push(Token::Byte(byte)),
        }
        at += 1;
    }
    tokens
}

fn class(pattern: &[u8], mut at: usize) -> Option<(Token, usize)> {
    let negated = matches!(pattern.get(at), Some(b'!' | b'^'));
    at += usize::from(negated);
    let mut ranges = Vec::new();
    while at < pattern.len() {
        if pattern[at] == b']' && !ranges.is_empty() {
            return Some((Token::Class { negated, ranges }, at));
        }
        if pattern.get(at..at + 2) == Some(b"[:") {
            let end = pattern[at + 2..]
                .windows(2)
                .position(|bytes| bytes == b":]")?
                + at
                + 2;
            let kind = &pattern[at + 2..end];
            for byte in 0u8..=127 {
                let contains = match kind {
                    b"alnum" => byte.is_ascii_alphanumeric(),
                    b"alpha" => byte.is_ascii_alphabetic(),
                    b"blank" => matches!(byte, b' ' | b'\t'),
                    b"cntrl" => byte.is_ascii_control(),
                    b"digit" => byte.is_ascii_digit(),
                    b"graph" => byte.is_ascii_graphic(),
                    b"lower" => byte.is_ascii_lowercase(),
                    b"print" => byte.is_ascii_graphic() || byte == b' ',
                    b"punct" => byte.is_ascii_punctuation(),
                    b"space" => matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 11 | 12),
                    b"upper" => byte.is_ascii_uppercase(),
                    b"xdigit" => byte.is_ascii_hexdigit(),
                    _ => return None,
                };
                if contains {
                    ranges.push((byte, byte));
                }
            }
            at = end + 2;
            continue;
        }
        if pattern[at] == b'\\' && at + 1 < pattern.len() {
            at += 1;
        }
        let start = pattern[at];
        if pattern.get(at + 1) == Some(&b'-')
            && pattern.get(at + 2).is_some_and(|byte| *byte != b']')
        {
            at += 2;
            if pattern[at] == b'\\' && at + 1 < pattern.len() {
                at += 1;
            }
            ranges.push((start, pattern[at]));
        } else {
            ranges.push((start, start));
        }
        at += 1;
    }
    None
}

fn epsilon(tokens: &[Token], active: &mut [bool], boundary: bool) {
    for (at, token) in tokens.iter().enumerate() {
        if active[at]
            && (matches!(token, Token::Star(_)) || boundary && matches!(token, Token::Directories))
        {
            active[at + 1] = true;
        }
    }
}

pub(super) fn matches(tokens: &[Token], value: &str) -> bool {
    // Ordinary ignore rules use stack scratch. Long rules remain bounded by
    // the pattern length, with no recursion or exponential backtracking.
    let mut stack_active = [false; 128];
    let mut stack_next = [false; 128];
    let mut heap_active;
    let mut heap_next;
    let (mut active, mut next) = if tokens.len() < stack_active.len() {
        (
            &mut stack_active[..tokens.len() + 1],
            &mut stack_next[..tokens.len() + 1],
        )
    } else {
        heap_active = vec![false; tokens.len() + 1];
        heap_next = vec![false; tokens.len() + 1];
        (heap_active.as_mut_slice(), heap_next.as_mut_slice())
    };
    active[0] = true;
    epsilon(tokens, active, true);
    for byte in value.bytes() {
        next.fill(false);
        for (at, token) in tokens.iter().enumerate().filter(|(at, _)| active[*at]) {
            match token {
                Token::Byte(wanted) if *wanted == byte => next[at + 1] = true,
                Token::Any if byte != b'/' => next[at + 1] = true,
                Token::Star(crosses) if *crosses || byte != b'/' => next[at] = true,
                Token::Directories => next[at] = true,
                Token::Class { negated, ranges }
                    if byte != b'/'
                        && ranges
                            .iter()
                            .any(|(first, last)| (*first..=*last).contains(&byte))
                            != *negated =>
                {
                    next[at + 1] = true
                }
                _ => {}
            }
        }
        std::mem::swap(&mut active, &mut next);
        epsilon(tokens, active, byte == b'/');
    }
    active[tokens.len()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GitIgnorePolicy;

    #[test]
    fn name_search_wildmatch_obeys_path_components_and_git_escapes() {
        for (pattern, value, expected) in [
            ("a/*.rs", "a/deep/file.rs", false),
            ("a/?.rs", "a//.rs", false),
            ("a/**/file.rs", "a/file.rs", true),
            ("a/**/file.rs", "a/b/c/file.rs", true),
            ("**/file.rs", "notfile.rs", false),
            ("**/file.rs", "file.rs", true),
            ("**/a*b/c.rs", "x/ax/y/ab/c.rs", true),
            ("file[0-9].rs", "file4.rs", true),
            ("file[!0-9].rs", "file4.rs", false),
            ("file[!0-9].rs", "filea.rs", true),
            ("file[[:digit:]].rs", "file4.rs", true),
            ("file[[:digit:]].rs", "filex.rs", false),
            (r"\#file", "#file", true),
            (r"file\*", "file*", true),
            (r"file\*", "files", false),
        ] {
            assert_eq!(
                matches(&tokens(pattern.as_bytes()), value),
                expected,
                "{pattern} / {value}"
            );
        }
        let policy = GitIgnorePolicy::parse("trailing   \nspace\\ \n\\!literal\n/only-root\n");
        for name in ["trailing", "space ", "!literal", "only-root/child"] {
            assert!(!policy.eligible(name, false, false), "{name}");
        }
        assert!(policy.eligible("nested/only-root", false, false));
        // Negative control: the former slash-crossing star excludes a visible
        // descendant that a Git-compatible single star cannot match.
        let policy = GitIgnorePolicy::parse("a/*.rs\n");
        assert!(policy.eligible("a/deep/file.rs", false, false));
        assert!(!policy.eligible("a/file.rs", false, false));
        let excluded_parent = GitIgnorePolicy::parse("cache/\n!cache/keep.rs\n");
        assert!(!excluded_parent.eligible("cache/keep.rs", false, false));
        let included_parent = GitIgnorePolicy::parse("cache/\n!cache/\n!cache/keep.rs\n");
        assert!(included_parent.eligible("cache/keep.rs", false, false));
    }

    #[test]
    fn name_search_common_ignore_rules_allocate_once_and_survive_serialization() {
        let policy = GitIgnorePolicy::parse("src/**/file[0-9].rs\n\\#literal\n");
        assert!(!policy.eligible("src/deep/file4.rs", false, false));
        let memory = acyclic_test_support::measure(|| {
            for _ in 0..1000 {
                assert!(!policy.eligible("src/deep/file4.rs", false, false));
            }
        });
        assert_eq!(
            memory.count_total, 0,
            "compiled rules must not allocate per path"
        );
        let restored: GitIgnorePolicy =
            serde_json::from_slice(&serde_json::to_vec(&policy).unwrap()).unwrap();
        assert_eq!(
            restored, policy,
            "ephemeral compilation cannot change policy identity"
        );
        assert!(!restored.eligible("src/deep/file4.rs", false, false));
        assert_eq!(restored, policy);
    }
}

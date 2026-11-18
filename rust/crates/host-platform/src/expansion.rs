//! Single-pass native environment expansion shared by process adapters.
use std::{
    borrow::Cow,
    ffi::{OsStr, OsString},
};

/// One syntactically valid `${NAME}` or `${NAME:-fallback}` reference.
/// Callers decide which names and fallback forms their dialect permits.
#[derive(Clone, Copy)]
pub struct EnvironmentReference<'a> {
    /// Environment key, without expansion syntax.
    pub name: &'a str,
    /// Literal fallback text, never recursively expanded.
    pub fallback: Option<&'a str>,
}

/// Expands references once, preserving native strings. `None` passed to the
/// resolver denotes malformed syntax. `Ok(None)` preserves the original text;
/// this lets strict MCP and reserved-name hook dialects share one scanner.
pub fn expand_environment<'a, E>(
    value: &'a str,
    mut resolve: impl FnMut(Option<EnvironmentReference<'_>>) -> Result<Option<OsString>, E>,
) -> Result<Cow<'a, OsStr>, E> {
    let mut expanded: Option<OsString> = None;
    let mut consumed = 0;
    for (start, _) in value.match_indices("${") {
        if start < consumed {
            continue;
        }
        // Delimiter searches return UTF-8 boundaries; adding its two ASCII
        // bytes retains that boundary, including after non-ASCII text.
        let tail = value.split_at(start + 2).1;
        let end = tail.find('}');
        let reference = end.and_then(|end| {
            let body = tail.split_at(end).0;
            let (name, fallback) = body
                .split_once(":-")
                .map_or((body, None), |(name, fallback)| (name, Some(fallback)));
            (!name.is_empty() && !name.contains(['=', '\0']))
                .then_some(EnvironmentReference { name, fallback })
        });
        if let Some(replacement) = resolve(reference)? {
            let output = expanded.get_or_insert_with(OsString::new);
            output.push(value.split_at(consumed).1.split_at(start - consumed).0);
            output.push(replacement);
            consumed = start + 2 + end.unwrap_or(tail.len()) + usize::from(end.is_some());
        }
    }
    match expanded {
        Some(mut expanded) => {
            expanded.push(value.split_at(consumed).1);
            Ok(Cow::Owned(expanded))
        }
        None => Ok(Cow::Borrowed(OsStr::new(value))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn replacements_are_literal_single_pass_native_values(value in "[^\\x00]{0,128}") {
            let expected = format!("prefix {value} {value} suffix");
            let expanded = expand_environment("prefix ${KEY} ${KEY} suffix", |reference| {
                assert_eq!(reference.unwrap().name, "KEY");
                Ok::<_, ()>(Some(OsString::from(&value)))
            }).unwrap();
            prop_assert_eq!(&*expanded, OsStr::new(&expected));
        }
    }

    #[test]
    fn dialect_policy_preserves_unknown_references_or_rejects_malformed_syntax() {
        let literal = "${UNKNOWN} ${BROKEN";
        assert!(matches!(
            expand_environment::<()>(literal, |_| Ok(None)).unwrap(),
            Cow::Borrowed(_)
        ));
        for invalid in ["${BROKEN", "${}", "${A=B}"] {
            assert!(
                expand_environment(invalid, |reference| { reference.map(|_| None).ok_or(()) })
                    .is_err()
            );
        }
        let value = expand_environment("${ABSENT:-${LITERAL}}", |reference| {
            Ok::<_, ()>(reference.unwrap().fallback.map(OsString::from))
        })
        .unwrap();
        assert_eq!(&*value, OsStr::new("${LITERAL}"));
    }
}

use super::source_selection;
use sdk_docs::GeneratedSource;
use std::{collections::BTreeSet, path::Path};

fn identity(source: &GeneratedSource) -> (&Path, &Path, &str) {
    (&source.physical_path, &source.logical_path, &source.sha256)
}

/// Check that deduplication preserves every complete source identity.
///
/// # Panics
/// Panics if the admitted result violates the independent contract checks.
pub fn check(sources: &[GeneratedSource]) {
    let expected = sources.iter().map(identity).collect::<BTreeSet<_>>();
    let mut actual = sources.to_vec();
    actual.extend_from_slice(sources);
    source_selection::deduplicate(&mut actual);
    assert!(
        actual.iter().map(identity).eq(expected),
        "deduplication changed the complete source identities"
    );
}

/// Check basename aliases and conservative selection of uncertain spellings.
///
/// # Panics
/// Panics if the admitted result violates the independent contract checks.
pub fn check_selection(leaf: &str, other: &str, windows: bool) {
    let source = |physical: String, logical: u8| GeneratedSource {
        physical_path: physical.into(),
        logical_path: format!("publish/{logical}").into(),
        sha256: format!("sha256:{logical:064x}"),
    };
    let sources = vec![
        source(format!("first/{leaf}.rs"), 0),
        source(format!("second/{leaf}.rs"), 1),
        source(format!("third/{other}.json"), 2),
    ];
    let filenames = BTreeSet::from([format!("src/{leaf}.rs")]);
    assert!(
        source_selection::referenced(&sources, &filenames, windows)
            .iter()
            .eq(sources.iter().take(2)),
        "selection must keep every matching alias and exclude unrelated sources"
    );
    let posix = vec![
        source(format!(r"/checkout/{leaf}\{other}.rs"), 0),
        source(format!("/checkout/{leaf}/{other}.rs"), 1),
    ];
    assert!(
        source_selection::referenced(
            &posix,
            &BTreeSet::from([format!(r"src/{leaf}\{other}.rs")]),
            false
        )
        .iter()
        .eq(posix.iter().take(1)),
        "selection must preserve literal POSIX backslashes"
    );
    if windows {
        for spelling in [
            format!(r"C:\SRC\{}.RS", leaf.to_ascii_uppercase()),
            format!(r"\\?\C:\src\{leaf}.rs"),
        ] {
            assert!(
                source_selection::referenced(&sources, &BTreeSet::from([spelling]), true)
                    .iter()
                    .eq(sources.iter().take(2)),
                "selection must preserve Windows case and verbatim aliases"
            );
        }
        for spelling in [
            "C:/src/SHORT~1.RS",
            "C:/src/é.rs",
            "C:/src/file.rs.",
            "C:/src/file.rs ",
            "C:/src/file.rs:stream",
        ] {
            assert_eq!(
                source_selection::referenced(
                    &sources,
                    &BTreeSet::from([spelling.to_owned()]),
                    true
                ),
                sources,
                "selection must retain uncertain Windows references"
            );
            let uncertain = vec![source(spelling.to_owned(), 0)];
            assert_eq!(
                source_selection::referenced(&uncertain, &filenames, true),
                uncertain,
                "selection must retain uncertain Windows sources"
            );
        }
    }
    assert!(
        source_selection::referenced(&sources, &BTreeSet::new(), windows).is_empty(),
        "selection without references must be empty"
    );
}

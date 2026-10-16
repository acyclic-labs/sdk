use sdk_docs::GeneratedSource;
use std::{collections::BTreeSet, ffi::OsString, path::Path};

fn basename(path: &Path, windows: bool) -> Option<OsString> {
    let text = path.to_str()?;
    let name = if windows {
        text.rsplit(['\\', '/']).next()?
    } else {
        text.rsplit('/').next()?
    };
    (!name.is_empty()).then(|| name.into())
}

fn uncertain(name: &std::ffi::OsStr) -> bool {
    let bytes = name.as_encoded_bytes();
    !bytes.is_ascii()
        || bytes.iter().any(|byte| matches!(byte, b'~' | b':'))
        || bytes.last().is_some_and(|byte| matches!(byte, b'.' | b' '))
}

// The base projection has already attested the entire source inventory, which
// final bundling validates again. Variants need only referenced filenames, but
// retain ALL basename aliases: filtering must never hide an ambiguous fallback.
// Uncertain Windows spellings keep the full inventory (8.3, Unicode, ADS, etc.).
pub(crate) fn referenced(
    sources: &[GeneratedSource],
    filenames: &BTreeSet<String>,
    windows: bool,
) -> Vec<GeneratedSource> {
    if filenames.is_empty() {
        return Vec::new();
    }
    let names = filenames
        .iter()
        .map(|path| basename(Path::new(path), windows))
        .collect::<Vec<_>>();
    let all = names
        .iter()
        .any(|name| name.as_ref().is_none_or(|name| windows && uncertain(name)));
    sources
        .iter()
        .filter(|source| {
            let Some(name) = basename(&source.physical_path, windows) else {
                return true;
            };
            all || windows && uncertain(&name)
                || names.iter().flatten().any(|other| {
                    name == *other
                        || windows
                            && name
                                .as_encoded_bytes()
                                .eq_ignore_ascii_case(other.as_encoded_bytes())
                })
        })
        .cloned()
        .collect()
}

pub(crate) fn deduplicate(sources: &mut Vec<GeneratedSource>) {
    sources.sort_unstable_by(|a, b| {
        (&a.physical_path, &a.logical_path, &a.sha256).cmp(&(
            &b.physical_path,
            &b.logical_path,
            &b.sha256,
        ))
    });
    sources.dedup();
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn source(physical: impl Into<std::path::PathBuf>, logical: u8, digest: u8) -> GeneratedSource {
        GeneratedSource {
            physical_path: physical.into(),
            logical_path: format!("publish/{logical}").into(),
            sha256: format!("sha256:{digest:064x}"),
        }
    }

    proptest! {
        #[test]
        fn reference_selection_keeps_every_basename_alias_and_windows_uncertainty(
            leaf in "[a-z]{1,12}", other in "[a-z]{1,12}", windows in any::<bool>(),
        ) {
            let sources = vec![source(format!("first/{leaf}.rs"), 0, 0), source(format!("second/{leaf}.rs"), 1, 1), source(format!("third/{other}.json"), 2, 2)];
            let filenames = BTreeSet::from([format!("src/{leaf}.rs")]);
            let encoded = serde_json::to_vec(&filenames).unwrap();
            let decoded: BTreeSet<String> = serde_json::from_slice(&encoded).unwrap();
            prop_assert_eq!(referenced(&sources, &filenames, windows), referenced(&sources, &decoded, windows));
            let posix = vec![source(format!(r"/checkout/{leaf}\{other}.rs"), 0, 0), source(format!("/checkout/{leaf}/{other}.rs"), 1, 1)];
            prop_assert_eq!(referenced(&posix, &BTreeSet::from([format!(r"src/{leaf}\{other}.rs")]), false), posix[..1].to_vec());
            prop_assert_eq!(referenced(&sources, &filenames, windows), sources[..2].to_vec());
            if windows {
                for spelling in [format!(r"C:\SRC\{}.RS", leaf.to_ascii_uppercase()), format!(r"\\?\C:\src\{leaf}.rs")] {
                    prop_assert_eq!(referenced(&sources, &BTreeSet::from([spelling]), true), sources[..2].to_vec());
                }
                for spelling in ["C:/src/SHORT~1.RS", "C:/src/é.rs", "C:/src/file.rs.", "C:/src/file.rs ", "C:/src/file.rs:stream"] {
                    prop_assert_eq!(referenced(&sources, &BTreeSet::from([spelling.to_owned()]), true), sources.clone());
                }
            }
            prop_assert!(referenced(&sources, &BTreeSet::new(), windows).is_empty());
            // A first-match-only selector would retain one alias and fail this oracle.
            prop_assert_ne!(referenced(&sources, &filenames, windows), sources[..1].to_vec());
        }

        #[test]
        fn bundling_deduplicates_only_complete_physical_logical_and_digest_identity(
            keys in prop::collection::vec((0u8..4, 0u8..4, 0u8..4), 0..32),
        ) {
            let expected = keys.iter().copied().collect::<BTreeSet<_>>();
            let mut sources = keys.iter().map(|(p,l,d)| source(format!("physical/{p}"), *l, *d)).collect::<Vec<_>>();
            deduplicate(&mut sources);
            let identities = sources.iter().map(|source| (source.physical_path.clone(), source.logical_path.clone(), source.sha256.clone())).collect::<BTreeSet<_>>();
            prop_assert_eq!(sources.len(), expected.len());
            for (p,l,d) in expected {
                let item = source(format!("physical/{p}"), l, d);
                prop_assert!(identities.contains(&(item.physical_path, item.logical_path, item.sha256)));
            }
            let once = sources.clone(); deduplicate(&mut sources);
            prop_assert_eq!(sources, once);
        }
    }
}

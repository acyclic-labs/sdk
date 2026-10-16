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

#[derive(Eq, PartialEq)]
struct Name(OsString);

impl Ord for Name {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        #[cfg(test)]
        COMPARISONS.set(COMPARISONS.get() + 1);
        self.0.cmp(&other.0)
    }
}
impl PartialOrd for Name {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
#[cfg(test)]
thread_local! { static COMPARISONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

fn key(mut name: OsString, windows: bool) -> Name {
    if windows {
        name.make_ascii_lowercase();
    }
    Name(name)
}

// The base projection has already attested the entire source inventory, which
// final bundling validates again. Keep ALL basename aliases and conservatively
// retain uncertain Windows spellings (8.3, Unicode, ADS, etc.). Index once rather
// than scanning every reference for every inventory entry.
pub(crate) fn referenced(
    sources: &[GeneratedSource],
    filenames: &BTreeSet<String>,
    windows: bool,
) -> Vec<GeneratedSource> {
    select(sources, filenames, windows, BTreeSet::contains)
}

fn select(
    sources: &[GeneratedSource],
    filenames: &BTreeSet<String>,
    windows: bool,
    contains: impl Fn(&BTreeSet<Name>, &Name) -> bool,
) -> Vec<GeneratedSource> {
    if filenames.is_empty() {
        return Vec::new();
    }
    let mut names = BTreeSet::new();
    let mut all = false;
    for path in filenames {
        match basename(Path::new(path), windows) {
            Some(name) if !windows || !uncertain(&name) => {
                names.insert(key(name, windows));
            }
            _ => all = true,
        }
    }
    sources
        .iter()
        .filter(|source| {
            let Some(name) = basename(&source.physical_path, windows) else {
                return true;
            };
            all || windows && uncertain(&name) || contains(&names, &key(name, windows))
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
        // The real key comparator counts insertion AND lookup work. A nested
        // scan using the same selector must violate this logarithmic bound.
        #[test]
        fn reference_selection_work_is_logarithmic_per_entry(
            references in 128usize..256, inventory in 128usize..256,
            windows in any::<bool>(),
        ) {
            let filenames = (0..references).map(|i| format!("src/a{i:04}.rs")).collect();
            let sources = (0..inventory).map(|i| source(format!("out/Z{i:04}.RS"), 0, 0)).collect::<Vec<_>>();
            let bound = 4 * (references + inventory) * (references + 1).ilog2() as usize;
            COMPARISONS.set(0);
            prop_assert!(referenced(&sources, &filenames, windows).is_empty());
            prop_assert!(COMPARISONS.get() <= bound, "{} > {}", COMPARISONS.get(), bound);
            COMPARISONS.set(0);
            let broken = select(&sources, &filenames, windows, |names, name| {
                names.iter().any(|other| other.cmp(name).is_eq())
            });
            prop_assert!(broken.is_empty());
            prop_assert!(COMPARISONS.get() > bound, "linear negative control escaped: {} <= {}", COMPARISONS.get(), bound);
        }
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

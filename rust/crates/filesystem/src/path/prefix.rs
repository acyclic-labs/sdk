//! Component-prefix lookup over the shared byte index.

use super::PortablePath;
use acyclic_collections::BytePrefixIndex;

/// Portable component boundaries over the one shared prefix traversal.
pub(crate) struct PathPrefixIndex<V>(BytePrefixIndex<V>);

impl<V> Default for PathPrefixIndex<V> {
    fn default() -> Self {
        Self(BytePrefixIndex::default())
    }
}

impl<V> PathPrefixIndex<V> {
    pub(crate) fn insert(&mut self, path: PortablePath, value: V) -> Option<V> {
        self.0.insert(path.0.into_bytes(), value)
    }

    pub(crate) fn longest_prefix(&self, path: &PortablePath) -> Option<&V> {
        self.lookup(path, |_| {})
    }

    fn lookup(&self, path: &PortablePath, visit: impl FnMut(usize)) -> Option<&V> {
        let key = path.as_str().as_bytes();
        let mut selected = None;
        self.0.visit_prefixes(
            key,
            |length, value| {
                if length == 1 || length == key.len() || key.get(length) == Some(&b'/') {
                    selected = Some(value);
                }
            },
            visit,
        );
        selected
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::VolumeLimits;

    #[test]
    fn component_routes_are_bounded_by_query_bytes_after_foreign_growth()
    -> Result<(), Box<dyn std::error::Error>> {
        let limits = VolumeLimits::default();
        let parse = |path: &str| PortablePath::parse(path, limits);
        let mut paths = vec!["/".to_owned(), "/é".to_owned(), "/é/漢".to_owned()];
        let mut deep = String::new();
        for _ in 0..48 {
            deep.push_str("/a");
            paths.push(deep.clone());
        }
        let queries = [
            "/".to_owned(),
            "/é/漢/file".to_owned(),
            "/éclair".to_owned(),
            "/é/漢字".to_owned(),
            format!("{deep}/file"),
            format!("{deep}b/file"),
            "/z/absent".to_owned(),
            "/foreign/000a/file".to_owned(),
        ];
        // Opposite insertion orders force both edge splitting and insertion
        // beneath already bound prefixes through the same implementation.
        for reverse in [false, true] {
            let mut index = PathPrefixIndex::default();
            let mut admitted = paths.clone();
            let mut order = paths.iter().collect::<Vec<_>>();
            if reverse {
                order.reverse();
            }
            for path in order {
                assert_eq!(index.insert(parse(path)?, path.clone()), None);
            }
            assert_eq!(
                index.insert(parse("/é")?, "/é".to_owned()),
                Some("/é".to_owned())
            );
            for grow in [false, true] {
                if grow {
                    for suffix in 0..4096 {
                        let foreign = format!("/foreign/{suffix:04x}");
                        index.insert(parse(&foreign)?, foreign.clone());
                        admitted.push(foreign);
                    }
                }
                for query in &queries {
                    let path = parse(query)?;
                    let mut scanned = 0;
                    let expected = admitted
                        .iter()
                        .inspect(|_| scanned += 1)
                        .filter(|prefix| {
                            prefix.as_str() == "/"
                                || query == *prefix
                                || query
                                    .strip_prefix(prefix.as_str())
                                    .is_some_and(|rest| rest.starts_with('/'))
                        })
                        .max_by_key(|prefix| prefix.len());
                    let mut work = 0;
                    assert_eq!(index.lookup(&path, |bytes| work += bytes), expected);
                    assert!(work <= 12 * query.len() + 1, "{query}: {work}");
                    if grow {
                        // The full-table semantic oracle is the negative
                        // control: identical routes violate the work bound.
                        assert!(scanned > 12 * query.len() + 1);
                    }
                }
            }
        }
        Ok(())
    }
}

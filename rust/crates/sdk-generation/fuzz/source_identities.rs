use super::source_selection;
use sdk_docs::GeneratedSource;
use std::{collections::BTreeSet, path::Path};

fn identity(source: &GeneratedSource) -> (&Path, &Path, &str) {
    (&source.physical_path, &source.logical_path, &source.sha256)
}

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

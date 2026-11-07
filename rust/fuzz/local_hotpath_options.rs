//! One acceptance oracle for the original benchmark argument parser.
#[path = "../crates/conformance/benches/local_hotpath/options.rs"]
mod options;

use acyclic_test_support::scale::{CORE_OPERATIONS, POPULATIONS};
use options::Options;
use std::collections::BTreeSet;

fn parse(input: &[u8]) -> Option<Options> {
    let text = std::str::from_utf8(input).ok()?;
    Options::parse_from(text.split('\0').map(str::to_owned)).ok()
}

pub(crate) fn check(input: &[u8]) -> bool {
    // Positive admission is part of this same fuzz entry: reject-all parser
    // mutations must fail even when arbitrary bytes are otherwise refused.
    assert!(
        parse(include_bytes!(
            "corpus/local_hotpath_options/accepted-minimum"
        ))
        .is_some()
    );
    let Some(options) = parse(input) else {
        return false;
    };
    assert!(options.proofs || options.samples != 0);
    assert_ne!(options.setup_credit, 0);
    assert!(!options.operations.is_empty());
    assert!(
        options
            .operations
            .iter()
            .all(|op| CORE_OPERATIONS.contains(op))
    );
    assert_eq!(
        options.operations.iter().collect::<BTreeSet<_>>().len(),
        options.operations.len()
    );
    assert!(!options.populations.is_empty());
    assert!(options.populations.iter().all(|n| POPULATIONS.contains(n)));
    assert!(options.populations.is_sorted_by(|left, right| left < right));
    // Exercise the original command producer with the same accepted strings.
    // Executing that command in a native shell remains a separate obligation.
    let _ = options.recovery();
    true
}

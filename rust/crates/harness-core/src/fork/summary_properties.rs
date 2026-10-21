use super::*;
use crate::conversation::Limits;
use crate::store::HistoryReadLimits;
use acyclic_test_support::differential::oracle::{Observer, Oracle, Stop};
use proptest::prelude::*;

#[path = "summary_fixture.rs"]
mod fixture;
use fixture::*;
#[path = "summary_oracles.rs"]
mod observations;
use observations::*;
#[path = "summary_controls.rs"]
mod controls;
use controls::oracle;

#[derive(Clone, Debug)]
struct Input {
    sizes: [u8; 2],
    payloads: Vec<u8>,
    references: Vec<u8>,
    file_limit: u8,
    render_limit: u8,
}

fn healthy(payloads: Vec<u8>, references: Vec<u8>) -> Input {
    Input {
        sizes: [1, 1],
        payloads,
        references,
        file_limit: 8,
        render_limit: 8,
    }
}

#[test]
fn summary_sizes_match_independent_byte_limits() {
    let oracle = oracle();
    proptest!(ProptestConfig::with_cases(64), |(
        sizes in proptest::array::uniform2(0..8_u8), payloads in prop::collection::vec(0..8_u8,0..4),
        references in prop::collection::vec(0..8_u8,0..4), file_limit in 1..9_u8, render_limit in 1..9_u8,
    )| { oracle.check(&Case::Sizes(Input { sizes,payloads,references,file_limit,render_limit })); });
}

#[test]
fn summary_metadata_rejects_foreign_and_duplicate_files() {
    let oracle = oracle();
    proptest!(ProptestConfig::with_cases(64), |(
        payloads in prop::collection::vec(0..8_u8,0..4), references in prop::collection::vec(0..8_u8,0..4),
    )| {
        let input = healthy(payloads,references);
        oracle.check(&Case::Metadata(input.clone(), None, false));
        for violation in VIOLATIONS { oracle.check(&Case::Metadata(input.clone(), Some(violation), false)); }
    });
}

#[test]
fn summary_inherited_file_order_matches_inputs() {
    let oracle = oracle();
    proptest!(ProptestConfig::with_cases(64), |(
        payloads in prop::collection::vec(0..8_u8,0..4), references in prop::collection::vec(0..8_u8,0..4),
    )| { oracle.check(&Case::Order(healthy(payloads,references))); });
}

#[test]
fn seed_summary_requires_the_complete_ordered_file_list() {
    let oracle = oracle();
    proptest!(ProptestConfig::with_cases(64), |(
        payloads in prop::collection::vec(0..8_u8,2..4), references in prop::collection::vec(0..8_u8,0..4),
    )| {
        let input = healthy(payloads,references);
        oracle.check(&Case::SeedOrder(input.clone(), None));
        for violation in SEED_VIOLATIONS { oracle.check(&Case::SeedOrder(input.clone(), Some(violation))); }
    });
}

#[test]
fn summary_boundary_and_reference_identity_cases() {
    let oracle = oracle();
    for (sizes, payloads, references, file_limit, render_limit) in [
        ([3, 3], vec![3], vec![3], 3, 3),
        ([0, 0], vec![], vec![], 1, 1),
        ([3, 3], vec![3], vec![3], 2, 3),
        ([3, 3], vec![3], vec![3], 3, 2),
    ] {
        oracle.check(&Case::Sizes(Input {
            sizes,
            payloads,
            references,
            file_limit,
            render_limit,
        }));
    }
    oracle.check(&Case::Metadata(healthy(vec![], vec![]), None, true));
}

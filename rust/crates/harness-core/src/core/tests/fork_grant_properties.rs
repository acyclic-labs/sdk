use super::*;
use crate::conversation::VolumeClass;
use crate::fork::{ReferenceGrant, SharedGrant};
use acyclic_test_support::differential::oracle::{Observer, Oracle, Stop};
use proptest::prelude::*;

#[path = "fork_grant_fixture.rs"]
mod fixture;
use fixture::*;
#[path = "fork_grant_faults.rs"]
mod faults;
use faults::*;
#[path = "fork_grant_oracles.rs"]
mod observations;
use observations::*;
#[path = "fork_grant_controls.rs"]
mod controls;
use controls::{oracle, standard};
#[path = "fork_grant_delegation.rs"]
mod delegation;
#[path = "fork_grant_manifest.rs"]
mod manifest;

#[derive(Clone, Copy, Debug)]
struct Input {
    operations: [u8; 6],
    references: u8,
    inherit: bool,
}
impl Input {
    fn fixture(self) -> Matrix {
        Matrix::new(self.operations, self.references, self.inherit)
    }
}

#[derive(Clone, Copy, Debug)]
enum Case {
    Validation(Input, Option<Violation>),
    Capability(Input, usize),
    InvalidApis(Input, Violation, usize),
    Delegation(u8, u8),
    Manifest(Input, usize, Access, ManifestCase),
}

#[test]
fn fork_grants_reject_invalid_bindings() {
    let oracle = oracle();
    proptest!(ProptestConfig::with_cases(64), |(operations in proptest::array::uniform6(0..4_u8), references in 0..64_u8, inherit in any::<bool>())| {
        let input = Input {operations,references,inherit};
        oracle.check(&Case::Validation(input,None));
        for violation in VIOLATIONS { oracle.check(&Case::Validation(input,Some(violation))); }
    });
}

#[test]
fn fork_capabilities_match_the_input_matrix() {
    let oracle = oracle();
    proptest!(ProptestConfig::with_cases(64), |(operations in proptest::array::uniform6(0..4_u8), references in 0..64_u8, inherit in any::<bool>())| {
        let input = Input {operations,references,inherit};
        for reader in 0..4 { oracle.check(&Case::Capability(input,reader)); }
    });
}

#[test]
fn invalid_seeds_are_rejected_by_every_capability_api() {
    let oracle = oracle();
    proptest!(ProptestConfig::with_cases(64), |(operations in proptest::array::uniform6(0..4_u8), references in 0..64_u8, inherit in any::<bool>())| {
        let input = Input {operations,references,inherit};
        for violation in VIOLATIONS { for reader in 0..3 { oracle.check(&Case::InvalidApis(input,violation,reader)); } }
    });
}

#[test]
fn parent_and_requested_fork_grants_obey_input_subset() {
    let oracle = oracle();
    proptest!(ProptestConfig::with_cases(64), |(parent in 0..16_u8, request in 0..16_u8)| {
        oracle.check(&Case::Delegation(parent,request));
    });
}

#[test]
fn manifest_backed_grants_require_reader_access() {
    let oracle = oracle();
    proptest!(ProptestConfig::with_cases(64), |(operations in proptest::array::uniform6(0..4_u8), references in 0..64_u8, inherit in any::<bool>(), reader in 0..3_usize)| {
        for access in ACCESS {
            let masks: &[u8] = if matches!(access,Access::SharedRead) { &[1,3] } else { &[1] };
            for &mask in masks {
                let mut operations = operations;
                operations[reader*2] = mask;
                for case in MANIFEST_CASES { oracle.check(&Case::Manifest(Input {operations,references,inherit},reader,access,case)); }
            }
        }
    });
}

#[test]
fn mandatory_permission_and_manifest_boundaries() {
    let oracle = oracle();
    for input in [
        Input {
            operations: [2; 6],
            references: 0,
            inherit: false,
        },
        Input {
            operations: [3; 6],
            references: 63,
            inherit: true,
        },
        standard(true),
    ] {
        for reader in 0..4 {
            oracle.check(&Case::Capability(input, reader));
        }
    }
    for parent in 0..16 {
        for request in 0..16 {
            oracle.check(&Case::Delegation(parent, request));
        }
    }
    for reader in 0..3 {
        for mask in [1, 3] {
            let mut input = Input {
                operations: [3; 6],
                references: 63,
                inherit: true,
            };
            input.operations[reader * 2] = mask;
            oracle.check(&Case::Manifest(
                input,
                reader,
                Access::SharedRead,
                ManifestCase::Valid,
            ));
        }
    }
}

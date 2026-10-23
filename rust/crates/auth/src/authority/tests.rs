use super::*;
use crate::testing::claims as common;
#[path = "../../tests/common/request_hardening.rs"]
mod hardening;
use proptest::{
    prelude::*,
    test_runner::{TestError, TestRunner},
};

#[test]
fn shrinking_request_property_detects_each_removed_guard() {
    for remove_resource_guard in [false, true] {
        let mut runner = TestRunner::new(ProptestConfig {
            cases: 128,
            failure_persistence: None,
            ..ProptestConfig::default()
        });
        let result = runner.run(&(hardening::invalid_name(), any::<bool>()), |(name, child)| {
            hardening::request_property(&name, child, |blocks, request, policy| {
                if !remove_resource_guard && matches!(request.target, Target::Resource { resource, .. } if !crate::valid_resource(resource)) { return Err(Deny::NonCanonicalResource); }
                if remove_resource_guard && !crate::valid_resource(request.audience) { return Err(Deny::NonCanonicalAudience); }
                if policy == PolicyDecision::Deny { return Err(Deny::OwnerPolicy); }
                if permits_canonical(blocks, request, true) { Ok(()) } else { Err(Deny::Capability) }
            })
        });
        assert!(
            matches!(result, Err(TestError::Fail(_, _))),
            "removed guard must produce a shrinking counterexample"
        );
    }
}

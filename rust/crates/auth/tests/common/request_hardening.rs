use super::{common::root, limits::limits};
use acyclic_auth::{
    authority::{Deny, PolicyDecision, Request, Target, attenuate},
    contract::{Block, Limits, RecordAttributes},
    valid_resource,
};
use proptest::prelude::*;

pub fn invalid_name() -> impl Strategy<Value = String> {
    (
        "[a-z]{1,12}",
        prop::sample::select(vec![
            "../secret",
            "./secret",
            "a//b",
            "report?.txt",
            "%2f",
            "%41",
            "a%b",
            "λ",
        ]),
    )
        .prop_map(|(name, tail)| format!("fs:/{name}/{tail}"))
}
pub fn request<'a>(
    resource: &'a str,
    audience: &'a str,
    attributes: &'a RecordAttributes,
    usage: &'a Limits,
) -> Request<'a> {
    Request {
        target: Target::Resource {
            action: "fs/read",
            resource,
        },
        audience,
        holder: None,
        now_unix_seconds: 2,
        keys: &[],
        attributes,
        usage,
        cpu_millis: 0,
        memory_bytes: 0,
    }
}
pub fn request_property(
    name: &str,
    child: bool,
    permit: impl Fn(&[Block], &Request<'_>, PolicyDecision) -> Result<(), Deny>,
) -> proptest::test_runner::TestCaseResult {
    prop_assert!(!valid_resource(name));
    let root = vec![root()];
    let blocks = if child {
        attenuate(&root, vec![], vec![6; 32])
            .map_err(|error| TestCaseError::fail(format!("attenuation failed: {error:?}")))?
    } else {
        root
    };
    let attributes = RecordAttributes::default();
    let usage = limits(None);
    let canonical = request("fs:/a", "workers:/app", &attributes, &usage);
    prop_assert_eq!(permit(&blocks, &canonical, PolicyDecision::Allow), Ok(()));
    prop_assert_eq!(
        permit(&blocks, &canonical, PolicyDecision::Deny),
        Err(Deny::OwnerPolicy)
    );
    for policy in [PolicyDecision::Allow, PolicyDecision::Deny] {
        prop_assert_eq!(
            permit(
                &blocks,
                &request(name, "workers:/app", &attributes, &usage),
                policy
            ),
            Err(Deny::NonCanonicalResource)
        );
        prop_assert_eq!(
            permit(
                &blocks,
                &request("fs:/a", name, &attributes, &usage),
                policy
            ),
            Err(Deny::NonCanonicalAudience)
        );
    }
    Ok(())
}

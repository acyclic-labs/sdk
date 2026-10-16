use super::*;
use crate::{AgentId, conversation::VolumeOwner, resources::ProviderRef};
#[cfg(not(acyclic_harness_loom))]
use crate::{
    Capabilities, TaskId,
    core::{AggregateKind, Authority, AuthorityIssuer},
};

#[cfg(not(acyclic_harness_loom))]
pub(super) fn authority(
    agent: Option<AgentId>,
    volume: &VolumeRef,
    operations: &[VolumeOperation],
) -> (AuthorityVerifier, Scope) {
    let issuer = AuthorityIssuer::new(
        "task-files",
        [7; 32],
        Authority {
            kind: AggregateKind::Task,
            id: TaskId::from_bytes([2; 16]).to_string(),
        },
    );
    let grants: Vec<_> = operations
        .iter()
        .map(|operation| volume.capability(*operation, &mut None).unwrap())
        .collect();
    let scope = issuer
        .issue("caller", &Capabilities::new(grants), agent, None, &mut None)
        .unwrap();
    (issuer.verifier(&mut None).unwrap(), scope)
}

pub(super) fn volume(agent: AgentId) -> VolumeRef {
    VolumeRef::new(
        ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None).unwrap(),
        "task-files".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )
    .unwrap()
}

use super::*;
use crate::{
    AgentId, Capabilities, TaskId,
    conversation::VolumeOwner,
    core::{AggregateKind, Authority, AuthorityIssuer},
    resources::ProviderRef,
};

pub(super) fn authority(
    agent: AgentId,
    volume: &VolumeRef,
    write: bool,
) -> (AuthorityVerifier, Scope) {
    let issuer = AuthorityIssuer::new(
        "task-files",
        [7; 32],
        Authority {
            kind: AggregateKind::Task,
            id: TaskId::from_bytes([2; 16]).to_string(),
        },
    );
    let mut grants = vec![volume.capability(VolumeOperation::Read, &mut None).unwrap()];
    if write {
        grants.push(
            volume
                .capability(VolumeOperation::Write, &mut None)
                .unwrap(),
        );
    }
    let scope = issuer
        .issue(
            "caller",
            &Capabilities::new(grants),
            Some(agent),
            None,
            &mut None,
        )
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

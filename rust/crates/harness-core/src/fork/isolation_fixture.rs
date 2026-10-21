use super::*;

#[derive(Clone, Copy, Debug)]
pub(super) enum Violation {
    SameConversation,
    AttachedChild,
    DuplicateAttached,
    ForeignScratchOwner,
    ForeignScratchProvider,
    WrongHistoryCut,
    MissingHistory,
    DuplicateSelection,
    AliasedResult,
    UnpairedResult,
    ForeignResultOwner,
}

pub(super) const VIOLATIONS: [Violation; 11] = [
    Violation::SameConversation,
    Violation::AttachedChild,
    Violation::DuplicateAttached,
    Violation::ForeignScratchOwner,
    Violation::ForeignScratchProvider,
    Violation::WrongHistoryCut,
    Violation::MissingHistory,
    Violation::DuplicateSelection,
    Violation::AliasedResult,
    Violation::UnpairedResult,
    Violation::ForeignResultOwner,
];

fn agent(index: u8) -> AgentId {
    let mut bytes = [0; 16];
    bytes[0] = index;
    bytes[15] = 3;
    AgentId::from_bytes(bytes)
}

fn result_allocation(seed: &ForkSeed, owner: AgentId) -> (VolumeRef, GenerationRef) {
    let provider = seed.child_private_volume.provider();
    let volume = VolumeRef::new(
        provider.clone(),
        "result".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
        &mut None,
    )
    .unwrap();
    let generation = GenerationRef::new(provider, &[71; 32], None, None)
        .unwrap()
        .0;
    (volume, generation)
}

pub(super) fn valid_seed(child: u8, readers: u16, revision: u8, result: bool) -> ForkSeed {
    let mut seed = seed().unwrap();
    seed.child_agent = agent(child);
    seed.attached_agents = (0..16)
        .filter(|index| *index != child && readers & (1 << index) != 0)
        .map(agent)
        .collect();
    seed.parent_revision = u64::from(revision);
    let ResourceRevision::History(history) = &seed.resources[0].source else {
        panic!("history fixture");
    };
    let history = ResourceRevision::History(
        StreamRef::new(
            history.as_resource().provider().clone(),
            history.as_resource().key().to_vec(),
            Some(revision.to_string()),
        )
        .unwrap(),
    );
    seed.resources[0] = CapturedResource {
        source: history.clone(),
        revision: history,
    };
    seed.child_private_volume = VolumeRef::new(
        seed.child_private_volume.provider().clone(),
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(seed.child_agent),
        &mut None,
    )
    .unwrap();
    let ResourceRevision::PrivateVolume { volume, .. } = &mut seed.resources[2].revision else {
        panic!("scratch fixture");
    };
    *volume = seed.child_private_volume.clone();
    if result {
        let (volume, generation) = result_allocation(&seed, seed.child_agent);
        seed.child_result_volume = Some(volume);
        seed.child_result_generation = Some(generation);
    }
    let artifact = ResourceRevision::Artifact(
        ArtifactRef::new(
            seed.child_private_volume.provider().clone(),
            vec![revision],
            Some("1".into()),
        )
        .unwrap(),
    );
    seed.resources.push(CapturedResource {
        source: artifact.clone(),
        revision: artifact,
    });
    seed
}

pub(super) fn violate(seed: &mut ForkSeed, violation: Violation) {
    match violation {
        Violation::SameConversation => seed.child = seed.parent.clone(),
        Violation::AttachedChild => seed.attached_agents.push(seed.child_agent),
        Violation::DuplicateAttached => {
            seed.attached_agents.extend([agent(16), agent(16)]);
        }
        Violation::ForeignScratchOwner => {
            seed.child_private_volume = VolumeRef::new(
                seed.child_private_volume.provider().clone(),
                seed.child_private_volume.id().into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent(16)),
                &mut None,
            )
            .unwrap();
            // Keep the capture consistent, isolating the child-owner requirement.
            let ResourceRevision::PrivateVolume { volume, .. } = &mut seed.resources[2].revision
            else {
                panic!("scratch fixture");
            };
            *volume = seed.child_private_volume.clone();
        }
        Violation::ForeignScratchProvider => {
            let provider =
                ProviderRef::new("foreign".into(), "filesystem".into(), "2".into(), &mut None)
                    .unwrap();
            seed.child_private_generation = GenerationRef::new(&provider, &[72; 32], None, None)
                .unwrap()
                .0;
        }
        Violation::WrongHistoryCut => seed.parent_revision += 1,
        Violation::MissingHistory => {
            seed.resources.remove(0);
        }
        // Artifacts do not affect the required singleton counts.
        Violation::DuplicateSelection => seed.resources.push(seed.resources[3].clone()),
        Violation::AliasedResult => {
            seed.child_result_volume = Some(seed.child_private_volume.clone());
            seed.child_result_generation = Some(seed.child_private_generation.clone());
        }
        Violation::UnpairedResult => {
            seed.child_result_volume = Some(result_allocation(seed, seed.child_agent).0);
            seed.child_result_generation = None;
        }
        Violation::ForeignResultOwner => {
            let (volume, generation) = result_allocation(seed, agent(16));
            seed.child_result_volume = Some(volume);
            seed.child_result_generation = Some(generation);
        }
    }
}

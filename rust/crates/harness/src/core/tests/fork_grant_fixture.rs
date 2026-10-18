use super::*;

pub(super) fn file(volume: &VolumeRef, path: &str, media: &str) -> FileRef {
    FileRef::new(
        volume.clone(),
        path.into(),
        "one".into(),
        FileDescriptor::from_bytes(b"[]", media, &mut None).unwrap(),
        path.rsplit('/').next().unwrap().into(),
        &mut None,
    )
    .unwrap()
}

pub(super) struct Matrix {
    pub(super) seed: ForkSeed,
    pub(super) readers: [AgentId; 4],
    pub(super) shared: [VolumeRef; 2],
    pub(super) files: [FileRef; 2],
    pub(super) project: VolumeRef,
    pub(super) inherited: Option<FileRef>,
    operations: [u8; 6],
    references: u8,
}

impl Matrix {
    pub(super) fn new(mut operations: [u8; 6], references: u8, inherit: bool) -> Self {
        // Every selected shared volume needs a nonempty child grant.
        operations[0] = operations[0].max(1);
        operations[1] = operations[1].max(1);
        let mut seed: ForkSeed =
            serde_json::from_slice(include_bytes!("../../../fixtures/v2/fork-seed.json")).unwrap();
        let readers = [
            seed.child_agent,
            AgentId::from_bytes([29; 16]),
            AgentId::from_bytes([30; 16]),
            AgentId::from_bytes([31; 16]),
        ];
        seed.attached_agents = readers[1..3].to_vec();
        let provider = seed.child_private_volume.provider().clone();
        let shared = [0, 1].map(|index| {
            VolumeRef::new(
                provider.clone(),
                format!("shared-{index}"),
                VolumeClass::SessionShared,
                VolumeOwner::Session("session".into()),
                &mut None,
            )
            .unwrap()
        });
        for volume in &shared {
            let revision = ResourceRevision::SharedVolume(volume.clone());
            seed.resources.push(CapturedResource {
                source: revision.clone(),
                revision,
            });
        }
        let foreign_private = VolumeRef::new(
            provider,
            "foreign".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([40; 16])),
            &mut None,
        )
        .unwrap();
        let files = [0, 1].map(|index| {
            file(
                &foreign_private,
                &format!("references/{index}.json"),
                "application/json",
            )
        });
        for reader in 0..3 {
            for index in 0..2 {
                let mask = operations[reader * 2 + index];
                let mut selected = BTreeSet::new();
                if mask & 1 != 0 {
                    selected.insert(VolumeOperation::Read);
                }
                if mask & 2 != 0 {
                    selected.insert(VolumeOperation::Write);
                }
                if !selected.is_empty() {
                    seed.shared_grants.push(SharedGrant {
                        volume: shared[index].clone(),
                        child_agent: readers[reader],
                        operations: selected,
                    });
                }
                if references & (1 << (reader * 2 + index)) != 0 {
                    seed.reference_grants.push(ReferenceGrant {
                        file: files[index].clone(),
                        reader: readers[reader],
                        attachment_manifest: None,
                    });
                }
            }
        }
        let ResourceRevision::Project {
            volume: project, ..
        } = &seed.resources[1].revision
        else {
            panic!("project fixture")
        };
        let project = project.clone();
        let inherited = inherit.then(|| {
            file(
                &seed.child_private_volume,
                ".system/inherited-conversation/prefix.json",
                "application/vnd.acyclic.harness.inherited-conversation+json",
            )
        });
        if let Some(prefix) = &inherited {
            seed.inherited_through_sequence = 1;
            seed.inherited_context.push(prefix.clone());
        }
        Self {
            seed,
            readers,
            shared,
            files,
            project,
            inherited,
            operations,
            references,
        }
    }

    // Membership and operation expectations come from input bits, never seed grants.
    // Typed identity encoders are shared; this property does not prove their digests.
    pub(super) fn expected(&self, reader: usize) -> [Option<BTreeSet<String>>; 3] {
        if reader == 3 {
            return [None, None, None];
        }
        let mut shared = BTreeSet::new();
        let mut references = BTreeSet::new();
        let mut read = BTreeSet::from([self
            .project
            .capability(VolumeOperation::Read, &mut None)
            .unwrap()]);
        for index in 0..2 {
            let mask = self.operations[reader * 2 + index];
            if mask & 1 != 0 {
                let capability = self.shared[index]
                    .capability(VolumeOperation::Read, &mut None)
                    .unwrap();
                shared.insert(capability.clone());
                read.insert(capability);
            }
            if mask & 2 != 0 {
                shared.insert(
                    self.shared[index]
                        .capability(VolumeOperation::Write, &mut None)
                        .unwrap(),
                );
            }
            if self.references & (1 << (reader * 2 + index)) != 0 {
                references.insert(self.files[index].read_capability(&mut None).unwrap());
            }
        }
        if let Some(prefix) = &self.inherited {
            references.insert(prefix.read_capability(&mut None).unwrap());
        }
        read.extend(references.iter().cloned());
        [Some(shared), Some(references), Some(read)]
    }

    pub(super) fn manifest(&self, volume: &VolumeRef) -> FileRef {
        file(
            volume,
            "attachments.json",
            "application/vnd.acyclic.harness.attachments+json",
        )
    }

    pub(super) fn violate(&self, seed: &mut ForkSeed, violation: Violation) {
        match violation {
            Violation::ForeignSharedReader => seed.shared_grants.push(SharedGrant {
                volume: self.shared[0].clone(),
                child_agent: self.readers[3],
                operations: BTreeSet::from([VolumeOperation::Read]),
            }),
            Violation::UnselectedShared => {
                let volume = VolumeRef::new(
                    self.shared[0].provider().clone(),
                    "unselected".into(),
                    VolumeClass::SessionShared,
                    VolumeOwner::Session("session".into()),
                    &mut None,
                )
                .unwrap();
                seed.shared_grants.push(SharedGrant {
                    volume,
                    child_agent: self.readers[0],
                    operations: BTreeSet::from([VolumeOperation::Read]),
                });
            }
            Violation::MissingChildGrant => seed.shared_grants.retain(|grant| {
                grant.child_agent != self.readers[0] || grant.volume != self.shared[0]
            }),
            Violation::DuplicateShared => seed.shared_grants.push(seed.shared_grants[0].clone()),
            Violation::EmptyOperations => seed.shared_grants[0].operations.clear(),
            Violation::ForeignReferenceReader => seed.reference_grants.push(ReferenceGrant {
                file: self.files[0].clone(),
                reader: self.readers[3],
                attachment_manifest: None,
            }),
            Violation::OwnerReference => seed.reference_grants.push(ReferenceGrant {
                file: file(&seed.child_private_volume, "owner.json", "application/json"),
                reader: self.readers[0],
                attachment_manifest: None,
            }),
            Violation::DuplicateReference => {
                let grant = ReferenceGrant {
                    file: file(self.files[0].volume(), "duplicate.json", "application/json"),
                    reader: self.readers[0],
                    attachment_manifest: None,
                };
                seed.reference_grants.extend([grant.clone(), grant]);
            }
            Violation::MissingManifest => {
                let manifest = self.manifest(&self.project);
                seed.reference_grants.push(ReferenceGrant {
                    file: manifest.clone(),
                    reader: self.readers[0],
                    attachment_manifest: Some(manifest),
                });
            }
            Violation::UncoveredManifest => {
                let manifest = self.manifest(self.files[0].volume());
                seed.attachment_manifests.push(manifest.clone());
                seed.reference_grants.push(ReferenceGrant {
                    file: self.files[0].clone(),
                    reader: self.readers[0],
                    attachment_manifest: Some(manifest),
                });
            }
            Violation::DuplicateManifest => {
                let manifest = self.manifest(self.files[0].volume());
                seed.attachment_manifests
                    .extend([manifest.clone(), manifest]);
            }
        }
    }
}

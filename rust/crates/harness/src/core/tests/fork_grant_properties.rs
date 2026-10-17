use super::*;
use crate::conversation::VolumeClass;
use crate::fork::{ReferenceGrant, SharedGrant};
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

#[path = "fork_grant_boundaries.rs"]
mod boundaries;

fn file(volume: &VolumeRef, path: &str, media: &str) -> FileRef {
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

struct Matrix {
    seed: ForkSeed,
    readers: [AgentId; 4],
    shared: [VolumeRef; 2],
    files: [FileRef; 2],
    project: VolumeRef,
    inherited: Option<FileRef>,
    operations: [u8; 6],
    references: u8,
}

impl Matrix {
    fn new(mut operations: [u8; 6], references: u8, inherit: bool) -> Self {
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
    fn expected(&self, reader: usize) -> [Option<BTreeSet<String>>; 3] {
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

    fn check_capabilities(&self, index: usize, fault: Fault) -> TestCaseResult {
        let reader = match fault {
            Fault::WrongReader => self.readers[(index + 1) % 3],
            Fault::GrantOutsider => self.readers[0],
            _ => self.readers[index],
        };
        let mut actual = [
            self.seed.shared_capabilities(reader),
            self.seed.reference_capabilities(reader),
            self.seed.attached_read_capabilities(reader),
        ]
        .map(|result| {
            result
                .ok()
                .map(|grants| grants.iter().map(str::to_owned).collect::<BTreeSet<_>>())
        });
        match fault {
            Fault::DropShared => actual[0].as_mut().unwrap().clear(),
            Fault::LeakShared => {
                actual[0].as_mut().unwrap().insert(
                    self.shared[0]
                        .capability(VolumeOperation::Write, &mut None)
                        .unwrap(),
                );
            }
            Fault::DropReference => actual[1].as_mut().unwrap().clear(),
            Fault::LeakReference => {
                actual[1]
                    .as_mut()
                    .unwrap()
                    .insert(self.files[1].read_capability(&mut None).unwrap());
            }
            Fault::PromoteWrite => {
                actual[2].as_mut().unwrap().insert(
                    self.shared[1]
                        .capability(VolumeOperation::Write, &mut None)
                        .unwrap(),
                );
            }
            Fault::DropProject => {
                actual[2].as_mut().unwrap().remove(
                    &self
                        .project
                        .capability(VolumeOperation::Read, &mut None)
                        .unwrap(),
                );
            }
            Fault::DropInherited => {
                let inherited = self
                    .inherited
                    .as_ref()
                    .unwrap()
                    .read_capability(&mut None)
                    .unwrap();
                actual[1].as_mut().unwrap().remove(&inherited);
                actual[2].as_mut().unwrap().remove(&inherited);
            }
            Fault::RejectAll => actual = [None, None, None],
            _ => {}
        }
        prop_assert_eq!(
            actual,
            self.expected(index),
            "fork capability matrix mismatch"
        );
        Ok(())
    }

    fn manifest(&self, volume: &VolumeRef) -> FileRef {
        file(
            volume,
            "attachments.json",
            "application/vnd.acyclic.harness.attachments+json",
        )
    }

    fn violate(&self, seed: &mut ForkSeed, violation: Violation) {
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

    fn check_validation(
        &self,
        violation: Option<Violation>,
        check_original: bool,
    ) -> TestCaseResult {
        let mut candidate = self.seed.clone();
        if let Some(violation) = violation {
            self.violate(&mut candidate, violation);
        }
        let submitted = if check_original {
            &self.seed
        } else {
            &candidate
        };
        let accepted = submitted.validate(&mut None).is_ok();
        prop_assert_eq!(
            accepted,
            violation.is_none(),
            "fork grant acceptance mismatch: {:?}",
            violation
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum Violation {
    ForeignSharedReader,
    UnselectedShared,
    MissingChildGrant,
    DuplicateShared,
    EmptyOperations,
    ForeignReferenceReader,
    OwnerReference,
    DuplicateReference,
    MissingManifest,
    UncoveredManifest,
    DuplicateManifest,
}
const VIOLATIONS: [Violation; 11] = [
    Violation::ForeignSharedReader,
    Violation::UnselectedShared,
    Violation::MissingChildGrant,
    Violation::DuplicateShared,
    Violation::EmptyOperations,
    Violation::ForeignReferenceReader,
    Violation::OwnerReference,
    Violation::DuplicateReference,
    Violation::MissingManifest,
    Violation::UncoveredManifest,
    Violation::DuplicateManifest,
];

#[derive(Clone, Copy)]
enum Fault {
    None,
    WrongReader,
    DropShared,
    LeakShared,
    DropReference,
    LeakReference,
    PromoteWrite,
    DropProject,
    DropInherited,
    GrantOutsider,
    RejectAll,
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn fork_grants_reject_invalid_bindings(operations in proptest::array::uniform6(0..4_u8), references in 0..64_u8, inherit in any::<bool>()) {
        let matrix = Matrix::new(operations, references, inherit);
        matrix.check_validation(None, false)?;
        for violation in VIOLATIONS { matrix.check_validation(Some(violation), false)?; }
    }

    #[test]
    fn fork_capabilities_match_the_input_matrix(operations in proptest::array::uniform6(0..4_u8), references in 0..64_u8, inherit in any::<bool>()) {
        let matrix = Matrix::new(operations, references, inherit);
        for reader in 0..4 { matrix.check_capabilities(reader, Fault::None)?; }
    }
}

fn expect_fault(result: TestCaseResult, target: &str) {
    let failure = result.unwrap_err();
    assert!(
        failure.to_string().contains(target),
        "wrong observation: {failure}"
    );
}

#[test]
fn negative_control_validates_the_original_seed() {
    for inherit in [false, true] {
        let matrix = Matrix::new([1, 2, 2, 1, 3, 0], 37, inherit);
        for violation in VIOLATIONS {
            expect_fault(
                matrix.check_validation(Some(violation), true),
                "fork grant acceptance mismatch",
            );
        }
    }
}

#[test]
fn negative_controls_detect_capability_matrix_faults() {
    for (operations, references, inherit) in [([2; 6], 0, false), ([3; 6], 63, true)] {
        let matrix = Matrix::new(operations, references, inherit);
        for reader in 0..4 {
            matrix.check_capabilities(reader, Fault::None).unwrap();
        }
    }
    let matrix = Matrix::new([1, 2, 2, 1, 3, 0], 37, true);
    for fault in [
        Fault::WrongReader,
        Fault::DropShared,
        Fault::LeakShared,
        Fault::DropReference,
        Fault::LeakReference,
        Fault::PromoteWrite,
        Fault::DropProject,
        Fault::DropInherited,
        Fault::GrantOutsider,
        Fault::RejectAll,
    ] {
        let reader = if matches!(fault, Fault::GrantOutsider) {
            3
        } else {
            0
        };
        expect_fault(
            matrix.check_capabilities(reader, fault),
            "fork capability matrix mismatch",
        );
    }
}

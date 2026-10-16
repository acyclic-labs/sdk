use super::*;

fn capabilities(seed: &ForkSeed, reader: AgentId) -> [Result<Capabilities>; 3] {
    [
        seed.shared_capabilities(reader),
        seed.reference_capabilities(reader),
        seed.attached_read_capabilities(reader),
    ]
}

fn check_invalid_apis(
    matrix: &Matrix,
    violation: Violation,
    reader: usize,
    bypass: Option<usize>,
) -> TestCaseResult {
    let mut candidate = matrix.seed.clone();
    matrix.violate(&mut candidate, violation);
    let mut actual = capabilities(&candidate, matrix.readers[reader]).map(|result| result.is_ok());
    if let Some(api) = bypass {
        // Deliberately issue through this API from the healthy seed instead.
        actual[api] = capabilities(&matrix.seed, matrix.readers[reader])[api].is_ok();
    }
    prop_assert_eq!(
        actual,
        [false; 3],
        "invalid seed issued capabilities: {:?}",
        violation
    );
    Ok(())
}

#[derive(Clone, Copy)]
enum DelegationFault {
    None,
    WidenParent,
    SkipAttenuation,
    RejectAll,
}

fn shared_bits(matrix: &Matrix, mask: u8) -> BTreeSet<String> {
    (0..4)
        .filter(|bit| mask & (1 << bit) != 0)
        .map(|bit| {
            matrix.shared[bit / 2]
                .capability(
                    if bit % 2 == 0 {
                        VolumeOperation::Read
                    } else {
                        VolumeOperation::Write
                    },
                    &mut None,
                )
                .unwrap()
        })
        .collect()
}

fn check_delegation(parent: u8, request: u8, fault: DelegationFault) -> TestCaseResult {
    // Attached reader 1 permits empty requests without weakening the selected-volume child rule.
    let matrix = Matrix::new([1, 1, request & 3, (request >> 2) & 3, 0, 0], 0, false);
    let owner = issuer();
    let issued_parent = if matches!(fault, DelegationFault::WidenParent) {
        parent | request
    } else {
        parent
    };
    let parent_scope = owner
        .issue(
            "parent",
            &Capabilities::new(shared_bits(&matrix, issued_parent)),
            Some(matrix.readers[1]),
            None,
            &mut None,
        )
        .unwrap();
    let requested = matrix.seed.shared_capabilities(matrix.readers[1]).unwrap();
    let child = if matches!(fault, DelegationFault::SkipAttenuation) {
        owner.issue(
            "child",
            &requested,
            parent_scope.agent(),
            Some(parent_scope.proof),
            &mut None,
        )
    } else {
        owner.attenuate(&parent_scope, "child", &requested)
    };
    let actual = if matches!(fault, DelegationFault::RejectAll) {
        None
    } else {
        child.ok().map(|scope| {
            (
                scope
                    .capabilities()
                    .iter()
                    .map(str::to_owned)
                    .collect::<BTreeSet<_>>(),
                scope.agent(),
                scope.parent_proof,
                owner
                    .verifier(&mut None)
                    .unwrap()
                    .verify(&scope, &mut None)
                    .is_ok(),
            )
        })
    };
    // Authorization uses only independently generated input bits, not Capabilities::is_subset_of.
    let expected = (request & !parent == 0).then(|| {
        (
            shared_bits(&matrix, request),
            Some(matrix.readers[1]),
            Some(parent_scope.proof),
            true,
        )
    });
    prop_assert_eq!(
        actual,
        expected,
        "fork delegation widened parent permissions"
    );
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum Access {
    Project,
    SharedRead,
    OwnedPrivate,
    ExactReference,
}
const ACCESS: [Access; 4] = [
    Access::Project,
    Access::SharedRead,
    Access::OwnedPrivate,
    Access::ExactReference,
];

#[derive(Clone, Copy, Debug)]
enum ManifestCase {
    Valid,
    WrongReader,
    NoAccess,
}
const MANIFEST_CASES: [ManifestCase; 3] = [
    ManifestCase::Valid,
    ManifestCase::WrongReader,
    ManifestCase::NoAccess,
];

#[derive(Clone, Copy)]
enum ManifestFault {
    None,
    RejectValid,
    AcceptInvalid,
    DropMember,
    RequireWrite,
}

fn check_manifest(
    operations: [u8; 6],
    references: u8,
    inherit: bool,
    reader: usize,
    access: Access,
    case: ManifestCase,
    fault: ManifestFault,
) -> TestCaseResult {
    let shared_mask = 1 | (operations[reader * 2] & 2);
    let mut operations = operations;
    // Other readers have write-only access to this manifest volume.
    for index in 0..3 {
        operations[index * 2] = 2;
    }
    if matches!(access, Access::SharedRead) && !matches!(case, ManifestCase::NoAccess) {
        operations[reader * 2] = shared_mask;
    }
    let matrix = Matrix::new(operations, references, inherit);
    let mut seed = matrix.seed.clone();
    let denied = matches!(case, ManifestCase::NoAccess);
    let target = if matches!(case, ManifestCase::WrongReader) {
        if matches!(access, Access::Project) {
            3
        } else {
            (reader + 1) % 3
        }
    } else {
        reader
    };
    let volume = match access {
        Access::Project if !denied => matrix.project.clone(),
        Access::SharedRead => matrix.shared[0].clone(),
        Access::OwnedPrivate if !denied => VolumeRef::new(
            matrix.project.provider().clone(),
            "reader-private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(matrix.readers[reader]),
            &mut None,
        )
        .unwrap(),
        Access::Project => VolumeRef::new(
            matrix.project.provider().clone(),
            "unselected-project".into(),
            VolumeClass::Project,
            matrix.project.owner().clone(),
            &mut None,
        )
        .unwrap(),
        _ => matrix.files[0].volume().clone(),
    };
    let manifest = matrix.manifest(&volume);
    seed.attachment_manifests.push(manifest.clone());
    if matches!(access, Access::ExactReference) && !denied {
        seed.reference_grants.push(ReferenceGrant {
            file: manifest.clone(),
            reader: matrix.readers[reader],
            attachment_manifest: None,
        });
    }
    let member = file(
        matrix.files[0].volume(),
        "manifest-member.json",
        "application/json",
    );
    seed.reference_grants.push(ReferenceGrant {
        file: member.clone(),
        reader: matrix.readers[target],
        attachment_manifest: Some(manifest.clone()),
    });
    let valid = matches!(case, ManifestCase::Valid);
    let mut actual = (
        seed.validate(&mut None).is_ok(),
        std::array::from_fn::<_, 4, _>(|index| {
            capabilities(&seed, matrix.readers[index]).map(|result| {
                result
                    .ok()
                    .map(|grants| grants.iter().map(str::to_owned).collect::<BTreeSet<_>>())
            })
        }),
    );
    let expected = (
        valid,
        std::array::from_fn::<_, 4, _>(|index| {
            if !valid {
                return [None, None, None];
            }
            let mut grants = matrix.expected(index);
            if index == reader {
                for api in [1, 2] {
                    grants[api]
                        .as_mut()
                        .unwrap()
                        .insert(member.read_capability(&mut None).unwrap());
                    if matches!(access, Access::ExactReference) {
                        grants[api]
                            .as_mut()
                            .unwrap()
                            .insert(manifest.read_capability(&mut None).unwrap());
                    }
                }
            }
            grants
        }),
    );
    match fault {
        ManifestFault::RejectValid => actual = (false, std::array::from_fn(|_| [None, None, None])),
        ManifestFault::AcceptInvalid => {
            actual.1[reader][1] =
                Some(BTreeSet::from([member.read_capability(&mut None).unwrap()]));
        }
        ManifestFault::DropMember => {
            for api in [1, 2] {
                actual.1[reader][api]
                    .as_mut()
                    .unwrap()
                    .remove(&member.read_capability(&mut None).unwrap());
            }
        }
        ManifestFault::RequireWrite => {
            if matches!(access, Access::SharedRead)
                && !seed.shared_grants.iter().any(|grant| {
                    grant.volume == *manifest.volume()
                        && grant.child_agent == matrix.readers[target]
                        && grant.operations.contains(&VolumeOperation::Write)
                })
            {
                actual = (false, std::array::from_fn(|_| [None, None, None]));
            }
        }
        ManifestFault::None => {}
    }
    prop_assert_eq!(
        actual,
        expected,
        "manifest grant boundary mismatch: {:?} {:?}",
        access,
        case
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn invalid_seeds_are_rejected_by_every_capability_api(
        operations in proptest::array::uniform6(0..4_u8), references in 0..64_u8, inherit in any::<bool>(),
    ) {
        let matrix = Matrix::new(operations, references, inherit);
        for violation in VIOLATIONS { for reader in 0..3 { check_invalid_apis(&matrix, violation, reader, None)?; } }
    }

    #[test]
    fn parent_and_requested_fork_grants_obey_input_subset(parent in 0..16_u8, request in 0..16_u8) {
        check_delegation(parent, request, DelegationFault::None)?;
    }

    #[test]
    fn manifest_backed_grants_require_reader_access(
        operations in proptest::array::uniform6(0..4_u8), references in 0..64_u8, inherit in any::<bool>(), reader in 0..3_usize,
    ) {
        for access in ACCESS {
            let masks: &[u8] = if matches!(access, Access::SharedRead) { &[1, 3] } else { &[1] };
            for &mask in masks {
                let mut operations = operations;
                operations[reader * 2] = mask;
                for case in MANIFEST_CASES {
                    check_manifest(operations, references, inherit, reader, access, case, ManifestFault::None)?;
                }
            }
        }
    }
}

#[test]
fn negative_controls_detect_invalid_seed_capability_issuance() {
    for inherit in [false, true] {
        let matrix = Matrix::new([1, 2, 2, 1, 3, 0], 37, inherit);
        for violation in VIOLATIONS {
            for reader in 0..3 {
                for api in 0..3 {
                    expect_fault(
                        check_invalid_apis(&matrix, violation, reader, Some(api)),
                        "invalid seed issued capabilities",
                    );
                }
            }
        }
    }
}

#[test]
fn negative_controls_detect_parent_widening() {
    // Exhaust the four-operation input masks, including empty and saturated grants.
    for parent in 0..16 {
        for request in 0..16 {
            check_delegation(parent, request, DelegationFault::None).unwrap();
        }
    }
    for (parent, request) in [(1, 2), (4, 8), (5, 10), (0, 15)] {
        for fault in [
            DelegationFault::WidenParent,
            DelegationFault::SkipAttenuation,
        ] {
            expect_fault(
                check_delegation(parent, request, fault),
                "fork delegation widened parent permissions",
            );
        }
    }
    expect_fault(
        check_delegation(15, 15, DelegationFault::RejectAll),
        "fork delegation widened parent permissions",
    );
}

#[test]
fn negative_controls_detect_manifest_grant_faults() {
    for reader in 0..3 {
        for access in ACCESS {
            for case in MANIFEST_CASES {
                let faults: &[ManifestFault] = if matches!(case, ManifestCase::Valid) {
                    &[ManifestFault::RejectValid, ManifestFault::DropMember]
                } else {
                    &[ManifestFault::AcceptInvalid]
                };
                for &fault in faults {
                    expect_fault(
                        check_manifest([3; 6], 63, true, reader, access, case, fault),
                        "manifest grant boundary mismatch",
                    );
                }
            }
        }
    }
}

#[test]
fn negative_control_requires_write_to_read_a_shared_manifest() {
    for reader in 0..3 {
        for mask in [1, 3] {
            let mut operations = [3; 6];
            operations[reader * 2] = mask;
            check_manifest(
                operations,
                63,
                true,
                reader,
                Access::SharedRead,
                ManifestCase::Valid,
                ManifestFault::None,
            )
            .unwrap();
            let result = check_manifest(
                operations,
                63,
                true,
                reader,
                Access::SharedRead,
                ManifestCase::Valid,
                ManifestFault::RequireWrite,
            );
            if mask == 1 {
                expect_fault(result, "manifest grant boundary mismatch");
            } else {
                result.unwrap();
            }
        }
    }
}

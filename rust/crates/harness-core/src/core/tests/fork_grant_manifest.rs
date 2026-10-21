use super::*;

pub(super) fn run(
    input: Input,
    reader: usize,
    access: Access,
    case: ManifestCase,
    fault: Option<ManifestFault>,
    see: &mut Observer<'_, Seen>,
) -> std::result::Result<(), Stop> {
    let shared_mask = 1 | (input.operations[reader * 2] & 2);
    let mut operations = input.operations;
    // Other readers have write-only access to this manifest volume.
    for index in 0..3 {
        operations[index * 2] = 2;
    }
    if matches!(access, Access::SharedRead) && !matches!(case, ManifestCase::NoAccess) {
        operations[reader * 2] = shared_mask;
    }
    let matrix = Matrix::new(operations, input.references, input.inherit);
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
    see.compare(
        expected,
        || {
            let mut actual = (
                seed.validate(&mut None).is_ok(),
                std::array::from_fn::<_, 4, _>(|index| capabilities(&seed, matrix.readers[index])),
            );
            match fault {
                Some(ManifestFault::RejectValid) => {
                    actual = (false, std::array::from_fn(|_| [None, None, None]))
                }
                Some(ManifestFault::AcceptInvalid) => {
                    actual.1[reader][1] =
                        Some(BTreeSet::from([member.read_capability(&mut None).unwrap()]));
                }
                Some(ManifestFault::DropMember) => {
                    for api in [1, 2] {
                        actual.1[reader][api]
                            .as_mut()
                            .unwrap()
                            .remove(&member.read_capability(&mut None).unwrap());
                    }
                }
                Some(ManifestFault::RequireWrite) => {
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
                None => {}
            }
            actual
        },
        |expected, actual| Seen::Manifest {
            actual: actual.clone(),
            expected,
        },
    )?;
    Ok(())
}

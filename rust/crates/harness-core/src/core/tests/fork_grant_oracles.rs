use super::*;

pub(super) type CapabilitySets = [Option<BTreeSet<String>>; 3];
pub(super) type DelegationView =
    Option<(BTreeSet<String>, Option<AgentId>, Option<[u8; 32]>, bool)>;
pub(super) type ManifestView = (bool, [CapabilitySets; 4]);

#[derive(Debug)]
pub(super) enum Seen {
    Validation {
        actual: bool,
        expected: bool,
    },
    Capability {
        actual: CapabilitySets,
        expected: CapabilitySets,
    },
    InvalidApis {
        actual: [bool; 3],
        expected: [bool; 3],
    },
    Delegation {
        actual: DelegationView,
        expected: DelegationView,
    },
    Manifest {
        actual: ManifestView,
        expected: ManifestView,
    },
}

pub(super) fn equal<T: std::fmt::Debug + PartialEq>(
    actual: &T,
    expected: &T,
) -> std::result::Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{actual:?} differs from independent {expected:?}"))
    }
}

pub(super) fn capabilities(seed: &ForkSeed, reader: AgentId) -> CapabilitySets {
    [
        seed.shared_capabilities(reader),
        seed.reference_capabilities(reader),
        seed.attached_read_capabilities(reader),
    ]
    .map(|result| {
        result
            .ok()
            .map(|grants| grants.iter().map(str::to_owned).collect())
    })
}

fn altered_capabilities(
    matrix: &Matrix,
    index: usize,
    fault: Option<CapabilityFault>,
) -> CapabilitySets {
    let reader = match fault {
        Some(CapabilityFault::WrongReader) => matrix.readers[(index + 1) % 3],
        Some(CapabilityFault::GrantOutsider) => matrix.readers[0],
        _ => matrix.readers[index],
    };
    let mut actual = capabilities(&matrix.seed, reader);
    match fault {
        Some(CapabilityFault::DropShared) => actual[0].as_mut().unwrap().clear(),
        Some(CapabilityFault::LeakShared) => {
            actual[0].as_mut().unwrap().insert(
                matrix.shared[0]
                    .capability(VolumeOperation::Write, &mut None)
                    .unwrap(),
            );
        }
        Some(CapabilityFault::DropReference) => actual[1].as_mut().unwrap().clear(),
        Some(CapabilityFault::LeakReference) => {
            actual[1]
                .as_mut()
                .unwrap()
                .insert(matrix.files[1].read_capability(&mut None).unwrap());
        }
        Some(CapabilityFault::PromoteWrite) => {
            actual[2].as_mut().unwrap().insert(
                matrix.shared[1]
                    .capability(VolumeOperation::Write, &mut None)
                    .unwrap(),
            );
        }
        Some(CapabilityFault::DropProject) => {
            actual[2].as_mut().unwrap().remove(
                &matrix
                    .project
                    .capability(VolumeOperation::Read, &mut None)
                    .unwrap(),
            );
        }
        Some(CapabilityFault::DropInherited) => {
            let inherited = matrix
                .inherited
                .as_ref()
                .unwrap()
                .read_capability(&mut None)
                .unwrap();
            actual[1].as_mut().unwrap().remove(&inherited);
            actual[2].as_mut().unwrap().remove(&inherited);
        }
        Some(CapabilityFault::RejectAll) => actual = [None, None, None],
        _ => {}
    }
    actual
}

pub(super) fn run(
    case: &Case,
    fault: Option<&Fault>,
    see: &mut Observer<'_, Seen>,
) -> std::result::Result<(), Stop> {
    match *case {
        Case::Validation(input, violation) => {
            let expected = violation.is_none();
            let matrix = input.fixture();
            let mut candidate = matrix.seed.clone();
            if let Some(violation) = violation {
                matrix.violate(&mut candidate, violation);
            }
            see.compare(
                expected,
                || {
                    let submitted = if matches!(fault, Some(Fault::CheckOriginal)) {
                        &matrix.seed
                    } else {
                        &candidate
                    };
                    submitted.validate(&mut None).is_ok()
                },
                |expected, actual| Seen::Validation {
                    actual: *actual,
                    expected,
                },
            )?;
        }
        Case::Capability(input, reader) => {
            let matrix = input.fixture();
            let expected = matrix.expected(reader);
            let fault = match fault {
                Some(Fault::Capability(fault)) => Some(*fault),
                _ => None,
            };
            see.compare(
                expected,
                || altered_capabilities(&matrix, reader, fault),
                |expected, actual| Seen::Capability {
                    actual: actual.clone(),
                    expected,
                },
            )?;
        }
        Case::InvalidApis(input, violation, reader) => {
            let matrix = input.fixture();
            let mut candidate = matrix.seed.clone();
            matrix.violate(&mut candidate, violation);
            see.compare(
                [false; 3],
                || {
                    let mut actual = capabilities(&candidate, matrix.readers[reader])
                        .map(|grants| grants.is_some());
                    if let Some(Fault::InvalidApi(api)) = fault {
                        actual[*api] =
                            capabilities(&matrix.seed, matrix.readers[reader])[*api].is_some();
                    }
                    actual
                },
                |expected, actual| Seen::InvalidApis {
                    actual: *actual,
                    expected,
                },
            )?;
        }
        Case::Delegation(parent, request) => {
            let fault = match fault {
                Some(Fault::Delegation(fault)) => Some(*fault),
                _ => None,
            };
            delegation::run(parent, request, fault, see)?;
        }
        Case::Manifest(input, reader, access, case) => {
            let fault = match fault {
                Some(Fault::Manifest(fault)) => Some(*fault),
                _ => None,
            };
            manifest::run(input, reader, access, case, fault, see)?;
        }
    }
    Ok(())
}

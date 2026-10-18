use super::*;

#[derive(Clone, Debug)]
pub(super) enum Case {
    Sizes(Input),
    Metadata(Input, Option<Violation>, bool),
    Order(Input),
    SeedOrder(Input, Option<SeedViolation>),
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Fault {
    FileBudget,
    RenderBudget,
    RejectAll,
    CheckOriginal,
    DropContext,
    DropPayload,
    DuplicatePayload,
    ReversePayloads,
    LeakReference,
}

#[derive(Debug)]
pub(super) enum Seen {
    Acceptance {
        boundary: &'static str,
        actual: bool,
        expected: bool,
    },
    Order {
        actual: Vec<FileRef>,
        expected: Vec<FileRef>,
    },
}

pub(super) fn acceptance(seen: &Seen, target: &str) -> std::result::Result<(), String> {
    match seen {
        Seen::Acceptance {
            boundary,
            actual,
            expected,
        } if *boundary == target && actual != expected => Err(format!(
            "{boundary}: accepted {actual}, expected {expected}"
        )),
        _ => Ok(()),
    }
}

pub(super) fn run(
    case: &Case,
    fault: Option<&Fault>,
    see: &mut Observer<'_, Seen>,
) -> std::result::Result<(), Stop> {
    match case {
        Case::Sizes(input) => {
            // Expected acceptance comes only from input byte lengths/limits.
            let expected = input
                .sizes
                .iter()
                .chain(&input.payloads)
                .chain(&input.references)
                .all(|size| *size <= input.file_limit)
                && input.sizes.iter().all(|size| *size <= input.render_limit);
            let fixture = input.fixture();
            let mut candidate = fixture.capture;
            match fault {
                Some(Fault::FileBudget) => candidate.selection.limits.file_bytes = 255,
                Some(Fault::RenderBudget) => candidate.selection.limits.render_bytes = 255,
                _ => {}
            }
            see.compare(
                expected,
                || {
                    candidate
                        .validate(&fixture.seed.child_private_volume)
                        .is_ok()
                        && !matches!(fault, Some(Fault::RejectAll))
                },
                |expected, actual| Seen::Acceptance {
                    boundary: "size",
                    actual: *actual,
                    expected,
                },
            )?;
        }
        Case::Metadata(input, violation, reference_identity) => {
            let expected = violation.is_none();
            let mut fixture = input.fixture();
            if *reference_identity {
                fixture.reference_identity_cases();
            }
            let candidate = fixture.metadata(*violation);
            see.compare(
                expected,
                || {
                    let submitted = if matches!(fault, Some(Fault::CheckOriginal)) {
                        &fixture.capture
                    } else {
                        &candidate
                    };
                    submitted
                        .validate(&fixture.seed.child_private_volume)
                        .is_ok()
                },
                |expected, actual| Seen::Acceptance {
                    boundary: "metadata",
                    actual: *actual,
                    expected,
                },
            )?;
        }
        Case::Order(input) => {
            let fixture = input.fixture();
            let expected = fixture.inherited;
            see.compare(
                expected,
                || {
                    let mut actual = fixture
                        .capture
                        .inherited_files()
                        .cloned()
                        .collect::<Vec<_>>();
                    match fault {
                        Some(Fault::DropContext) => {
                            actual.remove(0);
                        }
                        Some(Fault::DropPayload) => {
                            actual.remove(1);
                        }
                        Some(Fault::DuplicatePayload) => actual.push(actual[1].clone()),
                        Some(Fault::ReversePayloads) => actual[1..].reverse(),
                        Some(Fault::LeakReference) => {
                            actual.push(fixture.capture.references[0].clone())
                        }
                        _ => {}
                    }
                    actual
                },
                |expected, actual| Seen::Order {
                    actual: actual.clone(),
                    expected,
                },
            )?;
        }
        Case::SeedOrder(input, violation) => {
            let expected = violation.is_none();
            let fixture = input.fixture();
            let candidate = fixture.seed_order(*violation);
            see.compare(
                expected,
                || {
                    let submitted = if matches!(fault, Some(Fault::CheckOriginal)) {
                        &fixture.seed
                    } else {
                        &candidate
                    };
                    submitted.validate(&mut None).is_ok()
                },
                |expected, actual| Seen::Acceptance {
                    boundary: "seed-order",
                    actual: *actual,
                    expected,
                },
            )?;
        }
    }
    Ok(())
}

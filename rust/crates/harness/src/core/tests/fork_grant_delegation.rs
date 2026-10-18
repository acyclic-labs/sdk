use super::*;

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

pub(super) fn run(
    parent: u8,
    request: u8,
    fault: Option<DelegationFault>,
    see: &mut Observer<'_, Seen>,
) -> std::result::Result<(), Stop> {
    // Attached reader 1 permits empty requests without weakening the selected-volume child rule.
    let matrix = Matrix::new([1, 1, request & 3, (request >> 2) & 3, 0, 0], 0, false);
    let owner = issuer();
    let issued_parent = if matches!(fault, Some(DelegationFault::WidenParent)) {
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
    // Authorization uses only independently generated input bits, not Capabilities::is_subset_of.
    let expected = (request & !parent == 0).then(|| {
        (
            shared_bits(&matrix, request),
            Some(matrix.readers[1]),
            Some(parent_scope.proof),
            true,
        )
    });
    see.compare(
        expected,
        || {
            let child = if matches!(fault, Some(DelegationFault::SkipAttenuation)) {
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
            if matches!(fault, Some(DelegationFault::RejectAll)) {
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
            }
        },
        |expected, actual| Seen::Delegation {
            actual: actual.clone(),
            expected,
        },
    )?;
    Ok(())
}

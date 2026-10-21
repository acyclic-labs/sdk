use super::*;

pub(super) fn standard(inherit: bool) -> Input {
    Input {
        operations: [1, 2, 2, 1, 3, 0],
        references: 37,
        inherit,
    }
}

pub(super) fn oracle() -> Oracle<Case, Fault, Seen> {
    let mut builder = Oracle::declare(run)
        .invariant("validation", |seen| match seen {
            Seen::Validation { actual, expected } => equal(actual, expected),
            _ => Ok(()),
        })
        .invariant("capability", |seen| match seen {
            Seen::Capability { actual, expected } => equal(actual, expected),
            _ => Ok(()),
        })
        .invariant("invalid-apis", |seen| match seen {
            Seen::InvalidApis { actual, expected } => equal(actual, expected),
            _ => Ok(()),
        })
        .invariant("delegation", |seen| match seen {
            Seen::Delegation { actual, expected } => equal(actual, expected),
            _ => Ok(()),
        })
        .invariant("manifest", |seen| match seen {
            Seen::Manifest { actual, expected } => equal(actual, expected),
            _ => Ok(()),
        })
        .control(
            "capability",
            Case::Capability(standard(true), 0),
            Fault::Capability(CapabilityFault::RejectAll),
        );
    for inherit in [false, true] {
        for violation in VIOLATIONS {
            builder = builder.control(
                "validation",
                Case::Validation(standard(inherit), Some(violation)),
                Fault::CheckOriginal,
            );
            for reader in 0..3 {
                for api in 0..3 {
                    builder = builder.control(
                        "invalid-apis",
                        Case::InvalidApis(standard(inherit), violation, reader),
                        Fault::InvalidApi(api),
                    );
                }
            }
        }
    }
    for fault in [
        CapabilityFault::WrongReader,
        CapabilityFault::DropShared,
        CapabilityFault::LeakShared,
        CapabilityFault::DropReference,
        CapabilityFault::LeakReference,
        CapabilityFault::PromoteWrite,
        CapabilityFault::DropProject,
        CapabilityFault::DropInherited,
        CapabilityFault::GrantOutsider,
    ] {
        let reader = if matches!(fault, CapabilityFault::GrantOutsider) {
            3
        } else {
            0
        };
        builder = builder.control(
            "capability",
            Case::Capability(standard(true), reader),
            Fault::Capability(fault),
        );
    }
    for (parent, request) in [(1, 2), (4, 8), (5, 10), (0, 15)] {
        for fault in [
            DelegationFault::WidenParent,
            DelegationFault::SkipAttenuation,
        ] {
            builder = builder.control(
                "delegation",
                Case::Delegation(parent, request),
                Fault::Delegation(fault),
            );
        }
    }
    builder = builder.control(
        "delegation",
        Case::Delegation(15, 15),
        Fault::Delegation(DelegationFault::RejectAll),
    );
    for reader in 0..3 {
        for access in ACCESS {
            for case in MANIFEST_CASES {
                let faults: &[ManifestFault] = if matches!(case, ManifestCase::Valid) {
                    &[ManifestFault::RejectValid, ManifestFault::DropMember]
                } else {
                    &[ManifestFault::AcceptInvalid]
                };
                for &fault in faults {
                    builder = builder.control(
                        "manifest",
                        Case::Manifest(
                            Input {
                                operations: [3; 6],
                                references: 63,
                                inherit: true,
                            },
                            reader,
                            access,
                            case,
                        ),
                        Fault::Manifest(fault),
                    );
                }
            }
        }
        let mut input = Input {
            operations: [3; 6],
            references: 63,
            inherit: true,
        };
        input.operations[reader * 2] = 1;
        builder = builder.control(
            "manifest",
            Case::Manifest(input, reader, Access::SharedRead, ManifestCase::Valid),
            Fault::Manifest(ManifestFault::RequireWrite),
        );
    }
    builder.build()
}

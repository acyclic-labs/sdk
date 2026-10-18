#[derive(Clone, Copy, Debug)]
pub(super) enum Violation {
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
pub(super) const VIOLATIONS: [Violation; 11] = [
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

#[derive(Clone, Copy, Debug)]
pub(super) enum CapabilityFault {
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

#[derive(Clone, Copy, Debug)]
pub(super) enum DelegationFault {
    WidenParent,
    SkipAttenuation,
    RejectAll,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Access {
    Project,
    SharedRead,
    OwnedPrivate,
    ExactReference,
}
pub(super) const ACCESS: [Access; 4] = [
    Access::Project,
    Access::SharedRead,
    Access::OwnedPrivate,
    Access::ExactReference,
];

#[derive(Clone, Copy, Debug)]
pub(super) enum ManifestCase {
    Valid,
    WrongReader,
    NoAccess,
}
pub(super) const MANIFEST_CASES: [ManifestCase; 3] = [
    ManifestCase::Valid,
    ManifestCase::WrongReader,
    ManifestCase::NoAccess,
];

#[derive(Clone, Copy, Debug)]
pub(super) enum ManifestFault {
    RejectValid,
    AcceptInvalid,
    DropMember,
    RequireWrite,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Fault {
    CheckOriginal,
    Capability(CapabilityFault),
    InvalidApi(usize),
    Delegation(DelegationFault),
    Manifest(ManifestFault),
}

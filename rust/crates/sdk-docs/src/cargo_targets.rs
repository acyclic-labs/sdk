use cargo_metadata::{Target, TargetKind};

pub fn is_library_target(target: &Target) -> bool {
    target.kind.iter().any(|kind| {
        matches!(
            kind,
            TargetKind::ProcMacro
                | TargetKind::Lib
                | TargetKind::RLib
                | TargetKind::CDyLib
                | TargetKind::StaticLib
                | TargetKind::DyLib
        )
    })
}

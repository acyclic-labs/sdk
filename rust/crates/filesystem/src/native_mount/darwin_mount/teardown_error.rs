/// Actual provider return code from Darwin teardown.
#[derive(Debug)]
pub struct DarwinTeardownFailure(
    /// The provider's original return code.
    pub i32,
);

impl std::fmt::Display for DarwinTeardownFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Darwin mount loop exited with status {}", self.0)
    }
}

impl std::error::Error for DarwinTeardownFailure {}

/// Original Disk Arbitration refusal from Darwin mount retirement.
///
/// This status does not certify kernel detachment. The enclosing I/O error
/// classifies the refusal while retaining this native cause for inspection.
#[derive(Debug)]
pub struct DarwinRetirementFailure(
    /// The original `DAReturn` supplied to the terminal unmount callback.
    pub i32,
);

impl std::fmt::Display for DarwinRetirementFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Darwin mount retirement refused with status {:#x}",
            self.0
        )
    }
}

impl std::error::Error for DarwinRetirementFailure {}

pub(super) fn retirement_result(status: i32, dissent: i32) -> std::io::Result<()> {
    if dissent != 0 {
        return Err(std::io::Error::new(
            std::io::Error::from_raw_os_error(status).kind(),
            DarwinRetirementFailure(dissent),
        ));
    }
    if status != 0 {
        return Err(std::io::Error::from_raw_os_error(status));
    }
    Ok(())
}

/// A concurrent unmount may win before our request reaches Disk Arbitration.
/// Only the kernel mount-table check can confirm that retirement in this case.
pub(super) fn retirement_after_probe(
    result: Result<(), super::NativeMountError>,
    detached: bool,
) -> Result<(), super::NativeMountError> {
    use super::NativeMountError;
    match result {
        Err(
            NativeMountError::Io(error)
            | NativeMountError::Materialization(crate::NativeTreeMaterializationError::Io(error)),
        ) if detached && error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

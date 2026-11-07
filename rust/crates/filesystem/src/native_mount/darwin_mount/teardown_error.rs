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

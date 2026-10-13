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

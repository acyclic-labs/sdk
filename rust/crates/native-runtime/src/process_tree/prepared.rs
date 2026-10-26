use crate::meter::ReadBuffer;

/// Paid storage for a closed Unix spawn on the existing process-tree engine.
#[cfg(unix)]
pub struct PreparedClosedProcess {
    pub(super) arguments: ReadBuffer<std::mem::MaybeUninit<*mut libc::c_char>>,
    pub(super) environment: ReadBuffer<ReadBuffer<u8>>,
    pub(super) environment_pointers: ReadBuffer<*mut libc::c_char>,
}
// SAFETY: argument slots are inert outside synchronous spawn and are
// overwritten before reuse. Environment pointers address this frame's stable owned bytes.
#[cfg(unix)]
unsafe impl Send for PreparedClosedProcess {}

#[cfg(unix)]
impl std::fmt::Debug for PreparedClosedProcess {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedClosedProcess")
            .field(
                "argument_count",
                &self.arguments.data.len().saturating_sub(2),
            )
            .field("environment_entries", &self.environment.data.len())
            .finish_non_exhaustive()
    }
}

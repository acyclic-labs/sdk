use super::*;
impl GenerationRef {
    #[cfg(feature = "filesystem")]
    pub(crate) fn clone_with_io(&self, io: &crate::filesystem::PreparationIo) -> Result<Self> {
        crate::executor::with_history_io(Some(io), |work| self.0.copy(work).map(Self))
    }
}

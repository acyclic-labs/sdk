use super::*;

impl ConversationState {
    pub(super) fn require_native_work(
        &self,
        work: &Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if (self.native_custody.messages.is_some()
            || self.native_custody.digests.is_some()
            || self.native_custody.archive.is_some()
            || self.native_custody.index_buffer.is_some()
            || !self.native_custody.indexes.is_empty())
            && !work
                .as_deref()
                .is_some_and(|io| io.original_native_work().is_some())
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native conversation ownership requires explicit native work",
            });
        }
        let _ = work;
        Ok(())
    }
}

use super::*;

impl Checkpoint {
    pub(super) fn append(
        &mut self,
        build: impl FnOnce(&mut Publication) -> Result<Change, NativeObjectsError>,
    ) -> Result<(), NativeObjectsError> {
        // Incoming custody survives a flush of the preceding page.
        let mut custody = Publication::new(self.pending.meter());
        let item = build(&mut custody)?;
        if self.pending.should_flush(item.encoded_len()) {
            self.flush()?;
        }
        self.pending.insert(item, custody)
    }
}

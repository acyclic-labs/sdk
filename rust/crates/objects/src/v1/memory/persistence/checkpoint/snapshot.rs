use super::*;

impl Checkpoint {
    pub(super) fn snapshot(&mut self, state: &State) -> Result<(), NativeObjectsError> {
        super::super::super::durable::snapshot::walk(state, |entry| {
            self.append(|publication| entry.record(publication))
        })?;
        if self.pending.has_changes() || self.operations == 0 && self.pending.delta.sequence != 0 {
            self.flush()?;
        }
        Ok(())
    }
}

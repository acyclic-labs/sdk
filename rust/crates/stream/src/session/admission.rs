use super::*;

impl<P: StreamProvider> Session<P> {
    pub(super) fn refresh(&mut self, id: Option<u64>) -> Result<(), StreamError> {
        let readiness = policy::room(
            self.limits.pending_frames,
            self.limits.pending_bytes,
            self.pending.len(),
            self.pending_bytes,
            DATA_FRAME_BYTES,
            self.credit.frames > 0 && self.credit.bytes > 0,
        );
        let changed = readiness != self.readiness;
        self.readiness = readiness;
        if changed {
            for channel in self.channels.values() {
                update(channel, readiness)?;
            }
        } else if let Some(channel) = id.and_then(|id| self.channels.get(&id)) {
            update(channel, readiness)?;
        }
        Ok(())
    }
}

fn update(channel: &Channel, readiness: (bool, bool, bool)) -> Result<(), StreamError> {
    channel.enabled.set(
        channel.ending.is_none()
            && policy::enabled(
                channel.latest,
                channel.pending,
                readiness,
                channel.credit.frames > 0 && channel.credit.bytes > 0,
            ),
    )
}

pub(super) async fn next_record(
    records: &mut crate::RecordStream,
    enabled: &gate::Gate,
) -> Result<Option<crate::Record>, StreamError> {
    enabled.next(records).await?.transpose()
}

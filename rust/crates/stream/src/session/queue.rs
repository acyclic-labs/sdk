use super::*;

impl<P: StreamProvider> Session<P> {
    pub(super) fn record(
        &mut self,
        identity: wire::SessionChannel,
        record: Result<Event, StreamError>,
    ) -> Result<(), StreamError> {
        let Some(channel) = self.channels.get(&identity.channel_id).filter(|channel| {
            channel.generation == identity.generation && channel.ending.is_none()
        }) else {
            return Ok(());
        };
        let (after, item, key) = match record {
            Ok(Event::Opened) => {
                self.controls.push_back(frame(
                    identity.channel_id,
                    identity.generation,
                    Body::Opened(wire::SessionOpened {
                        resume: Some(channel.applied.clone()),
                    }),
                ));
                return Ok(());
            }
            Ok(Event::Child(child, after)) => (
                after,
                Item::Child(wire::Child {
                    path: child.path.to_string(),
                }),
                None,
            ),
            Ok(Event::Record(record)) => {
                let next = record
                    .sequence
                    .checked_add(1)
                    .ok_or(StreamError::OutOfRange)?;
                let (item, key) = if channel.latest {
                    let value = match wire::SessionLatestValue::decode(record.value.as_ref()) {
                        Ok(value) if !value.key.is_empty() => value,
                        _ => return self.record(identity, Err(StreamError::InvalidArgument)),
                    };
                    let key = (!value.attention).then(|| value.key.clone());
                    (Item::LatestValue(value), key)
                } else {
                    (Item::Record(wire_codec::record_wire(record)), None)
                };
                (cursor(next), item, key)
            }
            Err(error) => {
                let channel = self
                    .channels
                    .get_mut(&identity.channel_id)
                    .ok_or(StreamError::Unavailable)?;
                channel.ending = Some(error);
                channel.cancel.take();
                self.finish_if_drained(identity.channel_id)?;
                return Ok(());
            }
        };
        let next = after.next;
        let frame = frame(
            identity.channel_id,
            identity.generation,
            Body::Data(wire::SessionData {
                after: Some(after),
                accepted: next,
                durable: 0,
                item: Some(item),
            }),
        );
        // Replace only unsent ordinary values of the same key. Reinsertion
        // retains the order of every remaining cursor and every attention event.
        if let Some(key) = &key {
            let prior = self.pending.iter().rposition(|pending| {
                pending.frame.channel_id == identity.channel_id && pending.key.as_ref() == Some(key)
            });
            if let Some(index) = prior
                && let Some(old) = self.pending.remove(index)
            {
                self.pending_bytes -= old.frame.encoded_len();
                self.channels
                    .get_mut(&identity.channel_id)
                    .ok_or(StreamError::Unavailable)?
                    .pending -= 1;
            }
        }
        if self.pending_bytes.saturating_add(frame.encoded_len()) > self.limits.pending_bytes
            || self.pending.len() >= self.limits.pending_frames
        {
            return Err(StreamError::Capacity);
        }
        self.pending_bytes += frame.encoded_len();
        self.pending.push_back(Pending { frame, key });
        self.channels
            .get_mut(&identity.channel_id)
            .ok_or(StreamError::Unavailable)?
            .pending += 1;
        self.refresh(Some(identity.channel_id))?;
        Ok(())
    }

    fn finish_if_drained(&mut self, id: u64) -> Result<(), StreamError> {
        if self
            .pending
            .iter()
            .any(|pending| pending.frame.channel_id == id)
        {
            return Ok(());
        }
        if let Some(channel) = self.channels.get(&id)
            && let Some(error) = &channel.ending
        {
            self.controls.push_back(frame(
                id,
                channel.generation,
                Body::Closed(wire::SessionClosed {
                    code: error.code().into(),
                    message: String::new(),
                }),
            ));
            self.remove(id)?;
        }
        Ok(())
    }

    pub(super) fn ready(&mut self) -> Result<Option<wire::SessionFrame>, StreamError> {
        if self.credit.frames == 0 || self.credit.bytes == 0 {
            return Ok(None);
        }
        for index in 0..self.pending.len() {
            let pending = self.pending.get(index).ok_or(StreamError::Unavailable)?;
            let channel = self
                .channels
                .get(&pending.frame.channel_id)
                .ok_or(StreamError::Unavailable)?;
            if channel.credit.frames == 0 || channel.credit.bytes == 0 {
                continue;
            }
            // A blocked earlier record on the same channel cannot be overtaken.
            if self
                .pending
                .iter()
                .take(index)
                .any(|old| old.frame.channel_id == pending.frame.channel_id)
            {
                continue;
            }
            let mut credit = self.credit;
            if wire_codec::session::consume_credit(&mut credit, &pending.frame).is_err() {
                continue;
            }
            let mut local = channel.credit;
            if wire_codec::session::consume_credit(&mut local, &pending.frame).is_err() {
                continue;
            }
            self.credit = credit;
            let pending = self.pending.remove(index).ok_or(StreamError::Unavailable)?;
            self.pending_bytes -= pending.frame.encoded_len();
            let channel = self
                .channels
                .get_mut(&pending.frame.channel_id)
                .ok_or(StreamError::Unavailable)?;
            channel.credit = local;
            channel.pending -= 1;
            if let Some(Body::Data(data)) = &pending.frame.body {
                channel.sent = data.after.clone().ok_or(StreamError::Unavailable)?;
            }
            self.finish_if_drained(pending.frame.channel_id)?;
            self.refresh(Some(pending.frame.channel_id))?;
            return Ok(Some(pending.frame));
        }
        Ok(None)
    }
}

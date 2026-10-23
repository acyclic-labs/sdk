use super::*;

impl<P: StreamProvider + ?Sized> Session<P> {
    pub(super) fn record(
        &mut self,
        identity: wire::SessionChannel,
        record: Result<Event, StreamError>,
    ) -> Result<(), StreamError> {
        if let Some(channel) = self.channels.get(&identity.channel_id)
            && channel.generation == identity.generation
        {
            if let Err(reason) = self.validate_authority(identity.channel_id) {
                return self.close_authority(identity, reason);
            }
            if let Ok(Event::Child(child, _)) = &record {
                let count = self.channels.len();
                let binding = self
                    .channels
                    .get_mut(&identity.channel_id)
                    .and_then(|channel| channel.binding.as_mut())
                    .ok_or(StreamError::AccessDenied)?;
                match self
                    .authority
                    .permits_child(&self.slots, binding, &child.path, count)
                {
                    Ok(true) => {}
                    Ok(false) => return Ok(()),
                    Err(reason) => return self.close_authority(identity, reason),
                }
            }
        }
        let Some(channel) = self.channels.get(&identity.channel_id).filter(|channel| {
            channel.generation == identity.generation && channel.ending.is_none()
        }) else {
            return Ok(());
        };
        let (after, item, key) = match record {
            Ok(Event::Opened(preceding)) => {
                self.controls.push_back(frame(
                    identity.channel_id,
                    identity.generation,
                    Body::Opened(wire::SessionOpened {
                        resume: Some(channel.applied.clone()),
                        preceding_commit_id: preceding
                            .map_or_else(Bytes::new, |id| Bytes::copy_from_slice(id.as_bytes())),
                    }),
                ));
                return Ok(());
            }
            Ok(Event::Progress(mark)) => {
                let channel = self
                    .channels
                    .get_mut(&identity.channel_id)
                    .ok_or(StreamError::Unavailable)?;
                channel.progress = Some(mark);
                self.flush_progress(identity.channel_id)?;
                self.refresh_source(identity.channel_id);
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
                if channel.revalidate.is_some()
                    && matches!(
                        error,
                        StreamError::AccessDenied
                            | StreamError::NotFound
                            | StreamError::PrefixNotRetained
                    )
                {
                    let generation = channel.generation;
                    self.remove(identity.channel_id)?;
                    self.controls.push_back(frame(
                        identity.channel_id,
                        generation,
                        Body::Closed(wire::SessionClosed {
                            code: error.code().into(),
                            message: String::new(),
                        }),
                    ));
                    return Ok(());
                }
                let channel = self
                    .channels
                    .get_mut(&identity.channel_id)
                    .ok_or(StreamError::Unavailable)?;
                channel.ending = Some(error);
                self.sources.remove(identity.channel_id);
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
        // Conflation is local to one bounded LATEST queue. Validate the
        // replacement before mutating it so capacity failures are atomic.
        let channel = self
            .channels
            .get_mut(&identity.channel_id)
            .ok_or(StreamError::Unavailable)?;
        let prior = key.as_ref().and_then(|key| {
            channel
                .pending
                .iter()
                .rposition(|p| p.key.as_ref() == Some(key))
        });
        let replaced = prior
            .and_then(|index| channel.pending.get(index))
            .map_or(0, |pending| pending.frame.encoded_len());
        let bytes = self.pending_bytes - replaced + frame.encoded_len();
        let count = self.pending_frames + usize::from(prior.is_none());
        if bytes > self.limits.pending_bytes || count > self.limits.pending_frames {
            return Err(StreamError::Capacity);
        }
        if let Some(index) = prior {
            channel.pending.remove(index);
        }
        channel.pending.push_back(Pending { frame, key });
        self.pending_bytes = bytes;
        self.pending_frames = count;
        self.refresh_source(identity.channel_id);
        Ok(())
    }

    fn finish_if_drained(&mut self, id: u64) -> Result<(), StreamError> {
        if self
            .channels
            .get(&id)
            .is_some_and(|c| !c.pending.is_empty())
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
        if self.credit.frames == 0 || self.credit.bytes == 0 || self.runnable.is_empty() {
            return Ok(None);
        }
        // The index contains only locally credited heads. Size ordering also
        // excludes heads that exceed global byte credit without visiting them.
        let bound = (
            usize::try_from(self.credit.bytes).unwrap_or(usize::MAX),
            u64::MAX,
        );
        let selected = self
            .ready_after
            .filter(|after| *after < bound)
            .and_then(|after| {
                self.runnable
                    .range((
                        std::ops::Bound::Excluded(after),
                        std::ops::Bound::Included(bound),
                    ))
                    .next()
                    .copied()
            })
            .or_else(|| self.runnable.range(..=bound).next().copied());
        let Some((_, id)) = selected else {
            return Ok(None);
        };
        if let Err(reason) = self.validate_authority(id) {
            let generation = self
                .channels
                .get(&id)
                .ok_or(StreamError::Unavailable)?
                .generation;
            self.close_authority(
                wire::SessionChannel {
                    channel_id: id,
                    generation,
                },
                reason,
            )?;
            return Ok(self.controls.pop_front());
        }
        if let Some(revalidate) = self
            .channels
            .get(&id)
            .and_then(|channel| channel.revalidate.as_ref())
        {
            let validation = revalidate();
            if let Err(error) = validation {
                let generation = self
                    .channels
                    .get(&id)
                    .ok_or(StreamError::Unavailable)?
                    .generation;
                self.remove(id)?;
                return Ok(Some(frame(
                    id,
                    generation,
                    Body::Closed(wire::SessionClosed {
                        code: error.code().into(),
                        message: String::new(),
                    }),
                )));
            }
        }
        self.ready_after = selected;
        #[cfg(test)]
        {
            self.head_visits += 1;
        }
        let channel = self.channels.get_mut(&id).ok_or(StreamError::Unavailable)?;
        let pending = channel
            .pending
            .pop_front()
            .ok_or(StreamError::Unavailable)?;
        wire_codec::session::consume_credit(&mut self.credit, &pending.frame)?;
        wire_codec::session::consume_credit(&mut channel.credit, &pending.frame)?;
        self.pending_bytes -= pending.frame.encoded_len();
        self.pending_frames -= 1;
        if let Some(Body::Data(data)) = &pending.frame.body {
            channel.sent = data.after.clone().ok_or(StreamError::Unavailable)?;
        }
        self.finish_if_drained(id)?;
        self.flush_progress(id)?;
        self.refresh_source(id);
        Ok(Some(pending.frame))
    }

    fn flush_progress(&mut self, id: u64) -> Result<(), StreamError> {
        let Some(channel) = self.channels.get_mut(&id) else {
            return Ok(());
        };
        if !channel.pending.is_empty() {
            return Ok(());
        }
        if let Some(mark) = channel.progress.take() {
            let after = mark.after.as_ref().ok_or(StreamError::Unavailable)?;
            if after.next <= channel.sent.next {
                return Err(StreamError::Unavailable);
            }
            channel.sent = after.clone();
            self.controls
                .push_back(frame(id, channel.generation, Body::Watermark(mark)));
        }
        Ok(())
    }
}

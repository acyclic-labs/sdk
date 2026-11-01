use super::*;

impl<P: StreamProvider + ?Sized> Session<P> {
    pub(super) fn record(
        &mut self,
        identity: wire::SessionChannel,
        record: Result<Event, StreamError>,
    ) -> Result<(), StreamError> {
        // A terminal owns its typed reason and discards withheld data even
        // after the finite producer ended. Ordinary authority/data fences must
        // neither replace that reason nor defer closure until data credit.
        let terminal = matches!(&record, Ok(Event::Terminated(_)));
        if !terminal
            && let Some(channel) = self.channels.get(&identity.channel_id)
            && channel.generation == identity.generation
            && let Err(reason) = self.validate_authority(identity.channel_id)
        {
            return self.close_authority(identity, reason);
        }
        let Some(channel) = self.channels.get(&identity.channel_id).filter(|channel| {
            channel.generation == identity.generation && (terminal || channel.ending.is_none())
        }) else {
            return Ok(());
        };
        let (frame, key) = match record {
            Ok(Event::Terminated(error)) => {
                self.remove(identity.channel_id)?;
                self.controls
                    .retain(|frame| frame.channel_id != identity.channel_id);
                self.controls.push_back(frame(
                    identity.channel_id,
                    identity.generation,
                    Body::Closed(wire::SessionClosed {
                        code: error.code().into(),
                        message: String::new(),
                    }),
                ));
                return Ok(());
            }
            Ok(Event::Enroll(enrollment)) => {
                let slot = channel
                    .binding
                    .as_ref()
                    .and_then(|binding| binding.enrollment)
                    .ok_or(StreamError::InvalidResponse)?;
                let custody = enrollment.custody.clone();
                // Register the original lifetime before the final admission check.
                let terminal = resources::terminal(custody.terminated(), identity);
                if let Err(error) = self.authority.enroll(
                    &mut self.slots,
                    slot,
                    identity,
                    enrollment,
                    self.channels.len(),
                ) {
                    return self.record(identity, Err(error));
                }
                let channel = self
                    .channels
                    .get_mut(&identity.channel_id)
                    .ok_or(StreamError::Unavailable)?;
                channel.revalidate = Some(Arc::new(move || custody.check()));
                self.terminals.insert(identity, terminal, None);
                self.deadline = self.authority.deadline(&mut self.slots);
                self.refresh_source(identity.channel_id);
                return Ok(());
            }
            Ok(Event::Opened(preceding, applied)) => {
                self.controls.push_back(frame(
                    identity.channel_id,
                    identity.generation,
                    Body::Opened(wire::SessionOpened {
                        resume: Some(channel.applied.clone()),
                        preceding_commit_id: preceding
                            .map_or_else(Bytes::new, |id| Bytes::copy_from_slice(id.as_bytes())),
                        applied,
                    }),
                ));
                return Ok(());
            }
            Ok(Event::Progress(mark)) => {
                if mark.opening_tail.is_some() {
                    self.controls.push_back(frame(
                        identity.channel_id,
                        identity.generation,
                        Body::Watermark(mark),
                    ));
                    return Ok(());
                }
                let channel = self
                    .channels
                    .get_mut(&identity.channel_id)
                    .ok_or(StreamError::Unavailable)?;
                channel.progress = Some(mark);
                self.flush_progress(identity.channel_id)?;
                self.refresh_source(identity.channel_id);
                return Ok(());
            }
            Ok(Event::Record(record)) => {
                match Frame::from_record(
                    &identity,
                    record,
                    channel.followed.as_ref(),
                    channel.latest,
                ) {
                    Ok(frame) => frame,
                    Err(error) => return self.record(identity, Err(error)),
                }
            }
            Ok(Event::CallChunk {
                index,
                total,
                bytes,
                item,
            }) => {
                let next = index.checked_add(1).ok_or(StreamError::OutOfRange)?;
                (
                    Frame::data(
                        &identity,
                        wire::SessionCursor {
                            next,
                            opaque: Bytes::copy_from_slice(&total.to_le_bytes()),
                        },
                        item(bytes),
                    ),
                    None,
                )
            }
            Ok(Event::Socket { next, bytes }) => (
                Frame::data(&identity, cursor(next), Item::ActorSocket(bytes)),
                None,
            ),
            Ok(Event::End) => {
                self.end(identity.channel_id, wire_codec::session::END_CODE)?;
                return Ok(());
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
                self.end(identity.channel_id, error.code())?;
                return Ok(());
            }
        };
        self.enqueue(identity, frame, key)
    }

    fn enqueue(
        &mut self,
        identity: wire::SessionChannel,
        frame: Frame,
        key: Option<Bytes>,
    ) -> Result<(), StreamError> {
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
            .map_or(0, |pending| pending.frame.wire().encoded_len());
        let bytes = self.pending_bytes - replaced + frame.wire().encoded_len();
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

    /// Stops both producers; `code` closes the channel after its queued frames.
    pub(super) fn end(&mut self, id: u64, code: &'static str) -> Result<(), StreamError> {
        let channel = self.channels.get_mut(&id).ok_or(StreamError::Unavailable)?;
        channel.ending = Some(code);
        channel.socket.take();
        self.sources.remove(id);
        self.finish_if_drained(id)
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
            && let Some(code) = channel.ending
        {
            self.controls.push_back(frame(
                id,
                channel.generation,
                Body::Closed(wire::SessionClosed {
                    code: code.into(),
                    message: String::new(),
                }),
            ));
            self.remove(id)?;
        }
        Ok(())
    }

    pub(super) fn ready(&mut self) -> Result<Option<Frame>, StreamError> {
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
            return Ok(self.controls.pop_front().map(Into::into));
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
                return Ok(Some(
                    frame(
                        id,
                        generation,
                        Body::Closed(wire::SessionClosed {
                            code: error.code().into(),
                            message: String::new(),
                        }),
                    )
                    .into(),
                ));
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
        wire_codec::session::consume_credit(&mut self.credit, pending.frame.wire())?;
        wire_codec::session::consume_credit(&mut channel.credit, pending.frame.wire())?;
        self.pending_bytes -= pending.frame.wire().encoded_len();
        self.pending_frames -= 1;
        if let Some(Body::Data(data)) = &pending.frame.wire().body {
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

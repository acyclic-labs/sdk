use super::*;

impl<P: StreamProvider> Session<P> {
    pub(super) fn interest(&mut self, interest: wire::SessionInterest) -> Result<(), StreamError> {
        if interest.revision <= self.revision {
            return Err(StreamError::InvalidArgument);
        }
        let mut ids = BTreeSet::new();
        let mut removed = 0;
        for remove in &interest.remove {
            unique(&mut ids, *remove)?;
            if self.channels.contains_key(&remove.channel_id) {
                self.channel(remove.channel_id, remove.generation)?;
                removed += 1;
            }
        }
        let mut added = 0;
        for subscription in &interest.upsert {
            let channel = subscription
                .channel
                .as_ref()
                .ok_or(StreamError::InvalidArgument)?;
            unique(&mut ids, *channel)?;
            match self.channels.get(&channel.channel_id) {
                Some(old) if channel.generation <= old.generation => {
                    return Err(StreamError::InvalidArgument);
                }
                None => added += 1,
                Some(_) => {}
            }
        }
        if self.channels.len() - removed + added > self.limits.channels {
            return Err(StreamError::Capacity);
        }
        // Validate and stage the whole diff before changing the active set.
        // Provider opens run independently; refusal closes only that channel.
        let mut opened = Vec::new();
        for subscription in interest.upsert {
            opened.push(self.open(subscription)?);
        }
        for remove in interest.remove {
            self.remove(remove.channel_id)?;
        }
        for (identity, channel, source) in opened {
            self.remove(identity.channel_id)?;
            self.channels.insert(identity.channel_id, channel);
            self.sources.push(source);
            self.refresh(Some(identity.channel_id))?;
        }
        self.revision = interest.revision;
        self.controls.push_back(frame(
            0,
            0,
            Body::InterestApplied(wire::SessionInterestApplied {
                revision: self.revision,
            }),
        ));
        Ok(())
    }

    fn open(
        &self,
        subscription: wire::SessionSubscription,
    ) -> Result<(wire::SessionChannel, Channel, Source), StreamError> {
        let channel = subscription.channel.ok_or(StreamError::InvalidArgument)?;
        let resume = subscription.resume.ok_or(StreamError::InvalidArgument)?;
        let (cancel, cancelled) = oneshot::channel();
        let enabled = Arc::new(gate::Gate::new());
        let identity = channel;
        let children = matches!(subscription.kind, Some(Kind::Children(_)));
        let (latest, source) = match subscription.kind {
            Some(Kind::Follow(follow)) => {
                if !resume.opaque.is_empty() {
                    return Err(StreamError::InvalidArgument);
                }
                let latest = match wire::SessionDelivery::try_from(follow.delivery) {
                    Ok(wire::SessionDelivery::All) => false,
                    Ok(wire::SessionDelivery::Latest) => true,
                    _ => return Err(StreamError::InvalidArgument),
                };
                let path = StreamPath::new(follow.path)?;
                (
                    latest,
                    follow_source(
                        Arc::clone(&self.provider),
                        path,
                        resume.next,
                        identity,
                        cancelled,
                        Arc::clone(&enabled),
                    ),
                )
            }
            Some(Kind::Children(request)) => {
                if self.limits.pending_bytes < CHILD_FRAME_BYTES { return Err(StreamError::Capacity); }
                let request = wire_codec::children_from_wire(request)?;
                children::validate_resume(&request, &resume)?;
                (
                    false,
                    children::source(
                        Arc::clone(&self.provider),
                        request,
                        resume.clone(),
                        identity,
                        cancelled,
                        Arc::clone(&enabled),
                    ),
                )
            }
            _ => return Err(StreamError::Unsupported),
        };
        Ok((
            identity,
            Channel {
                generation: identity.generation,
                latest,
                children,
                pending: 0,
                enabled,
                credit: wire::SessionWindow::default(),
                sent: resume.clone(),
                applied: resume,
                ending: None,
                cancel: Some(cancel),
            },
            source,
        ))
    }

    pub(super) fn remove(&mut self, id: u64) -> Result<(), StreamError> {
        self.channels.remove(&id);
        self.pending
            .retain(|pending| pending.frame.channel_id != id);
        self.pending_bytes = self
            .pending
            .iter()
            .map(|pending| pending.frame.encoded_len())
            .sum();
        self.refresh(None)
    }
}

// Provider opens run independently so a remote owner never stalls other follows.
fn follow_source<P: StreamProvider>(
    provider: Arc<P>,
    path: StreamPath,
    from: u64,
    identity: wire::SessionChannel,
    cancelled: oneshot::Receiver<()>,
    enabled: Arc<gate::Gate>,
) -> Source {
    let state = (
        provider,
        path,
        from,
        None::<crate::RecordStream>,
        false,
        0_u8,
        enabled,
    );
    stream::try_unfold(
        state,
        |(provider, path, mut next, mut records, mut opened, mut ended, enabled)| async move {
            loop {
                if records.is_none() {
                    records = Some(provider.follow(path.clone(), next).await?);
                    if !opened {
                        opened = true;
                        return Ok(Some((
                            Event::Opened,
                            (provider, path, next, records, opened, ended, enabled),
                        )));
                    }
                }
                if let Some(record) = admission::next_record(
                    records.as_mut().ok_or(StreamError::Unavailable)?,
                    &enabled,
                )
                .await?
                {
                    next = record
                        .sequence
                        .checked_add(1)
                        .ok_or(StreamError::OutOfRange)?;
                    ended = 0;
                    return Ok(Some((
                        Event::Record(record),
                        (provider, path, next, records, opened, ended, enabled),
                    )));
                } else {
                    // EOF is transport recovery, never an idle record wakeup.
                    // Keep already queued records on this generation intact.
                    records = None;
                    ended += 1;
                    if ended >= 3 {
                        return Err(StreamError::Unavailable);
                    }
                    if ended > 1 {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                }
            }
        },
    )
    .take_until(cancelled)
    .map(move |event| (identity, event))
    .boxed()
}

fn unique(ids: &mut BTreeSet<u64>, channel: wire::SessionChannel) -> Result<(), StreamError> {
    if channel.channel_id == 0
        || channel.channel_id % 2 != 1
        || channel.generation == 0
        || !ids.insert(channel.channel_id)
    {
        return Err(StreamError::InvalidArgument);
    }
    Ok(())
}

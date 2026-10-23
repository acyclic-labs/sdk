use super::*;
struct Opened {
    identity: wire::SessionChannel,
    channel: Channel,
    source: Source,
    terminal: Option<Source>,
}
enum Admission {
    Open(Box<Opened>),
    Denied(wire::SessionChannel, authority::Rejection),
}

impl<P: StreamProvider + ?Sized> Session<P> {
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
            opened.push(self.open(subscription, self.channels.len() - removed + added)?);
        }
        for remove in interest.remove {
            self.remove(remove.channel_id)?;
        }
        for admission in opened {
            let opened = match admission {
                Admission::Open(opened) => opened,
                Admission::Denied(identity, reason) => {
                    self.close_authority(identity, reason)?;
                    continue;
                }
            };
            let Opened {
                identity,
                channel,
                source,
                terminal,
            } = *opened;
            self.remove(identity.channel_id)?;
            let mode = channel.source_mode();
            self.channels.insert(identity.channel_id, channel);
            self.sources.insert(identity, source, mode);
            if let Some(terminal) = terminal {
                self.terminals.insert(identity, terminal, None);
            }
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
        channels: usize,
    ) -> Result<Admission, StreamError> {
        let channel = subscription.channel.ok_or(StreamError::InvalidArgument)?;
        let resume = subscription.resume.ok_or(StreamError::InvalidArgument)?;
        let identity = channel;
        let opaque_cursor = matches!(
            subscription.kind,
            Some(Kind::Children(_) | Kind::FsPublications(_))
        );
        let binding;
        let mut terminal = None;
        let (latest, source, revalidate) = match subscription.kind {
            Some(Kind::Follow(follow)) => {
                let preceding = wire_codec::session::preceding_follow(&follow, &resume)?;
                if !resume.opaque.is_empty() {
                    return Err(StreamError::InvalidArgument);
                }
                let latest = match wire::SessionDelivery::try_from(follow.delivery) {
                    Ok(wire::SessionDelivery::All) => false,
                    Ok(wire::SessionDelivery::Latest) => true,
                    _ => return Err(StreamError::InvalidArgument),
                };
                if follow.resource.starts_with("fs:/") {
                    // Filesystem changes are typed records, never latest-value
                    // application payloads. Missing owner routing fails closed.
                    if latest { return Err(StreamError::InvalidArgument); }
                    let (authorization, filter, context) = match self.authority.bind_resource(
                        &self.slots, subscription.credential, follow.resource.clone(),
                        subscription.filter.unwrap_or_default(), channels,
                    ) {
                        Ok(binding) => binding,
                        Err(reason) => return Ok(Admission::Denied(identity, reason)),
                    };
                    let prepared = self.resources.as_ref().ok_or(StreamError::Unsupported)?.follow(ResourceFollowRequest {
                        resource: follow.resource, from: resume.next, preceding, filter,
                        authorization: context,
                    })?;
                    binding = Some(authorization);
                    terminal = Some(publications::terminal(prepared.terminal, identity));
                    (false, resources::source(prepared.open, identity, preceding), Some(prepared.revalidate))
                } else {
                let path = wire_codec::session::stream_path(&follow.resource)?;
                let (authorization, filter) = match self.authority.bind(
                    &self.slots,
                    subscription.credential,
                    &path,
                    subscription.filter.unwrap_or_default(),
                    channels,
                ) {
                    Ok(binding) => binding,
                    Err(reason) => return Ok(Admission::Denied(identity, reason)),
                };
                binding = Some(authorization);
                let source = follow_source(
                    Arc::clone(&self.provider),
                    path,
                    resume.next,
                    preceding,
                    filter,
                    identity,
                    self.executor.clone(),
                );
                (latest, source, None)
                }
            }
            Some(Kind::Children(request)) => {
                if self.limits.pending_bytes < CHILD_FRAME_BYTES {
                    return Err(StreamError::Capacity);
                }
                let request = wire_codec::children_from_wire(request)?;
                children::validate_resume(&request, &resume)?;
                binding = Some(
                    match self.authority.bind_children(
                        &self.slots,
                        subscription.credential,
                        request.parent.as_ref(),
                        subscription.filter.unwrap_or_default(),
                        channels,
                    ) {
                        Ok(binding) => binding,
                        Err(reason) => return Ok(Admission::Denied(identity, reason)),
                    },
                );
                (
                    false,
                    children::source(
                        Arc::clone(&self.provider),
                        request,
                        resume.clone(),
                        identity,
                    ),
                    None,
                )
            }
            _ => return Err(StreamError::Unsupported),
        };
        Ok(Admission::Open(Box::new(Opened {
            identity,
            channel: Channel {
                binding,
                generation: identity.generation,
                latest,
                opaque_cursor,
                revalidate,
                pending: VecDeque::new(),
                progress: None,
                indexed: None,
                credit: wire::SessionWindow::default(),
                sent: resume.clone(),
                applied: resume,
                ending: None,
            },
            source,
            terminal,
        })))
    }

    pub(super) fn remove(&mut self, id: u64) -> Result<(), StreamError> {
        self.sources.remove(id);
        self.terminals.remove(id);
        if let Some(channel) = self.channels.remove(&id) {
            if let Some(length) = channel.indexed {
                self.runnable.remove(&(length, id));
            }
            self.pending_frames -= channel.pending.len();
            self.pending_bytes -= channel
                .pending
                .iter()
                .map(|p| p.frame.encoded_len())
                .sum::<usize>();
        }
        Ok(())
    }
}

// Provider opens run independently so a remote owner never stalls other follows.
fn follow_source<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
    path: StreamPath,
    from: u64,
    preceding: Option<crate::CommitId>,
    filter: crate::FollowFilter,
    identity: wire::SessionChannel,
    executor: Arc<dyn runtime::Executor>,
) -> Source {
    let state = (
        provider,
        executor,
        path,
        from,
        preceding,
        filter,
        None::<crate::FollowStream>,
        false,
        0_u8,
    );
    Box::pin(
        stream::try_unfold(
            state,
            |(
                provider,
                executor,
                path,
                mut next,
                mut preceding,
                filter,
                mut records,
                mut opened,
                mut ended,
            )| async move {
                loop {
                    if records.is_none() {
                        records = Some(
                            provider
                                .follow_updates(path.clone(), next, preceding, filter.clone())
                                .await?,
                        );
                        if !opened {
                            opened = true;
                            return Ok(Some((
                                Event::Opened(preceding),
                                (
                                    provider, executor, path, next, preceding, filter, records,
                                    opened, ended,
                                ),
                            )));
                        }
                    }
                    if let Some(update) = records
                        .as_mut()
                        .ok_or(StreamError::Unavailable)?
                        .next()
                        .await
                        .transpose()?
                    {
                        let event = match update {
                            crate::FollowUpdate::Record(record) => {
                                next = record
                                    .sequence
                                    .checked_add(1)
                                    .ok_or(StreamError::OutOfRange)?;
                                preceding = preceding.map(|_| record.commit_id);
                                Event::Record(record)
                            }
                            crate::FollowUpdate::Progress {
                                next: cut,
                                preceding: anchor,
                            } => {
                                next = cut;
                                preceding = preceding.map(|_| anchor);
                                Event::Progress(wire::SessionWatermark {
                                    accepted: cut,
                                    durable: 0,
                                    after: Some(cursor(cut)),
                                    preceding_commit_id: Bytes::copy_from_slice(anchor.as_bytes()),
                                })
                            }
                        };
                        ended = 0;
                        return Ok(Some((
                            event,
                            (
                                provider, executor, path, next, preceding, filter, records, opened,
                                ended,
                            ),
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
                            executor.sleep(10).await;
                        }
                    }
                }
            },
        )
        .map(move |event| (identity, event)),
    )
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

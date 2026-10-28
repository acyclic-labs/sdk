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
    Failed(wire::SessionChannel, StreamError),
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
                Admission::Failed(identity, error) => {
                    self.remove(identity.channel_id)?;
                    self.controls.push_back(frame(
                        identity.channel_id,
                        identity.generation,
                        Body::Closed(wire::SessionClosed {
                            code: error.code().into(),
                            message: String::new(),
                        }),
                    ));
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
        let mut opaque_cursor = false;
        let binding;
        let mut terminal = None;
        let (latest, source, revalidate) = match subscription.kind {
            Some(Kind::Follow(follow)) => {
                let (preceding, latest, until) = follow_parameters(&follow, &resume)?;
                if follow.resource.starts_with("fs:/") {
                    // Filesystem changes are typed records, never latest-value
                    // application payloads. Missing owner routing fails closed.
                    if latest {
                        return Err(StreamError::InvalidArgument);
                    }
                    let (authorization, filter, context) = match self.authority.bind_resource(
                        &self.slots,
                        subscription.credential,
                        follow.resource.clone(),
                        subscription.filter.unwrap_or_default(),
                        channels,
                    ) {
                        Ok(binding) => binding,
                        Err(reason) => return Ok(Admission::Denied(identity, reason)),
                    };
                    let Some(provider) = self.resources.as_ref() else {
                        return Ok(Admission::Failed(identity, StreamError::Unsupported));
                    };
                    let prepared = match provider.follow(ResourceFollowRequest {
                        resource: follow.resource,
                        from: resume.next,
                        preceding,
                        filter,
                        authorization: context,
                    }) {
                        Ok(prepared) => prepared,
                        Err(error) => return Ok(Admission::Failed(identity, error)),
                    };
                    binding = Some(authorization);
                    terminal = Some(resources::terminal(prepared.terminal, identity));
                    (
                        false,
                        resources::source(prepared.open, identity, preceding),
                        Some(prepared.revalidate),
                    )
                } else {
                    let path = wire_codec::session::stream_path(&follow.resource)?;
                    let at_least = match wire_codec::cut_from_wire(follow.at_least.clone()) {
                        Ok(at_least) => at_least,
                        Err(error) => return Ok(Admission::Failed(identity, error)),
                    };
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
                        Follow {
                            provider: Arc::clone(&self.provider),
                            executor: self.executor.clone(),
                            path,
                            next: resume.next,
                            preceding,
                            until,
                            end: None,
                            filter,
                            at_least,
                            records: None,
                            opened: false,
                            ended: 0,
                        },
                        identity,
                    );
                    (latest, source, None)
                }
            }
            Some(Kind::ActorCall(invocation)) => {
                if invocation.is_empty()
                    || invocation.len() > crate::MAX_COMMAND_BYTES
                    || resume.next != 0
                    || !resume.opaque.is_empty()
                    || subscription.filter.is_some()
                {
                    return Err(StreamError::InvalidArgument);
                }
                let Some(provider) = self.calls.as_ref() else {
                    return Ok(Admission::Failed(identity, StreamError::Unsupported));
                };
                let resource = match provider.resource(&invocation) {
                    Ok(resource) => resource,
                    Err(error) => return Ok(Admission::Failed(identity, error)),
                };
                let (authorization, context) = match self.authority.bind_call(
                    &self.slots,
                    subscription.credential,
                    resource,
                    channels,
                ) {
                    Ok(binding) => binding,
                    Err(reason) => return Ok(Admission::Denied(identity, reason)),
                };
                binding = Some(authorization);
                opaque_cursor = true;
                (
                    false,
                    calls::source(Arc::clone(provider), invocation, context, identity),
                    None,
                )
            }
            Some(Kind::ChildrenPage(request)) => {
                if resume.next != 0 || !resume.opaque.is_empty() || subscription.filter.is_some() {
                    return Err(StreamError::InvalidArgument);
                }
                let (authorization, source) =
                    match self.page(request, subscription.credential, channels, identity)? {
                        Ok(page) => page,
                        Err(reason) => return Ok(Admission::Denied(identity, reason)),
                    };
                binding = Some(authorization);
                opaque_cursor = true;
                (false, source, None)
            }
            _ => return Err(StreamError::Unsupported),
        };
        Ok(Admission::Open(Box::new(Opened {
            identity,
            channel: Channel::opened(binding, identity, resume, revalidate, latest, opaque_cursor),
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

impl<P: StreamProvider + ?Sized> Session<P> {
    /// Admits one children page under `streams/list` on its parent.
    fn page(
        &self,
        request: wire::ChildrenPageRequest,
        credential: u32,
        channels: usize,
        identity: wire::SessionChannel,
    ) -> Result<Result<(authority::Binding, Source), authority::Rejection>, StreamError> {
        let request = wire_codec::children_page_from_wire(request)?;
        Ok(self
            .authority
            .bind_list(&self.slots, credential, request.parent.as_ref(), channels)
            .map(|(binding, visibility)| {
                let source =
                    pages::source(Arc::clone(&self.provider), request, visibility, identity);
                (binding, source)
            }))
    }
}

fn follow_parameters(
    follow: &wire::SessionFollow,
    resume: &wire::SessionCursor,
) -> Result<(Option<crate::CommitId>, bool, Option<u64>), StreamError> {
    let preceding = wire_codec::session::preceding_follow(follow, resume)?;
    let until = wire_codec::session::follow_bound(follow)?;
    if !resume.opaque.is_empty() {
        return Err(StreamError::InvalidArgument);
    }
    let latest = match wire::SessionDelivery::try_from(follow.delivery) {
        Ok(wire::SessionDelivery::All) => false,
        Ok(wire::SessionDelivery::Latest) => true,
        _ => return Err(StreamError::InvalidArgument),
    };
    Ok((preceding, latest, until))
}
impl Channel {
    fn opened(
        binding: Option<authority::Binding>,
        identity: wire::SessionChannel,
        resume: wire::SessionCursor,
        revalidate: Option<ResourceRevalidation>,
        latest: bool,
        opaque_cursor: bool,
    ) -> Self {
        Self {
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
        }
    }
}

// Provider opens run independently so a remote owner never stalls other follows.
struct Follow<P: ?Sized> {
    provider: Arc<P>,
    executor: Arc<dyn crate::runtime::Executor>,
    path: StreamPath,
    next: u64,
    preceding: Option<crate::CommitId>,
    /// Requested exclusive end; resolved against the tail at open into `end`.
    until: Option<u64>,
    end: Option<u64>,
    filter: crate::FollowFilter,
    /// The subscription's floor; every reopen keeps it.
    at_least: crate::RevisionCut,
    records: Option<crate::FollowStream>,
    opened: bool,
    ended: u8,
}

/// One ALL/LATEST follow. A bounded follow (`until`) is the session read: it
/// resolves the stream as `read` does, ends at `min(until, tail at open)` and
/// never waits for appends. Delivery, anchor checks and credit are shared.
fn follow_source<P: StreamProvider + ?Sized>(
    state: Follow<P>,
    identity: wire::SessionChannel,
) -> Source {
    Box::pin(
        stream::try_unfold(Some(state), |state| async move {
            let Some(mut state) = state else {
                return Ok(None);
            };
            let event = state.event().await?;
            let state = (!matches!(event, Event::End)).then_some(state);
            Ok(Some((event, state)))
        })
        .map(move |event| (identity, event)),
    )
}

impl<P: StreamProvider + ?Sized> Follow<P> {
    async fn event(&mut self) -> Result<Event, StreamError> {
        loop {
            if self.records.is_none() {
                // The first open waits for the floor before a bounded read pins
                // its tail, so the pinned range covers the caller's own writes.
                let applied = if self.opened {
                    None
                } else {
                    crate::revision::served(&*self.provider, &self.at_least).await?
                };
                if let (Some(until), None) = (self.until, self.end) {
                    // Pin the range like a read: an absent stream or a start
                    // beyond the tail fails, and appends after open never count.
                    let tail = self.provider.tail(self.path.clone()).await?;
                    if self.next > tail {
                        return Err(StreamError::OutOfRange);
                    }
                    self.end = Some(until.min(tail));
                }
                self.records = Some(
                    self.provider
                        .follow(crate::FollowRequest {
                            path: self.path.clone(),
                            from: self.next,
                            preceding: self.preceding,
                            filter: self.filter.clone(),
                            progress: true,
                            at_least: self.at_least.clone(),
                        })
                        .await?,
                );
                if !self.opened {
                    self.opened = true;
                    return Ok(Event::Opened(
                        self.preceding,
                        applied.as_ref().map(wire_codec::revision_to_wire),
                    ));
                }
            }
            if self.end.is_some_and(|end| self.next >= end) {
                return Ok(Event::End);
            }
            if let Some(update) = self
                .records
                .as_mut()
                .ok_or(StreamError::Unavailable)?
                .next()
                .await
                .transpose()?
            {
                self.ended = 0;
                return match update {
                    crate::FollowUpdate::Record(record) => {
                        self.next = record
                            .sequence
                            .checked_add(1)
                            .ok_or(StreamError::OutOfRange)?;
                        self.preceding = self.preceding.map(|_| record.commit_id);
                        Ok(Event::Record(record))
                    }
                    crate::FollowUpdate::Progress {
                        next: cut,
                        preceding: anchor,
                    } => {
                        self.next = cut;
                        self.preceding = self.preceding.map(|_| anchor);
                        Ok(Event::Progress(wire::SessionWatermark {
                            accepted: cut,
                            durable: 0,
                            after: Some(cursor(cut)),
                            preceding_commit_id: Bytes::copy_from_slice(anchor.as_bytes()),
                        }))
                    }
                };
            }
            // EOF is transport recovery, never an idle record wakeup.
            // Keep already queued records on this generation intact.
            self.records = None;
            self.ended += 1;
            if self.ended >= 3 {
                return Err(StreamError::Unavailable);
            }
            if self.ended > 1 {
                self.executor.sleep(10).await;
            }
        }
    }
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

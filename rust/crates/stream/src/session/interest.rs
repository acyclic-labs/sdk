use super::*;
struct Opened {
    identity: wire::SessionChannel,
    channel: Channel,
    source: Source,
    terminal: Option<Source>,
    socket: Option<Box<dyn SocketInbound>>,
    input: Option<calls::InputProducer>,
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
                mut channel,
                source,
                terminal,
                socket,
                input,
            } = *opened;
            channel.socket = socket;
            channel.input = input;
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
        let mut socket = None;
        let mut input = None;
        // Records of a stream follow spell only an origin other than its path.
        let mut followed = None;
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
                        resources::source(prepared.open, identity, preceding, follow.opening_cut),
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
                    followed = Some(path.clone());
                    let source = follow_source(
                        Follow {
                            provider: Arc::clone(&self.provider),
                            path,
                            next: resume.next,
                            preceding,
                            until,
                            bootstrap_head: follow.bootstrap_head && resume.next == 0,
                            bootstrap: None,
                            when_absent: crate::WhenAbsent::from_wire(follow.when_absent)?,
                            end: None,
                            filter,
                            at_least,
                            records: None,
                            key: wire_codec::read_key_from_wire(follow.key.clone())?,
                            read_limit: follow.read_limit,
                            key_records: None,
                            opened: false,
                            opening_cut: follow.opening_cut,
                        },
                        identity,
                    );
                    (latest, source, None)
                }
            }
            Some(Kind::ActorCall(invocation)) => {
                if invocation.invocation.is_empty()
                    || invocation.invocation.len() > crate::MAX_COMMAND_BYTES
                    || resume.next != 0
                    || !resume.opaque.is_empty()
                    || subscription.filter.is_some()
                {
                    return Err(StreamError::InvalidArgument);
                }
                let Some(provider) = self.calls.as_ref() else {
                    return Ok(Admission::Failed(identity, StreamError::Unsupported));
                };
                let resource = match provider.resource(&invocation.invocation) {
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
                let body = invocation.input.then(|| {
                    let (producer, body) =
                        calls::InputProducer::new(identity, sync::Arc::clone(&self.input_released));
                    input = Some(producer);
                    body
                });
                (
                    false,
                    calls::source(Arc::clone(provider), invocation, context, identity, body),
                    None,
                )
            }
            Some(Kind::ActorSocket(open)) => {
                if open.is_empty()
                    || open.len() > crate::MAX_COMMAND_BYTES
                    || resume.next != 0
                    || !resume.opaque.is_empty()
                    || subscription.filter.is_some()
                {
                    return Err(StreamError::InvalidArgument);
                }
                let Some(provider) = self.sockets.as_ref() else {
                    return Ok(Admission::Failed(identity, StreamError::Unsupported));
                };
                let prepared = match Arc::clone(provider).prepare(open) {
                    Ok(prepared) => prepared,
                    Err(error) => return Ok(Admission::Failed(identity, error)),
                };
                // A socket is admitted exactly as a call to the same actor.
                let (authorization, context) = match self.authority.bind_call(
                    &self.slots,
                    subscription.credential,
                    prepared.resource().to_owned(),
                    channels,
                ) {
                    Ok(binding) => binding,
                    Err(reason) => return Ok(Admission::Denied(identity, reason)),
                };
                let opened = match prepared.open(context) {
                    Ok(opened) => opened,
                    Err(error) => return Ok(Admission::Failed(identity, error)),
                };
                binding = Some(authorization);
                socket = Some(opened.inbound);
                (false, sockets::source(opened.outbound, identity), None)
            }
            Some(Kind::Heads(request)) => {
                if resume.next != 0 || !resume.opaque.is_empty() || subscription.filter.is_some() {
                    return Err(StreamError::InvalidArgument);
                }
                let request = wire_codec::heads_from_wire(request)?;
                let (authorization, context, visibility) = match self.authority.bind_heads(
                    &self.slots,
                    subscription.credential,
                    &request,
                    channels,
                ) {
                    Ok(binding) => binding,
                    Err(reason) => return Ok(Admission::Denied(identity, reason)),
                };
                binding = Some(authorization);
                opaque_cursor = true;
                (
                    false,
                    pages::heads(
                        Arc::clone(&self.provider),
                        request,
                        context,
                        visibility,
                        identity,
                    ),
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
            channel: Channel::opened(
                binding,
                identity,
                resume,
                revalidate,
                latest,
                opaque_cursor,
                followed,
            ),
            source,
            terminal,
            socket,
            input,
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
                .map(|p| p.frame.wire().encoded_len())
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
    wire_codec::session::validate_follow(follow, resume)?;
    let preceding = wire_codec::session::preceding_follow(follow, resume)?;
    let until = wire_codec::session::follow_bound(follow)?;
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
        followed: Option<StreamPath>,
    ) -> Self {
        Self {
            binding,
            followed,
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
            socket: None,
            input: None,
        }
    }
}

// Provider opens run independently so a remote owner never stalls other follows.
struct Follow<P: ?Sized> {
    provider: Arc<P>,
    path: StreamPath,
    next: u64,
    preceding: Option<crate::CommitId>,
    /// Requested exclusive end; resolved against the tail at open into `end`.
    until: Option<u64>,
    end: Option<u64>,
    filter: crate::FollowFilter,
    /// The subscription's original revision floor.
    at_least: crate::RevisionCut,
    when_absent: crate::WhenAbsent,
    records: Option<crate::FollowStream>,
    key: Option<crate::ReadKeySeek>,
    read_limit: u32,
    key_records: Option<crate::RecordStream>,
    bootstrap_head: bool,
    bootstrap: Option<crate::Record>,
    opened: bool,
    opening_cut: bool,
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
        if self.key.is_some() {
            return self.key_event().await;
        }
        if self.records.is_none() {
            let confirmation = self.preceding;
            let mut records = self
                .provider
                .follow(crate::FollowRequest {
                    bootstrap_head: self.bootstrap_head,
                    until: self.until,
                    when_absent: self.when_absent,
                    path: self.path.clone(),
                    from: self.next,
                    preceding: self.preceding,
                    filter: self.filter.clone(),
                    progress: true,
                    at_least: self.at_least.clone(),
                })
                .await?;
            self.bootstrap_head = false;
            self.next = records.selection.from;
            self.preceding = records.selection.preceding;
            self.end = records.selection.end;
            records.selection.bootstrap = records
                .selection
                .bootstrap
                .take()
                .map(|record| self.filter.project(record))
                .transpose()?
                .flatten();
            let selection = wire_codec::selection_to_wire(&records.selection);
            self.bootstrap = records.selection.bootstrap.take();
            let applied = wire_codec::revision_to_wire(&records.snapshot);
            self.records = Some(records);
            if !self.opened {
                self.opened = true;
                return Ok(Event::Opened(confirmation, Some(applied), Some(selection)));
            }
        }
        if let Some(record) = self.bootstrap.take() {
            return Ok(Event::Record(record));
        }
        if self.end.is_some_and(|end| self.next >= end) {
            return Ok(Event::End);
        }
        while let Some(update) = self
            .records
            .as_mut()
            .ok_or(StreamError::Unavailable)?
            .next()
            .await
            .transpose()?
        {
            return match update {
                crate::FollowUpdate::Opening { tail } => {
                    if !self.opening_cut {
                        continue;
                    }
                    Ok(Event::Progress(wire::SessionWatermark {
                        opening_tail: Some(tail),
                        accepted: self.next,
                        ..Default::default()
                    }))
                }
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
                        opening_tail: None,
                        accepted: cut,
                        durable: 0,
                        after: Some(cursor(cut)),
                        preceding_commit_id: Bytes::copy_from_slice(anchor.as_bytes()),
                    }))
                }
            };
        }
        // An unbounded provider follow must remain live. Its original EOF is
        // terminal; another invocation may explicitly open a new subscription.
        Err(StreamError::Unavailable)
    }

    async fn key_event(&mut self) -> Result<Event, StreamError> {
        if !self.opened {
            self.key_records = Some(
                self.provider
                    .read(crate::ReadRequest {
                        key: self.key.clone(),
                        path: self.path.clone(),
                        from: self.next,
                        limit: self.read_limit,
                        at_least: self.at_least.clone(),
                    })
                    .await?,
            );
            self.opened = true;
            let records = self.key_records.as_ref().ok_or(StreamError::Unavailable)?;
            return Ok(Event::Opened(
                None,
                Some(wire_codec::revision_to_wire(&records.snapshot)),
                Some(wire_codec::selection_to_wire(&records.selection)),
            ));
        }
        let Some(record) = self
            .key_records
            .as_mut()
            .ok_or(StreamError::Unavailable)?
            .next()
            .await
            .transpose()?
        else {
            return Ok(Event::End);
        };
        if !self.filter.matches(&record.headers) {
            return Err(StreamError::AccessDenied);
        }
        self.next = record
            .sequence
            .checked_add(1)
            .ok_or(StreamError::OutOfRange)?;
        Ok(Event::Record(record))
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

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "interest_tests.rs"]
mod tests;

use super::*;

impl Pool {
    pub(super) fn install(
        &mut self,
        (node, mut link, response): Ready,
    ) -> Result<String, StreamError> {
        if self.links.contains_key(&node) {
            return Err(StreamError::Unavailable);
        }
        link.epoch = self.next_link;
        self.next_link = self.next_link.checked_add(1).ok_or(StreamError::Capacity)?;
        self.links.insert(node.clone(), link);
        self.read(node.clone(), response);
        self.connections.advance()?;
        Ok(node)
    }
    pub(super) fn dial(&mut self, node: wire::SessionNode, channel: Option<Endpoint>) {
        let name = node.node_id.clone();
        if self.links.contains_key(&name) || self.dials.contains_key(&name) {
            return;
        }
        let connect = self.connect.clone();
        let identity = self.identity.clone();
        let epoch = self.dial_epoch;
        let executor = self.executor.clone();
        // Capture only the subscriber. Neither a dial nor a shared Link owns
        // an interest's span; physical spans are roots with causal links.
        let Some(owner) = self.dial_owner(&name) else {
            return;
        };
        let Some(route) = self.routes.get(&owner) else {
            return;
        };
        let recovering = route.generation > 1;
        let context = route
            .observation
            .in_scope(|| super::super::runtime::OperationSpan::new_root(tracing::Span::none()));
        self.dials.insert(
            name.clone(),
            Box::pin(context.scope(async move {
                let outcome = loop {
                    let result = async {
                        let channel = match &channel {
                            Some(channel) => channel.clone(),
                            None => connect(node.clone()).await?,
                        };
                        open_link(channel, &name, identity.clone(), executor.clone()).await
                    }
                    .await;
                    match result {
                        // Existing interests own recovery until cancellation.
                        // Initial admission and credential rejection stay bounded.
                        Err(StreamError::Unavailable) if recovering => executor.sleep(100).await,
                        result => break result,
                    }
                };
                (name, epoch, outcome)
            })),
        );
    }
    pub(super) fn dial_owner(&self, node: &str) -> Option<u64> {
        self.routes
            .iter()
            .find(|(_, route)| route.node == node && route.live())
            .map(|(id, _)| *id)
    }
    pub(super) fn read(&mut self, node: String, mut response: Response) {
        let Some(link) = self.links.get(&node) else {
            return;
        };
        let epoch = link.epoch;
        let sender = Arc::clone(&link.sender);
        self.reads.push(Box::pin(async move {
            let frame = {
                let closed = sender.cancelled();
                futures::pin_mut!(closed);
                match future::select(response.next(), closed).await {
                    future::Either::Left((frame, _)) => frame,
                    future::Either::Right(_) => None,
                }
            };
            (node, epoch, response, frame)
        }));
    }
    pub(super) fn send(
        &mut self,
        node: &str,
        value: wire::SessionFrame,
    ) -> Result<(), StreamError> {
        let link = self.links.get_mut(node).ok_or(StreamError::Unavailable)?;
        if value.channel_id == 0
            && let Some(Body::Window(grant)) = &value.body
        {
            wire_codec::session::grant_credit(&mut link.credit, grant)?;
        }
        let bytes = value.encoded_len();
        if let Err(error) = link.sender.push(value, bytes) {
            // A full control queue cannot safely retain remote interests after
            // cancellation. Evict this node's session, never the shared pool.
            self.fail_link(node, &error);
        }
        Ok(())
    }
    pub(super) fn subscribe(&mut self, id: u64) -> Result<(), StreamError> {
        let route = self.routes.get_mut(&id).ok_or(StreamError::Unavailable)?;
        if route.progress.cancelled() {
            return self.remove(id);
        }
        let progress::Position {
            next,
            opaque,
            preceding,
        } = route.progress.subscription()?;
        if let Kind::Follow(follow) = &mut route.kind {
            follow.preceding_commit_id =
                preceding.map_or_else(Bytes::new, |id| Bytes::copy_from_slice(&id));
        }
        route.received = next;
        route.received_opaque = opaque.clone().into();
        route.observation.in_scope(|| {
            route.observation.record("generation", route.generation);
            tracing::debug!(event = "subscribe", generation = route.generation);
        });
        route.opened = false;
        route.watermark = None;
        route.credit = wire::SessionWindow::default();
        let link = self
            .links
            .get_mut(&route.node)
            .ok_or(StreamError::Unavailable)?;
        if let Some(observation) = &link.observation {
            observation
                .operation
                .follows_from_same_subscriber(&route.observation);
        }
        let revision = link.revision.checked_add(1).ok_or(StreamError::Capacity)?;
        let subscription = wire::SessionSubscription {
            channel: Some(wire::SessionChannel {
                channel_id: id,
                generation: route.generation,
            }),
            resume: Some(wire::SessionCursor {
                next,
                opaque: opaque.into(),
            }),
            kind: Some(route.kind.clone()),
            credential: route.credential,
            filter: route.filter.clone(),
        };
        let value = frame(
            0,
            0,
            Body::Interest(wire::SessionInterest {
                revision,
                upsert: vec![subscription],
                remove: Vec::new(),
            }),
        );
        let bytes = value.encoded_len();
        match link.sender.push(value, bytes) {
            Ok(()) => link.revision = revision,
            Err(error) => self.reject(id, error),
        }
        Ok(())
    }
    pub(super) fn release(
        &mut self,
        node: &str,
        epoch: u64,
        bytes: u64,
    ) -> Result<(), StreamError> {
        let Some(link) = self.links.get_mut(node).filter(|link| link.epoch == epoch) else {
            return Ok(());
        };
        wire_codec::session::grant_credit(
            &mut link.released,
            &wire::SessionWindow { bytes, frames: 1 },
        )?;
        // Return capacity before either aggregate balance is exhausted, without
        // a timer or an acknowledgement on the record delivery path.
        if link.credit.frames
            > u32::try_from(crate::MAX_ITEMS / 2).map_err(|_| StreamError::Capacity)?
            && link.credit.bytes > (crate::MAX_COMMAND_BYTES / 2) as u64
        {
            return Ok(());
        }
        let grant = std::mem::take(&mut link.released);
        self.send(node, frame(0, 0, Body::Window(grant)))
    }
    pub(super) fn grant(&mut self, id: u64) -> Result<(), StreamError> {
        let Some(route) = self.routes.get_mut(&id).filter(|route| route.opened) else {
            return Ok(());
        };
        let capacity = u32::try_from(route.records.capacity())
            .map_err(|_| StreamError::Capacity)?
            .saturating_sub(u32::from(route.raw));
        let grant = wire::SessionWindow {
            frames: capacity.saturating_sub(route.credit.frames),
            bytes: (u64::from(capacity)
                * if matches!(route.kind, Kind::Children(_)) {
                    CHILD_FRAME_BYTES as u64
                } else {
                    FRAME_BYTES
                })
            .saturating_sub(route.credit.bytes),
        };
        if grant.frames == 0 && grant.bytes == 0 {
            return Ok(());
        }
        wire_codec::session::grant_credit(&mut route.credit, &grant)?;
        let node = route.node.clone();
        let generation = route.generation;
        self.send(&node, frame(id, generation, Body::Window(grant)))
    }

    pub(super) fn remove(&mut self, id: u64) -> Result<(), StreamError> {
        let Some(route) = self.routes.remove(&id) else {
            return Ok(());
        };
        let node = route.node.clone();
        if let Some(link) = self.links.get(&node) {
            link.channel.cancel_channel(wire::SessionChannel {
                channel_id: id,
                generation: route.generation,
            });
        }
        if let Some(link) = self.links.get_mut(&node) {
            link.revision = link.revision.checked_add(1).ok_or(StreamError::Capacity)?;
            let revision = link.revision;
            self.send(
                &node,
                frame(
                    0,
                    0,
                    Body::Interest(wire::SessionInterest {
                        revision,
                        upsert: Vec::new(),
                        remove: vec![wire::SessionChannel {
                            channel_id: id,
                            generation: route.generation,
                        }],
                    }),
                ),
            )?;
        }
        if !self.node_interested(&node) {
            self.dials.remove(&node);
            self.links.remove(&node);
            if node == self.home {
                self.reset_home();
            }
        }
        Ok(())
    }
    pub(super) fn reconnect(&mut self, node: &str) -> Result<(), StreamError> {
        let mut link = self.links.remove(node).ok_or(StreamError::Unavailable)?;
        if let Some(observation) = &mut link.observation {
            observation.finish(Some(&StreamError::Unavailable));
        }
        if node == self.home {
            self.reset_home();
        }
        // The authenticated discovery endpoint may now host a replacement
        // node. Rediscover there without retaining the previous destination;
        // moved-node endpoints must still prove their exact node identity.
        let destination = if Arc::ptr_eq(&link.channel, &self.home_channel) {
            ""
        } else {
            node
        };
        let registering = self
            .routes
            .values()
            .any(|route| route.node == node && route.live() && route.ready.is_some());
        for route in self
            .routes
            .values_mut()
            .filter(|route| route.node == node && route.live())
        {
            route.generation = route.progress.fence()?;
            route.observation.in_scope(|| {
                tracing::debug!(event = "reconnect", generation = route.generation);
            });
            route.opened = false;
            route.node = destination.into();
        }
        self.dial(
            wire::SessionNode {
                node_id: destination.into(),
                endpoint: String::new(),
                placement_epoch: 0,
            },
            Some(link.channel.clone()),
        );
        if registering && let Some(dial) = self.dials.remove(destination) {
            let executor = self.executor.clone();
            // An immediate Welcome/EOF peer can otherwise monopolize the
            // browser microtask queue and prevent registration deadlines.
            self.dials.insert(
                destination.into(),
                Box::pin(async move {
                    executor.sleep(10).await;
                    dial.await
                }),
            );
        }
        Ok(())
    }
}

async fn open_link(
    channel: Endpoint,
    destination: &str,
    identity: Bytes,
    executor: std::sync::Arc<dyn super::super::runtime::Executor>,
) -> Result<Ready, StreamError> {
    for attempt in 0..3 {
        let mut observation = Observation::session(attempt);
        let result = observation
            .operation
            .clone()
            .scope(deadline::until(
                handshake(channel.clone(), destination, identity.clone()),
                executor.sleep(1_000),
            ))
            .await
            .unwrap_or(Err(StreamError::Unavailable));
        let result = match result {
            Ok((node, mut link, response)) => {
                link.observation = Some(observation);
                Ok((node, link, response))
            }
            Err(error) => {
                observation.finish(Some(&error));
                Err(error)
            }
        };
        match result {
            Err(StreamError::Unavailable) if attempt < 2 => executor.sleep(10).await,
            result => return result,
        }
    }
    Err(StreamError::Unavailable)
}

async fn handshake(
    channel: Endpoint,
    destination: &str,
    identity: Bytes,
) -> Result<Ready, StreamError> {
    let sender = Arc::new(Outbound::new(crate::MAX_ITEMS, crate::MAX_COMMAND_BYTES));
    let credit = wire::SessionWindow {
        bytes: crate::MAX_COMMAND_BYTES as u64,
        frames: u32::try_from(crate::MAX_ITEMS).map_err(|_| StreamError::Capacity)?,
    };
    let hello = frame(
        0,
        0,
        Body::Hello(wire::SessionHello {
            version: 1,
            client_id: identity,
            destination_node: destination.into(),
            receive_bytes: credit.bytes,
            receive_frames: credit.frames,
            credentials: Vec::new(),
        }),
    );
    let bytes = hello.encoded_len();
    sender.push(hello, bytes)?;
    // Ownership is installed before either credential or connection I/O await.
    let link = Link {
        observation: None,
        channel,
        sender,
        revision: 0,
        applied_revision: 0,
        released: wire::SessionWindow::default(),
        epoch: 0,
        credit,
    };
    let input: transport::Requests = Box::pin(stream::unfold(
        Arc::clone(&link.sender),
        |sender| async move { sender.next().await.ok().map(|frame| (frame, sender)) },
    ));
    let mut response = link.channel.session(input).await?;
    let welcome = response
        .next()
        .await
        .transpose()?
        .ok_or(StreamError::Unavailable)?;
    if welcome.channel_id != 0 || welcome.generation != 0 {
        return Err(StreamError::Unavailable);
    }
    let Some(Body::Welcome(welcome)) = welcome.body else {
        return Err(StreamError::Unavailable);
    };
    if welcome.version != 1
        || welcome.node_id.is_empty()
        || (!destination.is_empty() && welcome.node_id != destination)
    {
        return Err(StreamError::Unavailable);
    }
    Ok((welcome.node_id, link, response))
}

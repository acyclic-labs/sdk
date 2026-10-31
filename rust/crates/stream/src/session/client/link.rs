use super::*;

impl Route {
    fn subscription(
        &mut self,
        id: u64,
        served: &crate::RevisionCut,
    ) -> Result<wire::SessionSubscription, StreamError> {
        let progress::Position {
            next,
            opaque,
            preceding,
        } = self.progress.subscription()?;
        if let Kind::Follow(follow) = &mut self.kind {
            follow.preceding_commit_id =
                preceding.map_or_else(Bytes::new, |id| Bytes::copy_from_slice(&id));
            // The route keeps the whole minimum it sends, so its open is
            // checked against exactly that cut. Only a Streams source carries
            // the stream floor; another source keeps its explicit minimum.
            if wire_codec::session::tracks_revisions(follow) {
                self.revisions
                    .raise_uncovered(&mut follow.at_least, served)?;
            }
        }
        self.received = next;
        self.received_opaque = opaque.clone().into();
        self.observation.in_scope(|| {
            self.observation.record("generation", self.generation);
            tracing::debug!(event = "subscribe", generation = self.generation);
        });
        self.opened = false;
        self.watermark = None;
        self.credit = wire::SessionWindow::default();
        Ok(wire::SessionSubscription {
            channel: Some(wire::SessionChannel {
                channel_id: id,
                generation: self.generation,
            }),
            resume: Some(wire::SessionCursor {
                next,
                opaque: opaque.into(),
            }),
            kind: Some(self.kind.clone()),
            credential: self.credential,
            filter: self.filter.clone(),
        })
    }
}

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
        // A rotation may race the handshake. Replay the current complete slot
        // set before subsequent subscriptions; the first request is already
        // bound to its admission credentials. Explicitly remove dropped slots.
        let opening_credentials = std::mem::take(&mut link.opening_credentials);
        let slots = opening_credentials
            .keys()
            .chain(self.credentials.keys())
            .copied()
            .collect::<BTreeSet<_>>();
        for slot in slots {
            if opening_credentials.get(&slot) == self.credentials.get(&slot) {
                continue;
            }
            let credential = self.credentials.get(&slot).cloned().unwrap_or_default();
            let value = frame(
                0,
                0,
                Body::Credential(wire::SessionCredential { slot, credential }),
            );
            let bytes = value.encoded_len();
            link.sender.push(value, bytes)?;
        }
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
        let credentials = self.credentials.clone();
        let epoch = self.dial_epoch;
        let executor = self.executor.clone();
        // Capture only the subscriber. Neither a dial nor a shared Link owns
        // an interest's span; physical spans are roots with causal links.
        let Some(owner) = self.dial_owner(&name) else {
            return;
        };
        let Some(route) = self.routes.get_mut(&owner) else {
            return;
        };
        let replayable = route.resumable();
        let subscription = match route.subscription(owner, &crate::RevisionCut::default()) {
            Ok(subscription) => subscription,
            Err(error) => {
                self.reject(owner, error);
                return;
            }
        };
        let interest = wire::SessionInterest {
            revision: 1,
            upsert: vec![subscription],
            remove: Vec::new(),
        };
        let context = route
            .observation
            .in_scope(|| super::super::runtime::OperationSpan::new_root(tracing::Span::none()));
        self.dials.insert(
            name.clone(),
            Box::pin(context.scope(async move {
                let mut attempt = 0_u32;
                let outcome = loop {
                    let mut observation = Observation::session(attempt);
                    let result = observation
                        .operation
                        .clone()
                        .scope(async {
                            let endpoint = match &channel {
                                Some(channel) => channel.clone(),
                                None => connect(node.clone()).await?,
                            };
                            handshake(
                                endpoint,
                                &name,
                                identity.clone(),
                                credentials.clone(),
                                interest.clone(),
                            )
                            .await
                        })
                        .await;
                    match result {
                        Ok((node, mut link, response)) => {
                            link.observation = Some(observation);
                            break Ok((node, link, response));
                        }
                        Err(error) => {
                            observation.finish(Some(&error));
                            // Admission carries the request. An effectful call
                            // may have committed before transport failed.
                            if error != StreamError::Unavailable || !replayable {
                                break Err(error);
                            }
                            attempt = attempt.saturating_add(1);
                            executor.sleep(10).await;
                        }
                    }
                };
                (name, epoch, outcome)
            })),
        );
    }
    pub(super) fn dial_owner(&self, node: &str) -> Option<u64> {
        self.routes
            .on_node(node)
            .find(|(_, route)| route.live())
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
        // A refused frame has already evicted its node's session.
        self.queue(node, value).map(|_refused| ())
    }
    /// Queue one frame for `node`; the inner result says whether it was
    /// queued. The outer error means `node` has no link.
    pub(super) fn queue(
        &mut self,
        node: &str,
        value: wire::SessionFrame,
    ) -> Result<Result<(), StreamError>, StreamError> {
        let link = self.links.get_mut(node).ok_or(StreamError::Unavailable)?;
        if value.channel_id == 0
            && let Some(Body::Window(grant)) = &value.body
        {
            wire_codec::session::grant_credit(&mut link.credit, grant)?;
        }
        let bytes = value.encoded_len();
        let queued = link.sender.push(value, bytes);
        if let Err(error) = &queued {
            // A full control queue cannot safely retain remote interests after
            // cancellation. Evict this node's session, never the shared pool.
            self.fail_link(node, error);
        }
        Ok(queued)
    }
    pub(super) fn subscribe(&mut self, id: u64) -> Result<(), StreamError> {
        let route = self.routes.get_mut(&id).ok_or(StreamError::Unavailable)?;
        if route.progress.cancelled() {
            return self.remove(id);
        }
        let served = &self
            .links
            .get(&route.node)
            .ok_or(StreamError::Unavailable)?
            .served;
        let subscription = match route.subscription(id, served) {
            Ok(subscription) => subscription,
            Err(error) => {
                self.reject(id, error);
                return Ok(());
            }
        };
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
        let Some(route) = self
            .routes
            .get_mut(&id)
            .filter(|route| route.opened && route.live())
        else {
            return Ok(());
        };
        let capacity = u32::try_from(route.records.capacity())
            .map_err(|_| StreamError::Capacity)?
            .min(
                u32::try_from(QUEUE)
                    .map_err(|_| StreamError::Capacity)?
                    .saturating_sub(route.buffered_data),
            );
        let grant = wire::SessionWindow {
            frames: capacity.saturating_sub(route.credit.frames),
            bytes: (u64::from(capacity) * FRAME_BYTES).saturating_sub(route.credit.bytes),
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
        self.cancel_subscription(
            &node,
            wire::SessionChannel {
                channel_id: id,
                generation: route.generation,
            },
        )?;
        if !self.node_interested(&node) {
            self.dials.remove(&node);
            self.links.remove(&node);
            if node == self.home {
                self.reset_home();
            }
        }
        Ok(())
    }
    pub(super) fn cancel_subscription(
        &mut self,
        node: &str,
        channel: wire::SessionChannel,
    ) -> Result<(), StreamError> {
        let Some(link) = self.links.get_mut(node) else {
            return Ok(());
        };
        link.channel.cancel_channel(channel);
        link.revision = link.revision.checked_add(1).ok_or(StreamError::Capacity)?;
        let revision = link.revision;
        self.send(
            node,
            frame(
                0,
                0,
                Body::Interest(wire::SessionInterest {
                    revision,
                    upsert: Vec::new(),
                    remove: vec![channel],
                }),
            ),
        )
    }
    pub(super) fn reconnect(&mut self, node: &str) -> Result<(), StreamError> {
        let mut link = self.links.remove(node).ok_or(StreamError::Unavailable)?;
        if let Some(observation) = &mut link.observation {
            observation.finish(Some(&StreamError::Unavailable));
        }
        if node == self.home {
            self.reset_home();
        }
        // Only resumable routes can open again: an opened finite read would
        // choose a new snapshot, a page could splice replies, and a call could
        // repeat a mutating invocation.
        let interrupted = self
            .routes
            .on_node(node)
            .filter(|(_, route)| !route.resumable())
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in interrupted {
            self.reject(id, StreamError::Unavailable);
        }
        // The authenticated discovery endpoint may now host a replacement
        // node. Rediscover there without retaining the previous destination;
        // moved-node endpoints must still prove their exact node identity.
        let destination = if std::sync::Arc::ptr_eq(&link.channel, &self.home_channel) {
            ""
        } else {
            node
        };
        let registering = self
            .routes
            .on_node(node)
            .any(|(_, route)| route.live() && route.ready.is_some());
        let ids = self
            .routes
            .on_node(node)
            .filter(|(_, route)| route.live())
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in ids {
            let route = self
                .routes
                .rehome(&id, destination)
                .ok_or(StreamError::Unavailable)?;
            route.generation = route.progress.fence()?;
            route.observation.in_scope(|| {
                tracing::debug!(event = "reconnect", generation = route.generation);
            });
            route.opened = false;
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
            // Back off after EOF so a repeatedly disconnected transport yields
            // to cancellation and other work before opening another RPC.
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

async fn handshake(
    channel: Endpoint,
    destination: &str,
    identity: Bytes,
    credentials: BTreeMap<u32, Bytes>,
    interest: wire::SessionInterest,
) -> Result<Ready, StreamError> {
    let sender = Arc::new(Outbound::new(crate::MAX_ITEMS, crate::MAX_COMMAND_BYTES));
    let credit = wire::SessionWindow {
        bytes: crate::MAX_COMMAND_BYTES as u64,
        frames: u32::try_from(crate::MAX_ITEMS).map_err(|_| StreamError::Capacity)?,
    };
    let opening_credentials = credentials.clone();
    let credentials = match credentials.keys().next_back().copied() {
        Some(last) => (0..=last)
            .map(|slot| credentials.get(&slot).cloned().unwrap_or_default())
            .collect::<Vec<_>>(),
        None => vec![],
    };
    let opening_subscription = interest
        .upsert
        .first()
        .and_then(|subscription| subscription.channel);
    let revision = interest.revision;
    let hello = frame(
        0,
        0,
        Body::Hello(wire::SessionHello {
            version: super::super::VERSION,
            interest: Some(interest),
            client_id: identity,
            destination_node: destination.into(),
            receive_bytes: credit.bytes,
            receive_frames: credit.frames,
            credentials,
        }),
    );
    let bytes = hello.encoded_len();
    if bytes > crate::MAX_COMMAND_BYTES {
        return Err(StreamError::LimitExceeded);
    }
    sender.push(hello, bytes)?;
    // Ownership is installed before either credential or connection I/O await.
    let link = Link {
        opening_subscription,
        opening_credentials,
        observation: None,
        channel,
        sender,
        revision,
        applied_revision: 0,
        released: wire::SessionWindow::default(),
        served: crate::RevisionCut::default(),
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
    if welcome.wire().channel_id != 0 || welcome.wire().generation != 0 {
        return Err(StreamError::InvalidResponse);
    }
    let Some(Body::Welcome(welcome)) = welcome.into_wire().body else {
        return Err(StreamError::InvalidResponse);
    };
    if welcome.version != super::super::VERSION {
        return Err(StreamError::Unsupported);
    }
    if welcome.node_id.is_empty() || (!destination.is_empty() && welcome.node_id != destination) {
        return Err(StreamError::InvalidResponse);
    }
    Ok((welcome.node_id, link, response))
}

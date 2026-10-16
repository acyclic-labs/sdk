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
        Ok(node)
    }
    pub(super) fn dial(&mut self, node: wire::SessionNode, channel: Option<TransportChannel>) {
        let name = node.node_id.clone();
        if self.links.contains_key(&name) || !self.connecting.insert(name.clone()) {
            return;
        }
        let connect = self.connect.clone();
        let identity = self.identity.clone();
        let authorization = self.authorization.clone();
        let credential = self.credential.clone();
        let epoch = self.dial_epoch;
        self.dials.push(Box::pin(async move {
            let outcome = async {
                let channel = match channel {
                    Some(channel) => channel,
                    None => connect(node).await?,
                };
                open_link(channel, &name, identity, authorization, credential).await
            }
            .await;
            (name, epoch, outcome)
        }));
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
        let (next, opaque) = route.progress.position()?;
        route.received = next;
        route.received_opaque = opaque.clone().into();
        route.opened = false;
        route.credit = wire::SessionWindow::default();
        let link = self
            .links
            .get_mut(&route.node)
            .ok_or(StreamError::Unavailable)?;
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
        let capacity =
            u32::try_from(route.records.capacity()).map_err(|_| StreamError::Capacity)?;
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
        let Some(link) = self.links.get_mut(&route.node) else {
            return Ok(());
        };
        link.revision = link.revision.checked_add(1).ok_or(StreamError::Capacity)?;
        let revision = link.revision;
        let node = route.node.clone();
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
        )
    }
    pub(super) fn reconnect(&mut self, node: &str) -> Result<(), StreamError> {
        let link = self.links.remove(node).ok_or(StreamError::Unavailable)?;
        for route in self.routes.values_mut().filter(|route| route.node == node) {
            route.generation = route.progress.fence()?;
            route.opened = false;
        }
        self.dial(
            wire::SessionNode {
                node_id: node.into(),
                endpoint: String::new(),
                placement_epoch: 0,
            },
            Some(link.channel.clone()),
        );
        Ok(())
    }
}

async fn open_link(
    channel: TransportChannel,
    destination: &str,
    identity: Bytes,
    authorization: Option<MetadataValue<Ascii>>,
    credential: Option<grpc::CredentialProvider>,
) -> Result<Ready, StreamError> {
    for attempt in 0..3 {
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handshake(
                channel.clone(),
                destination,
                identity.clone(),
                authorization.clone(),
                credential.clone(),
            ),
        )
        .await
        .unwrap_or(Err(StreamError::Unavailable));
        match result {
            Err(StreamError::Unavailable) if attempt < 2 => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            result => return result,
        }
    }
    Err(StreamError::Unavailable)
}

async fn handshake(
    channel: TransportChannel,
    destination: &str,
    identity: Bytes,
    authorization: Option<MetadataValue<Ascii>>,
    credential: Option<grpc::CredentialProvider>,
) -> Result<Ready, StreamError> {
    // Resolve inside each bounded handshake, including retries and moved-node RPCs.
    let authorization = grpc::request_authorization(authorization.as_ref(), credential.as_ref())
        .await
        .map_err(|error| grpc::status(&error))?;
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
        }),
    );
    let bytes = hello.encoded_len();
    sender.push(hello, bytes)?;
    // Install ownership before either handshake await. Cancelling a handshake
    // closes its request stream just as dropping an installed node link does.
    let link = Link {
        channel,
        sender,
        revision: 0,
        applied_revision: 0,
        released: wire::SessionWindow::default(),
        epoch: 0,
        credit,
    };
    let input = stream::unfold(Arc::clone(&link.sender), |sender| async move {
        sender.next().await.ok().map(|frame| (frame, sender))
    });
    let mut request = Request::new(input);
    request
        .metadata_mut()
        .insert("authorization", authorization);
    let mut response =
        wire::stream_session_service_client::StreamSessionServiceClient::new(link.channel.clone())
            .session(request)
            .await
            .map_err(|error| grpc::status(&error))?
            .into_inner();
    let welcome = response
        .message()
        .await
        .map_err(|error| grpc::status(&error))?
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

use super::*;

impl Pool {
    pub(super) async fn run(&mut self) -> Result<(), StreamError> {
        loop {
            tokio::select! {
                command = self.commands.recv() => match command { Some(command) => self.command(command).await?, None => return Ok(()) },
                released = self.released.next() => { self.released(released?).await?; },
                id = self.removed.next() => self.remove(id?).await?,
                ready = self.dials.next(), if !self.dials.is_empty() => {
                    let (node,result) = ready.ok_or(StreamError::Unavailable)?;
                    self.connecting.remove(&node);
                    match result {
                        Ok(ready) => {
                            self.install(ready)?;
                            self.pending.extend(self.routes.iter().filter(|(_,route)| route.node == node).map(|(id,route)| (*id,route.generation)));
                        }
                        Err(error) => self.fail_node(&node,&error),
                    }
                }
                () = future::ready(()), if !self.pending.is_empty() => {
                    if let Some((id,generation)) = self.pending.pop_front()
                        && self.routes.get(&id).is_some_and(|route| route.generation == generation && self.links.contains_key(&route.node)) { self.subscribe(id).await?; }
                }
                next = self.reads.next(), if !self.reads.is_empty() => match next {
                    Some((node,response,Some(Ok(value)))) => { self.receive(&node,value).await?; self.read(node,response); }
                    Some((node,_,Some(Err(error)))) if grpc::retryable(&error) => self.reconnect(&node)?,
                    Some((node,_,Some(Err(error)))) => { self.links.remove(&node); self.fail_node(&node,&grpc::status(&error)); }
                    Some((node,_,None)) => self.reconnect(&node)?,
                    None => return Err(StreamError::Unavailable),
                }
            }
        }
    }
    async fn command(&mut self, command: Command) -> Result<(), StreamError> {
        match command {
            Command::Open {
                id,
                acknowledge,
                kind,
                progress,
                records,
                ready,
            } => {
                if self.routes.len() >= Limits::default().channels {
                    let _ = ready.send(Err(StreamError::Capacity));
                    return Ok(());
                }
                if records.is_closed() {
                    return Ok(());
                }
                let from = progress.next()?;
                self.routes.insert(
                    id,
                    Route {
                        node: self.home.clone(),
                        kind,
                        generation: 1,
                        placement_epoch: 0,
                        received: from,
                        received_opaque: Bytes::new(),
                        acknowledge,
                        acknowledged: from,
                        opened: false,
                        credit: wire::SessionWindow::default(),
                        progress,
                        records,
                        ready: Some(ready),
                    },
                );
                if self.links.contains_key(&self.home) {
                    self.subscribe(id).await
                } else {
                    self.dial(
                        wire::SessionNode {
                            node_id: self.home.clone(),
                            endpoint: String::new(),
                            placement_epoch: 0,
                        },
                        Some(self.home_channel.clone()),
                    );
                    Ok(())
                }
            }
        }
    }
    async fn released(&mut self, released: Released) -> Result<(), StreamError> {
        let Released {
            id,
            node,
            bytes,
            epoch,
            generation,
        } = released;
        self.release(&node, epoch, bytes).await?;
        let Some(route) = self.routes.get_mut(&id) else {
            return Ok(());
        };
        let owner = route.node.clone();
        let current = route.generation;
        if node == owner && generation == current && route.acknowledge && route.credit.frames == 0 {
            let (applied, opaque) = route.progress.position()?;
            if applied <= route.acknowledged {
                return self.grant(id).await;
            }
            route.acknowledged = applied;
            self.send(
                &owner,
                frame(
                    id,
                    current,
                    Body::Ack(wire::SessionAck {
                        applied: Some(wire::SessionCursor {
                            next: applied,
                            opaque: opaque.into(),
                        }),
                    }),
                ),
            )
            .await?;
        }
        self.grant(id).await
    }
    async fn receive(&mut self, node: &str, input: wire::SessionFrame) -> Result<(), StreamError> {
        let id = input.channel_id;
        if id == 0 {
            return self.control(node, input.generation, input.body.as_ref());
        }
        if matches!(input.body, Some(Body::Data(_))) {
            let link = self.links.get_mut(node).ok_or(StreamError::Unavailable)?;
            wire_codec::session::consume_credit(&mut link.credit, &input)?;
        }
        let epoch = self.links.get(node).ok_or(StreamError::Unavailable)?.epoch;
        let length = input.encoded_len() as u64;
        let Some(route) = self
            .routes
            .get_mut(&id)
            .filter(|route| route.node == node && route.generation == input.generation)
        else {
            // Return aggregate credit for fenced data without touching its successor.
            if matches!(input.body, Some(Body::Data(_))) {
                self.release(node, epoch, length).await?;
            }
            return Ok(());
        };
        if matches!(input.body, Some(Body::Data(_))) {
            wire_codec::session::consume_credit(&mut route.credit, &input)?;
        }
        match input.body {
            Some(Body::Opened(opened)) => {
                if opened.resume.as_ref().is_none_or(|resume| {
                    resume.next != route.received || resume.opaque != route.received_opaque
                }) {
                    return Err(StreamError::Unavailable);
                }
                if let Some(ready) = route.ready.take() {
                    let _ = ready.send(Ok(id));
                }
                route.opened = true;
                self.grant(id).await
            }
            Some(Body::Data(data)) => {
                self.data(node, id, input.generation, data, length, epoch)
                    .await
            }
            Some(Body::Closed(closed)) => {
                let error =
                    StreamError::from_code(&closed.code).unwrap_or(StreamError::Unavailable);
                // Errors are delivered after all earlier records, using one reserved slot.
                if let Some(ready) = route.ready.take() {
                    let _ = ready.send(Ok(id));
                }
                let route = self.routes.remove(&id).ok_or(StreamError::Unavailable)?;
                tokio::spawn(async move {
                    let _ = route.records.send(Err(error)).await;
                });
                Ok(())
            }
            Some(Body::Moved(moved)) => {
                let destination = moved.node.ok_or(StreamError::Unavailable)?;
                if destination.node_id.is_empty()
                    || destination.placement_epoch <= route.placement_epoch
                {
                    return Err(StreamError::Unavailable);
                }
                route.placement_epoch = destination.placement_epoch;
                route.generation = route.progress.fence()?;
                route.node = destination.node_id.clone();
                route.opened = false;
                if self.links.contains_key(&destination.node_id) {
                    self.subscribe(id).await
                } else {
                    self.dial(destination, None);
                    Ok(())
                }
            }
            Some(Body::Watermark(mark)) if mark.durable <= mark.accepted => Ok(()),
            _ => Err(StreamError::Unavailable),
        }
    }
    fn control(
        &mut self,
        node: &str,
        generation: u64,
        body: Option<&Body>,
    ) -> Result<(), StreamError> {
        if generation != 0 {
            return Err(StreamError::Unavailable);
        }
        match body {
            Some(Body::InterestApplied(applied)) => {
                let link = self.links.get_mut(node).ok_or(StreamError::Unavailable)?;
                if applied.revision < link.applied_revision || applied.revision > link.revision {
                    return Err(StreamError::Unavailable);
                }
                link.applied_revision = applied.revision;
                Ok(())
            }
            Some(Body::Window(_)) => Ok(()),
            _ => Err(StreamError::Unavailable),
        }
    }
    async fn data(
        &mut self,
        node: &str,
        id: u64,
        generation: u64,
        data: wire::SessionData,
        length: u64,
        epoch: u64,
    ) -> Result<(), StreamError> {
        let route = self.routes.get_mut(&id).ok_or(StreamError::Unavailable)?;
        let after = data.after.ok_or(StreamError::Unavailable)?;
        let (item, sequence) = match (&route.kind, data.item) {
            (Kind::Follow(_), Some(Item::Record(record))) if after.opaque.is_empty() => {
                let record = wire_codec::record(record)?;
                let sequence = record.sequence;
                (Received::Record(record), sequence)
            }
            (Kind::Children(request), Some(Item::Child(child))) => {
                let path = StreamPath::new(child.path).map_err(|_| StreamError::Unavailable)?;
                if path.parent().as_ref().map(StreamPath::as_str) != request.parent.as_deref() {
                    return Err(StreamError::Unavailable);
                }
                (Received::Child(crate::Child { path }), route.received)
            }
            _ => return Err(StreamError::Unavailable),
        };
        if sequence != route.received
            || after.next != sequence.checked_add(1).ok_or(StreamError::OutOfRange)?
            || data.durable > data.accepted
            || data.accepted < after.next
        {
            return Err(StreamError::Unavailable);
        }
        route.received = after.next;
        route.received_opaque = after.opaque.clone();
        let delivery = Delivery {
            item,
            sequence,
            opaque: after.opaque,
            generation,
            credit: Credit::new(
                self.release_sender.clone(),
                Released {
                    id,
                    node: node.into(),
                    bytes: length,
                    epoch,
                    generation,
                },
            ),
        };
        match route.records.try_send(Ok(delivery)) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => self.remove(id).await,
            Err(mpsc::error::TrySendError::Full(_)) => Err(StreamError::Capacity),
        }
    }

    fn fail_node(&mut self, node: &str, error: &StreamError) {
        let ids = self
            .routes
            .iter()
            .filter(|(_, route)| route.node == node)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in ids {
            if let Some(mut route) = self.routes.remove(&id) {
                if let Some(ready) = route.ready.take() {
                    let _ = ready.send(Ok(id));
                }
                let error = error.clone();
                tokio::spawn(async move {
                    let _ = route.records.send(Err(error)).await;
                });
            }
        }
    }
    pub(super) fn fail(&mut self, error: &StreamError) {
        for (id, mut route) in std::mem::take(&mut self.routes) {
            if let Some(ready) = route.ready.take() {
                let _ = ready.send(Ok(id));
            }
            let error = error.clone();
            tokio::spawn(async move {
                let _ = route.records.send(Err(error)).await;
            });
        }
    }
}

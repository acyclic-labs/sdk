use super::*;

#[cfg(acyclic_stream_loom)]
pub(super) mod model;

impl Pool {
    pub(super) async fn run(&mut self) -> Result<(), StreamError> {
        loop {
            self.sync_attempts()?;
            tokio::select! {
                changed = self.abandoned.changed(false) => changed?,
                command = self.commands.recv() => match command { Some(command) => self.command(command)?, None => return Ok(()) },
                released = self.released.next() => { self.released(released?)?; },
                id = self.removed.next() => self.remove(id?)?,
                ready = self.dials.next(), if !self.dials.is_empty() => {
                    let (node,epoch,result) = ready.ok_or(StreamError::Unavailable)?;
                    self.dial_ready(node, epoch, result)?;
                }
                () = future::ready(()), if !self.pending.is_empty() => {
                    if let Some((id,generation)) = self.pending.pop_front()
                        && self.routes.get(&id).is_some_and(|route| route.generation == generation && self.links.contains_key(&route.node)) { self.subscribe(id)?; }
                }
                next = self.reads.next(), if !self.reads.is_empty() => match next {
                    Some((node,epoch,response,value)) => {
                        if self.links.get(&node).is_none_or(|link| link.epoch != epoch) { continue; }
                        match value {
                            Some(Ok(value)) => { if self.receive_frame(&node,value) { self.read(node,response); } }
                            Some(Err(error)) if grpc::retryable(&error) => self.reconnect(&node)?,
                            Some(Err(error)) => self.fail_link(&node,&grpc::status(&error)),
                            None => self.reconnect(&node)?,
                        }
                    }
                    None => return Err(StreamError::Unavailable),
                }
            }
            self.close_idle();
        }
    }
    fn sync_attempts(&mut self) -> Result<(), StreamError> {
        // Clear the wake flag before loading the epoch: abandonment either
        // appears in this load or leaves a wake for the next dispatch turn.
        self.abandoned.set(false)?;
        let epoch = self.attempt_epoch.load(Ordering::Acquire);
        if epoch == self.dial_epoch {
            return Ok(());
        }
        self.dial_epoch = epoch;
        self.dials.clear();
        self.connecting.clear();
        let stale = self
            .routes
            .iter()
            .filter(|(_, route)| !route.opened && route.attempt_epoch != epoch)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in stale {
            self.reject(id, StreamError::Unavailable);
        }
        Ok(())
    }
    fn dial_ready(
        &mut self,
        node: String,
        epoch: u64,
        result: Result<Ready, StreamError>,
    ) -> Result<(), StreamError> {
        self.sync_attempts()?;
        if epoch != self.dial_epoch {
            // A credential callback may settle after its final owner cancelled.
            // Neither its failure nor its success belongs to the successor dial.
            return Ok(());
        }
        self.connecting.remove(&node);
        match result {
            Ok(ready) => {
                let installed = self.install(ready)?;
                if node.is_empty() && self.home.is_empty() {
                    self.home = installed.clone();
                    for route in self
                        .routes
                        .values_mut()
                        .filter(|route| route.node.is_empty())
                    {
                        route.node = installed.clone();
                    }
                }
                self.pending.extend(
                    self.routes
                        .iter()
                        .filter(|(_, route)| route.node == installed)
                        .map(|(id, route)| (*id, route.generation)),
                );
            }
            Err(error) => self.fail_node(&node, &error),
        }
        Ok(())
    }
    fn close_idle(&mut self) {
        if self.active.load(Ordering::Acquire) == 0 {
            self.close_links();
        }
    }
    fn close_links(&mut self) {
        self.links.clear();
        self.reads.clear();
        self.dials.clear();
        self.connecting.clear();
        self.pending.clear();
        self.routes.clear();
    }
    fn receive_frame(&mut self, node: &str, value: wire::SessionFrame) -> bool {
        match self.receive(node, value) {
            Ok(()) => true,
            Err(error) => {
                self.fail_link(node, &error);
                false
            }
        }
    }
    pub(super) fn fail_link(&mut self, node: &str, error: &StreamError) {
        self.links.remove(node);
        self.fail_node(node, error);
    }
    fn command(&mut self, command: Command) -> Result<(), StreamError> {
        self.sync_attempts()?;
        match command {
            Command::Open {
                id,
                attempt_epoch,
                acknowledge,
                kind,
                progress,
                records,
                ready,
            } => {
                if attempt_epoch != self.dial_epoch {
                    let _ = ready.send(Err(StreamError::Unavailable));
                    return Ok(());
                }
                if self.routes.len() >= Limits::default().channels {
                    let _ = ready.send(Err(StreamError::Capacity));
                    return Ok(());
                }
                if records.is_closed() || progress.cancelled() {
                    return Ok(());
                }
                let from = progress.next()?;
                self.routes.insert(
                    id,
                    Route {
                        node: self.home.clone(),
                        attempt_epoch,
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
                    self.subscribe(id)
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
    fn released(&mut self, released: Released) -> Result<(), StreamError> {
        let Released {
            id,
            node,
            bytes,
            epoch,
            generation,
        } = released;
        self.release(&node, epoch, bytes)?;
        let Some(route) = self.routes.get_mut(&id) else {
            return Ok(());
        };
        let owner = route.node.clone();
        let current = route.generation;
        if node == owner && generation == current && route.acknowledge && route.credit.frames == 0 {
            let (applied, opaque) = route.progress.position()?;
            if applied <= route.acknowledged {
                return self.grant(id);
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
            )?;
        }
        self.grant(id)
    }
    fn receive(&mut self, node: &str, input: wire::SessionFrame) -> Result<(), StreamError> {
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
                self.release(node, epoch, length)?;
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
                if let Kind::Follow(follow) = &route.kind {
                    let preceding = wire_codec::session::preceding_follow(
                        follow,
                        opened.resume.as_ref().ok_or(StreamError::Unavailable)?,
                    )?;
                    if let Err(error) = wire_codec::session::confirm_follow(preceding, &opened) {
                        if let Some(ready) = route.ready.take() {
                            let _ = ready.send(Err(error.clone()));
                        }
                        let records = route.records.clone();
                        self.remove(id)?;
                        tokio::spawn(async move {
                            let _ = records.send(Err(error)).await;
                        });
                        return Ok(());
                    }
                }
                if let Some(ready) = route.ready.take() {
                    let _ = ready.send(Ok(id));
                }
                route.opened = true;
                self.grant(id)
            }
            Some(Body::Data(data)) => self.data(node, id, input.generation, data, length, epoch),
            Some(Body::Closed(closed)) => {
                let error =
                    StreamError::from_code(&closed.code).unwrap_or(StreamError::Unavailable);
                if matches!(&route.kind, Kind::Follow(follow) if !follow.preceding_commit_id.is_empty())
                    && let Some(ready) = route.ready.take()
                {
                    let _ = ready.send(Err(error));
                    self.routes.remove(&id);
                    return Ok(());
                }
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
                    self.subscribe(id)
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
    fn data(
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
            Err(mpsc::error::TrySendError::Closed(_)) => self.remove(id),
            Err(mpsc::error::TrySendError::Full(_)) => Err(StreamError::Capacity),
        }
    }

    pub(super) fn reject(&mut self, id: u64, error: StreamError) {
        if let Some(mut route) = self.routes.remove(&id) {
            if let Some(ready) = route.ready.take() {
                let _ = ready.send(Err(error.clone()));
            }
            let records = route.records;
            tokio::spawn(async move {
                let _ = records.send(Err(error)).await;
            });
        }
    }
    pub(super) fn fail_node(&mut self, node: &str, error: &StreamError) {
        let ids = self
            .routes
            .iter()
            .filter(|(_, route)| route.node == node)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in ids {
            if let Some(mut route) = self.routes.remove(&id) {
                if let Some(ready) = route.ready.take() {
                    let _ = ready.send(Err(error.clone()));
                }
                let error = error.clone();
                let records = route.records;
                tokio::spawn(async move {
                    let _ = records.send(Err(error)).await;
                });
            }
        }
    }
    pub(super) fn fail(&mut self, error: &StreamError) {
        for (id, mut route) in std::mem::take(&mut self.routes) {
            if let Some(ready) = route.ready.take() {
                let checked = matches!(&route.kind, Kind::Follow(follow) if !follow.preceding_commit_id.is_empty());
                let _ = ready.send(if checked { Err(error.clone()) } else { Ok(id) });
            }
            let error = error.clone();
            let records = route.records;
            tokio::spawn(async move {
                let _ = records.send(Err(error)).await;
            });
        }
    }
}

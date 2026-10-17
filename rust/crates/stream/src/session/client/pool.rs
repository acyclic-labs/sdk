use super::*;

#[cfg(all(any(test, acyclic_stream_loom), not(target_arch = "wasm32")))]
pub(super) mod fixture;
#[cfg(acyclic_stream_loom)]
pub(super) mod model;
#[cfg(all(test, not(acyclic_stream_loom), not(target_arch = "wasm32")))]
mod ordering;
#[cfg(acyclic_stream_loom)]
pub(super) mod retry_model;

#[cfg(acyclic_stream_loom)]
pub(super) mod discovery_model;

impl Pool {
    pub(super) async fn run(&mut self) -> Result<(), StreamError> {
        loop {
            self.sync_attempts()?;
            self.close_idle();
            tokio::select! {
                changed = self.abandoned.changed(false) => changed?,
                command = self.commands.recv() => match command { Some(command) => self.command(command)?, None => return Ok(()) },
                released = self.released.next() => { self.released(released?)?; },
                id = self.removed.next() => self.remove(id?)?,
                ready = future::poll_fn(|cx| poll_dials(&mut self.dials, cx)), if !self.dials.is_empty() => {
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
                            Some(Err(StreamError::Unavailable)) => self.reconnect(&node)?,
                            Some(Err(error)) => self.fail_link(&node,&error),
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
    pub(super) fn dial_ready(
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
        if !self.node_interested(&node) {
            return Ok(());
        }
        match result {
            Ok(ready) => {
                let installed = self.install(ready)?;
                if node.is_empty() && self.home.is_empty() {
                    self.admissions.bind_home(&installed)?;
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
    pub(super) fn close_idle(&mut self) {
        if self.active.load(Ordering::Acquire) == 0 {
            self.close_links();
        } else {
            let idle = self
                .dials
                .keys()
                .filter(|node| !self.node_interested(node))
                .cloned()
                .collect::<Vec<_>>();
            for node in idle {
                // Dropping the owned future cancels connector/credential/Welcome
                // work immediately; it cannot later publish a Link or failure.
                self.dials.remove(&node);
            }
        }
    }
    pub(super) fn node_interested(&self, node: &str) -> bool {
        self.routes
            .values()
            .any(|route| route.node == node && route.live())
            || self.admissions.interested(node)
    }
    fn close_links(&mut self) {
        self.links.clear();
        self.reads.clear();
        self.dials.clear();
        self.pending.clear();
        self.routes.clear();
    }
    pub(super) fn receive_frame(&mut self, node: &str, value: wire::SessionFrame) -> bool {
        match self.receive(node, value) {
            Ok(()) => true,
            Err(error) => {
                self.fail_link(node, &error);
                false
            }
        }
    }
    pub(super) fn fail_link(&mut self, node: &str, error: &StreamError) {
        if let Some(mut link) = self.links.remove(node)
            && let Some(observation) = &mut link.observation
        {
            observation.finish(Some(error));
        }
        self.fail_node(node, error);
    }
    fn command(&mut self, command: Command) -> Result<(), StreamError> {
        // Cursor drops happen before their owner's replacement Open. Removal
        // uses a separate wake queue, so dispatch must preserve that ordering
        // even when select chooses the command first.
        while let Some(id) = self.removed.try_next()? {
            self.remove(id)?;
        }
        self.apply_command(command)
    }
    fn apply_command(&mut self, command: Command) -> Result<(), StreamError> {
        self.sync_attempts()?;
        match command {
            Command::Open {
                id,
                attempt_epoch,
                acknowledge,
                kind,
                raw,
                progress,
                records,
                ready,
                observation,
            } => {
                self.admissions.remove(id)?;
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
                        raw,
                        watermark: None,
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
                        observation,
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
        if bytes != 0 {
            self.release(&node, epoch, bytes)?;
        }
        self.flush_watermark(id)?;
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
        if matches!(input.body, Some(Body::Closed(_) | Body::Moved(_)))
            && let Some(link) = self.links.get(node)
        {
            link.channel.cancel_channel(wire::SessionChannel {
                channel_id: id,
                generation: input.generation,
            });
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
                        self.executor.spawn(Box::pin(async move {
                            let _ = records.send(Err(error)).await;
                        }));
                        return Ok(());
                    }
                }
                if let Some(ready) = route.ready.take() {
                    let _ = ready.send(Ok(id));
                }
                route.observation.record("generation", route.generation);
                route.opened = true;
                self.grant(id)
            }
            Some(Body::Data(data)) => self.data(node, id, input.generation, data, length, epoch),
            Some(Body::Closed(closed)) => {
                let error = StreamError::from_session_close(&closed.code);
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
                self.executor.spawn(Box::pin(async move {
                    let _ = route.records.send(Err(error)).await;
                }));
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
            Some(Body::Watermark(mark)) if mark.durable <= mark.accepted => {
                if route.raw {
                    route.watermark = Some(frame(id, input.generation, Body::Watermark(mark)));
                    self.flush_watermark(id)
                } else {
                    Ok(())
                }
            }
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
        let after = data.after.as_ref().ok_or(StreamError::Unavailable)?;
        let sequence = match (&route.kind, data.item.as_ref()) {
            (Kind::Follow(follow), _) => {
                wire_codec::session::follow_sequence(follow.delivery, route.received, &data)?
            }
            (Kind::Children(request), Some(Item::Child(child))) => {
                let path = StreamPath::new(&child.path).map_err(|_| StreamError::Unavailable)?;
                if path.parent().as_ref().map(StreamPath::as_str) != request.parent.as_deref()
                    || after.next
                        != route
                            .received
                            .checked_add(1)
                            .ok_or(StreamError::OutOfRange)?
                    || data.durable > data.accepted
                    || data.accepted < after.next
                {
                    return Err(StreamError::Unavailable);
                }
                route.received
            }
            _ => return Err(StreamError::Unavailable),
        };
        route.received = after.next;
        route.received_opaque = after.opaque.clone();
        let opaque = after.opaque.clone();
        let item = if route.raw {
            Received::Frame(frame(id, generation, Body::Data(data)))
        } else if matches!(route.kind, Kind::Follow(_)) {
            Received::Data(data)
        } else if let Some(Item::Child(child)) = data.item {
            Received::Child(crate::Child {
                path: StreamPath::new(child.path).map_err(|_| StreamError::Unavailable)?,
            })
        } else {
            return Err(StreamError::Unavailable);
        };
        let delivery = Delivery {
            item,
            sequence,
            opaque,
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

    fn flush_watermark(&mut self, id: u64) -> Result<(), StreamError> {
        let Some(route) = self.routes.get_mut(&id) else {
            return Ok(());
        };
        // Reserve slots for every data frame already granted to the peer.
        // Control-only durability updates occupy at most one additional pending value.
        if route.records.capacity() <= route.credit.frames as usize {
            return Ok(());
        }
        let Some(value) = route.watermark.take() else {
            return Ok(());
        };
        let epoch = self
            .links
            .get(&route.node)
            .ok_or(StreamError::Unavailable)?
            .epoch;
        let delivery = Delivery {
            item: Received::Frame(value),
            sequence: route.received,
            opaque: route.received_opaque.clone(),
            generation: route.generation,
            credit: Credit::new(
                self.release_sender.clone(),
                Released {
                    id,
                    node: route.node.clone(),
                    bytes: 0,
                    epoch,
                    generation: route.generation,
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
            self.executor.spawn(Box::pin(async move {
                let _ = records.send(Err(error)).await;
            }));
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
                self.executor.spawn(Box::pin(async move {
                    let _ = records.send(Err(error)).await;
                }));
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
            self.executor.spawn(Box::pin(async move {
                let _ = records.send(Err(error)).await;
            }));
        }
    }
}

pub(super) fn poll_dials(
    dials: &mut BTreeMap<String, Dial>,
    cx: &mut std::task::Context<'_>,
) -> std::task::Poll<Option<(String, u64, Result<Ready, StreamError>)>> {
    for (node, dial) in dials.iter_mut() {
        if let std::task::Poll::Ready(result) = dial.as_mut().poll(cx) {
            let node = node.clone();
            dials.remove(&node);
            return std::task::Poll::Ready(Some(result));
        }
    }
    std::task::Poll::Pending
}

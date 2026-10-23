//! Fault injection and bounded credit around the real shared Streams client.
use super::swarm_runtime as runtime;
use acyclic_harness::Error;
use acyclic_stream::{MemoryStream, session, wire};
use futures::{StreamExt as _, channel::mpsc, stream};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use wire::{session_frame::Body, session_subscription::Kind};

type Sender = mpsc::UnboundedSender<wire::SessionFrame>;
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Fault {
    None,
    DropAttention,
    DropBoundary,
    DuplicateContent,
    IgnoreRemoval,
    CorruptSnapshot,
    ScanRetainedViews,
    LogarithmicProviderWork,
    QuadraticProviderWork,
    StalledReopen,
}
#[derive(Default, serde::Serialize)]
pub(super) struct Counts {
    pub sessions: usize,
    pub server_follows: usize,
    pub server_records: usize,
    pub delivered_records: usize,
    pub discovered_children: usize,
    pub children: usize,
    pub actor_channels: usize,
    pub status_follows: usize,
    pub membership_follows: usize,
    pub content_follows: usize,
    pub removed: usize,
    pub peak_full: usize,
    #[serde(skip)]
    pub live_full: usize,
    #[serde(skip)]
    pub root_credit: u64,
    #[serde(skip)]
    statuses: BTreeSet<String>,
    #[serde(skip)]
    full: BTreeSet<u64>,
    #[serde(skip)]
    always_followed: BTreeSet<u64>,
    #[serde(skip)]
    released: bool,
    #[serde(skip)]
    held: Vec<wire::SessionFrame>,
    #[serde(skip)]
    injected: Option<mpsc::UnboundedReceiver<wire::SessionFrame>>,
}
pub(super) struct Wire {
    pub client: session::Client,
    pub runtime: runtime::Runtime,
    pub sender: Sender,
    // Instrumentation lock order: this sole lock is never nested, polled or
    // held across an await. Provider observation and I/O mapping use it serially.
    pub counts: Arc<Mutex<Counts>>,
}
pub(super) fn release(sender: &Sender, counts: &Mutex<Counts>) -> acyclic_harness::Result<()> {
    let pending = {
        let mut counts = counts.lock().unwrap_or_else(|p| p.into_inner());
        if counts.released || counts.held.is_empty() {
            return Err(Error::Invalid(
                "backpressure did not exhaust channel credit".into(),
            ));
        }
        counts.released = true;
        std::mem::take(&mut counts.held)
    };
    for frame in pending {
        sender
            .unbounded_send(frame)
            .map_err(|_| Error::Storage("credit gate ended".into()))?;
    }
    Ok(())
}
impl Counts {
    fn request(
        &mut self,
        frame: &mut wire::SessionFrame,
        fault: Fault,
    ) -> Result<bool, acyclic_stream::StreamError> {
        match &mut frame.body {
            Some(Body::Hello(hello)) => self.root_credit += u64::from(hello.receive_frames),
            Some(Body::Window(window)) if frame.channel_id == 0 => {
                self.root_credit += u64::from(window.frames)
            }
            Some(Body::Window(_))
                if !self.released && !self.always_followed.contains(&frame.channel_id) =>
            {
                self.held.push(frame.clone());
                return Ok(false);
            }
            Some(Body::Interest(update)) => {
                for removed in &update.remove {
                    if self.full.remove(&removed.channel_id) {
                        self.removed += 1;
                    }
                }
                for subscription in &update.upsert {
                    let id = subscription
                        .channel
                        .ok_or(acyclic_stream::StreamError::InvalidArgument)?
                        .channel_id;
                    match &subscription.kind {
                        Some(Kind::Children(_)) => {
                            self.children += 1;
                            self.always_followed.insert(id);
                        }
                        Some(Kind::Follow(follow)) if follow.resource == "streams:/swarm/files" => {
                            if follow.delivery != wire::SessionDelivery::All as i32
                                || follow.preceding_commit_id.len() != 32
                                || self.membership_follows != 0
                            {
                                return Err(acyclic_stream::StreamError::InvalidArgument);
                            }
                            self.membership_follows += 1;
                            self.always_followed.insert(id);
                        }
                        Some(Kind::Follow(follow))
                            if follow.delivery == wire::SessionDelivery::Latest as i32 =>
                        {
                            if !follow.resource.ends_with("/status")
                                || !self.statuses.insert(follow.resource.clone())
                            {
                                return Err(acyclic_stream::StreamError::InvalidArgument);
                            }
                            self.status_follows += 1;
                        }
                        Some(Kind::Follow(follow))
                            if follow.delivery == wire::SessionDelivery::All as i32 =>
                        {
                            if !follow.resource.ends_with("/files") || !self.full.insert(id) {
                                return Err(acyclic_stream::StreamError::InvalidArgument);
                            }
                            self.content_follows += 1;
                        }
                        _ => {
                            self.actor_channels += 1;
                            return Err(acyclic_stream::StreamError::InvalidArgument);
                        }
                    }
                }
                if fault == Fault::IgnoreRemoval {
                    update.remove.clear();
                }
            }
            _ => {}
        }
        Ok(true)
    }
}
impl Wire {
    pub fn open(
        provider: Arc<MemoryStream>,
        fault: Fault,
        agents: usize,
        paced: bool,
    ) -> acyclic_harness::Result<Self> {
        let (sender, receiver) = mpsc::unbounded();
        let counts = Arc::new(Mutex::new(Counts {
            released: paced,
            injected: Some(receiver),
            ..Default::default()
        }));
        let requests = counts.clone();
        let map_requests: runtime::RequestMap = Arc::new(move |source| {
            let injected = requests
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .injected
                .take()
                .expect("one physical session");
            let counts = requests.clone();
            Box::pin(
                stream::select(source, injected).filter_map(move |mut frame| {
                    let result = counts
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .request(&mut frame, fault);
                    // Fixture invariants fail at their source, before any client fold.
                    let keep = result.expect("valid stream-only interest/credit");
                    futures::future::ready(keep.then_some(frame))
                }),
            )
        });
        let responses = counts.clone();
        let map_responses: runtime::ResponseMap = Arc::new(move |source| {
            let counts = responses.clone();
            let mut injected = false;
            Box::pin(source.flat_map(move |item| {
                if let Ok(frame) = &item {
                    let mut counts = counts.lock().unwrap_or_else(|p| p.into_inner());
                    match &frame.body {
                        Some(Body::Welcome(_)) => counts.sessions += 1,
                        Some(Body::Data(data)) => {
                            match &data.item {
                                Some(wire::session_data::Item::Child(_)) => {
                                    counts.discovered_children += 1
                                }
                                Some(
                                    wire::session_data::Item::Record(_)
                                    | wire::session_data::Item::LatestValue(_),
                                ) => counts.delivered_records += 1,
                                _ => {}
                            }
                            counts.root_credit = counts
                                .root_credit
                                .checked_sub(1)
                                .expect("session exceeded aggregate credit");
                        }
                        _ => {}
                    }
                }
                let mut extra = None;
                let mut keep = true;
                if !injected
                    && let Ok(wire::SessionFrame {
                        body: Some(Body::Data(data)),
                        ..
                    }) = &item
                {
                    match (&data.item, fault) {
                        (
                            Some(wire::session_data::Item::LatestValue(value)),
                            Fault::DropAttention,
                        ) if value.attention => keep = false,
                        (Some(wire::session_data::Item::Record(_)), Fault::DropBoundary) => {
                            keep = false
                        }
                        (Some(wire::session_data::Item::Record(_)), Fault::DuplicateContent) => {
                            extra = Some(Ok(item.as_ref().unwrap().clone()))
                        }
                        _ => {}
                    }
                    injected = !keep || extra.is_some();
                }
                stream::iter([keep.then_some(item), extra].into_iter().flatten())
            }))
        });
        let observed = Arc::new(super::swarm_provider::Observed {
            memory: provider,
            counts: counts.clone(),
            fault,
            agents,
        });
        let (client, runtime, _) = runtime::open(observed, map_requests, map_responses);
        Ok(Self {
            client,
            runtime,
            sender,
            counts,
        })
    }
}

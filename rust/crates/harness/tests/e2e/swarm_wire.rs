use acyclic_harness::Error;
use acyclic_stream::{MemoryStream, StreamError, session, wire};
use futures::{Sink, StreamExt as _, channel::mpsc, stream};
use std::{
    collections::BTreeSet,
    pin::Pin,
    sync::{Arc, Mutex},
};
use wire::{session_frame::Body, session_subscription::Kind};

type Sender = mpsc::UnboundedSender<Result<wire::SessionFrame, StreamError>>;
pub(super) type SinkType = Pin<Box<dyn Sink<wire::SessionInterest, Error = Error> + Send>>;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Fault {
    None,
    DropAttention,
    DropBoundary,
    DuplicateContent,
}

#[derive(Default, serde::Serialize)]
pub(super) struct Counts {
    pub sessions: usize,
    pub children: usize,
    pub actor_channels: usize,
    pub status_follows: usize,
    pub content_follows: usize,
    pub removed: usize,
    pub peak_full: usize,
    #[serde(skip)]
    pub root_credit: usize,
    #[serde(skip)]
    statuses: BTreeSet<String>,
    #[serde(skip)]
    full: BTreeSet<u64>,
}

pub(super) struct Wire {
    pub sink: SinkType,
    pub incoming: futures::stream::BoxStream<'static, acyclic_harness::Result<wire::SessionFrame>>,
    pub sender: Sender,
    // Test instrumentation: this lock is never nested or held across an await.
    pub counts: Arc<Mutex<Counts>>,
}

fn frame(channel_id: u64, body: Body) -> wire::SessionFrame {
    wire::SessionFrame {
        channel_id,
        generation: u64::from(channel_id != 0),
        body: Some(body),
    }
}

fn send(sender: &Sender, channel: u64, body: Body) -> acyclic_harness::Result<()> {
    sender
        .unbounded_send(Ok(frame(channel, body)))
        .map_err(|_| Error::Storage("swarm session input ended".into()))
}

pub(super) fn window(sender: &Sender, channel: u64, frames: u32) -> acyclic_harness::Result<()> {
    send(
        sender,
        channel,
        Body::Window(wire::SessionWindow {
            bytes: 1_000_000,
            frames,
        }),
    )
}

pub(super) fn root_window(sender: &Sender, frames: u32) -> acyclic_harness::Result<()> {
    send(
        sender,
        0,
        Body::Window(wire::SessionWindow {
            bytes: u64::from(frames) * 65_536,
            frames,
        }),
    )
}

impl Wire {
    pub fn open(provider: Arc<MemoryStream>, fault: Fault) -> acyclic_harness::Result<Self> {
        let (sender, receiver) = mpsc::unbounded();
        send(
            &sender,
            0,
            Body::Hello(wire::SessionHello {
                version: 1,
                client_id: b"swarm-load".to_vec().into(),
                destination_node: "node".into(),
                receive_bytes: 1_000_000,
                receive_frames: 0,
            }),
        )?;
        let counts = Arc::new(Mutex::new(Counts::default()));
        let outgoing = session::serve(
            provider,
            "node".into(),
            Default::default(),
            receiver.boxed(),
        )
        .map_err(|error| Error::Storage(error.to_string()))?;
        let sink_counts = counts.clone();
        let sink = Box::pin(futures::sink::unfold(
            sender.clone(),
            move |sender, update: wire::SessionInterest| {
                let counts = sink_counts.clone();
                async move {
                    {
                        let mut counts = counts.lock().unwrap_or_else(|p| p.into_inner());
                        for removed in &update.remove {
                            if !counts.full.remove(&removed.channel_id) {
                                return Err(Error::Invalid(
                                    "removed a non-content interest".into(),
                                ));
                            }
                            counts.removed += 1;
                        }
                        for subscription in &update.upsert {
                            let id = subscription
                                .channel
                                .ok_or_else(|| Error::Invalid("missing channel".into()))?
                                .channel_id;
                            match &subscription.kind {
                                Some(Kind::Children(_)) => counts.children += 1,
                                Some(Kind::Follow(follow))
                                    if follow.delivery == wire::SessionDelivery::Latest as i32 =>
                                {
                                    if !follow.path.ends_with("/status")
                                        || !counts.statuses.insert(follow.path.clone())
                                    {
                                        return Err(Error::Invalid(
                                            "status was omitted, duplicated or changed fidelity"
                                                .into(),
                                        ));
                                    }
                                    counts.status_follows += 1;
                                }
                                Some(Kind::Follow(follow))
                                    if follow.delivery == wire::SessionDelivery::All as i32 =>
                                {
                                    if !follow.path.ends_with("/files") || !counts.full.insert(id) {
                                        return Err(Error::Invalid(
                                            "invalid full-fidelity interest".into(),
                                        ));
                                    }
                                    counts.content_follows += 1;
                                }
                                _ => {
                                    counts.actor_channels += 1;
                                    return Err(Error::Invalid(
                                        "non-stream swarm subscription".into(),
                                    ));
                                }
                            }
                        }
                        counts.peak_full = counts.peak_full.max(counts.full.len());
                    }
                    let channels: Vec<_> = update.upsert.iter().filter_map(|s| s.channel).collect();
                    send(&sender, 0, Body::Interest(update))?;
                    for channel in channels {
                        window(&sender, channel.channel_id, 2)?;
                    }
                    Ok::<_, Error>(sender)
                }
            },
        ));
        let observed = counts.clone();
        let grant = sender.clone();
        let mut injected = false;
        let incoming = outgoing
            .map(move |item| {
                let item = item.map_err(|error| Error::Storage(error.to_string()));
                if let Ok(frame) = &item {
                    match &frame.body {
                        Some(Body::Welcome(_)) => {
                            observed.lock().unwrap_or_else(|p| p.into_inner()).sessions += 1
                        }
                        Some(Body::Data(_)) => {
                            let mut counts = observed.lock().unwrap_or_else(|p| p.into_inner());
                            counts.root_credit = counts
                                .root_credit
                                .checked_sub(1)
                                .expect("session exceeded root frame credit");
                            window(&grant, frame.channel_id, 1).expect("channel credit");
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
                            extra = Some(Ok(item.as_ref().expect("matched data frame").clone()))
                        }
                        _ => {}
                    }
                    injected = !keep || extra.is_some();
                }
                stream::iter([keep.then_some(item), extra].into_iter().flatten())
            })
            .flatten()
            .boxed();
        Ok(Self {
            sink,
            incoming,
            sender,
            counts,
        })
    }
}

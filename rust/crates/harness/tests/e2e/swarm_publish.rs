use super::swarm_case::{Layout, SwarmCase};
use acyclic_harness::{
    Error,
    task_volume::{Channel, FileKey, TaskVolume, Write},
};
use acyclic_stream::{AppendRequest, MemoryStream, StreamPath, StreamProvider as _, wire};
use futures::{
    StreamExt as _,
    channel::{mpsc, oneshot},
};
use prost::Message as _;
use std::{sync::Arc, time::Instant};

pub(super) type Volume = Arc<TaskVolume<MemoryStream>>;
pub(super) type Receipt = (usize, u64);
pub(super) struct Gate {
    pub phase: usize,
    pub resume: oneshot::Sender<()>,
}

pub(super) fn path(name: &str) -> StreamPath {
    StreamPath::new(name).expect("generated stream path")
}
pub(super) fn agent(id: usize) -> StreamPath {
    path(&format!("swarm/a{id:05}"))
}
pub(super) fn id(agent: &StreamPath) -> acyclic_harness::Result<usize> {
    agent
        .as_str()
        .strip_prefix("swarm/a")
        .and_then(|id| id.parse().ok())
        .ok_or_else(|| Error::Invalid("unexpected swarm agent path".into()))
}

pub(super) async fn chunk(
    volume: &TaskVolume<MemoryStream>,
    sequence: u64,
    text: &str,
) -> acyclic_harness::Result<bool> {
    let cut = volume.head().await.map_err(storage)?;
    // This simulated writer publishes exactly one chunk per accepted append.
    // The header occupies sequence zero; checking its fence needs no file scan.
    let through = cut.position - 1;
    if through == sequence {
        return Ok(false);
    }
    if through + 1 != sequence {
        return Err(Error::Invalid("producer chunk sequence differs".into()));
    }
    let mut write = Write::new(
        FileKey::parse(&format!(".tasks/t/turns/1/chunks/{sequence}")).map_err(storage)?,
        text.as_bytes().to_vec(),
    );
    write.channel = Some(Channel::Text);
    volume.append(cut, &write).await.map_err(storage)?;
    Ok(true)
}

pub(super) fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

pub(super) async fn layout(
    config: &TaskVolume<MemoryStream>,
    layout: &Layout,
) -> acyclic_harness::Result<()> {
    let names = |range: [usize; 2]| {
        (range[0]..range[1])
            .map(|id| agent(id).as_str().to_owned())
            .collect::<Vec<_>>()
    };
    let bytes = serde_json::to_vec(
        &serde_json::json!({"in_view":names(layout.visible),"prefetch":names(layout.prefetch)}),
    )
    .map_err(storage)?;
    let cut = config.head().await.map_err(storage)?;
    config
        .append(
            cut,
            &Write::new(
                FileKey::parse("workspace/layout.json").map_err(storage)?,
                bytes,
            ),
        )
        .await
        .map_err(storage)?;
    Ok(())
}

pub(super) fn status(phase: usize, state: &str, tokens: u64) -> serde_json::Value {
    serde_json::json!({"phase":phase,"state":state,"tokens":tokens,"cost_micros":0})
}

pub(super) async fn publish_status(
    provider: &MemoryStream,
    id: usize,
    value: serde_json::Value,
    attention: bool,
) -> acyclic_harness::Result<u64> {
    let value = wire::SessionLatestValue {
        key: b"status".to_vec().into(),
        value: serde_json::to_vec(&value).map_err(storage)?.into(),
        attention,
    };
    let outcome = provider
        .append(AppendRequest {
            path: path(&format!("{}/status", agent(id).as_str())),
            records: vec![value.encode_to_vec().into()],
            if_tail: None,
            idempotency_key: None,
        })
        .await
        .map_err(storage)?;
    match outcome {
        acyclic_stream::AppendOutcome::Committed(receipt) => Ok(receipt.end),
        _ => Err(Error::Invalid("status append was not committed".into())),
    }
}

pub(super) struct Producer<'a> {
    pub case: &'a SwarmCase,
    pub provider: &'a MemoryStream,
    pub volumes: &'a [Volume],
    pub phases: mpsc::UnboundedReceiver<usize>,
    pub receipts: mpsc::UnboundedReceiver<Receipt>,
    pub status_receipts: Option<mpsc::UnboundedReceiver<Receipt>>,
    pub gates: mpsc::UnboundedSender<Gate>,
    pub measured: &'a mut Vec<u128>,
}

impl Producer<'_> {
    async fn status(
        &mut self,
        id: usize,
        value: serde_json::Value,
        attention: bool,
    ) -> acyclic_harness::Result<()> {
        let through = publish_status(self.provider, id, value, attention).await?;
        if let Some(receipts) = &mut self.status_receipts {
            if receipts.next().await != Some((id, through)) {
                return Err(Error::Invalid("status publication receipt differs".into()));
            }
        }
        Ok(())
    }

    pub async fn run(mut self) -> acyclic_harness::Result<()> {
        for expected in 0..self.case.layouts.len() {
            if self.phases.next().await != Some(expected) {
                return Err(Error::Invalid("producer phase differs".into()));
            }
            if expected == 0
                && let Some(receipts) = &mut self.status_receipts
            {
                // Consume every initial idle value before publishing new statuses.
                // State::status already rejects duplicates, reversals and bad IDs.
                for _ in 0..self.case.agents {
                    if !receipts.next().await.is_some_and(|(_, next)| next == 1) {
                        return Err(Error::Invalid("initial status receipt differs".into()));
                    }
                }
            }
            let through = self.case.through(expected);
            for id in 0..self.case.agents {
                for tokens in [through - 2, through - 1] {
                    self.status(id, status(expected + 1, "running", tokens), false)
                        .await?;
                }
                if id % self.case.attention_every == 0 {
                    self.status(id, status(expected + 1, "approval", through - 1), true)
                        .await?;
                }
            }
            if expected == 0 {
                let (resume, paused) = oneshot::channel();
                self.gates
                    .unbounded_send(Gate {
                        phase: expected,
                        resume,
                    })
                    .map_err(storage)?;
                paused.await.map_err(storage)?;
            }
            let layout = &self.case.layouts[expected];
            let first = if expected == 0 { 1 } else { 0 };
            let previous = if expected == 0 {
                0
            } else {
                self.case.through(expected - 1)
            };
            for (offset, token) in self.case.turns[expected].iter().enumerate().skip(first) {
                let sequence = previous + offset as u64 + 1;
                for (id, volume) in self.volumes.iter().enumerate() {
                    let started = Instant::now();
                    if chunk(volume, sequence, token).await?
                        && layout.selected().any(|selected| selected == id)
                    {
                        // Each live publication waits for its push receipt, so a
                        // ready producer cannot monopolize an in-memory executor.
                        loop {
                            let receipt =
                                self.receipts.next().await.ok_or_else(|| {
                                    Error::Storage("content receipt ended".into())
                                })?;
                            if receipt == (id, sequence) {
                                break;
                            }
                            if receipt.1 > sequence {
                                return Err(Error::Invalid("content receipt order differs".into()));
                            }
                        }
                        if layout.rendered(id) {
                            self.measured.push(started.elapsed().as_nanos());
                        }
                    }
                }
            }
            for id in 0..self.case.agents {
                self.status(id, status(expected + 1, "settled", through), false)
                    .await?;
            }
        }
        // Remain attached through the final quiescence check.
        self.phases.next().await;
        Err(Error::Storage("producer unexpectedly detached".into()))
    }
}

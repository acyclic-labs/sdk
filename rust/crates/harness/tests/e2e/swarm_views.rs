use super::{
    swarm_case::SwarmCase,
    swarm_publish::{self as publish, Receipt},
    swarm_wire::Fault,
};
use acyclic_harness::{
    Error,
    client::{ClientViewHandle, QueryViewLimits, SwarmChange, query_verification::FileViews},
    task_volume::{Change, FileValue, Generation, HeadFile, HeadSnapshot},
};
use acyclic_stream::{CommitId, Record, wire};
use futures::channel::mpsc;
use std::collections::{BTreeMap, BTreeSet};

pub(super) enum Action {
    None,
    Gap {
        id: usize,
        sequence: u64,
        text: String,
    },
    Layout(usize),
    Phase(usize),
}

pub(super) struct State<'a> {
    case: &'a SwarmCase,
    pub phase: usize,
    pub complete: bool,
    waiting_layout: bool,
    heads: BTreeMap<usize, FileViews>,
    status_cursors: Vec<u64>,
    settled: Vec<usize>,
    attention: BTreeSet<(usize, usize)>,
    statuses_done: usize,
    content_done: usize,
    pub snapshots: usize,
    peak_heads: usize,
    visible_updates: usize,
    fold_items: u64,
    fault: Fault,
    full_records: usize,
    receipts: mpsc::UnboundedSender<Receipt>,
    status_receipts: Option<mpsc::UnboundedSender<Receipt>>,
}

impl<'a> State<'a> {
    pub fn new(
        case: &'a SwarmCase,
        receipts: mpsc::UnboundedSender<Receipt>,
        fault: Fault,
        status_receipts: Option<mpsc::UnboundedSender<Receipt>>,
    ) -> Self {
        Self {
            case,
            phase: 0,
            complete: false,
            waiting_layout: false,
            heads: BTreeMap::new(),
            status_cursors: vec![0; case.agents],
            settled: vec![0; case.agents],
            attention: BTreeSet::new(),
            statuses_done: 0,
            content_done: 0,
            snapshots: 0,
            peak_heads: 0,
            visible_updates: 0,
            fold_items: 0,
            fault,
            full_records: 0,
            receipts,
            status_receipts,
        }
    }

    pub fn accept(
        &mut self,
        update: SwarmChange,
        views: &ClientViewHandle,
    ) -> acyclic_harness::Result<Action> {
        let action = match update {
            SwarmChange::Agent { .. } | SwarmChange::Retired { .. } => Action::None,
            SwarmChange::Snapshot { agent, cursor } => {
                // Independent oracle uses the same canonical file fold. Only
                // segment handles are cloned; production has a single cache.
                let snapshot = views.read(|view| HeadSnapshot {
                    files: view
                        .files(&agent)
                        .map(|file| HeadFile {
                            publication: file.publication,
                            channel: file.channel,
                            key: file.key.clone(),
                            segments: file.segments.clone(),
                            length: file.length,
                            through_sequence: file.through_sequence,
                        })
                        .collect(),
                    generation: Generation {
                        volume: cursor.volume,
                        epoch: cursor.epoch,
                        position: cursor.position,
                        commit: cursor.commit,
                    },
                    cursor,
                })?;
                self.snapshot(publish::id(&agent)?, snapshot)?
            }
            SwarmChange::Layout => {
                let demand = views.read(|view| view.layout().clone())?;
                let expected = &self.case.layouts[self.phase];
                if demand.visible().cloned().collect::<Vec<_>>()
                    != (expected.visible[0]..expected.visible[1])
                        .map(publish::agent)
                        .collect::<Vec<_>>()
                    || demand.full_fidelity().cloned().collect::<BTreeSet<_>>()
                        != expected.selected().map(publish::agent).collect()
                {
                    return Err(Error::Invalid("swarm layout differs".into()));
                }
                self.heads
                    .retain(|id, _| expected.selected().any(|selected| selected == *id));
                self.visible_updates += demand.visible().count();
                self.waiting_layout = false;
                Action::Phase(self.phase)
            }
            SwarmChange::Data {
                agent,
                overview,
                data,
                ..
            } => {
                let id = publish::id(&agent)?;
                if id >= self.case.agents {
                    return Err(Error::Invalid("unexpected agent delivery".into()));
                }
                let next = data
                    .after
                    .as_ref()
                    .ok_or_else(|| Error::Invalid("missing delivery cut".into()))?
                    .next;
                if data.durable > data.accepted || next > data.accepted {
                    return Err(Error::Invalid("invalid wire watermarks".into()));
                }
                if overview {
                    self.status(id, next, data.item)?;
                } else {
                    self.content(id, next, data)?;
                    views.read(|view| {
                        let key = publish::agent(id);
                        let expected = &self.heads[&id];
                        if view.cursor(&key).map(|cut| (cut.position, cut.commit))
                            != Some((expected.cursor().position, expected.cursor().commit))
                            || view
                                .files(&key)
                                .flat_map(|file| {
                                    file.segments.iter().flat_map(|segment| segment.iter())
                                })
                                .ne(expected.files().flat_map(|file| {
                                    file.segments.iter().flat_map(|segment| segment.iter())
                                }))
                        {
                            Err(Error::Invalid(
                                "shared swarm view differs from complete file fold".into(),
                            ))
                        } else {
                            Ok(())
                        }
                    })??;
                }
                Action::None
            }
            SwarmChange::Watermark { .. } => Action::None,
        };
        if matches!(action, Action::None) && !self.waiting_layout && !self.complete {
            let needed = (self.phase + 1) * self.case.agents.div_ceil(self.case.attention_every);
            if self.statuses_done == self.case.agents
                && self.content_done == self.case.layouts[self.phase].selected().count()
                && self.attention.len() == needed
            {
                self.verify_prefix()?;
                if self.phase + 1 == self.case.layouts.len() {
                    self.complete = true;
                } else {
                    self.phase += 1;
                    self.statuses_done = 0;
                    self.content_done = 0;
                    let previous = &self.case.layouts[self.phase - 1];
                    let next = &self.case.layouts[self.phase];
                    if (previous.visible[0]..previous.visible[1])
                        .eq(next.visible[0]..next.visible[1])
                        && previous.selected().collect::<BTreeSet<_>>()
                            == next.selected().collect::<BTreeSet<_>>()
                    {
                        // An unchanged interest file correctly emits no new
                        // layout. Advance the publisher from the settled cut.
                        return Ok(Action::Phase(self.phase));
                    }
                    self.waiting_layout = true;
                    return Ok(Action::Layout(self.phase));
                }
            }
        }
        Ok(action)
    }

    fn snapshot(&mut self, id: usize, snapshot: HeadSnapshot) -> acyclic_harness::Result<Action> {
        if !self.case.layouts[self.phase]
            .selected()
            .any(|selected| selected == id)
        {
            return Err(Error::Invalid("snapshot outside selected agents".into()));
        }
        let limits = QueryViewLimits {
            files: 1,
            segments: self.case.turns.iter().map(Vec::len).sum(),
            bytes: self
                .case
                .turns
                .iter()
                .flatten()
                .map(|s| s.len() as u64)
                .sum(),
        };
        let view = FileViews::snapshot(snapshot, limits).map_err(publish::storage)?;
        self.heads.insert(id, view);
        self.snapshots += 1;
        self.peak_heads = self.peak_heads.max(self.heads.len());
        let first = usize::from(self.phase == 0);
        let previous = if self.phase == 0 {
            0
        } else {
            self.case.through(self.phase - 1)
        };
        Ok(Action::Gap {
            id,
            sequence: previous + first as u64 + 1,
            text: self.case.turns[self.phase][first].clone(),
        })
    }

    fn status(
        &mut self,
        id: usize,
        next: u64,
        item: Option<wire::session_data::Item>,
    ) -> acyclic_harness::Result<()> {
        if next <= self.status_cursors[id] {
            return Err(Error::Invalid(
                "duplicate or reversed overview delivery".into(),
            ));
        }
        self.status_cursors[id] = next;
        let Some(wire::session_data::Item::LatestValue(value)) = item else {
            return Err(Error::Invalid("overview changed wire fidelity".into()));
        };
        let status: serde_json::Value =
            serde_json::from_slice(&value.value).map_err(publish::storage)?;
        let phase = status["phase"]
            .as_u64()
            .ok_or_else(|| Error::Invalid("missing status phase".into()))?
            as usize;
        if phase > self.phase + 1 {
            return Err(Error::Invalid("future overview status".into()));
        }
        if value.key.as_ref() != b"status" {
            return Err(Error::Invalid("status key differs".into()));
        }
        if value.attention {
            if phase == 0
                || id % self.case.attention_every != 0
                || !self.attention.insert((id, phase))
                || status != publish::status(phase, "approval", self.case.through(phase - 1) - 1)
            {
                return Err(Error::Invalid(
                    "attention was corrupted or duplicated".into(),
                ));
            }
        } else if status["state"] == "settled" {
            if phase == 0
                || status != publish::status(phase, "settled", self.case.through(phase - 1))
                || self.settled[id] >= phase
                || phase != self.phase + 1
            {
                return Err(Error::Invalid("terminal overview status differs".into()));
            }
            self.settled[id] = phase;
            self.statuses_done += 1;
        }
        if let Some(receipts) = &self.status_receipts {
            receipts
                .unbounded_send((id, next))
                .map_err(publish::storage)?;
        }
        Ok(())
    }

    fn content(
        &mut self,
        id: usize,
        next: u64,
        data: wire::SessionData,
    ) -> acyclic_harness::Result<()> {
        let Some(wire::session_data::Item::Record(record)) = data.item else {
            return Err(Error::Invalid("content changed wire fidelity".into()));
        };
        let record = Record {
            sequence: record.sequence,
            value: record.value.into(),
            commit_id: CommitId::from_bytes(
                record
                    .commit_id
                    .as_ref()
                    .try_into()
                    .map_err(publish::storage)?,
            ),
            committed_at_micros: record.committed_at_micros,
        };
        if record.sequence + 1 != next {
            return Err(Error::Invalid("content wire position differs".into()));
        }
        let file = FileValue::from_record(&record)
            .map_err(publish::storage)?
            .ok_or_else(|| Error::Invalid("content header repeated".into()))?;
        let view = self
            .heads
            .get_mut(&id)
            .ok_or_else(|| Error::Invalid("content preceded snapshot".into()))?;
        let mut cursor = view.cursor().clone();
        let before = cursor.position;
        cursor.position = next;
        cursor.commit = *record.commit_id.as_bytes();
        cursor.accepted = cursor.accepted.max(data.accepted);
        cursor.durable = cursor.durable.max(data.durable);
        let change = Change {
            file: Some(file),
            files: vec![],
            removed: vec![],
            cursor,
        };
        let (result, examined) = acyclic_harness::test_support::measure_task_volume_work(|| {
            if self.fault == Fault::ScanRetainedViews {
                // Broken fold: inspect retained entries on every new publication.
                std::hint::black_box(view.files().count());
            }
            let apply = view.prepare(&change)?;
            apply(view)
        });
        result.map_err(publish::storage)?;
        if examined != 3 {
            return Err(Error::Invalid(format!(
                "file fold examined {examined} items instead of 3"
            )));
        }
        self.fold_items += examined;
        self.full_records += 1;
        if self.case.layouts[self.phase].rendered(id) {
            self.visible_updates += 1;
        }
        let through = self.case.through(self.phase) + 1;
        if before < through && next == through {
            self.content_done += 1;
        }
        self.receipts
            .unbounded_send((id, record.sequence))
            .map_err(publish::storage)?;
        Ok(())
    }

    fn verify_prefix(&self) -> acyclic_harness::Result<()> {
        let expected: String = self.case.turns[..=self.phase]
            .iter()
            .flatten()
            .cloned()
            .collect();
        for id in self.case.layouts[self.phase].selected() {
            let view = self
                .heads
                .get(&id)
                .ok_or_else(|| Error::Invalid("selected view missing".into()))?;
            let files: Vec<_> = view.files().collect();
            if files.len() != 1
                || files[0]
                    .segments
                    .iter()
                    .flat_map(|s| s.iter())
                    .copied()
                    .collect::<Vec<_>>()
                    != expected.as_bytes()
                || view.cursor().position != self.case.through(self.phase) + 1
            {
                return Err(Error::Invalid(format!(
                    "selected agent {id} has a tier gap or extra content"
                )));
            }
        }
        Ok(())
    }

    pub fn report(&self) -> serde_json::Value {
        serde_json::json!({"statuses":self.statuses_done,"attention":self.attention.len(),"snapshots":self.snapshots,
            "peak_heads":self.peak_heads,"retained_full":self.heads.len(),"visible_updates":self.visible_updates,"fold_items":self.fold_items,"full_records":self.full_records})
    }

    pub fn verify(&self) -> acyclic_harness::Result<()> {
        if !self.complete {
            return Err(Error::Invalid(format!(
                "swarm did not reach its known cut: {}",
                self.report()
            )));
        }
        self.verify_prefix()?;
        for id in 0..self.case.agents {
            let expected = 1 + self.case.layouts.len() as u64
                * (3 + u64::from(id % self.case.attention_every == 0));
            if self.status_cursors[id] != expected {
                return Err(Error::Invalid(
                    "overview omitted its final accepted cut".into(),
                ));
            }
        }
        Ok(())
    }
}

use super::*;
use acyclic_stream::StreamWatermarks;
use bytes::Bytes;

/// Exact latest values for one agent; the producer owns their schema.
#[derive(Clone, Debug, PartialEq)]
pub struct StatusView {
    after: Option<wire::SessionCursor>,
    watermarks: StreamWatermarks,
    values: BTreeMap<Bytes, StatusValue>,
}

#[derive(Clone, Debug, PartialEq)]
struct StatusValue {
    after: wire::SessionCursor,
    value: wire::SessionLatestValue,
}

impl Default for StatusView {
    fn default() -> Self {
        Self {
            after: None,
            watermarks: StreamWatermarks {
                accepted: 0,
                durable: 0,
            },
            values: BTreeMap::new(),
        }
    }
}

impl StatusView {
    /// Greatest delivered status cursor; platform I/O owns replay and fencing.
    pub fn cursor(&self) -> Option<&wire::SessionCursor> {
        self.after.as_ref()
    }
    /// Accepted and confirmed durable status prefixes.
    pub fn watermarks(&self) -> StreamWatermarks {
        self.watermarks
    }
    /// Latest values in stable key order, with producer-defined payloads.
    pub fn values(&self) -> impl Iterator<Item = &wire::SessionLatestValue> {
        self.values.values().map(|v| &v.value)
    }
}

// The only owner of status payloads and their aggregate quota. Callers cannot
// mutate a row, replace its map key, or retire it separately from its charge.
#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct Agents {
    entries: BTreeMap<StreamPath, AgentViews>,
    bytes: u64,
    values: usize,
}

fn size(value: &wire::SessionLatestValue, cursor: &wire::SessionCursor) -> Option<u64> {
    (value.key.len() as u64)
        .checked_add(value.value.len() as u64)?
        .checked_add(cursor.opaque.len() as u64)
}

#[derive(Clone, Debug, Default, PartialEq)]
struct AgentViews {
    status: StatusView,
    files: BTreeMap<([u8; 32], [u8; 32]), FileQueryView>,
}

impl Agents {
    pub(super) fn get(&self, agent: &StreamPath) -> Option<&StatusView> {
        self.entries.get(agent).map(|agent| &agent.status)
    }
    pub(super) fn keys(&self) -> impl Iterator<Item = &StreamPath> {
        self.entries.keys()
    }
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }
    pub(super) fn contains(&self, agent: &StreamPath) -> bool {
        self.entries.contains_key(agent)
    }
    pub(super) fn discover(&mut self, agent: StreamPath) {
        self.entries.entry(agent).or_default();
    }
    pub(super) fn files(
        &self,
        agent: &StreamPath,
    ) -> Option<&BTreeMap<([u8; 32], [u8; 32]), FileQueryView>> {
        self.entries.get(agent).map(|agent| &agent.files)
    }
    pub(super) fn files_mut(
        &mut self,
        agent: &StreamPath,
    ) -> Option<&mut BTreeMap<([u8; 32], [u8; 32]), FileQueryView>> {
        self.entries.get_mut(agent).map(|agent| &mut agent.files)
    }
    pub(super) fn entry_files(
        &mut self,
        agent: StreamPath,
    ) -> &mut BTreeMap<([u8; 32], [u8; 32]), FileQueryView> {
        &mut self.entries.entry(agent).or_default().files
    }

    pub(super) fn retire(&mut self, agent: &StreamPath) {
        if let Some(retired) = self.entries.remove(agent) {
            #[cfg(all(test, not(acyclic_harness_loom)))]
            if LEAK.with(std::cell::Cell::get) {
                return;
            }
            self.values -= retired.status.values.len();
            self.bytes -= retired
                .status
                .values
                .values()
                .map(|v| size(&v.value, &v.after).expect("retained status was admitted"))
                .sum::<u64>();
        }
    }

    pub(super) fn watermark(
        &mut self,
        agent: &StreamPath,
        mark: &wire::SessionWatermark,
    ) -> Result<()> {
        let current = self
            .get(agent)
            .map(|status| status.watermarks)
            .unwrap_or(StreamWatermarks {
                accepted: 0,
                durable: 0,
            });
        if mark.durable > mark.accepted
            || mark.accepted < current.accepted
            || mark.durable < current.durable
        {
            return Err(Error::Invalid("status watermark regressed".into()));
        }
        self.entries
            .entry(agent.clone())
            .or_default()
            .status
            .watermarks = StreamWatermarks {
            accepted: mark.accepted,
            durable: mark.durable,
        };
        Ok(())
    }

    pub(super) fn data(
        &mut self,
        agent: &StreamPath,
        data: &wire::SessionData,
        limits: ClientViewLimits,
    ) -> Result<()> {
        let after = data
            .after
            .as_ref()
            .ok_or_else(|| Error::Invalid("status cursor missing".into()))?;
        if after.next == 0 || data.durable > data.accepted || after.next > data.accepted {
            return Err(Error::Invalid("invalid status watermarks".into()));
        }
        let value = match &data.item {
            Some(wire::session_data::Item::LatestValue(value)) => value,
            _ => return Err(Error::Invalid("status received a non-value frame".into())),
        };
        let old = self
            .entries
            .get(agent)
            .and_then(|s| s.status.values.get(&value.key));
        if old.is_some_and(|old| {
            old.after.next > after.next
                || (old.after.next == after.next && old.after.opaque != after.opaque)
        }) {
            return Err(Error::Conflict("status key cursor regressed".into()));
        }
        if old.is_some_and(|old| old.after == *after && old.value != *value) {
            return Err(Error::Conflict(
                "one status cut contains conflicting values".into(),
            ));
        }
        let retained =
            size(value, after).ok_or_else(|| Error::Invalid("status bytes overflow".into()))?;
        let bytes = self
            .bytes
            .checked_sub(
                old.and_then(|old| size(&old.value, &old.after))
                    .unwrap_or(0),
            )
            .and_then(|bytes| bytes.checked_add(retained))
            .ok_or_else(|| Error::Invalid("status bytes overflow".into()))?;
        let values = self
            .values
            .checked_add(usize::from(old.is_none()))
            .ok_or_else(|| Error::Invalid("status count overflow".into()))?;
        if bytes > limits.overview_bytes || values > limits.overview_values {
            return Err(Error::Invalid("status view bound reached".into()));
        }
        let length = usize::try_from(retained)
            .map_err(|_| Error::Invalid("status bytes overflow".into()))?;
        let previous = (self.bytes, self.values);
        self.bytes = bytes;
        self.values = values;
        let reservation = Reservation {
            statuses: self,
            previous: Some(previous),
        };
        #[cfg(all(test, not(acyclic_harness_loom)))]
        if ABANDON.with(std::cell::Cell::get) {
            return Err(Error::Invalid("abandoned status reservation".into()));
        }
        reservation.publish(agent, data, after.clone(), value.clone(), length);
        Ok(())
    }
}

// Exclusive ownership prevents another admission from using these bytes before
// publication. Dropping an unfinished reservation restores the exact old charge.
#[must_use]
struct Reservation<'a> {
    statuses: &'a mut Agents,
    previous: Option<(u64, usize)>,
}

impl Reservation<'_> {
    fn publish(
        mut self,
        agent: &StreamPath,
        data: &wire::SessionData,
        mut after: wire::SessionCursor,
        mut value: wire::SessionLatestValue,
        length: usize,
    ) {
        let key_end = value.key.len();
        let value_end = key_end + value.value.len();
        let mut compact = Vec::with_capacity(length);
        compact.extend_from_slice(&value.key);
        compact.extend_from_slice(&value.value);
        compact.extend_from_slice(&after.opaque);
        let compact = Bytes::from(compact.into_boxed_slice());
        value.key = compact.slice(..key_end);
        value.value = compact.slice(key_end..value_end);
        after.opaque = compact.slice(value_end..);
        let status = &mut self
            .statuses
            .entries
            .entry(agent.clone())
            .or_default()
            .status;
        if status
            .after
            .as_ref()
            .is_none_or(|old| old.next <= after.next)
        {
            status.after = Some(after.clone());
        }
        status.watermarks.accepted = status.watermarks.accepted.max(data.accepted);
        status.watermarks.durable = status.watermarks.durable.max(data.durable);
        let key = value.key.clone();
        // insert alone retains the old map key and its retired allocation.
        status.values.remove(&key);
        status.values.insert(key, StatusValue { after, value });
        self.previous = None;
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        #[cfg(all(test, not(acyclic_harness_loom)))]
        if LEAK.with(std::cell::Cell::get) {
            return;
        }
        if let Some((bytes, values)) = self.previous {
            self.statuses.bytes = bytes;
            self.statuses.values = values;
        }
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
thread_local! {
    static ABANDON: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static LEAK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(all(test, not(acyclic_harness_loom), not(target_arch = "wasm32")))]
mod retention;
#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

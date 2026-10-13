//! Authenticated transitions over the existing atomic operation locations.
use super::effect_heads::Tree;
use super::{HistoryCursor, HistoryReadLimits, history::HistoryIo, operations};
use crate::{
    EffectId, Error, Result,
    core::{Authority, AuthorityVerifier, Event, EventPayload},
    wire_codec::CodecQuota,
};
use acyclic_stream::{Record, StreamProvider};
use operations::decode_location_with_work as decode_location;
use operations::one_record as read;
use operations::publication_path as path;
use operations::sdk_record_retained_bytes as record_bytes;
use operations::tail_optional_with_work as tail;
use operations::verify_operation_binding as verify_binding;
use operations::verify_operation_locator as verify_locator;
use std::sync::Arc;

/// Reads authenticated effect events at a bounded committed cut.
pub async fn effect_events<P: StreamProvider>(
    client: &Arc<P>,
    verifier: &AuthorityVerifier,
    cursor: &HistoryCursor,
    effect: EffectId,
    limits: HistoryReadLimits,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(Vec<Event>, u64)> {
    let tree = Tree {
        client,
        authority: &cursor.authority,
    };
    verifier.verify_audience(&cursor.authority, work)?;
    if limits.maximum_events == 0
        || limits.maximum_bytes == 0
        || cursor.after_revision > cursor.through_revision
    {
        return Err(Error::Invalid("invalid effect history bounds".into()));
    }
    if cursor.through_revision == 0 {
        return Ok((Vec::new(), 0));
    }
    let mut quota = CodecQuota::from_retained(work, 0);
    let (root, mut used) = tree
        .root_at(
            verifier,
            cursor.through_revision,
            limits.maximum_bytes,
            quota.work(),
        )
        .await?;
    let (route, leaf) = tree
        .walk(effect, root, &mut used, limits.maximum_bytes, quota.work())
        .await?;
    let held = route.capacity() as u64 * std::mem::size_of::<([u8; 32], u8, [u8; 32])>() as u64;
    quota.adopt(held)?;
    let head = leaf.and_then(|(_, selected, head)| (selected == effect).then_some(head));
    drop(route);
    quota.relinquish(held)?;
    if head.is_some_and(|head| head.0 > cursor.through_revision) {
        return Err(Error::Storage(
            "effect head exceeds the original cut".into(),
        ));
    }
    let Some(head) = head else {
        return Ok((Vec::new(), 0));
    };
    let count = head
        .1
        .checked_add(1)
        .filter(|count| *count <= u64::from(limits.maximum_events))
        .ok_or_else(|| Error::Invalid("effect history exceeds event allowance".into()))?;
    let (stream, held) = path(
        &cursor.authority,
        "harness/v2/effect-transitions/",
        Some((&effect, 36)),
        quota.work(),
    )?;
    quota.adopt(held)?;
    let mut events = Vec::new();
    let mut previous = 0;
    for position in 0..count {
        let remaining = limits.maximum_bytes - used;
        let record = read(client, &stream, position, quota.work())
            .await?
            .ok_or_else(|| Error::Storage("effect index is incomplete".into()))?;
        let raw = record_bytes(&record, quota.work());
        quota.adopt(raw)?;
        let ((event, owned), bytes) = tree
            .verified(verifier, effect, &record, remaining, quota.work())
            .await?;
        quota.adopt(owned)?;
        if event.revision <= previous
            || event.revision > head.0
            || (position == head.1 && event.revision != head.0)
        {
            return Err(Error::Storage("effect transition differs from cut".into()));
        }
        used += bytes;
        previous = event.revision;
        if let Some(io) = quota.work().as_deref_mut() {
            io.stage_output(bytes - record.value.len() as u64, 1)?;
            let old = events.capacity();
            super::history::ensure_slot(&mut events, limits.maximum_events as usize, io)?;
            quota.adopt(((events.capacity() - old) * std::mem::size_of::<Event>()) as u64)?;
        }
        events.push(event);
        drop(record);
        quota.relinquish(raw)?;
    }
    drop(stream);
    quota.relinquish(held)?;
    let owned = quota.retained();
    quota.transfer();
    Ok((events, owned))
}

impl<P: StreamProvider> Tree<'_, P> {
    async fn verified(
        &self,
        verifier: &AuthorityVerifier,
        effect: EffectId,
        record: &Record,
        maximum: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<((Event, u64), u64)> {
        if record.value.len() as u64 >= maximum {
            return Err(Error::Invalid("effect lookup exceeds byte bound".into()));
        }

        let (client, authority) = (self.client, self.authority);
        let mut quota = CodecQuota::from_retained(work, 0);
        let location = decode_location(&record.value, quota.work())?;
        let held = location.authority.id.capacity() as u64;
        quota.adopt(held)?;
        if location.effect_position != Some(record.sequence) {
            return Err(Error::Storage("effect position differs".into()));
        }
        let ((event, owned), used) = verify_locator(
            client,
            authority,
            verifier,
            record,
            &location,
            maximum,
            quota.work(),
        )
        .await?;
        quota.adopt(owned)?;
        if crate::core::event_effect_id(&event.payload) != Some(effect) {
            return Err(Error::Storage("effect identity differs".into()));
        }
        drop(location);
        quota.relinquish(held)?;
        quota.transfer();
        Ok(((event, owned), used))
    }
    pub(super) async fn retirement_proof_with_work(
        &self,
        event: &Event,
        canonical: &Record,
        maximum: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(bool, u64)> {
        let (client, authority) = (self.client, self.authority);
        let EventPayload::EffectResolved { observation } = &event.payload else {
            return Ok((false, 0));
        };
        if !crate::core::settled_effect(&observation.status) {
            return Ok((false, 0));
        }
        if maximum == 0 {
            return Err(Error::Invalid(
                "effect retirement exceeds byte allowance".into(),
            ));
        }
        let mut quota = CodecQuota::from_retained(work, 0);
        let stream = path(
            authority,
            "harness/v2/aggregate-operations/",
            Some((&event.operation_id, 36)),
            quota.work(),
        )?;
        quota.adopt(stream.1)?;
        let tail = tail(client, &stream.0, quota.work()).await?.unwrap_or(0);
        quota.transfer();
        let operation = self.record(stream, 0, &mut quota).await?;
        let Some(operation) = operation else {
            return if tail == 0 {
                Ok((false, 0))
            } else {
                Err(Error::Storage("terminal operation is missing".into()))
            };
        };
        if tail != 1 {
            return Err(Error::Storage("terminal operation is not immutable".into()));
        }
        let mut used = 0;
        super::effect_heads::charge(&mut used, operation.value.len() as u64, maximum)?;
        let location = decode_location(&operation.value, quota.work())?;
        quota.adopt(location.authority.id.capacity() as u64)?;
        verify_binding(
            authority,
            &operation,
            &location,
            canonical,
            event,
            quota.work(),
        )?;
        let Some(position) = location.effect_position else {
            return Ok((false, used));
        };
        if used == maximum {
            return Err(Error::Invalid(
                "effect retirement exceeds byte allowance".into(),
            ));
        }
        let stream = path(
            authority,
            "harness/v2/effect-transitions/",
            Some((&observation.effect_id, 36)),
            quota.work(),
        )?;
        let record = self
            .record(stream, position, &mut quota)
            .await?
            .ok_or_else(|| Error::Storage("terminal transition is missing".into()))?;
        super::effect_heads::charge(&mut used, record.value.len() as u64, maximum)?;
        if record.value != operation.value || record.commit_id != canonical.commit_id {
            return Err(Error::Storage(
                "terminal transition atomic binding differs".into(),
            ));
        }
        Ok((true, used))
    }
    pub(super) async fn retirement_proof_for_event_with_work(
        &self,
        event: &Event,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        let authority = self.authority;
        let EventPayload::EffectResolved { observation } = &event.payload else {
            return Ok(false);
        };
        if !crate::core::settled_effect(&observation.status) {
            return Ok(false);
        }
        let position = event
            .revision
            .checked_sub(1)
            .ok_or_else(|| Error::Invalid("effect revision must be positive".into()))?;
        let mut quota = CodecQuota::from_retained(work, 0);
        let stream = path(authority, "harness/v2/", None, quota.work())?;
        let record = self
            .record(stream, position, &mut quota)
            .await?
            .ok_or_else(|| Error::Storage("terminal canonical event is missing".into()))?;
        let expected = crate::wire_codec::encode_event(authority, event, quota.work())?;
        let encoded = expected.capacity() as u64;
        quota.adopt(encoded)?;
        if record.value.as_ref() != expected.as_slice() {
            return Err(Error::Storage("terminal canonical event differs".into()));
        }
        drop(expected);
        quota.relinquish(encoded)?;
        self.retirement_proof_with_work(
            event,
            &record,
            2 * acyclic_stream::MAX_RECORD_BYTES as u64,
            quota.work(),
        )
        .await
        .map(|(verified, _)| verified)
    }
}

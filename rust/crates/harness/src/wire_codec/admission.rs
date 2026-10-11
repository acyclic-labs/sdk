//! Logical custody for the closed EventEnvelope schema, never allocator RSS.

use crate::{Error, Result, core::{Authority, Event}, store::history::HistoryIo, wire};
use prost::encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field};
use std::mem::size_of;

#[derive(Clone, Copy)]
enum Carrier { Envelope, Protocol, Authority, Scope, Reference }

#[derive(Default)]
struct Fields { bytes: u64, buffers: u64, capabilities: u64 }

impl Fields {
    fn buffer(&mut self, bytes: usize) -> Result<()> {
        self.bytes = self.bytes.checked_add(bytes as u64)
            .ok_or_else(|| Error::Storage("event carrier byte count overflows".into()))?;
        self.buffers += 1;
        Ok(())
    }

    fn quota(&self) -> Result<u64> {
        // String/Vec<u8> grow to at most twice the admitted payload, with the
        // standard minimum eight-byte byte-buffer growth. Vec<String> has a
        // four-slot minimum and geometric growth. Duplicate wire occurrences
        // count even when prost subsequently replaces their values.
        let buffers = self.bytes.checked_mul(2)
            .and_then(|bytes| self.buffers.checked_mul(8).and_then(|minimum| bytes.checked_add(minimum)));
        let slots = if self.capabilities == 0 { 0 } else {
            self.capabilities.checked_mul(2).ok_or_else(|| Error::Storage("event capability count overflows".into()))?.max(4)
        };
        buffers.and_then(|bytes| slots.checked_mul(size_of::<String>() as u64).and_then(|slots| bytes.checked_add(slots)))
            .ok_or_else(|| Error::Storage("event carrier quota overflows".into()))
    }
}

fn scan(mut input: &[u8], carrier: Carrier, fields: &mut Fields,
    work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
    while !input.is_empty() {
        if let Some(work) = work.as_deref_mut() { work.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?; }
        let (tag, kind) = decode_key(&mut input).map_err(|error| Error::Storage(error.to_string()))?;
        let nested = match (carrier, tag) {
            (Carrier::Envelope, 1) => Some(Carrier::Protocol),
            (Carrier::Envelope, 2) | (Carrier::Reference, 1) => Some(Carrier::Authority),
            (Carrier::Envelope, 6) => Some(Carrier::Scope),
            (Carrier::Envelope, 7) => Some(Carrier::Reference),
            _ => None,
        };
        let owned = matches!((carrier, tag),
            (Carrier::Envelope, 4 | 5 | 8 | 9 | 10)
            | (Carrier::Protocol, 1 | 2)
            | (Carrier::Authority, 2)
            | (Carrier::Scope, 1 | 2 | 3 | 4));
        if kind == WireType::LengthDelimited && (nested.is_some() || owned) {
            let bytes = decode_varint(&mut input).map_err(|error| Error::Storage(error.to_string()))?;
            let bytes = usize::try_from(bytes).ok().filter(|bytes| *bytes <= input.len())
                .ok_or_else(|| Error::Storage("event carrier length exceeds its frame".into()))?;
            let (value, rest) = input.split_at(bytes);
            input = rest;
            if let Some(nested) = nested { scan(value, nested, fields, work)?; }
            else {
                fields.buffer(bytes)?;
                if matches!(carrier, Carrier::Scope) && tag == 2 { fields.capabilities += 1; }
            }
        } else {
            // Prost owns unknown-field and group framing semantics. This is an
            // allocation admission scan, not another semantic event decoder.
            skip_field(kind, tag, &mut input, DecodeContext::default())
                .map_err(|error| Error::Storage(error.to_string()))?;
        }
    }
    Ok(())
}

pub(super) fn decode_quota(input: &[u8], work: &mut Option<&mut dyn HistoryIo>) -> Result<u64> {
    let mut fields = Fields::default();
    scan(input, Carrier::Envelope, &mut fields, work)?;
    fields.quota()?.checked_add(crate::contract::audited_serde_diagnostic_bytes(input.len() as u64,
        concat!("EventEnvelope ProtocolIdentity Authority RecordedScope EventReference ",
            "protocol authority revision operation_id intent_digest scope causal_parent event_type canonical_payload_json attestation ",
            "version descriptor_digest kind id capabilities issuer agent_id ",
            "event authority is missing event intent digest must be 32 bytes event scope is missing ",
            "event type disagrees with its payload event attestation must be 32 bytes ",
            "event envelope is not canonical v2 encoding ",
            "failed to decode Protobuf message buffer underflow invalid varint invalid wire type ",
            "invalid tag value unexpected end group tag recursion limit reached invalid string value ").len() as u64)?)
        .ok_or_else(|| Error::Storage("event diagnostic quota overflows".into()))
}

pub(super) fn encode_metadata_quota(authority: &Authority, event: &Event) -> Result<u64> {
    let (issuer, agent) = event.scope.wire_parts();
    let mut fields = Fields::default();
    for bytes in [super::EVENT_WIRE_VERSION.len(), 64, authority.id.len(), 36, 32,
        event.scope.id().len(), issuer.len(), event.payload.tag().len(), 32] {
        fields.buffer(bytes)?;
    }
    if agent.is_some() { fields.buffer(36)?; }
    for capability in event.scope.capabilities().iter() {
        fields.buffer(capability.len())?;
        fields.capabilities += 1;
    }
    if let Some(parent) = &event.causal_parent { fields.buffer(parent.authority.id.len())?; }
    fields.quota()
}

/// A quota guard is private producer custody. It releases on every error,
/// including cancellation; output custody can be transferred to its owner.
pub(crate) struct Quota<'a, 'b> {
    work: &'a mut Option<&'b mut dyn HistoryIo>,
    bytes: u64,
}

impl<'a, 'b> Quota<'a, 'b> {
    pub(crate) fn new(work: &'a mut Option<&'b mut dyn HistoryIo>, bytes: u64) -> Result<Self> {
        if let Some(work) = work.as_deref_mut() {
            work.admit_owned(bytes)?;
            work.retain_owned(bytes)?;
        }
        Ok(Self { work, bytes })
    }
    /// Takes responsibility for an existing allowance on this exact observer.
    /// The caller transfers already-paid custody; this does not admit new data.
    /// Unlike `new`, transfer must also work after cancellation or quota denial.
    pub(crate) fn from_retained(
        work: &'a mut Option<&'b mut dyn HistoryIo>,
        bytes: u64,
    ) -> Self {
        Self { work, bytes }
    }
    pub(crate) fn work(&mut self) -> &mut Option<&'b mut dyn HistoryIo> { self.work }
    pub(super) fn replace(&mut self, bytes: u64) -> Result<()> {
        if bytes <= self.bytes {
            if let Some(work) = self.work.as_deref_mut() { work.release_owned(self.bytes - bytes)?; }
        } else if let Some(work) = self.work.as_deref_mut() {
            let extra = bytes - self.bytes;
            work.admit_owned(extra)?;
            work.retain_owned(extra)?;
        }
        self.bytes = bytes;
        Ok(())
    }
    pub(crate) fn adopt(&mut self, bytes: u64) -> Result<()> {
        self.bytes = self.bytes.checked_add(bytes)
            .ok_or_else(|| Error::Storage("event quota custody overflows".into()))?;
        Ok(())
    }
    /// Releases an allowance whose protected stage has ended, before its successor.
    pub(crate) fn relinquish(&mut self, bytes: u64) -> Result<()> {
        let remaining = self.bytes.checked_sub(bytes)
            .ok_or_else(|| Error::Storage("codec release exceeds custody".into()))?;
        if let Some(work) = self.work.as_deref_mut() { work.release_owned(bytes)?; }
        self.bytes = remaining;
        Ok(())
    }
    pub(crate) fn retained(&self) -> u64 { self.bytes }
    pub(super) fn transfer_part(&mut self, bytes: u64) -> Result<()> {
        self.bytes = self.bytes.checked_sub(bytes)
            .ok_or_else(|| Error::Storage("event quota transfer exceeds custody".into()))?;
        Ok(())
    }
    pub(crate) fn transfer(&mut self) { self.bytes = 0; }
}
impl Drop for Quota<'_, '_> {
    fn drop(&mut self) {
        if let Some(work) = self.work.as_deref_mut() { let _ = work.release_owned(self.bytes); }
    }
}

pub(super) fn envelope_owned(envelope: &wire::EventEnvelope) -> u64 {
    let protocol = envelope.protocol.as_ref().map_or(0, |value| (value.version.capacity() + value.descriptor_digest.capacity()) as u64);
    let authority = envelope.authority.as_ref().map_or(0, |value| value.id.capacity() as u64);
    let scope = envelope.scope.as_ref().map_or(0, |value| {
        (value.id.capacity() + value.issuer.capacity() + value.agent_id.capacity()
            + value.capabilities.capacity() * size_of::<String>()) as u64
            + value.capabilities.iter().map(|value| value.capacity() as u64).sum::<u64>()
    });
    let parent = envelope.causal_parent.as_ref().and_then(|value| value.authority.as_ref())
        .map_or(0, |value| value.id.capacity() as u64);
    protocol + authority + scope + parent + (envelope.operation_id.capacity()
        + envelope.intent_digest.capacity() + envelope.event_type.capacity()
        + envelope.canonical_payload_json.capacity() + envelope.attestation.capacity()) as u64
}

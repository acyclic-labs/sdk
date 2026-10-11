//! Logical custody for the closed EventEnvelope schema, never allocator RSS.

use crate::{Error, Result, core::{Authority, Event}, store::history::HistoryIo, wire};
use prost::encoding::{DecodeContext, WireType, decode_key, skip_field};
use std::mem::size_of;

fn boundary(work: &Option<&mut dyn HistoryIo>, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
        return Error::NativeBoundary { code: wire::ErrorCode::Storage, reason };
    }
    let _ = work;
    Error::Storage(reason.into())
}

/// Prost 0.14's concrete diagnostic owns one Box<Inner> and a vector of
/// borrowed (message, field) pairs. Its description enum's largest variant is
/// two Strings plus its discriminant; closed EventEnvelope decoding never
/// constructs either owned String. No Display buffer or serde witness is used.
pub(super) struct ProstDiagnostic {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    custody: Option<acyclic_fs::NativeAllocation>,
}

pub(super) fn prost_diagnostic_preclaim(
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<ProstDiagnostic> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        let custody = if let Some(io) = work.as_deref_mut()
            .filter(|io| io.original_native_work().is_some())
        {
            // The longest closed path is Envelope -> Reference -> Authority:
            // three location pairs, with std Vec's four-slot minimum. Unknown
            // groups add no generated-message location names.
            let bytes = (size_of::<(u64, String, String)>()
                + size_of::<Vec<(&'static str, &'static str)>>()
                + 4 * size_of::<(&'static str, &'static str)>()) as u64;
            io.admit_owned(bytes)?;
            io.retain_owned(bytes)?;
            Some(io.take_native_custody(bytes)?)
        } else { None };
        return Ok(ProstDiagnostic { custody });
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { let _ = work; Ok(ProstDiagnostic {}) }
}

impl ProstDiagnostic {
    pub(super) fn forward(mut self, source: prost::DecodeError) -> Error {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(custody) = self.custody.take() {
            return Error::NativeProstDecode {
                code: wire::ErrorCode::Storage, source, custody: Some(custody),
            };
        }
        Error::Storage(source.to_string())
    }
}

#[derive(Clone, Copy)]
enum Carrier { Envelope, Protocol, Authority, Scope, Reference }

#[derive(Default)]
struct Fields { bytes: u64, buffers: u64, capabilities: u64 }

impl Fields {
    fn buffer(&mut self, bytes: usize, work: &Option<&mut dyn HistoryIo>) -> Result<()> {
        self.bytes = self.bytes.checked_add(bytes as u64)
            .ok_or_else(|| boundary(work, "event carrier byte count overflows"))?;
        self.buffers = self.buffers.checked_add(1)
            .ok_or_else(|| boundary(work, "event carrier buffer count overflows"))?;
        Ok(())
    }

    fn quota(&self, work: &Option<&mut dyn HistoryIo>) -> Result<u64> {
        // String/Vec<u8> grow to at most twice the admitted payload, with the
        // standard minimum eight-byte byte-buffer growth. Vec<String> has a
        // four-slot minimum and geometric growth. Duplicate wire occurrences
        // count even when prost subsequently replaces their values.
        let buffers = self.bytes.checked_mul(2)
            .and_then(|bytes| self.buffers.checked_mul(8).and_then(|minimum| bytes.checked_add(minimum)));
        let slots = if self.capabilities == 0 { 0 } else {
            self.capabilities.checked_mul(2).ok_or_else(|| boundary(work, "event capability count overflows"))?.max(4)
        };
        buffers.and_then(|bytes| slots.checked_mul(size_of::<String>() as u64).and_then(|slots| bytes.checked_add(slots)))
            .and_then(|bytes| bytes.checked_add(size_of::<wire::EventEnvelope>() as u64))
            .ok_or_else(|| boundary(work, "event carrier quota overflows"))
    }
}

fn scan(mut input: &[u8], carrier: Carrier, fields: &mut Fields,
    work: &mut Option<&mut dyn HistoryIo>, diagnostic: &mut Option<ProstDiagnostic>) -> Result<()> {
    while !input.is_empty() {
        if let Some(work) = work.as_deref_mut() { work.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?; }
        let before = input.len();
        let (tag, kind) = decode_key(&mut input)
            .map_err(|source| diagnostic.take().expect("preclaimed prost diagnostic").forward(source))?;
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
            let framed = input;
            // The original prost primitive owns length/underflow semantics and
            // constructs the actual guarded cause. Only borrow its admitted
            // frame afterward; do not replace protobuf parsing.
            skip_field(kind, tag, &mut input, DecodeContext::default())
                .map_err(|source| diagnostic.take().expect("preclaimed prost diagnostic").forward(source))?;
            let header = framed.iter().position(|byte| byte & 0x80 == 0)
                .expect("prost accepted length varint") + 1;
            let bytes = framed.len() - input.len() - header;
            let value = &framed[header..header + bytes];
            if let Some(nested) = nested {
                // Count the framing here; the borrowed child scan counts its
                // own bytes. This is lexical work, never another SDK read.
                if let Some(io) = work.as_deref_mut() {
                    io.codec_charge(crate::contract::JsonCodecCharge::Work {
                        bytes: (before - input.len() - bytes) as u64, items: 0,
                    })?;
                }
                scan(value, nested, fields, work, diagnostic)?;
                continue;
            } else {
                fields.buffer(bytes, work)?;
                if matches!(carrier, Carrier::Scope) && tag == 2 {
                    fields.capabilities = fields.capabilities.checked_add(1)
                        .ok_or_else(|| boundary(work, "event capability count overflows"))?;
                }
            }
        } else {
            // Prost owns unknown-field and group framing semantics. This is an
            // allocation admission scan, not another semantic event decoder.
            skip_field(kind, tag, &mut input, DecodeContext::default())
                .map_err(|source| diagnostic.take().expect("preclaimed prost diagnostic").forward(source))?;
        }
        if let Some(io) = work.as_deref_mut() {
            io.codec_charge(crate::contract::JsonCodecCharge::Work {
                bytes: (before - input.len()) as u64, items: 0,
            })?;
        }
    }
    Ok(())
}

pub(super) fn decode_quota(input: &[u8], work: &mut Option<&mut dyn HistoryIo>) -> Result<u64> {
    if input.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(boundary(work, "event exceeds SDK record boundary"));
    }
    // This guard precedes even decode_key: constructing DecodeError itself
    // allocates its original Box, independently of later carrier admission.
    let mut diagnostic = Some(prost_diagnostic_preclaim(work)?);
    let mut fields = Fields::default();
    scan(input, Carrier::Envelope, &mut fields, work, &mut diagnostic)?;
    fields.quota(work)
}

pub(super) fn encode_metadata_quota(authority: &Authority, event: &Event) -> Result<u64> {
    let (issuer, agent) = event.scope.wire_parts();
    let mut fields = Fields::default();
    for bytes in [super::EVENT_WIRE_VERSION.len(), 64, authority.id.len(), 36, 32,
        event.scope.id().len(), issuer.len(), event.payload.tag().len(), 32] {
        fields.buffer(bytes, &None)?;
    }
    if agent.is_some() { fields.buffer(36, &None)?; }
    for capability in event.scope.capabilities().iter() {
        fields.buffer(capability.len(), &None)?;
        fields.capabilities = fields.capabilities.checked_add(1)
            .ok_or_else(|| boundary(&None, "event capability count overflows"))?;
    }
    if let Some(parent) = &event.causal_parent { fields.buffer(parent.authority.id.len(), &None)?; }
    // The closed EventPayload serializer may return its actual owned SDK
    // diagnostic after the canonical meter has relinquished its workspace.
    // Keep an independent original producer claim until that cause is moved.
    fields.quota(&None)?.checked_add(crate::contract::sdk_json_diagnostic_bytes())
        .ok_or_else(|| boundary(&None, "event encoding diagnostic quota overflows"))
}

/// Bytes copied from existing producer fields into the original closed carrier.
/// UUID and descriptor display output are computed, not borrowed source copies.
pub(super) fn encode_metadata_copy_bytes(authority: &Authority, event: &Event) -> Result<u64> {
    let (issuer, _) = event.scope.wire_parts();
    let mut bytes = 0u64;
    for length in [super::EVENT_WIRE_VERSION.len(), authority.id.len(),
        event.scope.id().len(), issuer.len(), event.payload.tag().len(), 32, 32]
        .into_iter()
        .chain(event.scope.capabilities().iter().map(|value| value.len()))
        .chain(event.causal_parent.iter().map(|parent| parent.authority.id.len()))
    {
        bytes = bytes.checked_add(length as u64)
            .ok_or_else(|| boundary(&None, "event metadata copy bytes overflow"))?;
    }
    Ok(bytes)
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
            .ok_or_else(|| boundary(self.work, "event quota custody overflows"))?;
        Ok(())
    }
    /// Releases an allowance whose protected stage has ended, before its successor.
    pub(crate) fn relinquish(&mut self, bytes: u64) -> Result<()> {
        let remaining = self.bytes.checked_sub(bytes)
            .ok_or_else(|| boundary(self.work, "codec release exceeds custody"))?;
        if let Some(work) = self.work.as_deref_mut() { work.release_owned(bytes)?; }
        self.bytes = remaining;
        Ok(())
    }
    pub(crate) fn retained(&self) -> u64 { self.bytes }
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

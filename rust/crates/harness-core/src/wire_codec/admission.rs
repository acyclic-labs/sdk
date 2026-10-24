//! Admission for the closed event-envelope schema.
use super::*;
use prost::encoding::{DecodeContext, WireType, decode_key, skip_field};
use std::mem::size_of;

fn diagnostic(work: &mut Option<&mut dyn HistoryIo>) -> Result<NativeCustody> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = work
        .as_deref_mut()
        .filter(|io| io.original_native_work().is_some())
    {
        // Closed message nesting has at most four diagnostic location slots.
        let bytes = (size_of::<(u64, String, String)>()
            + size_of::<Vec<(&'static str, &'static str)>>()
            + 4 * size_of::<(&'static str, &'static str)>()) as u64;
        io.admit_owned(bytes)?;
        io.retain_owned(bytes)?;
        return Ok(Some(io.take_native_custody(bytes)?));
    }
    let _ = work;
    Ok(None)
}
pub(super) fn prost_error(custody: NativeCustody, source: prost::DecodeError) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if custody.is_some() {
        return Error::NativeProstDecode {
            code: Storage,
            source,
            custody,
        };
    }
    let _ = custody;
    let message = source.to_string();
    drop(source);
    Error::Storage(message)
}
#[derive(Clone, Copy)]
enum Carrier {
    Envelope,
    Protocol,
    Authority,
    Scope,
    Reference,
}
fn scan(
    mut input: &[u8],
    carrier: Carrier,
    work: &mut Option<&mut dyn HistoryIo>,
    guard: &mut NativeCustody,
) -> Result<(u64, u64)> {
    use crate::executor::json::JsonCodecCharge::Work;
    let (mut buffers, mut capabilities) = (0, 0);
    // The SDK record bound and closed nesting bound all counters below.
    while !input.is_empty() {
        if let Some(io) = work.as_deref_mut() {
            io.codec_charge(Work { bytes: 0, items: 1 })?;
        }
        let before = input.len();
        let (tag, kind) =
            decode_key(&mut input).map_err(|source| prost_error(guard.take(), source))?;
        let framed = input;
        skip_field(kind, tag, &mut input, DecodeContext::default())
            .map_err(|source| prost_error(guard.take(), source))?;
        let nested = match (carrier, tag) {
            (Carrier::Envelope, 1) => Some(Carrier::Protocol),
            (Carrier::Envelope, 2) | (Carrier::Reference, 1) => Some(Carrier::Authority),
            (Carrier::Envelope, 6) => Some(Carrier::Scope),
            (Carrier::Envelope, 7) => Some(Carrier::Reference),
            _ => None,
        };
        let owned = matches!(
            (carrier, tag),
            (Carrier::Envelope, 4 | 5 | 8 | 9 | 10)
                | (Carrier::Protocol, 1 | 2)
                | (Carrier::Authority, 2)
                | (Carrier::Scope, 1..=4)
        );
        let mut charged = before - input.len();
        if kind == WireType::LengthDelimited && (nested.is_some() || owned) {
            let header = framed
                .iter()
                .position(|byte| byte & 0x80 == 0)
                .ok_or_else(|| boundary(work, Storage, "event length prefix is invalid"))?
                + 1;
            let length = framed.len() - input.len() - header;
            if let Some(nested) = nested {
                charged -= length;
                if let Some(io) = work.as_deref_mut() {
                    io.codec_charge(Work {
                        bytes: charged as u64,
                        items: 0,
                    })?;
                }
                let payload = framed.get(header..header + length).ok_or_else(|| {
                    boundary(work, Storage, "event length-delimited field is invalid")
                })?;
                let (bytes, caps) = scan(payload, nested, work, guard)?;
                buffers += bytes;
                capabilities += caps;
                continue;
            }
            buffers += 2 * length as u64 + 8;
            capabilities += u64::from(matches!(carrier, Carrier::Scope) && tag == 2);
        }
        if let Some(io) = work.as_deref_mut() {
            io.codec_charge(Work {
                bytes: charged as u64,
                items: 0,
            })?;
        }
    }
    Ok((buffers, capabilities))
}
fn carrier_quota(buffers: u64, capabilities: u64) -> Result<u64> {
    let slots = if capabilities == 0 {
        Some(0)
    } else {
        capabilities.checked_mul(2).map(|n| n.max(4))
    };
    slots
        .and_then(|n| n.checked_mul(size_of::<String>() as u64))
        .and_then(|n| buffers.checked_add(n))
        .and_then(|n| n.checked_add(size_of::<wire::EventEnvelope>() as u64))
        .ok_or_else(|| Error::Storage("event custody overflow".into()))
}
pub(super) fn decode_quota(
    input: &[u8],
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(u64, NativeCustody)> {
    if input.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(boundary(work, Storage, "event exceeds SDK record boundary"));
    }
    let mut guard = diagnostic(work)?;
    let (buffers, capabilities) = scan(input, Carrier::Envelope, work, &mut guard)?;
    Ok((carrier_quota(buffers, capabilities)?, guard))
}
pub(super) fn metadata_quota(authority: &Authority, event: &Event) -> Result<(u64, u64)> {
    let (issuer, agent) = event.scope.wire_parts();
    let buffer = |n: u64| n.checked_mul(2).and_then(|n| n.checked_add(8));
    let overflow = || Error::Storage("event metadata overflow".into());
    let mut buffers: u64 =
        (64 * 2 + 8) + (36 * 2 + 8) + if agent.is_some() { 36 * 2 + 8 } else { 0 };
    let mut copied = 0_u64;
    for length in [
        EVENT_WIRE_VERSION.len(),
        authority.id.len(),
        event.scope.id().len(),
        issuer.len(),
        event.payload.tag().len(),
        32,
        32,
    ]
    .into_iter()
    .chain(event.scope.capabilities().iter().map(str::len))
    .chain(
        event
            .causal_parent
            .iter()
            .map(|parent| parent.authority.id.len()),
    ) {
        copied = copied.checked_add(length as u64).ok_or_else(overflow)?;
        buffers = buffers
            .checked_add(buffer(length as u64).ok_or_else(overflow)?)
            .ok_or_else(overflow)?;
    }
    let owned = carrier_quota(buffers, event.scope.capabilities().iter().count() as u64)?
        .checked_add(crate::executor::json::sdk_json_diagnostic_bytes())
        .ok_or_else(overflow)?;
    Ok((owned, copied))
}
/// Custody released on drop or transferred to the output owner.
pub(crate) struct Quota<'a, 'b> {
    work: &'a mut Option<&'b mut dyn HistoryIo>,
    bytes: u64,
}
impl<'a, 'b> Quota<'a, 'b> {
    pub(crate) fn new(work: &'a mut Option<&'b mut dyn HistoryIo>, bytes: u64) -> Result<Self> {
        if let Some(io) = work.as_deref_mut() {
            io.admit_owned(bytes)?;
            io.retain_owned(bytes)?;
        }
        Ok(Self { work, bytes })
    }
    pub(crate) fn from_retained(work: &'a mut Option<&'b mut dyn HistoryIo>, bytes: u64) -> Self {
        Self { work, bytes }
    }
    pub(crate) fn work(&mut self) -> &mut Option<&'b mut dyn HistoryIo> {
        self.work
    }
    pub(crate) fn observer(&mut self) -> Result<&mut (dyn HistoryIo + 'b)> {
        self.work
            .as_deref_mut()
            .ok_or_else(|| Error::Invalid("funded codec quota requires its observer".into()))
    }
    pub(crate) fn replace(&mut self, bytes: u64) -> Result<()> {
        if let Some(work) = self.work.as_deref_mut() {
            if bytes <= self.bytes {
                work.release_owned(self.bytes - bytes)?;
            } else {
                work.admit_owned(bytes - self.bytes)?;
                work.retain_owned(bytes - self.bytes)?;
            }
        }
        self.bytes = bytes;
        Ok(())
    }
    pub(crate) fn adopt(&mut self, bytes: u64) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| boundary(self.work, Storage, "custody overflow"))?;
        Ok(())
    }
    pub(crate) fn relinquish(&mut self, bytes: u64) -> Result<()> {
        let remaining = self
            .bytes
            .checked_sub(bytes)
            .ok_or_else(|| boundary(self.work, Storage, "release exceeds custody"))?;
        self.replace(remaining)
    }
    pub(crate) fn retained(&self) -> u64 {
        self.bytes
    }
    pub(crate) fn transfer(&mut self) {
        self.bytes = 0;
    }
    pub(crate) fn json_error(&mut self, error: Error) -> Error {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut error = error;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Error::NativeJson { custody, .. } = &mut error
            && custody.is_none()
        {
            let bytes = self.bytes;
            *custody = Some(
                match self.observer().and_then(|io| io.take_native_custody(bytes)) {
                    Ok(custody) => custody,
                    Err(error) => return error,
                },
            );
            self.transfer();
        }
        error
    }
}
impl Drop for Quota<'_, '_> {
    fn drop(&mut self) {
        if let Some(work) = self.work.as_deref_mut() {
            let _ = work.release_owned(self.bytes);
        }
    }
}
pub(super) fn envelope_owned(envelope: &wire::EventEnvelope) -> u64 {
    let scope = envelope.scope.as_ref().map_or(0, |scope| {
        scope.id.capacity()
            + scope.issuer.capacity()
            + scope.agent_id.capacity()
            + scope.capabilities.capacity() * size_of::<String>()
            + scope
                .capabilities
                .iter()
                .map(String::capacity)
                .sum::<usize>()
    });
    (scope
        + envelope
            .protocol
            .as_ref()
            .map_or(0, |p| p.version.capacity() + p.descriptor_digest.capacity())
        + envelope.authority.as_ref().map_or(0, |a| a.id.capacity())
        + envelope
            .causal_parent
            .as_ref()
            .and_then(|p| p.authority.as_ref())
            .map_or(0, |a| a.id.capacity())
        + envelope.operation_id.capacity()
        + envelope.intent_digest.capacity()
        + envelope.event_type.capacity()
        + envelope.canonical_payload_json.capacity()
        + envelope.attestation.capacity()) as u64
}

//! Bounded framing of the closed EventEnvelope protocol.
use super::*;
use crate::{NativeCustody, store::history::HistoryIo};
use prost::encoding::{DecodeContext, WireType, decode_key, skip_field};
use std::mem::size_of;

pub(super) fn prost_diagnostic_preclaim(
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<NativeCustody> {
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
            code: wire::ErrorCode::Storage,
            source,
            custody,
        };
    }
    let _ = custody;
    Error::Storage(source.to_string())
}

#[derive(Clone, Copy)]
enum Carrier {
    Envelope,
    Protocol,
    Authority,
    Scope,
    Reference,
}

fn buffer(bytes: usize, work: &Option<&mut dyn HistoryIo>) -> Result<u64> {
    (bytes as u64)
        .checked_mul(2)
        .and_then(|n| n.checked_add(8))
        .ok_or_else(|| {
            boundary(
                work,
                wire::ErrorCode::Storage,
                "event buffer custody overflows",
            )
        })
}

fn carrier_quota(
    buffers: u64,
    capabilities: u64,
    work: &Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    let slots = if capabilities == 0 {
        0
    } else {
        capabilities
            .checked_mul(2)
            .ok_or_else(|| {
                boundary(
                    work,
                    wire::ErrorCode::Storage,
                    "event capability count overflows",
                )
            })?
            .max(4)
    };
    slots
        .checked_mul(size_of::<String>() as u64)
        .and_then(|slots| buffers.checked_add(slots))
        .and_then(|n| n.checked_add(size_of::<wire::EventEnvelope>() as u64))
        .ok_or_else(|| {
            boundary(
                work,
                wire::ErrorCode::Storage,
                "event carrier custody overflows",
            )
        })
}

fn scan(
    mut input: &[u8],
    carrier: Carrier,
    work: &mut Option<&mut dyn HistoryIo>,
    diagnostic: &mut NativeCustody,
) -> Result<(u64, u64)> {
    let mut buffers = 0_u64;
    let mut capabilities = 0_u64;
    while !input.is_empty() {
        if let Some(io) = work.as_deref_mut() {
            io.codec_charge(crate::executor::json::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        }
        let before = input.len();
        let (tag, kind) =
            decode_key(&mut input).map_err(|source| prost_error(diagnostic.take(), source))?;
        let framed = input;
        skip_field(kind, tag, &mut input, DecodeContext::default())
            .map_err(|source| prost_error(diagnostic.take(), source))?;
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
                | (Carrier::Scope, 1 | 2 | 3 | 4)
        );
        let mut charged = before - input.len();
        if kind == WireType::LengthDelimited && (nested.is_some() || owned) {
            let header = framed
                .iter()
                .position(|byte| byte & 0x80 == 0)
                .expect("prost accepted length varint")
                + 1;
            let length = framed.len() - input.len() - header;
            if let Some(nested) = nested {
                charged -= length;
                if let Some(io) = work.as_deref_mut() {
                    io.codec_charge(crate::executor::json::JsonCodecCharge::Work {
                        bytes: charged as u64,
                        items: 0,
                    })?;
                }
                let (bytes, caps) =
                    scan(&framed[header..header + length], nested, work, diagnostic)?;
                buffers = buffers.checked_add(bytes).ok_or_else(|| {
                    boundary(
                        work,
                        wire::ErrorCode::Storage,
                        "event carrier custody overflows",
                    )
                })?;
                capabilities = capabilities.checked_add(caps).ok_or_else(|| {
                    boundary(
                        work,
                        wire::ErrorCode::Storage,
                        "event capability count overflows",
                    )
                })?;
                continue;
            } else {
                buffers = buffers.checked_add(buffer(length, work)?).ok_or_else(|| {
                    boundary(
                        work,
                        wire::ErrorCode::Storage,
                        "event carrier custody overflows",
                    )
                })?;
                if matches!(carrier, Carrier::Scope) && tag == 2 {
                    capabilities = capabilities.checked_add(1).ok_or_else(|| {
                        boundary(
                            work,
                            wire::ErrorCode::Storage,
                            "event capability count overflows",
                        )
                    })?;
                }
            }
        }
        if let Some(io) = work.as_deref_mut() {
            io.codec_charge(crate::executor::json::JsonCodecCharge::Work {
                bytes: charged as u64,
                items: 0,
            })?;
        }
    }
    Ok((buffers, capabilities))
}

pub(super) fn decode_quota(input: &[u8], work: &mut Option<&mut dyn HistoryIo>) -> Result<u64> {
    if input.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(boundary(
            work,
            wire::ErrorCode::Storage,
            "event exceeds SDK record boundary",
        ));
    }
    let mut diagnostic = prost_diagnostic_preclaim(work)?;
    let (buffers, capabilities) = scan(input, Carrier::Envelope, work, &mut diagnostic)?;
    carrier_quota(buffers, capabilities, work)
}

pub(super) fn encode_metadata_quota(authority: &Authority, event: &Event) -> Result<u64> {
    let (issuer, agent) = event.scope.wire_parts();
    let mut buffers = 0_u64;
    for bytes in [
        super::EVENT_WIRE_VERSION.len(),
        64,
        authority.id.len(),
        36,
        32,
        event.scope.id().len(),
        issuer.len(),
        event.payload.tag().len(),
        32,
    ]
    .into_iter()
    .chain(agent.map(|_| 36))
    .chain(event.scope.capabilities().iter().map(|value| value.len()))
    .chain(
        event
            .causal_parent
            .iter()
            .map(|parent| parent.authority.id.len()),
    ) {
        buffers = buffers.checked_add(buffer(bytes, &None)?).ok_or_else(|| {
            boundary(
                &None,
                wire::ErrorCode::Storage,
                "event metadata custody overflows",
            )
        })?;
    }
    carrier_quota(
        buffers,
        event.scope.capabilities().iter().count() as u64,
        &None,
    )?
    .checked_add(crate::executor::json::sdk_json_diagnostic_bytes())
    .ok_or_else(|| {
        boundary(
            &None,
            wire::ErrorCode::Storage,
            "event encoding diagnostic quota overflows",
        )
    })
}

/// Copied producer bytes, excluding computed UUID and descriptor strings.
pub(super) fn encode_metadata_copy_bytes(authority: &Authority, event: &Event) -> Result<u64> {
    let (issuer, _) = event.scope.wire_parts();
    let mut bytes = 0u64;
    for length in [
        super::EVENT_WIRE_VERSION.len(),
        authority.id.len(),
        event.scope.id().len(),
        issuer.len(),
        event.payload.tag().len(),
        32,
        32,
    ]
    .into_iter()
    .chain(event.scope.capabilities().iter().map(|value| value.len()))
    .chain(
        event
            .causal_parent
            .iter()
            .map(|parent| parent.authority.id.len()),
    ) {
        bytes = bytes.checked_add(length as u64).ok_or_else(|| {
            boundary(
                &None,
                wire::ErrorCode::Storage,
                "event metadata copy bytes overflow",
            )
        })?;
    }
    Ok(bytes)
}

/// Producer custody released on drop or transferred to the output owner.
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
    /// Takes responsibility for already-paid custody on this observer.
    pub(crate) fn from_retained(work: &'a mut Option<&'b mut dyn HistoryIo>, bytes: u64) -> Self {
        Self { work, bytes }
    }
    pub(crate) fn work(&mut self) -> &mut Option<&'b mut dyn HistoryIo> {
        self.work
    }
    pub(crate) fn replace(&mut self, bytes: u64) -> Result<()> {
        if bytes <= self.bytes {
            if let Some(work) = self.work.as_deref_mut() {
                work.release_owned(self.bytes - bytes)?;
            }
        } else if let Some(work) = self.work.as_deref_mut() {
            let extra = bytes - self.bytes;
            work.admit_owned(extra)?;
            work.retain_owned(extra)?;
        }
        self.bytes = bytes;
        Ok(())
    }
    pub(crate) fn adopt(&mut self, bytes: u64) -> Result<()> {
        self.bytes = self.bytes.checked_add(bytes).ok_or_else(|| {
            boundary(
                self.work,
                wire::ErrorCode::Storage,
                "event quota custody overflows",
            )
        })?;
        Ok(())
    }
    /// Releases an allowance whose protected stage has ended, before its successor.
    pub(crate) fn relinquish(&mut self, bytes: u64) -> Result<()> {
        let remaining = self.bytes.checked_sub(bytes).ok_or_else(|| {
            boundary(
                self.work,
                wire::ErrorCode::Storage,
                "codec release exceeds custody",
            )
        })?;
        if let Some(work) = self.work.as_deref_mut() {
            work.release_owned(bytes)?;
        }
        self.bytes = remaining;
        Ok(())
    }
    pub(crate) fn retained(&self) -> u64 {
        self.bytes
    }
    pub(crate) fn transfer(&mut self) {
        self.bytes = 0;
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
    let protocol = envelope.protocol.as_ref().map_or(0, |value| {
        (value.version.capacity() + value.descriptor_digest.capacity()) as u64
    });
    let authority = envelope
        .authority
        .as_ref()
        .map_or(0, |value| value.id.capacity() as u64);
    let scope = envelope.scope.as_ref().map_or(0, |value| {
        (value.id.capacity()
            + value.issuer.capacity()
            + value.agent_id.capacity()
            + value.capabilities.capacity() * size_of::<String>()) as u64
            + value
                .capabilities
                .iter()
                .map(|value| value.capacity() as u64)
                .sum::<u64>()
    });
    let parent = envelope
        .causal_parent
        .as_ref()
        .and_then(|value| value.authority.as_ref())
        .map_or(0, |value| value.id.capacity() as u64);
    protocol
        + authority
        + scope
        + parent
        + (envelope.operation_id.capacity()
            + envelope.intent_digest.capacity()
            + envelope.event_type.capacity()
            + envelope.canonical_payload_json.capacity()
            + envelope.attestation.capacity()) as u64
}

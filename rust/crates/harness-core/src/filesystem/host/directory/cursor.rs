//! Confidential physical progress; no denied name crosses the reader boundary.
use super::*;
use acyclic_auth::cursor::CursorKey;
use base64::{Engine as _, engine::general_purpose::STANDARD};

fn key(io: &PreparationIo) -> Result<&'static CursorKey> {
    static KEY: std::sync::OnceLock<Option<CursorKey>> = std::sync::OnceLock::new();
    KEY.get_or_init(|| CursorKey::new().ok())
        .as_ref()
        .ok_or_else(|| io.invalid("directory cursor key is unavailable"))
}

fn context(volume: &VolumeRef, path: &str, io: &PreparationIo) -> Result<[u8; 32]> {
    let identity = volume.identity_digest(u64::MAX, &mut Some(&mut io.history_io()))?;
    let domain = b"acyclic:directory:cursor:v1";
    io.charged(WorkCounters {
        bytes_hashed: (domain.len() + identity.len() + path.len()) as u64,
        ..Default::default()
    })?;
    let mut hash = blake3::Hasher::new();
    hash.update(domain);
    hash.update(&identity);
    hash.update(path.as_bytes());
    Ok(*hash.finalize().as_bytes())
}

pub(in crate::filesystem) fn encoded_len(
    generation_bytes: usize,
    name_bytes: usize,
) -> Option<usize> {
    generation_bytes
        .checked_add(name_bytes)?
        .checked_add(5 + CursorKey::sealed_len(0))?
        .div_ceil(3)
        .checked_mul(4)
}

pub(in crate::filesystem) fn seal<'io>(
    volume: &VolumeRef,
    path: &str,
    generation: GenerationId,
    after: &LogicalName,
    io: &'io PreparationIo,
) -> Result<(String, DecodeReservation<'io>)> {
    let generation = generation.to_bytes();
    let length = 5usize
        .checked_add(generation.len())
        .and_then(|length| length.checked_add(after.as_bytes().len()))
        .ok_or_else(|| io.invalid("directory cursor size overflow"))?;
    let mut plain = io.buffer(length)?;
    plain.0[..4].copy_from_slice(&(generation.len() as u32).to_be_bytes());
    plain.0[4..4 + generation.len()].copy_from_slice(&generation);
    plain.0[4 + generation.len()] = match after.encoding() {
        NameEncoding::Utf8 => 0,
        NameEncoding::PosixBytes => 1,
        NameEncoding::WindowsUtf16Le => 2,
    };
    plain.0[5 + generation.len()..].copy_from_slice(after.as_bytes());
    seal_payload(volume, path, &plain.0, io)
}

/// The same confidential envelope also carries the shared lazy cursor codec.
pub(super) fn seal_payload<'io>(
    volume: &VolumeRef,
    path: &str,
    plain: &[u8],
    io: &'io PreparationIo,
) -> Result<(String, DecodeReservation<'io>)> {
    let mut sealed = io.buffer(CursorKey::sealed_len(plain.len()))?;
    key(io)?
        .seal_into(&context(volume, path, io)?, plain, &mut sealed.0)
        .map_err(|_| io.invalid("directory cursor could not be sealed"))?;
    let encoded_length = sealed
        .0
        .len()
        .div_ceil(3)
        .checked_mul(4)
        .ok_or_else(|| io.invalid("directory cursor size overflow"))?;
    let mut encoded = io.buffer(encoded_length)?;
    let written = STANDARD
        .encode_slice(&sealed.0, &mut encoded.0)
        .map_err(|_| io.invalid("directory cursor encoding failed"))?;
    encoded.0.truncate(written);
    let text = String::from_utf8(encoded.0)
        .map_err(|_| io.invalid("directory cursor encoding is invalid"))?;
    Ok((text, encoded.1))
}

pub(super) fn open<'io>(
    provider: &ProviderRef,
    volume: &VolumeRef,
    path: &str,
    token: &str,
    io: &'io PreparationIo,
) -> Result<(GenerationRef, LogicalName, DecodeReservation<'io>)> {
    let plain = open_payload(volume, path, token, io)?;
    decode_authored(provider, plain, io)
}

pub(super) fn open_payload<'io>(
    volume: &VolumeRef,
    path: &str,
    token: &str,
    io: &'io PreparationIo,
) -> Result<(Vec<u8>, DecodeReservation<'io>)> {
    let mut sealed = io.buffer(token.len())?;
    let written = STANDARD
        .decode_slice(token, &mut sealed.0)
        .map_err(|_| io.invalid("invalid directory cursor"))?;
    sealed.0.truncate(written);
    let length = written
        .checked_sub(CursorKey::sealed_len(0))
        .ok_or_else(|| io.invalid("invalid directory cursor"))?;
    let mut plain = io.buffer(length)?;
    key(io)?
        .open_into(&context(volume, path, io)?, &sealed.0, &mut plain.0)
        .map_err(|_| io.invalid("invalid directory cursor"))?;
    Ok(plain)
}

fn decode_authored<'io>(
    provider: &ProviderRef,
    plain: (Vec<u8>, DecodeReservation<'io>),
    io: &'io PreparationIo,
) -> Result<(GenerationRef, LogicalName, DecodeReservation<'io>)> {
    let header: [u8; 4] = plain
        .0
        .get(..4)
        .ok_or_else(|| io.invalid("invalid directory cursor"))?
        .try_into()
        .map_err(|_| io.invalid("invalid directory cursor"))?;
    let generation_end = 4usize
        .checked_add(u32::from_be_bytes(header) as usize)
        .ok_or_else(|| io.invalid("invalid directory cursor"))?;
    let generation = plain
        .0
        .get(4..generation_end)
        .and_then(GenerationId::from_bytes)
        .ok_or_else(|| io.invalid("invalid directory cursor generation"))?;
    let encoding = match plain.0.get(generation_end) {
        Some(0) => NameEncoding::Utf8,
        Some(1) => NameEncoding::PosixBytes,
        Some(2) => NameEncoding::WindowsUtf16Le,
        _ => return Err(io.invalid("invalid directory cursor name")),
    };
    let name = plain
        .0
        .get(generation_end + 1..)
        .ok_or_else(|| io.invalid("invalid directory cursor name"))?;
    let mut owned = io.buffer(name.len())?;
    owned.0.copy_from_slice(name);
    let after = LogicalName::new(encoding, owned.0)
        .map_err(|_| io.invalid("invalid directory cursor name"))?;
    let reference = bounded_io::generation_ref(provider, generation, Some(io))?;
    owned
        .1
        .absorb(io.guard_owned(reference.logical_owned_bytes()?)?)?;
    Ok((reference, after, owned.1))
}

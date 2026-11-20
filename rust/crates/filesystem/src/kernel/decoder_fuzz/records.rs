//! Fixed durable records share the page oracle without inventing copy receipts.
use super::super::CanonicalDecodeError;
use super::super::source_state::*;
use {
    crate::{Digest, VolumeId},
    acyclic_fs_format::object_reference::GenerationId,
};

/// Require original writer admission, complete framing and canonical identity.
pub(crate) fn record<
    T: PartialEq + std::fmt::Debug,
    E: std::fmt::Debug + std::error::Error + 'static,
>(
    input: &[u8],
    expected: &T,
    encoded: Result<Vec<u8>, E>,
    decode: impl Fn(&[u8]) -> Result<T, E>,
    encode: impl Fn(&T) -> Result<Vec<u8>, E>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let bytes = super::canonical::<_, _, false>(expected, encoded, &decode)?;
    for end in [0, bytes.len() / 2, bytes.len() - 1] {
        let truncated = bytes.get(..end).ok_or("a truncation is a prefix")?;
        if decode(truncated).is_ok() {
            return Err("truncated filesystem record admitted".into());
        };
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    if !(decode(&trailing).is_err()) {
        return Err("trailing filesystem record bytes admitted".into());
    };
    if let Ok(value) = decode(input)
        && super::canonical::<_, _, false>(&value, encode(&value), &decode)? != input
    {
        return Err("filesystem record changed its original bytes".into());
    }
    Ok(bytes)
}

fn fact(input: &[u8], selector: u8) -> SourceFact {
    let mut identity = [0; 32];
    for (slot, byte) in identity.iter_mut().zip(input) {
        *slot = *byte;
    }
    let (mut volume, mut root) = ([0; 16], [0; 16]);
    for (slot, byte) in volume.iter_mut().chain(&mut root).zip(identity) {
        *slot = byte;
    }
    let (mut paths, mut extents) = ([0; 4], [0; 4]);
    for (slot, byte) in paths.iter_mut().chain(&mut extents).zip(identity) {
        *slot = byte;
    }
    SourceFact {
        volume_id: VolumeId::from_bytes(volume),
        root_identity: root,
        mode: if selector & 1 == 0 {
            DurableSourceMode::Pinned
        } else {
            DurableSourceMode::Tracking
        },
        maximum_paths: (selector & 32 == 0).then(|| u32::from_le_bytes(paths).max(1)),
        maximum_extent_spans: (selector & 64 == 0).then(|| u32::from_le_bytes(extents).max(1)),
        capture_policy: Digest::from_bytes(identity),
        state: match (selector / 2) % 11 {
            0 => DurableSourceState::Clean,
            1 => DurableSourceState::PendingCapture,
            2 => DurableSourceState::Conflict,
            3 => DurableSourceState::Sealed,
            reason => DurableSourceState::NeedsRescan(match reason {
                4 => SourceInvalidation::InitialSnapshotRequired,
                5 => SourceInvalidation::QueueOverflow,
                6 => SourceInvalidation::NativeRescanRequired,
                7 => SourceInvalidation::BackendError,
                8 => SourceInvalidation::UnrepresentablePath,
                9 => SourceInvalidation::AmbiguousRename,
                _ => SourceInvalidation::RootChanged,
            }),
        },
        generation_id: GenerationId::new(Digest::from_bytes(identity)),
    }
}

/// Exercise the production source and distributed record decoders.
pub fn check(input: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let value = fact(input, input.first().copied().unwrap_or(0));
    let bytes = record(
        input,
        &value,
        encode_source_fact(value),
        |bytes| decode_source_fact(bytes, 4096),
        |value| encode_source_fact(*value),
    )?;
    let domain = DOMAIN.len();
    for version in [0_u16, 1, u16::MAX] {
        let mut changed = bytes.clone();
        changed
            .get_mut(domain..domain + 2)
            .ok_or("a source fact carries its version")?
            .copy_from_slice(&version.to_le_bytes());
        if !(matches!(decode_source_fact(&changed, 4096), Err(SourceFactError::Decode(CanonicalDecodeError::UnsupportedVersion {found, expected: 2})) if found == version))
        {
            return Err("source version admitted".into());
        };
    }
    let paths = domain + 2 + 32 + 1;
    let extents = paths + 1 + if value.maximum_paths.is_some() { 4 } else { 0 };
    let state = extents
        + 1
        + if value.maximum_extent_spans.is_some() {
            4
        } else {
            0
        }
        + 32;
    for offset in [domain + 2 + 32, paths, extents, state] {
        let mut changed = bytes.clone();
        *changed
            .get_mut(offset)
            .ok_or("a source fact carries its tags")? = 255;
        if !(decode_source_fact(&changed, 4096).is_err()) {
            return Err("unknown source tag admitted".into());
        };
    }
    for bound in 0..2 {
        let mut invalid = value;
        match bound {
            0 => invalid.maximum_paths = Some(0),
            1 => invalid.maximum_extent_spans = Some(0),
            _ => unreachable!(),
        }
        if !(matches!(encode_source_fact(invalid), Err(SourceFactError::ZeroBound))) {
            return Err("filesystem oracle invariant failed".into());
        };
        let present = if bound == 0 {
            value.maximum_paths
        } else {
            value.maximum_extent_spans
        };
        if present.is_none() {
            continue;
        }
        let mut changed = bytes.clone();
        let offset = if bound == 0 { paths } else { extents } + 1;
        changed
            .get_mut(offset..offset + 4)
            .ok_or("a present source bound is encoded")?
            .fill(0);
        if !(matches!(
            decode_source_fact(&changed, 4096),
            Err(SourceFactError::ZeroBound)
        )) {
            return Err("zero source bound admitted".into());
        };
    }
    if matches!(value.state, DurableSourceState::NeedsRescan(_)) {
        let mut changed = bytes;
        *changed
            .get_mut(state + 1)
            .ok_or("a rescan state carries its reason")? = 255;
        if !(decode_source_fact(&changed, 4096).is_err()) {
            return Err("unknown source reason admitted".into());
        };
    }
    crate::distributed::record_codec_fuzz::check(input)?;
    Ok(())
}

/// Original library writer output; these are authored values, not native captures.
pub fn seeds() -> super::Seeds {
    let mut seeds = Vec::new();
    for selector in 0..22 {
        seeds.push((format!("source-fact-{selector:02}"), {
            let value = fact(&[selector; 32], selector);
            record(
                &[],
                &value,
                encode_source_fact(value),
                |bytes| decode_source_fact(bytes, 4096),
                |value| encode_source_fact(*value),
            )?
        }));
    }
    seeds.extend(crate::distributed::record_codec_fuzz::seeds()?);
    Ok(seeds)
}

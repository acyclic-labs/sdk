//! The original append-log writer and private payload decoder, without disk I/O.
use super::{LOG_FRAME, PUT, REMOVE, decode, encode, replay};
use crate::{Error, wire::ErrorCode};

fn body(frame: &[u8]) -> Option<Vec<u8>> {
    LOG_FRAME.read(frame, frame.len() as u64).ok().flatten()
}

fn canonical(bytes: &[u8]) -> bool {
    let Some((operation, key, value)) = decode(bytes) else {
        return true;
    };
    if !matches!(operation, PUT | REMOVE) || value.end != bytes.len() {
        return false;
    }
    let Some(value) = bytes.get(value) else {
        return false;
    };
    if operation == REMOVE && !value.is_empty() {
        return false;
    }
    encode(operation, key, value)
        .ok()
        .and_then(|frame| body(&frame))
        .is_some_and(|written| written == bytes)
}

fn replay_matches(log: &[u8], expected: &[(&str, &[u8], usize, usize)]) -> bool {
    let Ok((end, index, live, whole)) = replay(log, log.len() as u64) else {
        return false;
    };
    end == log.len() as u64
        && whole
        && index.len() == expected.len()
        && live
            == expected
                .iter()
                .map(|(_, _, _, size)| *size as u64)
                .sum::<u64>()
        && expected.iter().all(|(key, value, start, size)| {
            let end = start + size - acyclic_log_frame::LogFrame::CHECKSUM_BYTES;
            index.get(*key).is_some_and(|entry| {
                let slot = entry.slot;
                // A held value is exactly the logged one.
                entry.value.as_deref().is_none_or(|held| held == *value)
                    && slot.frame == *start as u64
                    && slot.bytes == *size as u64
                    && slot.value == (end - value.len()) as u64
                    && slot.length == value.len() as u64
                    && log.get(end - value.len()..end) == Some(*value)
            })
        })
}

fn replay_sequence(key: &str, value: &[u8]) -> bool {
    let Ok(retained) = encode(PUT, "retained", b"other") else {
        return false;
    };
    let Ok(initial) = encode(PUT, key, b"prior") else {
        return false;
    };
    let Ok(replaced) = encode(PUT, key, value) else {
        return false;
    };
    let Ok(removed) = encode(REMOVE, key, &[]) else {
        return false;
    };
    let start = retained.len() + initial.len();
    let mut log = retained.clone();
    log.extend_from_slice(&initial);
    if !replay_matches(
        &log,
        &[
            ("retained", b"other", 0, retained.len()),
            (key, b"prior", retained.len(), initial.len()),
        ],
    ) {
        return false;
    }
    log.extend_from_slice(&replaced);
    if !replay_matches(
        &log,
        &[
            ("retained", b"other", 0, retained.len()),
            (key, value, start, replaced.len()),
        ],
    ) {
        return false;
    }
    log.extend_from_slice(&removed);
    replay_matches(&log, &[("retained", b"other", 0, retained.len())])
}

pub(crate) fn check(bytes: &[u8]) -> bool {
    // Reach payload parsing both directly and through the actual frame reader.
    if !canonical(bytes) || body(bytes).is_some_and(|body| !canonical(&body)) {
        return false;
    }
    let Some(prefix) = bytes.get(..bytes.len().min(32)) else {
        return false;
    };
    let key = format!("k{}", String::from_utf8_lossy(prefix));
    for (operation, value) in [(PUT, bytes), (REMOVE, &[][..])] {
        let Ok(frame) = encode(operation, &key, value) else {
            return false;
        };
        let Some(original) = body(&frame) else {
            return false;
        };
        // Independent operands make rejecting every payload a failing oracle.
        if !canonical(&original)
            || !decode(&original).is_some_and(|(decoded, name, range)| {
                decoded == operation && name == key && original.get(range) == Some(value)
            })
        {
            return false;
        }
        for end in [0, 1, 4] {
            if original
                .get(..end)
                .is_some_and(|prefix| decode(prefix).is_some())
            {
                return false;
            }
        }
        let mut damaged = original.clone();
        for operation in [0, u8::MAX] {
            let Some(first) = damaged.first_mut() else {
                return false;
            };
            *first = operation;
            if decode(&damaged).is_some() {
                return false;
            }
            // Preserve the real checksum so the payload reaches replay's error.
            let mut frame = Vec::new();
            if LOG_FRAME.encode(&[&damaged], &mut frame).is_err() {
                return false;
            }
            if replay(frame.as_slice(), frame.len() as u64)
                .err()
                .map(|error| error.kind())
                != Some(std::io::ErrorKind::InvalidData)
            {
                return false;
            }
        }
        let Some(first) = damaged.first_mut() else {
            return false;
        };
        *first = operation;
        // A non-UTF8 key must reach this parser, rather than fail its checksum.
        let Some(key_byte) = damaged.get_mut(5) else {
            return false;
        };
        *key_byte = u8::MAX;
        if decode(&damaged).is_some() {
            return false;
        }
        damaged = original.clone();
        let Some(body_length) = original.len().checked_sub(4) else {
            return false;
        };
        if let Ok(too_long) = u32::try_from(body_length) {
            let Some(length) = damaged.get_mut(1..5) else {
                return false;
            };
            length.copy_from_slice(&too_long.to_le_bytes());
            if decode(&damaged).is_some() {
                return false;
            }
        }
        if operation == REMOVE {
            damaged = original;
            damaged.push(0);
            if decode(&damaged).is_some() {
                return false;
            }
        }
    }
    replay_sequence(&key, bytes)
}

/// Prepare an obsolete value using the original local compaction chunk size.
pub(crate) fn compaction_chunk_bytes() -> Result<usize, Error> {
    usize::try_from(super::COMPACTION_CHUNK)
        .map_err(|source| Error::from(ErrorCode::Unavailable).with_source(source))
}

pub(crate) fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, Error> {
    [
        ("local-put-empty", PUT, "", &[][..]),
        (
            "local-put-utf8",
            PUT,
            "objects/\u{e9}",
            &b"value\0bytes"[..],
        ),
        ("local-remove", REMOVE, "objects/\u{e9}", &[][..]),
    ]
    .into_iter()
    .map(|(name, operation, key, value)| {
        let encoded = encode(operation, key, value)
            .map_err(|source| Error::from(ErrorCode::Unavailable).with_source(source))?;
        Ok((name, encoded))
    })
    .collect()
}

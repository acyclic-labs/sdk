//! Closed model-admission cells need no generic JSON diagnostic reservation.
use super::*;

// Current-attempt reasoning bytes plus the three abandoned-output counters.
const ATTEMPT_BYTES: usize = 32;

pub(super) fn encode(
    value: Option<&ModelEventAdmission>,
    io: Option<&PreparationIo>,
) -> Result<OwnedBytes> {
    let length = match value {
        None => 5,
        Some(value) => value
            .calls
            .iter()
            .try_fold(30 + ATTEMPT_BYTES, |length, call| {
                length
                    .checked_add(8)
                    .and_then(|length| length.checked_add(call.len()))
                    .ok_or_else(|| corrupt(io))
            })?,
    };
    crate::executor::with_history_io(io, |work| {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut bytes = crate::core::reserve_copy_vector(length, &mut quota)?;
        if let Some(work) = quota.work().as_deref_mut() {
            work.codec_charge(crate::executor::json::JsonCodecCharge::Work {
                bytes: length as u64,
                items: value.map_or(1, |value| value.calls.len() as u64 + 1),
            })?;
            work.copy_bytes(length as u64)?;
        }
        bytes.extend_from_slice(b"EXM3");
        match value {
            None => bytes.push(0),
            Some(value) => {
                bytes.push(1);
                bytes.extend_from_slice(&(value.count as u64).to_le_bytes());
                bytes.push(u8::from(value.completed));
                bytes.extend_from_slice(&value.text_bytes.to_le_bytes());
                bytes.extend_from_slice(&(value.calls.len() as u64).to_le_bytes());
                for call in &value.calls {
                    bytes.extend_from_slice(&(call.len() as u64).to_le_bytes());
                    bytes.extend_from_slice(call.as_bytes());
                }
                let abandoned = value.abandoned;
                for counter in [
                    value.reasoning_bytes,
                    abandoned.attempts,
                    abandoned.content_bytes,
                    abandoned.reasoning_bytes,
                ] {
                    bytes.extend_from_slice(&counter.to_le_bytes());
                }
            }
        }
        quota.transfer();
        crate::store::operations::owned_bytes_from_retained_vec(bytes, quota.work())
            .map(|(bytes, _)| bytes)
    })
}

fn take<'a>(bytes: &mut &'a [u8], length: usize, io: Option<&PreparationIo>) -> Result<&'a [u8]> {
    let (value, rest) = bytes.split_at_checked(length).ok_or_else(|| corrupt(io))?;
    *bytes = rest;
    Ok(value)
}

fn counter(bytes: &mut &[u8], io: Option<&PreparationIo>) -> Result<u64> {
    Ok(u64::from_le_bytes(
        take(bytes, 8, io)?.try_into().map_err(|_| corrupt(io))?,
    ))
}

fn number(bytes: &mut &[u8], io: Option<&PreparationIo>) -> Result<usize> {
    usize::try_from(counter(bytes, io)?).map_err(|_| corrupt(io))
}

pub(super) fn decode(
    mut bytes: &[u8],
    io: Option<&PreparationIo>,
) -> Result<Option<ModelEventAdmission>> {
    bytes = acyclic_auth::format::header(
        bytes,
        b"EXM",
        b"3",
        || corrupt(io),
        || {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "unsupported execution model cell version",
            )
        },
    )?;
    match take(&mut bytes, 1, io)? {
        [0] if bytes.is_empty() => return Ok(None),
        [1] => (),
        _ => return Err(corrupt(io)),
    }
    let count = number(&mut bytes, io)?;
    let completed = match take(&mut bytes, 1, io)? {
        [0] => false,
        [1] => true,
        _ => return Err(corrupt(io)),
    };
    let text_bytes = counter(&mut bytes, io)?;
    let length = number(&mut bytes, io)?;
    if length > bytes.len().saturating_sub(ATTEMPT_BYTES) / 8 {
        return Err(corrupt(io));
    }
    crate::executor::with_history_io(io, |work| {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut calls = crate::core::reserve_copy_vector(length, &mut quota)?;
        if let Some(work) = quota.work().as_deref_mut() {
            work.codec_charge(crate::executor::json::JsonCodecCharge::Inspect {
                items: length as u64 + 1,
            })?;
        }
        for _ in 0..length {
            let length = number(&mut bytes, io)?;
            let text =
                std::str::from_utf8(take(&mut bytes, length, io)?).map_err(|_| corrupt(io))?;
            calls.push(crate::core::copy_text(text, &mut quota)?);
        }
        let reasoning_bytes = counter(&mut bytes, io)?;
        let abandoned = crate::executor::ModelAbandonedOutput {
            attempts: counter(&mut bytes, io)?,
            content_bytes: counter(&mut bytes, io)?,
            reasoning_bytes: counter(&mut bytes, io)?,
        };
        if !bytes.is_empty() {
            return Err(corrupt(io));
        }
        quota.transfer();
        Ok(Some(ModelEventAdmission {
            count,
            calls,
            completed,
            text_bytes,
            reasoning_bytes,
            abandoned,
        }))
    })
}

#[cfg(feature = "test-support")]
pub(in crate::filesystem::execution_journal) mod codec_fuzz {
    use super::*;

    pub(in crate::filesystem::execution_journal) fn check(bytes: &[u8]) {
        if let Ok(value) = decode(bytes, None) {
            let encoded = encode(value.as_ref(), None).expect("admitted model cell encodes");
            assert_eq!(encoded.as_ref(), bytes);
            let restored = decode(&encoded, None).expect("writer output decodes");
            assert_eq!(encode(restored.as_ref(), None).unwrap(), encoded);
            for version in [b'0', b'2', b'4', u8::MAX] {
                let mut header = bytes[..4].to_vec();
                header[3] = version;
                assert_eq!(
                    decode(&header, None).err().unwrap().code(),
                    crate::wire::ErrorCode::Unsupported
                );
            }
            assert_ne!(
                decode(&bytes[..4], None).err().unwrap().code(),
                crate::wire::ErrorCode::Unsupported
            );
            let mut trailing = bytes.to_vec();
            trailing.push(0);
            assert!(decode(&trailing, None).is_err());
        }
    }

    pub(in crate::filesystem::execution_journal) fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>>
    {
        let mut seeds = Vec::new();
        for (name, value) in [
            ("execution-model-cell-absent", None),
            (
                "execution-model-cell-empty",
                Some(ModelEventAdmission::default()),
            ),
            (
                "execution-model-cell-populated",
                Some(ModelEventAdmission {
                    count: usize::MAX,
                    calls: vec!["".into(), "tool-\u{1f980}".into()],
                    completed: true,
                    text_bytes: u64::MAX,
                    reasoning_bytes: u64::MAX,
                    abandoned: crate::executor::ModelAbandonedOutput {
                        attempts: u64::MAX,
                        content_bytes: u64::MAX,
                        reasoning_bytes: u64::MAX,
                    },
                }),
            ),
        ] {
            let bytes = encode(value.as_ref(), None)?;
            decode(&bytes, None)?;
            check(&bytes);
            seeds.push((name, bytes.to_vec()));
        }
        Ok(seeds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_cells_reject_truncation_tags_lengths_utf8_and_trailing_bytes() -> Result<()> {
        let value = ModelEventAdmission {
            count: 7,
            calls: vec!["call".into()],
            completed: true,
            text_bytes: 37,
            reasoning_bytes: 5,
            abandoned: crate::executor::ModelAbandonedOutput {
                attempts: 2,
                content_bytes: 11,
                reasoning_bytes: 13,
            },
        };
        let encoded = encode(Some(&value), None)?;
        for version in [b'0', b'2', b'4', u8::MAX] {
            let mut header = encoded[..4].to_vec();
            header[3] = version;
            assert_eq!(
                decode(&header, None).err().map(|error| error.code()),
                Some(crate::wire::ErrorCode::Unsupported)
            );
        }
        let decoded = decode(&encoded, None)?.expect("model cell");
        assert_eq!(
            (
                decoded.count,
                decoded.completed,
                decoded.text_bytes,
                decoded.reasoning_bytes,
                decoded.abandoned
            ),
            (7, true, 37, 5, value.abandoned)
        );
        assert_eq!(decoded.calls, value.calls);
        for length in 0..encoded.len() {
            assert!(decode(&encoded[..length], None).is_err());
        }
        for (position, byte) in [(0, b'?'), (4, 2), (13, 2), (38, 255)] {
            let mut forged = encoded.to_vec();
            forged[position] = byte;
            assert!(decode(&forged, None).is_err());
        }
        let mut forged = encoded.to_vec();
        forged[22..30].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(decode(&forged, None).is_err());
        forged = encoded.to_vec();
        forged.push(0);
        assert!(decode(&forged, None).is_err());
        assert!(decode(&encode(None, None)?, None)?.is_none());
        Ok(())
    }

    #[test]
    fn recovered_model_cell_preserves_the_cumulative_text_limit() -> Result<()> {
        let state = ModelEventAdmission {
            count: 1,
            text_bytes: 3,
            ..Default::default()
        };
        let mut recovered = decode(&encode(Some(&state), None)?, None)?.expect("model cell");
        let limits = Limits {
            file_bytes: Some(4),
            ..Limits::default()
        };
        let event = ModelEvent::Content { delta: "ab".into() };
        assert!(
            recovered.observe(&event, limits, None).is_err(),
            "checkpoint reset cumulative model text admission"
        );
        let mut omitted = ModelEventAdmission {
            text_bytes: 0,
            ..state
        };
        assert!(
            omitted.observe(&event, limits, None).is_ok(),
            "omitted-counter negative control did not admit excess text"
        );
        Ok(())
    }
}

//! Closed model-admission cells need no generic JSON diagnostic reservation.
use super::*;

pub(super) fn encode(
    value: Option<&ModelEventAdmission>,
    io: Option<&PreparationIo>,
) -> Result<OwnedBytes> {
    let length = match value {
        None => 5,
        Some(value) => value.calls.iter().try_fold(30_usize, |length, call| {
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
        bytes.extend_from_slice(b"EXM2");
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

fn number(bytes: &mut &[u8], io: Option<&PreparationIo>) -> Result<usize> {
    let value = u64::from_le_bytes(take(bytes, 8, io)?.try_into().map_err(|_| corrupt(io))?);
    usize::try_from(value).map_err(|_| corrupt(io))
}

pub(super) fn decode(
    mut bytes: &[u8],
    io: Option<&PreparationIo>,
) -> Result<Option<ModelEventAdmission>> {
    if take(&mut bytes, 4, io)? != b"EXM2" {
        return Err(corrupt(io));
    }
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
    let text_bytes = u64::from_le_bytes(
        take(&mut bytes, 8, io)?
            .try_into()
            .map_err(|_| corrupt(io))?,
    );
    let length = number(&mut bytes, io)?;
    if length > bytes.len() / 8 {
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
        if !bytes.is_empty() {
            return Err(corrupt(io));
        }
        quota.transfer();
        Ok(Some(ModelEventAdmission {
            count,
            calls,
            completed,
            text_bytes,
        }))
    })
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
        };
        let encoded = encode(Some(&value), None)?;
        let decoded = decode(&encoded, None)?.expect("model cell");
        assert_eq!(
            (
                decoded.count,
                decoded.calls,
                decoded.completed,
                decoded.text_bytes
            ),
            (7, value.calls, true, 37)
        );
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
            file_bytes: 4,
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

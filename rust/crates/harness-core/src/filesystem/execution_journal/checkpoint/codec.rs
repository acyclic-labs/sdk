//! Fixed-width private checkpoint frames avoid decimal encoding of digests.
use super::*;

const STATE_BYTES: usize = 335;
const FRAME_BYTES: usize = STATE_BYTES + 64;

pub(super) fn state_bytes(
    state: &IndexState,
    io: Option<&PreparationIo>,
) -> Result<[u8; STATE_BYTES]> {
    if let Some(io) = io {
        io.charge(acyclic_fs::WorkCounters {
            bytes_encoded: STATE_BYTES as u64,
            items_examined: 1,
            ..Default::default()
        })?;
    }
    let mut bytes = [0; STATE_BYTES];
    let mut cursor = 0;
    let mut put = |value: &[u8]| -> Result<()> {
        bytes
            .get_mut(cursor..)
            .and_then(|bytes| bytes.get_mut(..value.len()))
            .ok_or_else(|| missing(io))?
            .copy_from_slice(value);
        cursor += value.len();
        Ok(())
    };
    put(b"EXC5")?;
    put(&state.operation.into_bytes())?;
    put(&state.turn.into_bytes())?;
    put(&state.volume)?;
    put(&state.tail.to_le_bytes())?;
    put(&state.nodes_tail.to_le_bytes())?;
    for root in state.roots {
        match root {
            Some(root) => {
                put(&[1])?;
                put(&root.sequence.to_le_bytes())?;
                put(&root.digest)?;
            }
            None => put(&[0; 41])?,
        }
    }
    put(&state.pending_dispatches.to_le_bytes())?;
    put(&state.anchor)?;
    match state.latest_response {
        Some(step) => {
            put(&[1])?;
            put(&step.to_le_bytes())?;
        }
        None => put(&[0; 5])?,
    }
    put(&[u8::from(state.model_dispatched)])?;
    Ok(bytes)
}

#[cfg(test)]
pub(super) fn encode(
    checkpoint: &Checkpoint,
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::OwnedBytes> {
    let state = state_bytes(&checkpoint.state, io)?;
    encode_state(&state, &checkpoint.proof, io)
}

pub(super) fn encode_state(
    state: &[u8; STATE_BYTES],
    proof: &[u8; 64],
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::OwnedBytes> {
    if let Some(io) = io {
        io.charge(acyclic_fs::WorkCounters {
            bytes_encoded: 64,
            bytes_copied: FRAME_BYTES as u64,
            ..Default::default()
        })?;
    }
    crate::executor::with_history_io(io, |work| {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut bytes = crate::core::reserve_copy_vector(FRAME_BYTES, &mut quota)?;
        bytes.extend_from_slice(state);
        bytes.extend_from_slice(proof);
        quota.transfer();
        crate::store::operations::owned_bytes_from_retained_vec(bytes, quota.work())
            .map(|value| value.0)
    })
}

pub(super) fn decode(bytes: &[u8], io: Option<&PreparationIo>) -> Result<Checkpoint> {
    if bytes.len() != FRAME_BYTES || bytes.get(..4) != Some(b"EXC5") {
        return Err(missing(io));
    }
    fn take<const N: usize>(input: &mut &[u8], io: Option<&PreparationIo>) -> Result<[u8; N]> {
        let (value, rest) = input.split_at_checked(N).ok_or_else(|| missing(io))?;
        let value = value.try_into().map_err(|_| missing(io))?;
        *input = rest;
        Ok(value)
    }
    if let Some(io) = io {
        io.charge(acyclic_fs::WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
    }
    let mut input = bytes.get(4..).ok_or_else(|| missing(io))?;
    let operation = OperationId::from_bytes(take(&mut input, io)?);
    let turn = OperationId::from_bytes(take(&mut input, io)?);
    let volume = take(&mut input, io)?;
    let tail = u64::from_le_bytes(take(&mut input, io)?);
    let nodes_tail = u64::from_le_bytes(take(&mut input, io)?);
    let mut roots = [None; 5];
    for root in &mut roots {
        let tag: [u8; 1] = take(&mut input, io)?;
        let sequence = u64::from_le_bytes(take(&mut input, io)?);
        let digest = take(&mut input, io)?;
        *root = match tag {
            [0] if sequence == 0 && digest == [0; 32] => None,
            [1] => Some(NodeRef { sequence, digest }),
            _ => return Err(missing(io)),
        };
    }
    let pending_dispatches = u64::from_le_bytes(take(&mut input, io)?);
    let anchor = take(&mut input, io)?;
    let tag: [u8; 1] = take(&mut input, io)?;
    let ordinal = u32::from_le_bytes(take(&mut input, io)?);
    let latest_response = match tag {
        [0] if ordinal == 0 => None,
        [1] => Some(ordinal),
        _ => return Err(missing(io)),
    };
    let model_dispatched = match take::<1>(&mut input, io)? {
        [0] if latest_response.is_none() => false,
        [1] => true,
        _ => return Err(missing(io)),
    };
    let proof = take(&mut input, io)?;
    Ok(Checkpoint {
        state: IndexState {
            operation,
            turn,
            volume,
            tail,
            nodes_tail,
            roots,
            pending_dispatches,
            anchor,
            latest_response,
            model_dispatched,
        },
        proof,
    })
}

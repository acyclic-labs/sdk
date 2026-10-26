use super::*;

pub(super) async fn stream_history(root: &Path, seed: u64) -> Result<(), String> {
    let provider = LocalStream::open(root, LocalStreamLimits::default())
        .await
        .map_err(|error| error.to_string())?;
    let client = Arc::new(provider);
    let stream = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), "consumer/history")
        .map_err(|error| error.to_string())?;
    let mut rng = seed | 1;
    let mut expected = Vec::<Bytes>::new();
    for step in 0..256 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let payload = Bytes::copy_from_slice(&rng.to_le_bytes());
        let tail = u64::try_from(expected.len()).map_err(|error| error.to_string())?;
        if step % 4 == 1 {
            let outcome = stream
                .append_at(payload, tail + 1, key(seed, step)?)
                .await
                .map_err(|error| error.to_string())?;
            if outcome != (AppendOutcome::TailConflict { actual_tail: tail }) {
                return Err(format!("step {step}: stale append changed the tail"));
            }
        } else if step % 4 != 2 {
            let outcome = stream
                .append_at(payload.clone(), tail, key(seed, step)?)
                .await
                .map_err(|error| error.to_string())?;
            let AppendOutcome::Committed(receipt) = outcome else {
                return Err(format!("step {step}: exact-tail append conflicted"));
            };
            if receipt.start != tail || receipt.end != tail + 1 {
                return Err(format!("step {step}: append receipt has wrong range"));
            }
            expected.push(payload);
        }
        let tail = u64::try_from(expected.len()).map_err(|error| error.to_string())?;
        if stream.tail().await.map_err(|error| error.to_string())? != tail {
            return Err(format!("step {step}: tail differs from model"));
        }
        let from = if expected.is_empty() {
            0
        } else {
            rng % (tail + 1)
        };
        let observed = stream
            .read(from, 8)
            .await
            .map_err(|error| error.to_string())?
            .collect::<Vec<_>>()
            .await;
        for (index, record) in observed.iter().enumerate() {
            let record = record.as_ref().map_err(|error| error.to_string())?;
            let offset = usize::try_from(from).map_err(|error| error.to_string())? + index;
            if record.sequence != from + index as u64
                || expected.get(offset).map(Bytes::as_ref) != Some(record.value.as_ref())
            {
                return Err(format!("step {step}: read differs from model at {offset}"));
            }
        }
        let remaining =
            expected.len() - usize::try_from(from).map_err(|error| error.to_string())?;
        if observed.len() != remaining.min(8) {
            return Err(format!("step {step}: read length differs from model"));
        }
    }
    drop(stream);
    drop(client);
    let reopened = LocalStream::open(root, LocalStreamLimits::default())
        .await
        .map_err(|error| error.to_string())?;
    let client = Arc::new(reopened);
    let stream = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), "consumer/history")
        .map_err(|error| error.to_string())?;
    let recovered = stream
        .read(
            0,
            u32::try_from(expected.len()).map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| error.to_string())?
        .collect::<Vec<_>>()
        .await;
    if recovered.len() != expected.len()
        || recovered.iter().zip(&expected).any(|(record, value)| {
            !record
                .as_ref()
                .is_ok_and(|record| record.value.as_ref() == value.as_ref())
        })
    {
        return Err("reopened stream differs from model".into());
    }
    Ok(())
}

fn key(seed: u64, step: usize) -> Result<IdempotencyKey, String> {
    IdempotencyKey::new(format!("history/{seed}/{step}").into_bytes())
        .map_err(|error| error.to_string())
}

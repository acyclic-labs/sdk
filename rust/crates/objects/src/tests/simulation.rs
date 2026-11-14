//! Seeded operation histories over the shipping Objects implementation.
use super::*;
use acyclic_native_runtime::testing::{Schedule, fill_seeded, seeded_runtime};

type Failure = Box<dyn std::error::Error>;

async fn history() -> Result<(), Failure> {
    let store = Store::new(MemoryMedium::default());
    let blobs = store.account(b"simulation")?;
    let mut known: Vec<(Vec<u8>, Vec<Digest>)> = Vec::new();
    for step in 0..32u8 {
        let mut choices = [0; 8];
        assert!(fill_seeded(&mut choices));
        let body = vec![step; usize::from(choices[0] % 31) + 1];
        let duplicate = known
            .get(usize::from(choices[1]) % known.len().max(1))
            .cloned()
            .unwrap_or_else(|| (body.clone(), Vec::new()));
        let digest = Digest::of(&body);
        let child = vec![b'c', b'h', b'i', b'l', b'd', step];
        // Repeated digests and an earlier-member reference exercise dependency
        // ordering in the real batch driver, alongside independent admissions.
        let inputs = [
            (body.clone(), Vec::new()),
            (child.clone(), vec![digest]),
            duplicate,
            (body.clone(), Vec::new()),
        ];
        let puts = blobs
            .put_batch(
                inputs
                    .iter()
                    .map(|(body, references)| (body.clone().into(), references.clone()))
                    .collect(),
            )
            .await;
        assert_eq!(puts.len(), inputs.len());
        for (put, (input, _)) in puts.into_iter().zip(&inputs) {
            assert_eq!(
                put?.verified().ok_or("unverified put")?.digest,
                Digest::of(input)
            );
        }
        known.push((body, Vec::new()));
        known.push((child, vec![digest]));
        // Verify the entire acknowledged prefix after every operation, with
        // independently computed ranges and deliberately permuted read order.
        let mut expected = Vec::new();
        let mut requests = Vec::new();
        for index in 0..known.len() {
            let (input, _) = &known[(index + usize::from(choices[2])) % known.len()];
            let start = usize::from(choices[3]) % input.len();
            let end = start + 1 + usize::from(choices[4]) % (input.len() - start);
            requests.push((
                Digest::of(input),
                ByteRange::new(start as u64, end as u64),
                64,
            ));
            expected.push(input[start..end].to_vec());
        }
        let reads = blobs.get_batch(requests).await;
        assert_eq!(reads.len(), expected.len());
        for (read, bytes) in reads.into_iter().zip(expected) {
            assert_eq!(read?.body().as_ref(), bytes);
        }
        let invalid = blobs.get(digest, ByteRange::new(1, 1), 64).await;
        assert_eq!(
            invalid.err().ok_or("empty range accepted")?.code,
            ErrorCode::RangeNotSatisfiable
        );
    }
    Ok(())
}

fn replay(seed: u64) -> Result<(), Failure> {
    eprintln!("seed={seed}");
    seeded_runtime(seed)?.block_on(async {
        tokio::time::pause();
        let (schedule, mut boundaries) = Schedule::seeded(seed);
        let turn = schedule.scope(history());
        tokio::pin!(turn);
        loop {
            tokio::select! {
                result = &mut turn => return result,
                boundary = boundaries.recv() => boundary.ok_or("boundary channel closed")?.release(),
            }
        }
    })
}

#[test]
fn seeded_puts_dependencies_and_ranges() -> Result<(), Failure> {
    for seed in [0, 1, 7, 42] {
        replay(seed).map_err(|error| format!("seed={seed}: {error}"))?;
    }
    Ok(())
}

#[test]
#[ignore = "deep seeded sweep; run on an idle worker"]
fn deep_puts_dependencies_and_ranges() -> Result<(), Failure> {
    for seed in 0..4096 {
        replay(seed).map_err(|error| format!("seed={seed}: {error}"))?;
    }
    Ok(())
}

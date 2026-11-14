//! Seeded fault histories reuse the real Store and its tuple oracle.
use super::*;
use acyclic_native_runtime::testing::{Schedule, fill_seeded, seeded_runtime};

fn replay(seed: u64) -> Result<(), Failure> {
    seeded_runtime(seed)?.block_on(async {
        let (schedule, mut boundaries) = Schedule::seeded(seed);
        let turn = schedule.scope(async {
            let mut choices = [0; 8];
            assert!(fill_seeded(&mut choices));
            let plan = choices
                .into_iter()
                .map(|choice| match choice % 3 {
                    0 => Fault::None,
                    1 => Fault::Drop,
                    _ => Fault::LoseAck,
                })
                .collect();
            run(Shape::Atomic, plan).await?;
            composition::activation_extensions(choices.into_iter().map(|choice| 3 + choice % 3)).await
        });
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
fn seeded_delivery_alarm_and_composed_effects() -> Result<(), Failure> {
    for seed in [0, 1, 7, 42] {
        eprintln!("seed={seed}");
        replay(seed).map_err(|error| format!("seed={seed}: {error}"))?;
    }
    Ok(())
}

#[test]
#[ignore = "deep seeded sweep; run on an idle worker"]
fn deep_delivery_alarm_and_composed_effects() -> Result<(), Failure> {
    for seed in 0..4096 {
        replay(seed).map_err(|error| format!("seed={seed}: {error}"))?;
    }
    Ok(())
}

//! Seeded fault histories reuse the real Store and its tuple oracle.
use super::*;
use acyclic_native_runtime::testing::{
    Schedule, fill_seeded, seeded_runtime,
    sweep::{seeds, sweep},
};

mod custody;

fn replay(seed: u64) -> Result<(), Failure> {
    seeded_runtime(seed)?.block_on(async {
        tokio::time::pause();
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
            run(Shape::Atomic, plan).await
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

/// `DST_SEEDS=<a>..<b>` runs a deep sweep; `DST_SEED=<n>` replays one seed.
#[test]
fn seeded_delivery_alarm_and_composed_effects() -> Result<(), Failure> {
    sweep("actors-store", seeds(0..4), |seed| {
        replay(seed).map_err(|error| error.to_string())
    })?;
    Ok(())
}

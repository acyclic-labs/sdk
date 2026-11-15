//! A stream fork copies state bytes but never the source's settlement duty.
use super::*;
use acyclic_native_runtime::testing::{
    Schedule, fill_seeded, seeded_runtime,
    sweep::{seeds, sweep},
};

#[test]
fn seeded_forks_preserve_custody_ownership() -> Result<(), Failure> {
    sweep("actors-custody", seeds(0..4), |seed| {
        seeded_runtime(seed).map_err(|error| error.to_string())?.block_on(async {
            tokio::time::pause();
            let (schedule, mut boundaries) = Schedule::seeded(seed);
            let turn = schedule.scope(async {
                let mut choices = [0; 2];
                assert!(fill_seeded(&mut choices));
                composition::copied_custody(choices.into_iter().map(|choice| choice % 2 == 0)).await
            });
            tokio::pin!(turn);
            loop {
                tokio::select! {
                    result = &mut turn => return result.map_err(|error| error.to_string()),
                    boundary = boundaries.recv() => boundary.ok_or("boundary channel closed")?.release(),
                }
            }
        })
    })?;
    Ok(())
}

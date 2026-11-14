//! A stream fork copies state bytes but never the source's settlement duty.
use super::*;
use acyclic_native_runtime::testing::{Schedule, fill_seeded, seeded_runtime};

#[test]
fn seeded_forks_preserve_custody_ownership() -> Result<(), Failure> {
    for seed in [0, 1, 7, 42] {
        eprintln!("seed={seed}");
        seeded_runtime(seed)?.block_on(async {
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
                    result = &mut turn => return result,
                    boundary = boundaries.recv() => boundary.ok_or("boundary channel closed")?.release(),
                }
            }
        })?;
    }
    Ok(())
}

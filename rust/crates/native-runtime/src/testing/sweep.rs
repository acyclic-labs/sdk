//! Seed sweeps over independent deterministic worlds, and failure shrinking.
use std::{fmt, ops::Range, time::Duration};

/// The seeds a sweep runs. `DST_SEED=<n>` reproduces exactly one seed and
/// `DST_SEEDS=<start>..<end>` replaces `default` for a deeper sweep; otherwise
/// `default`.
///
/// # Errors
///
/// Either variable set but malformed, selecting no seed, or naming
/// `u64::MAX`, so a typo never silently runs the wrong range or none.
pub fn seeds(default: Range<u64>) -> Result<Range<u64>, String> {
    let parse = |name: &str, value: &str| -> Result<u64, String> {
        value
            .parse()
            .map_err(|_| format!("{name}: {value:?} is not a u64"))
    };
    if let Ok(seed) = std::env::var("DST_SEED") {
        let seed = parse("DST_SEED", &seed)?;
        let end = seed
            .checked_add(1)
            .ok_or("DST_SEED must be below u64::MAX")?;
        return Ok(seed..end);
    }
    let Ok(range) = std::env::var("DST_SEEDS") else {
        return Ok(default);
    };
    let (start, end) = range
        .split_once("..")
        .ok_or_else(|| format!("DST_SEEDS={range:?} is not <start>..<end>"))?;
    let seeds = parse("DST_SEEDS start", start)?..parse("DST_SEEDS end", end)?;
    // An empty range would pass without running a single world.
    if seeds.is_empty() {
        return Err(format!("DST_SEEDS={range} selects no seed"));
    }
    Ok(seeds)
}

/// The first failing seed of a sweep and the world's own minimal trace.
#[derive(Clone, Eq, PartialEq)]
pub struct Failure {
    /// Sweep name, as the test reports it.
    pub sweep: &'static str,
    /// Seed whose world failed.
    pub seed: u64,
    /// The world's report: the broken invariant and its (minimized) trace.
    pub report: String,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DST {} failed at seed {}; reproduce with DST_SEED={}\n{}",
            self.sweep, self.seed, self.seed, self.report
        )
    }
}

// A test returning `Err` prints `Debug`; it must still name the seed and the
// `DST_SEED` reproduction rather than a struct dump.
impl fmt::Debug for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl std::error::Error for Failure {}

/// A passing sweep's extent and wall time.
#[derive(Clone, Copy, Debug)]
pub struct Swept {
    /// Seeds run.
    pub seeds: u64,
    /// Wall time of all of them, including each world's setup.
    pub elapsed: Duration,
}

impl Swept {
    /// Wall time per thousand seeds, the unit sweep budgets are stated in.
    #[must_use]
    pub fn per_thousand(&self) -> Duration {
        self.elapsed
            .checked_div(u32::try_from(self.seeds.max(1)).unwrap_or(u32::MAX))
            .unwrap_or_default()
            * 1000
    }
}

/// Runs `world` once per seed, in order, and stops at the first failure: an
/// `Err` report or a panic. Each world builds its own runtime and state from
/// its seed alone, so a failure reproduces with [`seeds`]' `DST_SEED`.
///
/// # Errors
///
/// The first failing seed and its report.
pub fn sweep(
    name: &'static str,
    seeds: Range<u64>,
    mut world: impl FnMut(u64) -> Result<(), String>,
) -> Result<Swept, Failure> {
    let started = std::time::Instant::now();
    let count = seeds.end.saturating_sub(seeds.start);
    for seed in seeds {
        if let Err(report) = run(seed, &mut world) {
            return Err(Failure {
                sweep: name,
                seed,
                report,
            });
        }
    }
    Ok(Swept {
        seeds: count,
        elapsed: started.elapsed(),
    })
}

/// [`sweep`] over worlds that share nothing: seeds run side by side on the
/// host's available parallelism, and each seed's value returns in seed order.
/// The lowest failing seed is reported, so the result never depends on thread
/// timing.
///
/// # Errors
///
/// The lowest failing seed and its report.
pub fn sweep_parallel<T: Send>(
    name: &'static str,
    seeds: Range<u64>,
    world: impl Fn(u64) -> Result<T, String> + Sync,
) -> Result<(Swept, Vec<T>), Failure> {
    use std::sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    };
    let started = std::time::Instant::now();
    let count = seeds.end.saturating_sub(seeds.start);
    let next = AtomicU64::new(seeds.start);
    // Seeds are taken in order, so every seed below a failure still runs and
    // seeds above it cannot change the reported one.
    let failed_at = AtomicU64::new(u64::MAX);
    let outcomes = Mutex::new(Vec::new());
    let workers = std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .min(usize::try_from(count).unwrap_or(usize::MAX));
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    // Bounded: the counter stops at the range's end, so it
                    // never wraps past `u64::MAX` into unrequested seeds.
                    let Ok(seed) =
                        next.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |seed| {
                            (seed < seeds.end).then(|| seed + 1)
                        })
                    else {
                        break;
                    };
                    if seed > failed_at.load(Ordering::Relaxed) {
                        break;
                    }
                    let outcome = run(seed, &world);
                    if outcome.is_err() {
                        failed_at.fetch_min(seed, Ordering::Relaxed);
                    }
                    outcomes
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push((seed, outcome));
                }
            });
        }
    });
    let mut outcomes = outcomes
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    outcomes.sort_unstable_by_key(|(seed, _)| *seed);
    let mut values = Vec::with_capacity(outcomes.len());
    for (seed, outcome) in outcomes {
        values.push(outcome.map_err(|report| Failure {
            sweep: name,
            seed,
            report,
        })?);
    }
    Ok((
        Swept {
            seeds: count,
            elapsed: started.elapsed(),
        },
        values,
    ))
}

/// One seed's world, with a panic reported as its message.
fn run<T>(seed: u64, world: impl FnOnce(u64) -> Result<T, String>) -> Result<T, String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| world(seed))).unwrap_or_else(|panic| {
        Err(panic
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| panic.downcast_ref::<&str>().map(|text| (*text).to_owned()))
            .unwrap_or_else(|| "non-string panic".into()))
    })
}

/// Shrinks a failing choice list (scheduled actions, injected faults) to a
/// 1-minimal one: removing any single remaining choice makes `fails` false.
/// Delta debugging over complements; `fails(&choices)` must hold on entry.
pub fn minimize<T: Clone>(mut choices: Vec<T>, mut fails: impl FnMut(&[T]) -> bool) -> Vec<T> {
    let mut parts = 2;
    while !choices.is_empty() {
        let chunk = choices.len().div_ceil(parts);
        let reduced = (0..choices.len()).step_by(chunk).find_map(|start| {
            let end = (start + chunk).min(choices.len());
            let candidate: Vec<T> = choices
                .iter()
                .enumerate()
                .filter(|(index, _)| !(start..end).contains(index))
                .map(|(_, choice)| choice.clone())
                .collect();
            fails(&candidate).then_some(candidate)
        });
        match reduced {
            Some(candidate) => {
                choices = candidate;
                parts = parts.saturating_sub(1).max(2);
            }
            None if chunk == 1 => break,
            None => parts = (parts * 2).min(choices.len()),
        }
    }
    choices
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(64))]
        // The oracle fails exactly when a hidden subset is present, so every
        // 1-minimal answer holds exactly one copy of each needed value.
        #[test]
        fn minimize_finds_the_exact_failing_subset(
            choices in proptest::collection::vec(0u8..32, 0..48),
            needed in proptest::collection::btree_set(0u8..32, 0..4),
        ) {
            let fails = |candidate: &[u8]| needed.iter().all(|value| candidate.contains(value));
            proptest::prop_assume!(fails(&choices));
            let minimal = minimize(choices, fails);
            proptest::prop_assert!(fails(&minimal));
            proptest::prop_assert_eq!(minimal.len(), needed.len());
        }
    }

    #[test]
    fn sweep_reports_the_first_failing_seed_and_its_panic() {
        let failure = sweep("example", 3..9, |seed| {
            assert!(seed != 5, "invariant broken at {seed}");
            Ok(())
        })
        .expect_err("seed 5 fails");
        assert_eq!(
            (failure.seed, failure.report.as_str()),
            (5, "invariant broken at 5")
        );
        assert!(format!("{failure:?}").contains("DST_SEED=5"));
    }

    #[test]
    fn parallel_sweep_reports_the_lowest_failing_seed_and_orders_values() {
        let failure = sweep_parallel("example", 3..64, |seed| {
            assert!(seed % 7 != 5, "invariant broken at {seed}");
            Ok(seed)
        })
        .expect_err("seed 5 fails");
        assert_eq!(
            (failure.seed, failure.report.as_str()),
            (5, "invariant broken at 5")
        );
        let (swept, values) = sweep_parallel("example", 3..64, Ok).expect("every seed passes");
        assert_eq!((swept.seeds, values), (61, (3..64).collect::<Vec<_>>()));
        // A range ending at u64::MAX runs exactly its seeds and no others.
        let (_, values) = sweep_parallel("example", u64::MAX - 2..u64::MAX, Ok).expect("passes");
        assert_eq!(values, [u64::MAX - 2, u64::MAX - 1]);
    }
}

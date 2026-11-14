//! Seed sweeps over independent deterministic worlds, and failure shrinking.
use std::{fmt, ops::Range, time::Duration};

/// The seeds a sweep runs. `DST_SEED=<n>` reproduces exactly one seed and
/// `DST_SEEDS=<start>..<end>` replaces `default` for a deeper sweep; otherwise
/// `default`.
///
/// # Errors
///
/// Returns an invalid-input error when an override is malformed, selects no
/// seed, or cannot be represented as a half-open range.
pub fn seeds(default: Range<u64>) -> std::io::Result<Range<u64>> {
    fn variable(name: &str) -> std::io::Result<Option<String>> {
        match std::env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(error) => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, error)),
        }
    }
    let seed = variable("DST_SEED")?;
    let range = if seed.is_none() {
        variable("DST_SEEDS")?
    } else {
        None
    };
    parse_seeds(default, seed, range)
}

fn parse_seeds(
    default: Range<u64>,
    seed: Option<String>,
    range: Option<String>,
) -> std::io::Result<Range<u64>> {
    let invalid = |message| std::io::Error::new(std::io::ErrorKind::InvalidInput, message);
    let parse = |value: &str| {
        value
            .parse::<u64>()
            .map_err(|error| invalid(error.to_string()))
    };
    let selected = if let Some(seed) = seed {
        let seed = parse(&seed)?;
        seed..seed
            .checked_add(1)
            .ok_or_else(|| invalid("DST_SEED must be below u64::MAX".into()))?
    } else if let Some(range) = range {
        let (start, end) = range
            .split_once("..")
            .ok_or_else(|| invalid("DST_SEEDS must be <start>..<end>".into()))?;
        parse(start)?..parse(end)?
    } else {
        default
    };
    if selected.is_empty() {
        return Err(invalid("seed range must not be empty".into()));
    }
    Ok(selected)
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
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| world(seed)));
        let report = match outcome {
            Ok(Ok(())) => continue,
            Ok(Err(report)) => report,
            Err(panic) => panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|text| (*text).to_owned()))
                .unwrap_or_else(|| "non-string panic".into()),
        };
        return Err(Failure {
            sweep: name,
            seed,
            report,
        });
    }
    Ok(Swept {
        seeds: count,
        elapsed: started.elapsed(),
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
                .filter(|(index, _)| *index < start || *index >= end)
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

    #[test]
    fn seed_overrides_reject_empty_and_unrepresentable_sweeps() -> std::io::Result<()> {
        assert_eq!(parse_seeds(1..4, None, None)?, 1..4);
        assert_eq!(
            parse_seeds(1..4, Some("9".into()), Some("bad".into()))?,
            9..10
        );
        assert_eq!(parse_seeds(1..4, None, Some("3..7".into()))?, 3..7);
        for seed in ["bad", "18446744073709551615"] {
            assert_eq!(
                parse_seeds(1..4, Some(seed.into()), None)
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::InvalidInput
            );
        }
        for range in ["bad", "3..3", "7..3", "x..9", "1..x"] {
            assert_eq!(
                parse_seeds(1..4, None, Some(range.into()))
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::InvalidInput
            );
        }
        assert!(parse_seeds(0..0, None, None).is_err());
        Ok(())
    }

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
}

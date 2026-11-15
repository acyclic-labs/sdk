//! A class family: the generator whose worlds exercise one class over
//! production code, and the one sweep every family test runs them with. The
//! coverage gate counts the variants a generator declares; the family's own
//! test proves its default seeds' worlds pass and reach exactly those.
use acyclic_native_runtime::testing::sweep;
use std::collections::{BTreeMap, BTreeSet};

/// One variant: a value for every dimension of its class.
pub(super) type Variant = BTreeMap<&'static str, &'static str>;

pub(super) struct Generator {
    pub class: &'static str,
    pub seeds: std::ops::Range<u64>,
    /// The variants the seed's world exercises when it reaches its fault.
    pub variants: fn(u64) -> Vec<Variant>,
    /// The seed's world, asserting the class invariant: `Ok(true)` when it
    /// reached its variants, `Ok(false)` when it passed without reaching
    /// them (its fault fell outside the window under test).
    pub world: fn(u64) -> std::result::Result<bool, String>,
}

impl Generator {
    /// The variants the default seeds exercise.
    pub fn declared(&self) -> BTreeSet<Variant> {
        self.seeds.clone().flat_map(self.variants).collect()
    }

    /// Runs every seed's world and holds the variants they reached to the
    /// declared ones. `DST_SEED` or `DST_SEEDS` choose the seeds and only
    /// report what they reached.
    pub fn sweep(&self) {
        let class = self.class;
        let seeds =
            sweep::seeds(self.seeds.clone()).unwrap_or_else(|error| panic!("{class}: {error}"));
        let (swept, worlds) = sweep::sweep_parallel(class, seeds, |seed| {
            let started = std::time::Instant::now();
            let reached = (self.world)(seed)?;
            Ok((started.elapsed(), seed, reached))
        })
        .unwrap_or_else(|failure| panic!("{failure}"));
        let covered: BTreeSet<Variant> = worlds
            .iter()
            .filter(|(_, _, reached)| *reached)
            .flat_map(|(_, seed, _)| (self.variants)(*seed))
            .collect();
        let slowest = worlds
            .iter()
            .map(|(elapsed, seed, _)| (*elapsed, *seed))
            .max();
        eprintln!(
            "DST family {class}: {} worlds executed, {} reached their fault, {} variants covered, in {:?}; slowest (time, seed) {slowest:?}",
            worlds.len(),
            worlds.iter().filter(|(_, _, reached)| *reached).count(),
            covered.len(),
            swept.elapsed,
        );
        if std::env::var_os("DST_SEED").is_some() || std::env::var_os("DST_SEEDS").is_some() {
            return;
        }
        let declared = self.declared();
        assert_eq!(
            covered,
            declared,
            "{class}: the default seeds reached {} of the {} variants the coverage gate counts as covered",
            covered.len(),
            declared.len()
        );
    }
}

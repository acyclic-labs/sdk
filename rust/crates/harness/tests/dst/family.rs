//! A class family: the generator whose worlds exercise one class over
//! production code. Each default seed's world is its own test, from the
//! family's `cases!` list, and must reach its fault, so together they reach
//! every variant the coverage gate counts as covered. `DST_SEEDS` deep
//! sweeps run through the one sweep test here.
use acyclic_native_runtime::testing::sweep;
use std::collections::{BTreeMap, BTreeSet};

/// One variant: a value for every dimension of its class.
pub(super) type Variant = BTreeMap<&'static str, &'static str>;

pub(super) struct Generator {
    pub class: &'static str,
    /// The default seeds, each its own test.
    pub seeds: &'static [u64],
    /// The variants the seed's world exercises when it reaches its fault.
    pub variants: fn(u64) -> Vec<Variant>,
    /// The seed's world, asserting the class invariant: `Ok(true)` when it
    /// reached its variants, `Ok(false)` when it passed without reaching
    /// them (its fault fell outside the window under test).
    pub world: fn(u64) -> std::result::Result<bool, String>,
}

/// Every class generator.
pub(super) const GENERATORS: &[&Generator] = &[
    &super::settlement::GENERATOR,
    &super::compaction::GENERATOR,
    &super::redelivery::GENERATOR,
    &super::containment::GENERATOR,
    &super::grants::GENERATOR,
    &super::protective::GENERATOR,
    &super::publication::GENERATOR,
    &super::refusal::GENERATOR,
    &super::configuration::GENERATOR,
    &super::records::GENERATOR,
    &super::determinism::GENERATOR,
    &super::observability::GENERATOR,
];

impl Generator {
    /// The variants the default seeds exercise.
    pub fn declared(&self) -> BTreeSet<Variant> {
        self.seeds.iter().copied().flat_map(self.variants).collect()
    }

    /// One default seed's world: it holds the invariant and reaches the
    /// variants it declares.
    pub fn run(&self, seed: u64) -> std::result::Result<(), String> {
        let class = self.class;
        let started = std::time::Instant::now();
        let reached = (self.world)(seed)?;
        eprintln!(
            "DST {class} seed {seed}: reached {reached} in {:?}",
            started.elapsed()
        );
        if reached {
            Ok(())
        } else {
            Err(format!(
                "{class} seed {seed} passed without reaching its fault, so its {} declared variants are not covered",
                (self.variants)(seed).len()
            ))
        }
    }
}

/// Sweeps `DST_SEEDS` (or `DST_SEED`) through every generator, or the one
/// `DST_CLASS` names, reporting the variants they reach. The default seeds
/// are the family tests, so with neither variable set nothing is swept.
#[test]
fn deep_sweep() {
    if std::env::var_os("DST_SEED").is_none() && std::env::var_os("DST_SEEDS").is_none() {
        return;
    }
    let class = std::env::var("DST_CLASS").ok();
    let selected: Vec<&Generator> = GENERATORS
        .iter()
        .copied()
        .filter(|generator| {
            class
                .as_deref()
                .is_none_or(|class| class == generator.class)
        })
        .collect();
    assert!(
        !selected.is_empty(),
        "DST_CLASS={class:?} names no generator"
    );
    for generator in selected {
        let class = generator.class;
        let seeds = sweep::seeds(0..0).unwrap_or_else(|error| panic!("{class}: {error}"));
        let (swept, worlds) = sweep::sweep_parallel(class, seeds, |seed| {
            let reached = (generator.world)(seed)?;
            Ok((seed, reached))
        })
        .unwrap_or_else(|failure| panic!("{failure}"));
        let covered: BTreeSet<Variant> = worlds
            .iter()
            .filter(|(_, reached)| *reached)
            .flat_map(|(seed, _)| (generator.variants)(*seed))
            .collect();
        eprintln!(
            "DST sweep {class}: {} worlds executed, {} reached their fault, {} variants covered, in {:?}",
            worlds.len(),
            worlds.iter().filter(|(_, reached)| *reached).count(),
            covered.len(),
            swept.elapsed,
        );
    }
}

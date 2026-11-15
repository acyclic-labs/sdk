//! The class coverage gate. `classes.json` names every class of failure the
//! simulator must rule out, each with the dimensions whose product is its
//! variants. A class is exercised by one generator over production code; the
//! gate runs every generator at its default seeds, records the variants their
//! worlds actually reached, and holds the number still uncovered (classes
//! without a generator, plus unreached variants of generated classes) exactly
//! at the recorded `uncovered`: it may only fall, and the file falls with it.
use super::*;
use acyclic_native_runtime::testing::sweep;
use std::collections::{BTreeMap, BTreeSet};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Classes {
    version: u32,
    uncovered: u64,
    classes: Vec<Class>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Class {
    id: String,
    name: String,
    family: String,
    invariant: String,
    dimensions: BTreeMap<String, Vec<String>>,
}

impl Class {
    fn variants(&self) -> u64 {
        self.dimensions
            .values()
            .map(|values| values.len() as u64)
            .product()
    }
}

/// One variant: a value for every dimension of its class.
type Variant = BTreeMap<String, String>;

/// The variants generator worlds reached.
#[derive(Default)]
pub(super) struct Coverage(BTreeSet<(String, Variant)>);

impl Coverage {
    /// Records that a passing world exercised `class` at this variant.
    pub fn cover(&mut self, class: &str, variant: &[(&str, &str)]) {
        self.0.insert((
            class.into(),
            variant
                .iter()
                .map(|(dimension, value)| ((*dimension).into(), (*value).into()))
                .collect(),
        ));
    }
}

/// A class's generator: one world per seed, asserting the class invariant.
struct Generator {
    class: &'static str,
    seeds: std::ops::Range<u64>,
    world: fn(u64, &mut Coverage) -> std::result::Result<(), String>,
}

const GENERATORS: &[Generator] = &[
    Generator {
        class: "A1",
        seeds: settlement::SEEDS,
        world: settlement::world,
    },
    Generator {
        class: "E2",
        seeds: redelivery::SEEDS,
        world: redelivery::world,
    },
    Generator {
        class: "F1",
        seeds: containment::SEEDS,
        world: containment::world,
    },
];

fn classes() -> Classes {
    let classes: Classes =
        serde_json::from_str(include_str!("classes.json")).expect("classes.json decodes");
    assert_eq!(classes.version, 1, "classes.json version");
    let mut ids = BTreeSet::new();
    for class in &classes.classes {
        assert!(ids.insert(&class.id), "class {} listed twice", class.id);
        assert!(
            !class.name.is_empty() && !class.family.is_empty() && !class.invariant.is_empty(),
            "class {} lacks a name, family or invariant",
            class.id
        );
        for (dimension, values) in &class.dimensions {
            let distinct: BTreeSet<_> = values.iter().collect();
            assert!(
                !values.is_empty() && distinct.len() == values.len(),
                "class {} dimension {dimension} is empty or repeats a value",
                class.id
            );
        }
    }
    classes
}

#[test]
fn every_class_variant_is_generated_or_counted_uncovered() {
    let started = std::time::Instant::now();
    let classes = classes();
    let listed: BTreeMap<_, _> = classes
        .classes
        .iter()
        .map(|class| (class.id.as_str(), class))
        .collect();
    for generator in GENERATORS {
        assert!(
            listed.contains_key(generator.class),
            "generator for unlisted class {}",
            generator.class
        );
    }
    // Generators are independent worlds; run them side by side.
    let swept: Vec<_> = std::thread::scope(|scope| {
        let workers: Vec<_> = GENERATORS
            .iter()
            .map(|generator| {
                scope.spawn(move || {
                    let seeds = sweep::seeds(generator.seeds.clone())
                        .unwrap_or_else(|error| panic!("{}: {error}", generator.class));
                    let swept = sweep::sweep_parallel(generator.class, seeds, |seed| {
                        let started = std::time::Instant::now();
                        let mut coverage = Coverage::default();
                        (generator.world)(seed, &mut coverage)?;
                        Ok((started.elapsed(), seed, coverage))
                    });
                    (generator.class, swept)
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().expect("generator thread"))
            .collect()
    });
    let mut covered = BTreeMap::<&str, BTreeSet<Variant>>::new();
    // Every generator's failure, so one run reproduces each broken class.
    let failures: Vec<_> = swept
        .iter()
        .filter_map(|(_, swept)| swept.as_ref().err().map(ToString::to_string))
        .collect();
    assert!(
        failures.is_empty(),
        "{}",
        failures.join(
            "
"
        )
    );
    for (class, swept) in swept {
        let (swept, seeds) = swept.expect("checked above");
        let slowest = seeds
            .iter()
            .map(|(elapsed, seed, _)| (*elapsed, *seed))
            .max();
        eprintln!(
            "DST class {class}: {} seeds in {:?}; slowest (time, seed) {slowest:?}",
            swept.seeds, swept.elapsed
        );
        for (recorded, variant) in seeds.into_iter().flat_map(|(_, _, coverage)| coverage.0) {
            let listing = listed
                .get(recorded.as_str())
                .unwrap_or_else(|| panic!("{class} covered unlisted class {recorded}"));
            let dimensions: BTreeSet<_> = variant.keys().collect();
            assert_eq!(
                dimensions,
                listing.dimensions.keys().collect(),
                "{class} covered {recorded} with dimensions {variant:?}"
            );
            for (dimension, value) in &variant {
                assert!(
                    listing.dimensions[dimension].contains(value),
                    "{class} covered {recorded} at unlisted {dimension}={value}"
                );
            }
            let class = listed
                .get_key_value(recorded.as_str())
                .map(|(id, _)| *id)
                .expect("listed class");
            covered.entry(class).or_default().insert(variant);
        }
    }
    let generated: BTreeSet<_> = GENERATORS.iter().map(|generator| generator.class).collect();
    let mut uncovered = 0;
    let mut report = Vec::new();
    for class in &classes.classes {
        let missing = if generated.contains(class.id.as_str()) {
            class.variants()
                - covered
                    .get(class.id.as_str())
                    .map_or(0, |set| set.len() as u64)
        } else {
            1
        };
        if missing > 0 {
            report.push(format!("{} {missing}", class.id));
        }
        uncovered += missing;
    }
    eprintln!(
        "DST classes: {uncovered} uncovered ({}) in {:?}",
        report.join(", "),
        started.elapsed()
    );
    let reproducing =
        std::env::var_os("DST_SEED").is_some() || std::env::var_os("DST_SEEDS").is_some();
    if reproducing {
        // A chosen seed range reports coverage; only the default seeds ratchet.
        return;
    }
    assert_eq!(
        uncovered, classes.uncovered,
        "classes.json records {} uncovered but the default seeds leave {uncovered}; \
         uncovered may only fall, and classes.json falls with it",
        classes.uncovered
    );
}

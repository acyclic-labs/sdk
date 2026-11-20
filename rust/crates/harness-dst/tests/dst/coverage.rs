//! The class coverage gate. `classes.json` names every class of failure the
//! simulator must rule out, each with the dimensions whose product is its
//! variants. A class is exercised by one generator over production code. The
//! gate only counts: every variant no generator declares is uncovered, and
//! that number is held exactly at the recorded `uncovered`, so it may only
//! fall, and the file falls with it. It never runs a world; each default
//! seed's own test proves its world reaches the variants it declares.
use super::*;
use family::{GENERATORS, Generator};
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

fn classes() -> std::result::Result<Classes, String> {
    let classes: Classes = serde_json::from_str(include_str!("classes.json"))
        .map_err(|error| format!("invalid classes.json: {error}"))?;
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
    Ok(classes)
}

/// The variants no generator declares, with the per-class report. A class
/// without a generator leaves every variant uncovered, so losing one never
/// lowers the count. A generator for an unlisted class, a class generated
/// twice, or a declared variant outside its class's dimensions is an error.
fn uncovered(
    classes: &Classes,
    generators: &[&Generator],
) -> std::result::Result<(u64, Vec<String>), String> {
    let mut declared = BTreeMap::<&str, BTreeSet<family::Variant>>::new();
    for generator in generators {
        let class = generator.class;
        let listing = classes
            .classes
            .iter()
            .find(|listed| listed.id == class)
            .ok_or_else(|| format!("generator for unlisted class {class}"))?;
        let std::collections::btree_map::Entry::Vacant(entry) = declared.entry(class) else {
            return Err(format!("class {class} has two generators"));
        };
        for variant in entry.insert(generator.declared()).iter() {
            if !variant
                .keys()
                .copied()
                .eq(listing.dimensions.keys().map(String::as_str))
            {
                return Err(format!(
                    "{class} declares {variant:?} outside its dimensions"
                ));
            }
            if let Some((dimension, value)) = variant.iter().find(|(dimension, value)| {
                listing
                    .dimensions
                    .get(**dimension)
                    .is_none_or(|values| values.iter().all(|listed| listed != *value))
            }) {
                return Err(format!("{class} declares unlisted {dimension}={value}"));
            }
        }
    }
    let mut uncovered = 0;
    let mut report = Vec::new();
    for class in &classes.classes {
        let missing = class.variants()
            - declared
                .get(class.id.as_str())
                .map_or(0, |variants| variants.len() as u64);
        if missing > 0 {
            report.push(format!("{} {missing}", class.id));
        }
        uncovered += missing;
    }
    Ok((uncovered, report))
}

#[test]
fn every_class_variant_is_generated_or_counted_uncovered() -> std::result::Result<(), String> {
    let started = std::time::Instant::now();
    let classes = classes()?;
    let (uncovered, report) = uncovered(&classes, GENERATORS)?;
    eprintln!(
        "DST classes: {uncovered} uncovered ({}) in {:?}",
        report.join(", "),
        started.elapsed()
    );
    assert_eq!(
        uncovered, classes.uncovered,
        "classes.json records {} uncovered but the generators leave {uncovered}; \
         uncovered may only fall, and classes.json falls with it",
        classes.uncovered
    );
    Ok(())
}

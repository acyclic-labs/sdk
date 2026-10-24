//! Every typed decode admits through its type's layout.
//!
//! The typed decode entry points require `HeapLayout`, so the compiler holds
//! every caller to the layout path. This guard holds the rest: the bounds that
//! layouts replaced, an item count times the largest slot of a schema, stay
//! deleted in every crate and in the plugin, and only the layout entry points
//! state a decode's owned bytes. Callers may add diagnostics to an admission
//! they received, never make one of their own.

use std::path::{Path, PathBuf};

/// Helpers that charged a decode as items times a schema's largest slot.
const DELETED: &[&str] = &[
    "decode_owned_allowance",
    "maximum_slot_bytes",
    "preparation_slot_bytes",
    "json_typed_value_decode_owned_bytes",
    "json_decode_owned_bytes",
    "intent_decode_owned_bytes",
    "recursive_decode_slot_bytes",
    "fork_collection_slot_bytes",
    "json_value_virtual_owned_bytes",
    "LOGICAL_SCHEMA_SLOT_BYTES",
    "SNAPSHOT_LAYOUT",
    "operator_owned_quota",
    "native_metadata_quota",
    "recovery_conversion_quota",
    "scheduler_json_owned_bytes",
    "guarded_pi_decode_measured",
];

/// The only files that state a decode's owned bytes from its layout.
const LAYOUT_ENTRY_POINTS: &[&str] = &["executor/json/records.rs"];

/// The layout derivation itself sizes containers from their real slots.
const LAYOUT_DERIVATION: &[&str] = &["executor/json/layout.rs", "executor/json/layout/"];

/// The untyped `Value` decoder's node bound, which is not a typed target:
/// every node it decodes is a `Value`. Named exactly, so no other site in
/// that file can use the pattern.
const VALUE_DECODER: (&str, &str) = (
    "executor/json/mod.rs",
    "let slots = nodes(shape)?.checked_mul(",
);

/// A file that decodes or admits a typed record names one of these. Slot
/// arithmetic elsewhere, such as hash table sizing, bounds no decode.
const DECODE_MARKERS: &[&str] = &[
    "JsonCodecCharge",
    "Admission",
    "json_from_slice_bounded",
    "json_from_value_bounded",
    "guarded_pi_decode",
    "decode_guarded",
    "PreparationRecord",
    "from_slice",
    "from_value",
];

fn violations(path: &str, source: &str) -> Vec<String> {
    let mut found = Vec::new();
    let decodes = DECODE_MARKERS.iter().any(|marker| source.contains(marker));
    let normalized = path.replace('\\', "/");
    for name in DELETED {
        if source.contains(name) {
            found.push(format!("{path}: uses deleted bound `{name}`"));
        }
    }
    let lines: Vec<&str> = source.lines().collect();
    for (index, line) in lines.iter().enumerate() {
        let lowered = line.to_ascii_lowercase();
        let derivation = LAYOUT_DERIVATION
            .iter()
            .any(|allowed| normalized.contains(allowed));
        let value_decoder = normalized.ends_with(VALUE_DECODER.0) && line.trim() == VALUE_DECODER.1;
        if decodes
            && !derivation
            && !value_decoder
            && lowered.contains("slot")
            && lowered.contains("mul(")
        {
            found.push(format!(
                "{path}:{}: multiplies by a slot outside the layout derivation",
                index + 1
            ));
        }
        // A construction states its owned bytes with an expression; a
        // pattern binds them. Outside the entry points, a construction may
        // only forward an admission destructured just before it.
        let construction = line.trim_start().starts_with("maximum_owned_bytes:")
            && index > 0
            && lines
                .get(index.saturating_sub(1))
                .is_some_and(|previous| previous.trim_end().ends_with("Admission {"))
            && !line.contains("maximum_owned_bytes: _");
        if construction
            && !LAYOUT_ENTRY_POINTS
                .iter()
                .any(|allowed| normalized.ends_with(allowed))
        {
            let forwarded = lines
                .get(index.saturating_sub(40)..index.saturating_sub(1))
                .unwrap_or_default()
                .windows(2)
                .any(|pair| match pair {
                    [head, binding] => {
                        head.trim_end().ends_with("Admission {")
                            && binding.trim() == "maximum_owned_bytes,"
                    }
                    _ => false,
                });
            if !forwarded {
                found.push(format!(
                    "{path}:{}: states a decode's owned bytes outside the layout path",
                    index + 1
                ));
            }
        }
    }
    found
}

fn sources(directory: &Path, into: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        if name == "target" || name == "node_modules" || name == ".git" {
            continue;
        }
        if path.is_dir() {
            sources(&path, into);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            into.push(path);
        }
    }
}

#[test]
fn every_typed_decode_admits_through_its_layout() {
    // Cargo runs a package's tests from its manifest directory.
    let roots = [PathBuf::from(".."), PathBuf::from("../../../plugin/src")];
    let mut files = Vec::new();
    for root in &roots {
        assert!(root.is_dir(), "guard root {} is missing", root.display());
        sources(root, &mut files);
    }
    assert!(files.len() > 1000, "only {} sources scanned", files.len());
    let this = Path::new(file!())
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let mut found = Vec::new();
    for file in files {
        if file.file_name().and_then(|name| name.to_str()) == Some(this) {
            continue;
        }
        let Ok(source) = std::fs::read_to_string(&file) else {
            continue;
        };
        found.extend(violations(&file.display().to_string(), &source));
    }
    assert!(found.is_empty(), "{}", found.join("\n"));
}

/// Negative control: call sites using the old bound fail the guard, and the
/// layout path and forwarded admissions pass it.
#[test]
fn the_guard_rejects_a_call_site_using_the_old_bound() {
    let old_helper = "let owned = decode_owned_allowance(shape, maximum_slot_bytes::<T>())?;";
    assert_eq!(violations("site.rs", old_helper).len(), 2);
    let old_product = "let owned = shape.lexical_items.saturating_mul(SLOT_BYTES);\nlet value = serde_json::from_slice::<T>(bytes)?;";
    assert_eq!(violations("site.rs", old_product).len(), 1);
    let table = "let bytes = slots.checked_mul(size_of::<Slot>())?;";
    assert!(violations("table.rs", table).is_empty());
    let own_admission = "charge(JsonCodecCharge::Admission {\n    maximum_owned_bytes: items * 64,\n    value_shape,\n})?;";
    assert_eq!(violations("site.rs", own_admission).len(), 1);
    let layout = "charge(JsonCodecCharge::Admission {\n    maximum_owned_bytes: layout.bytes(),\n    value_shape,\n})?;";
    assert!(violations("src/executor/json/records.rs", layout).is_empty());
    let forwarded = "match charge {\n    Admission {\n        maximum_owned_bytes,\n        value_shape,\n    } => {\n        let bytes = maximum_owned_bytes + diagnostics;\n        let charge = Admission {\n            maximum_owned_bytes: bytes,\n            value_shape,\n        };\n    }\n}";
    assert!(violations("site.rs", forwarded).is_empty());
}

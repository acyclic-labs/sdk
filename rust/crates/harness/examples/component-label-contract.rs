#![allow(missing_docs)]

use acyclic_harness::{
    COMPONENT_LABEL_FORBIDDEN_EXACT, COMPONENT_LABEL_FORBIDDEN_SEPARATORS,
    COMPONENT_LABEL_MAX_BYTES, is_valid_component_label,
};
use serde_json::json;

fn main() {
    let mut invalid_ranges = Vec::new();
    let mut range_start = None;
    let mut previous = 0;
    for code_point in 0..=0x10ffff {
        let invalid = char::from_u32(code_point)
            .is_some_and(|character| character.is_whitespace() || character.is_control());
        match (range_start, invalid) {
            (None, true) => range_start = Some(code_point),
            (Some(start), false) => {
                invalid_ranges.push([start, previous]);
                range_start = None;
            }
            _ => {}
        }
        if invalid {
            previous = code_point;
        }
    }
    if let Some(start) = range_start {
        invalid_ranges.push([start, previous]);
    }

    let mut validation_vectors = Vec::new();
    for value in [
        "".to_owned(),
        ".".to_owned(),
        "..".to_owned(),
        "/".to_owned(),
        "\\".to_owned(),
        "alpha".to_owned(),
        "a-b".to_owned(),
        "a_b".to_owned(),
        "é".to_owned(),
        "😀".to_owned(),
        "a\n".to_owned(),
        "a\t".to_owned(),
        "a\u{00a0}".to_owned(),
        "a\u{2003}".to_owned(),
        "a\u{007f}".to_owned(),
        "a".repeat(COMPONENT_LABEL_MAX_BYTES),
        "a".repeat(COMPONENT_LABEL_MAX_BYTES + 1),
        "é".repeat(127),
        "é".repeat(128),
        "😀".repeat(63),
        "😀".repeat(64),
    ] {
        validation_vectors.push(json!({
            "value": value,
            "valid": is_valid_component_label(&value),
        }));
    }

    println!(
        "{}",
        json!({
            "max_bytes": COMPONENT_LABEL_MAX_BYTES,
            "forbidden_exact": COMPONENT_LABEL_FORBIDDEN_EXACT,
            "forbidden_separators": COMPONENT_LABEL_FORBIDDEN_SEPARATORS,
            "invalid_ranges": invalid_ranges,
            "validation_vectors": validation_vectors,
        })
    );
}

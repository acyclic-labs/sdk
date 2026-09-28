#![allow(missing_docs)]

use acyclic_harness::core::{AUTHORITY_ID_FORBIDDEN_EXACT, AUTHORITY_ID_FORBIDDEN_SEPARATORS};
use serde_json::json;

fn main() {
    let mut control_ranges = Vec::new();
    let mut range_start = None;
    let mut previous = 0;
    for code_point in 0..=0x10ffff {
        let is_control = char::from_u32(code_point).is_some_and(char::is_control);
        match (range_start, is_control) {
            (None, true) => range_start = Some(code_point),
            (Some(start), false) => {
                control_ranges.push([start, previous]);
                range_start = None;
            }
            _ => {}
        }
        if is_control {
            previous = code_point;
        }
    }
    if let Some(start) = range_start {
        control_ranges.push([start, previous]);
    }

    println!(
        "{}",
        json!({
            "forbidden_exact": AUTHORITY_ID_FORBIDDEN_EXACT,
            "forbidden_separators": AUTHORITY_ID_FORBIDDEN_SEPARATORS,
            "control_ranges": control_ranges,
        })
    );
}

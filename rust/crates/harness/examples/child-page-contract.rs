#![allow(missing_docs)]

use acyclic_harness::runtime::{DEFAULT_CHILD_PAGE, MAX_CHILD_PAGE, MAX_CHILD_SLOT_BYTES};
use serde_json::json;

fn main() {
    println!(
        "{}",
        json!({
            "default_page": DEFAULT_CHILD_PAGE,
            "maximum_page": MAX_CHILD_PAGE,
            "maximum_slot_bytes": MAX_CHILD_SLOT_BYTES,
        })
    );
}

#![allow(missing_docs)]

use acyclic_harness::runtime::DEFAULT_CHILD_PAGE;
use serde_json::json;

fn main() {
    println!(
        "{}",
        json!({
            "default_page": DEFAULT_CHILD_PAGE,
        })
    );
}

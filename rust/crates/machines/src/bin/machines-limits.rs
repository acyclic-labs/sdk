#![allow(missing_docs, reason = "contract binary, not public API")]

use acyclic_machines::{MAX_EVENT_PAGE_SIZE, MAX_OUTPUT_READ_BYTES, MAX_PAGE_SIZE};

fn main() {
    println!(
        "{}",
        serde_json::json!({
            "maxPageSize": MAX_PAGE_SIZE,
            "maxEventPageSize": MAX_EVENT_PAGE_SIZE,
            "maxOutputReadBytes": MAX_OUTPUT_READ_BYTES,
        })
    );
}

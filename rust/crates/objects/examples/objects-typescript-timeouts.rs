#![allow(missing_docs, reason = "contract example binary, not public API")]

fn main() {
    println!(
        "{} {}",
        acyclic_objects::v1::DEFAULT_REQUEST_TIMEOUT.as_millis(),
        acyclic_objects::v1::MAX_TRANSFER_TIMEOUT.as_millis(),
    );
}

#![allow(missing_docs, reason = "contract example binary, not public API")]

fn main() {
    println!(
        "{} {}",
        acyclic_inference::MAXIMUM_MESSAGE_BYTES,
        acyclic_inference::MAXIMUM_HTTP_JSON_BYTES,
    );
}

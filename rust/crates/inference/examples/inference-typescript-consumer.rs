#![allow(missing_docs, reason = "contract example binary, not public API")]

fn main() {
    println!(
        "{{\"maximum_message_bytes\":{}}}",
        acyclic_inference::MAXIMUM_MESSAGE_BYTES,
    );
}

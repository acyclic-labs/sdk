#![allow(missing_docs, reason = "contract binary, not public API")]

fn main() {
    match serde_json::to_string(acyclic_stream::HTTP_RESPONSE_CONTRACT) {
        Ok(contract) => println!("{contract}"),
        Err(error) => {
            eprintln!("Stream HTTP contract serialization failed: {error}");
            std::process::exit(1);
        }
    }
}

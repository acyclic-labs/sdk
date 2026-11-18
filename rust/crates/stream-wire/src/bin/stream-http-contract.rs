//! Emits the hosted Stream HTTP response contract.

fn main() {
    match serde_json::to_string(acyclic_stream_wire::HTTP_RESPONSE_CONTRACT) {
        Ok(contract) => println!("{contract}"),
        Err(error) => {
            eprintln!("Stream HTTP contract serialization failed: {error}");
            std::process::exit(1);
        }
    }
}

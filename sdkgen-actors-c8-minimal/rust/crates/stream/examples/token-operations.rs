//! Emit the canonical Stream token operation vocabulary for SDK generation.

fn main() {
    match serde_json::to_string(acyclic_stream::TOKEN_OPERATIONS) {
        Ok(json) => println!("{json}"),
        Err(error) => {
            eprintln!("failed to serialize Stream token operation inventory: {error}");
            std::process::exit(1);
        }
    }
}

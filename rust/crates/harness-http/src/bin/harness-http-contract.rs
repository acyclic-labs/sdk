#![allow(missing_docs)]

fn main() {
    match serde_json::to_string(acyclic_harness_http::HTTP_ROUTE_CONTRACT) {
        Ok(contract) => println!("{contract}"),
        Err(error) => {
            eprintln!("Harness HTTP route contract serialization failed: {error}");
            std::process::exit(1);
        }
    }
}

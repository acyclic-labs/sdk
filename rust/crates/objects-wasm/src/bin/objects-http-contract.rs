#![allow(missing_docs)]

#[path = "../response_contract.rs"]
mod response_contract;

fn main() {
    match serde_json::to_string(response_contract::HTTP_RESPONSE_CONTRACT) {
        Ok(contract) => println!("{contract}"),
        Err(error) => {
            eprintln!("Objects HTTP contract serialization failed: {error}");
            std::process::exit(1);
        }
    }
}

#![allow(missing_docs)]

//! Emits the Rust-owned hosted enum mapping tables consumed by the
//! TypeScript hosted adapter generator.

use acyclic_fs::hosted_contract::contract_json;

fn main() {
    println!("{}", contract_json());
}

//! Local SDK command-line demonstration.

use acyclic_contracts::Outcome;
use acyclic_harness::{TaskGroup, recursive_sum};

#[tokio::main]
async fn main() {
    let result = recursive_sum(TaskGroup::new(8), (1..=32).collect(), 4).await;
    match result {
        Outcome::Succeeded(value) => println!("recursive result: {value}"),
        outcome => eprintln!("recursive workload did not succeed: {outcome:?}"),
    }
}

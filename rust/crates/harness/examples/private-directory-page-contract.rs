#![allow(missing_docs)]

use acyclic_harness::{
    conversation::MAX_PRIVATE_DIRECTORY_PAGE, runtime::DEFAULT_PRIVATE_DIRECTORY_PAGE,
};
use serde_json::json;

fn main() {
    println!(
        "{}",
        json!({
            "default_page": DEFAULT_PRIVATE_DIRECTORY_PAGE,
            "maximum_page": MAX_PRIVATE_DIRECTORY_PAGE,
        })
    );
}

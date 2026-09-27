#![allow(missing_docs)]

use acyclic_harness::conversation::MAX_CONVERSATION_PAGE_MESSAGES;
use serde_json::json;

fn main() {
    println!(
        "{}",
        json!({ "maximum_messages": MAX_CONVERSATION_PAGE_MESSAGES })
    );
}

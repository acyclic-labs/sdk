//! Emit the conversation pagination contract.

use acyclic_harness_core::conversation::{
    DEFAULT_CONVERSATION_PAGE_MESSAGES, MAX_CONVERSATION_PAGE_MESSAGES,
};
use serde_json::json;

fn main() {
    println!(
        "{}",
        json!({ "default_messages": DEFAULT_CONVERSATION_PAGE_MESSAGES, "maximum_messages": MAX_CONVERSATION_PAGE_MESSAGES })
    );
}

//! Emit the canonical Stream token operation vocabulary for SDK generation.

fn main() {
    let operations: Vec<_> = acyclic_stream::TokenOperation::ALL
        .iter()
        .map(|operation| {
            serde_json::json!({
                "operation": operation.as_str(),
                "actions": operation.actions().map(|action| action.as_str()).collect::<Vec<_>>()
            })
        })
        .collect();
    match serde_json::to_string(&operations) {
        Ok(json) => println!("{json}"),
        Err(error) => {
            eprintln!("failed to serialize Stream token operation inventory: {error}");
            std::process::exit(1);
        }
    }
}

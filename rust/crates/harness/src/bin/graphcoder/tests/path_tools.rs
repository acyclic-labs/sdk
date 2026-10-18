use serde_json::{Value, json};

/// Drive the real provider by the exchanges it received, without a second
/// model state machine or a counter that can advance independently of history.
pub(super) fn response(request: &Value) -> String {
    let results: Vec<_> = request["input"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["type"] == "function_call_output")
        .collect();
    let (name, arguments) = match results.len() {
        0 => ("read", json!({"path":"example.txt"})),
        1 => {
            assert!(results[0]["output"].to_string().contains("before λ"));
            (
                "edit",
                json!({"path":"example.txt","old_string":"before λ","new_string":"after λ"}),
            )
        }
        2 => ("read", json!({"path":"example.txt"})),
        3 => {
            let output = results[2]["output"].to_string();
            assert!(output.contains("after λ"), "{output}");
            assert!(!output.contains("before λ"), "{output}");
            return super::ANSWER.to_owned();
        }
        count => panic!("unexpected tool results: {count}"),
    };
    let call = json!({
        "type":"response.output_item.done","output_index":0,
        "item":{"type":"function_call","call_id":format!("path-{}",results.len()),
            "name":name,"arguments":arguments.to_string()}
    });
    let completed = json!({"type":"response.completed","response":{
        "status":"completed","usage":{"input_tokens":64,"output_tokens":2}
    }});
    format!("data: {call}\n\ndata: {completed}\n\n")
}

#[tokio::test]
async fn path_only_read_edit_read_publish_and_reopen_through_cli_and_http() {
    super::scenario(super::Mode::FileTools).await;
}

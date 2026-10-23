use super::events::{process_finished, running_session};
use serde_json::json;

#[test]
fn native_background_completion_is_observed_without_executing_processes() {
    assert_eq!(
        running_session(&json!(
            "Process running with session ID 17\nOutput: waiting"
        )),
        Some(json!(17))
    );
    assert_eq!(running_session(&json!({"session_id":17})), Some(json!(17)));
    assert_eq!(
        running_session(&json!({"session_id":17,"exit_code":0})),
        None
    );
    assert!(process_finished(&json!("Process exited with code 1")));
    assert!(!process_finished(&json!("still running")));
    assert!(!process_finished(&json!(null)));
    assert_eq!(
        running_session(&json!("Process running with session ID invalid")),
        None
    );
}

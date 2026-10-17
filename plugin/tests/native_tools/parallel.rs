//! Separate installed hooks share the surviving provider owner.
use super::*;

pub(super) async fn overlapping_hook_processes_publish_two_then_eight_writes(
    fixture: &Fixture,
) -> Result {
    let cwd = fixture.path();
    let session = uuid::Uuid::new_v4().to_string();
    // Two cold callers auto-start exactly one owner; both hook processes exit
    // before any tool hook starts.
    let attachment = json!({"sessionId":session,"cwd":cwd});
    futures::future::try_join_all(
        (0..2).map(|_| hook(cwd, "pi", "session_start", attachment.clone())),
    )
    .await?;
    let address = cwd.parent().unwrap().join("owner/address.json");
    let instance: Value = serde_json::from_slice(&std::fs::read(&address)?)?;
    for count in [2, 8] {
        let calls = (0..count).map(|index| {
            let name = format!("parallel-{count}-{index}.txt");
            hook(
                cwd,
                "pi",
                "tool_call",
                json!({
                    "sessionId":session,"cwd":cwd,"toolCallId":name,
                    "toolName":"write","input":{"path":name,"content":name},
                }),
            )
        });
        for answer in futures::future::try_join_all(calls).await? {
            assert_eq!(answer["isError"], false, "{answer}");
        }
        for index in 0..count {
            let name = format!("parallel-{count}-{index}.txt");
            assert_eq!(std::fs::read_to_string(cwd.join(&name))?, name);
        }
        let retained: Value = serde_json::from_slice(&std::fs::read(&address)?)?;
        assert_eq!(retained["instance"], instance["instance"]);
    }
    // A second checkout uses the same process while retaining an independent
    // provider, project binding and session history.
    let second = cwd.parent().unwrap().join("second");
    std::fs::create_dir(&second)?;
    hook(
        &second,
        "pi",
        "session_start",
        json!({"sessionId":session,"cwd":second}),
    )
    .await?;
    let answer = hook(
        &second,
        "pi",
        "tool_call",
        json!({"sessionId":session,"cwd":second,"toolCallId":"second",
            "toolName":"write","input":{"path":"independent.txt","content":"second"}}),
    )
    .await?;
    assert_eq!(answer["isError"], false, "{answer}");
    assert_eq!(
        std::fs::read_to_string(second.join("independent.txt"))?,
        "second"
    );
    assert!(!cwd.join("independent.txt").exists());
    let retained: Value = serde_json::from_slice(&std::fs::read(&address)?)?;
    assert_eq!(retained["instance"], instance["instance"]);
    fixture.close().await?;
    // Shutdown waits for all providers to close before releasing the lock.
    hook(
        cwd,
        "pi",
        "session_start",
        json!({"sessionId":session,"cwd":cwd}),
    )
    .await?;
    let restarted: Value = serde_json::from_slice(&std::fs::read(&address)?)?;
    assert_ne!(restarted["instance"], instance["instance"]);
    // The shared suite closes this restarted owner after its other independent
    // checkouts complete; the restart assertion remains in this scenario.
    Ok(())
}

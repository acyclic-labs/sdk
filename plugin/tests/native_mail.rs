//! Stock mail and inspection through every host's shared hook handler.
#[path = "support/native_actions.rs"]
mod native_actions;
#[path = "support/native_hooks.rs"]
mod native_hooks;
use acyclic_harness::TaskId;
use native_actions::Tools;
use native_hooks::{Fixture, Result};
use serde_json::json;

#[tokio::test]
async fn mailbox_and_inspection_commands_use_authenticated_task_files_for_every_host() -> Result {
    let fixture = Fixture::new()?;
    let owner = fixture.start_owner().await?;
    let result = tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let [codex, claude, pi] = Tools::hosts(fixture.path()).await?;
        let expected = tokio::try_join!(
            mailbox(&fixture, &codex),
            mailbox(&fixture, &claude),
            mailbox(&fixture, &pi)
        )?;
        let roots = [codex.task, claude.task, pi.task];
        let tasks = codex.discover_tasks(&roots).await?;
        assert_eq!(
            tasks.len(),
            3,
            "all three hosts' children share the selected workspace's discovery"
        );
        for (parent, child) in roots.into_iter().zip([expected.0, expected.1, expected.2]) {
            assert!(tasks.contains(&child));
            let inspected = codex
                .call(
                    &format!("foreign-{child}"),
                    "acyclic_inspect",
                    json!({"task":child}),
                )
                .await?;
            assert_eq!(inspected["admission"]["parent"], parent.to_string());
            assert!(inspected["outcome"].is_null());
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await;
    fixture.close_owner(owner).await?;
    result??;
    Ok(())
}

async fn mailbox(fixture: &Fixture, tools: &Tools<'_>) -> Result<TaskId> {
    let host = tools.host;
    let child = tools.child("mail-child").await?;
    let filename = format!("{}-message.txt", host.name());
    let written = tools
            .call(
                "message",
                "write",
                json!({"path":filename,"text":"mail payload","media_type":"text/plain","display_name":filename}),
            )
            .await?;
    let file = written
        .get("file")
        .ok_or("write did not return a pinned file")?;
    for (after, name) in ["acyclic_send", "acyclic_steer"].into_iter().enumerate() {
        assert!(
            tools
                .call(name, name, json!({"recipient":child.task,"payload":file}))
                .await?
                .is_null()
        );
        let item = child
            .call(
                &format!("receive-{after}"),
                "acyclic_receive",
                json!({"after":after}),
            )
            .await?;
        assert_eq!(item["sequence"], after + 1);
        assert_eq!(item["payload"], *file);
    }
    let next = format!("{}-after-rejection.txt", host.name());
    let (reply_file, ()) = tokio::try_join!(
        async {
            let denied = child
                .reject(
                    "parent-source-denied",
                    "acyclic_send",
                    json!({"recipient":tools.task,"payload":file}),
                )
                .await?;
            assert_eq!(
                child
                    .reject(
                        "parent-source-denied",
                        "acyclic_send",
                        json!({"recipient":tools.task,"payload":file})
                    )
                    .await?,
                denied
            );
            let mut written = child
                .call(
                    "reply-source",
                    "write",
                    json!({
                        "path":format!("{}-reply.txt",host.name()),"text":"mail payload",
                        "media_type":"text/plain","display_name":filename,
                    }),
                )
                .await?;
            let reply_file = written
                .get_mut("file")
                .ok_or("child reply has no pinned file")?
                .take();
            assert_ne!(
                &reply_file, file,
                "a child reply must retain its isolated content binding"
            );
            assert_eq!(reply_file["descriptor"], file["descriptor"]);
            child
                .call(
                    "reply",
                    "acyclic_send",
                    json!({"recipient":tools.task,"payload":reply_file}),
                )
                .await?;
            Ok::<_, Box<dyn std::error::Error>>(reply_file)
        },
        async {
            let inspected = tools
                .call("inspect", "acyclic_inspect", json!({"task":tools.task}))
                .await?;
            assert_eq!(inspected["task"], tools.task.to_string());
            assert!(inspected["admission"]["parent"].is_null());
            let unknown = json!({"task":TaskId::from_bytes([253;16])});
            let failed = tools
                .reject("unknown", "acyclic_inspect", unknown.clone())
                .await?;
            assert_eq!(
                tools.reject("unknown", "acyclic_inspect", unknown).await?,
                failed
            );
            tools
            .call(
                "after-rejection",
                "write",
                json!({"path":next,"text":"session usable","media_type":"text/plain","display_name":next}),
            )
            .await?;
            Ok::<_, Box<dyn std::error::Error>>(())
        }
    )?;
    let reply = tools
        .call("reply", "acyclic_receive", json!({"after":0}))
        .await?;
    assert_eq!(
        reply["sequence"], 1,
        "rejected mail must not reach the parent"
    );
    assert_eq!(reply["payload"], reply_file);
    assert_eq!(
        std::fs::read_to_string(fixture.path().join(filename))?,
        "mail payload"
    );
    assert_eq!(
        std::fs::read_to_string(fixture.path().join(next))?,
        "session usable"
    );
    Ok(child.task)
}

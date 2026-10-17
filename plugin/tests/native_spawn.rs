//! A registered child is driven by the surviving owner, without a model or host.
#[path = "support/native_actions.rs"]
mod native_actions;
#[path = "support/native_hooks.rs"]
mod native_hooks;
use acyclic_harness::{
    OperationId, TaskId,
    external_executor::ExternalHost,
    filesystem::{MAIL_SEND_TASK_COMMAND_KIND, TASK_ADMIT_COMMAND_KIND},
};
use native_actions::Tools;
use native_hooks::{Fixture, Result};
use serde_json::json;

#[tokio::test]
async fn registered_children_execute_and_reply_through_every_native_hook() -> Result {
    let fixture = Fixture::new()?;
    let hosts = [
        ExternalHost::Codex,
        ExternalHost::ClaudeCode,
        ExternalHost::Pi,
    ];
    let workspaces = hosts.map(|host| fixture.path().with_file_name(host.name()));
    for workspace in &workspaces {
        std::fs::create_dir(workspace)?;
    }
    let owner = fixture.start_owner().await?;
    let result = async {
        let cases =
            futures::future::try_join_all(hosts.into_iter().zip(&workspaces).map(
                |(host, workspace)| async move {
                    child(Tools::open(workspace, host, None).await?).await
                },
            ))
            .await?;
        let inspections = discover_children(&cases).await?;
        futures::future::try_join_all(
            cases
                .into_iter()
                .map(|(parent, _, branch)| finish_child(parent, branch, &inspections)),
        )
        .await?;
        for (host, workspace) in hosts.into_iter().zip(&workspaces) {
            for source in hosts {
                assert_eq!(
                    workspace
                        .join(format!("{}-payload.txt", source.name()))
                        .exists(),
                    source == host,
                    "one owner's workspace binding must not share another checkout's writes"
                );
            }
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    fixture.close_owner(owner).await?;
    result?;
    Ok(())
}

async fn child(parent: Tools<'_>) -> Result<(Tools<'_>, TaskId, serde_json::Value)> {
    let host = parent.host;
    parent
        .reject(
            "malformed-spawn",
            "acyclic_spawn",
            json!({
                "name":"acyclic.command_session", "version":"1", "input":null,
            }),
        )
        .await?;
    let payload = parent
        .call(
            "payload",
            "write",
            json!({
                "path":format!("{}-payload.txt",host.name()),"text":"registered child reply",
                "media_type":"text/plain","display_name":"payload.txt",
            }),
        )
        .await?;
    let mail = json!({"recipient":parent.task,"payload":payload["file"]});
    let command = parent
        .call(
            "command",
            "write",
            json!({
                "path":format!("{}-command.json",host.name()),"text":serde_json::to_string(&mail)?,
                "media_type":"application/json","display_name":"command.json",
            }),
        )
        .await?;
    let spawn = json!({"name":"acyclic.command_session","version":"1","input":{
        "command":{"operation_id":OperationId::new(),"kind":MAIL_SEND_TASK_COMMAND_KIND,"payload":command["file"]},
        "terminal":true,
    }});
    let mut denied = spawn.clone();
    denied["input"]["command"]["payload"]["volume"]["id"] = json!("ungranted-project");
    parent
        .reject("ungranted-source", "acyclic_spawn", denied)
        .await?;
    let returned = parent.call("spawn", "acyclic_spawn", spawn.clone()).await?;
    let child: TaskId = serde_json::from_value(returned.clone())?;
    assert_eq!(
        parent.call("spawn", "acyclic_spawn", spawn).await?,
        returned
    );
    assert!(
        parent
            .call("wait", "acyclic_wait", json!({"child":child}))
            .await?
            .is_null()
    );
    let reply = parent
        .call("reply", "acyclic_receive", json!({"after":0}))
        .await?;
    assert_eq!(reply["task_id"], parent.task.to_string());
    assert_eq!(reply["sequence"], 1);
    assert_eq!(reply["payload"], payload["file"]);
    let inspected = parent
        .call("inspect", "acyclic_inspect", json!({"task":child}))
        .await?;
    assert_eq!(inspected["outcome"], json!({"Succeeded":null}));
    assert_eq!(inspected["admission"]["parent"], parent.task.to_string());
    let nested = json!({"name":"acyclic.command_session","version":"1","input":{
        "command":{"operation_id":OperationId::new(),"kind":MAIL_SEND_TASK_COMMAND_KIND,"payload":command["file"]},
        "terminal":false,
    }});
    let admit = parent
        .call(
            "nested-command",
            "write",
            json!({
                "path":format!("{}-nested.json",host.name()),"text":serde_json::to_string(&nested)?,
                "media_type":"application/json","display_name":"nested.json",
            }),
        )
        .await?;
    let branch = parent.call("branch","acyclic_spawn",json!({
        "name":"acyclic.command_session","version":"1","input":{
            "command":{"operation_id":OperationId::new(),"kind":TASK_ADMIT_COMMAND_KIND,"payload":admit["file"]},
            "terminal":false,
        }
    })).await?;
    let reply = parent
        .call("nested-reply", "acyclic_receive", json!({"after":1}))
        .await?;
    assert_eq!(reply["task_id"], parent.task.to_string());
    assert_eq!(reply["sequence"], 2);
    assert_eq!(reply["payload"], payload["file"]);
    Ok((parent, child, branch))
}

async fn finish_child(
    parent: Tools<'_>,
    branch: serde_json::Value,
    inspections: &[serde_json::Value],
) -> Result {
    let children: Vec<_> = inspections
        .iter()
        .filter(|value| value["admission"]["parent"] == branch)
        .collect();
    assert_eq!(
        children.len(),
        1,
        "the registered branch admitted exactly one grandchild"
    );
    let leaf = children[0]["task"].clone();
    let inspected = parent
        .call("leaf", "acyclic_inspect", json!({"task":leaf}))
        .await?;
    assert_eq!(inspected["admission"]["parent"], branch);
    assert!(inspected["outcome"].is_null());
    parent
        .call("cancel-branch", "acyclic_cancel", json!({"child":branch}))
        .await?;
    for task in [branch, leaf] {
        let inspected = parent
            .call(
                &format!("cancelled-{task}"),
                "acyclic_inspect",
                json!({"task":task}),
            )
            .await?;
        assert_eq!(inspected["outcome"], "Cancelled");
    }
    Ok(())
}

async fn discover_children(
    cases: &[(Tools<'_>, TaskId, serde_json::Value)],
) -> Result<Vec<serde_json::Value>> {
    let inspections =
        futures::future::try_join_all(cases.iter().map(|(parent, child, branch)| async move {
            let known = [
                parent.task,
                *child,
                TaskId::parse(branch.as_str().ok_or("missing branch task")?)?,
            ];
            let children = parent.discover_tasks(&known).await?;
            assert_eq!(
                children.len(),
                1,
                "the registered branch admitted exactly one grandchild"
            );
            let task = children[0];
            parent
                .call(
                    &format!("discover-{task}"),
                    "acyclic_inspect",
                    json!({"task":task}),
                )
                .await
        }))
        .await?;
    assert_eq!(
        inspections.len(),
        3,
        "each host's branch admitted one grandchild"
    );
    Ok(inspections)
}

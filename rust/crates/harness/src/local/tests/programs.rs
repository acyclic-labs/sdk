//! Authored files through the real actor, code caller and native shell owner.
use super::*;
use crate::client::{AgentCommand, AgentReply};
use crate::model::{ModelContent, ModelEvent, ToolCall};
use crate::test_support::ScriptedEvent;

fn code(id: &str, source: &str) -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
            id.into(),
            "code".into(),
            json!({"code": source}),
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({}),
        }),
    ])
}

fn program_code(id: &str, source: &str) -> ScriptedTurn {
    code(
        id,
        &format!(
            r#"
        import {{list}} from 'acyclic';
        const names = [];
        for await (const entry of list('volume:///home/tools')) {{
            if ('description' in entry || 'inputSchema' in entry) throw new Error('eager manifest listing');
            names.push(entry.name);
        }}
        if (!names.includes('echo') || !names.includes('unselected')) throw new Error('program names missing');
        {source}
    "#
        ),
    )
}

fn result<'a>(request: &'a crate::model::ModelRequest, id: &str) -> Result<&'a serde_json::Value> {
    let content = request
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .find_map(|part| match part {
            crate::model::ModelContentPart::ToolResult {
                call_id, content, ..
            } if call_id == id => Some(content),
            _ => None,
        })
        .ok_or_else(|| Error::NotFound(format!("program result {id}")))?;
    match content {
        crate::model::ToolResultContent::Json { value } => Ok(value),
        other => Err(Error::Invalid(format!("program result {id}: {other:?}"))),
    }
}

fn echoed(request: &crate::model::ModelRequest, id: &str, expected: &str) -> Result<()> {
    let value = result(request, id)?;
    assert_eq!(value["exit_code"], 0, "{id}: {value}");
    let stdout = value["stdout"]
        .as_str()
        .ok_or_else(|| Error::Invalid(format!("program stdout is absent: {value}")))?;
    let actual: serde_json::Value =
        serde_json::from_str(stdout).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(actual, json!({"text": expected}));
    Ok(())
}

#[tokio::test]
async fn authored_programs_apply_on_the_next_step_with_a_stable_prefix_and_shell_permissions()
-> Result<()> {
    for denied in [false, true] {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        std::fs::create_dir(directory.path().join(".git"))
            .map_err(|error| Error::Storage(error.to_string()))?;
        let provider = scripted([ScriptedTurn::Text("ready".into())]);
        let mut config = checkout_config(directory.path(), (model()?, provider.clone()))?;
        if denied {
            config = config.approval_policy(crate::approvals::ApprovalPolicy {
                approvers: vec![crate::approvals::Approver::Rules {
                    principal: "owner".into(),
                    rules: vec![crate::approvals::ApprovalRule {
                        tool: Some("shell".into()),
                        arguments: None,
                        selectors: Vec::new(),
                        target: None,
                        verdict: crate::approvals::RuleVerdict::Deny,
                    }],
                    fallback: crate::approvals::RuleVerdict::Allow,
                }],
            });
        }
        let harness = Arc::new(super::open_scripted(config).await?);
        let connection = harness.connect().await?;
        let AgentReply::Spawned { agent: worker, .. } = connection
            .execute(&AgentCommand::Spawn {
                parent: connection.root().clone(),
                id: crate::agent::IntentId::try_from("program-author")
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                brief: ModelContent::Text("Prepare to author a program".into()),
                role: "worker".into(),
            })
            .await?
        else {
            return Err(Error::Invalid("worker did not open".into()));
        };
        agents::settled(&harness, &worker).await?;
        let first = provider.request_count();
        let argument = concat!(
            r#"program-value ' " \ $(touch program-injected); ‘’‚‛"#,
            "\n🦀"
        );
        let call = format!("tools.echo({})", json!({"text": argument}));
        let invoke = if denied {
            format!(
                "export default await (async () => {{ try {{ await {call}; throw new Error('unexpected execution'); }} catch (error) {{ if (error.class !== 'denied') throw error; return 'denied-program'; }} }})();"
            )
        } else {
            format!("export default await {call};")
        };
        provider.extend([
            code("author", r#"
                import {write} from 'acyclic';
                await write('volume:///home/tools/echo/program.ts', {
                    version: 1, name: 'echo', description: 'Echo argument data',
                    inputSchema: {type: 'object', properties: {text: {type: 'string'}}, required: ['text'], additionalProperties: false},
                    entry: {sh: 'main.sh', ps1: 'main.ps1'}
                }, 'absent');
                await write('volume:///home/tools/echo/main.sh', 'cat', 'absent');
                await write('volume:///home/tools/echo/main.ps1', "param(); $value = [Console]::In.ReadToEnd() | ConvertFrom-Json; if (!$value.text.EndsWith('🦀')) { throw 'UTF-8 input changed' }; [Console]::Out.Write(($value | ConvertTo-Json -Compress))", 'absent');
                await write('volume:///home/tools/unselected/program.ts', "throw new Error('unselected program was evaluated');", 'absent');
                export default 'authored';
            "#),
            program_code("invoke", &invoke),
            ScriptedTurn::Text("done".into()),
        ]);
        let conversation = Conversation::open(connection.select(worker.clone()).await?);
        let mut observed = ObservedConversation::observe(conversation, None).await?;
        let entry = observed
            .send("author-and-call", "Write and call a program")
            .await?;
        assert!(
            entry.outcome.is_completed(),
            "{:?}; requests: {:#?}",
            entry.outcome,
            provider.requests()
        );
        let requests = provider.requests();
        assert_eq!(requests.len(), first + 3);
        assert_eq!(result(&requests[first + 1], "author")?, "authored");
        let prefix = |request: &crate::model::ModelRequest| -> Result<_> {
            let system: Vec<_> = request
                .messages
                .iter()
                .take_while(|message| message.role == crate::model::ModelRole::System)
                .collect();
            let bytes =
                serde_json::to_vec(&system).map_err(|error| Error::Invalid(error.to_string()))?;
            Ok(blake3::hash(&bytes))
        };
        assert_eq!(prefix(&requests[first])?, prefix(&requests[first + 1])?);
        assert_eq!(
            requests[first].model_tools().collect::<Vec<_>>(),
            requests[first + 1].model_tools().collect::<Vec<_>>()
        );
        if denied {
            assert_eq!(result(&requests[first + 2], "invoke")?, "denied-program");
        } else {
            echoed(&requests[first + 2], "invoke", argument)?;
        }
        let forked_first = provider.request_count();
        provider.extend([
            program_code("inherited-call", if denied {
                "export default await (async () => { try { await tools.echo({text: 'child-value-🦀'}); throw new Error('unexpected execution'); } catch (error) { if (error.class !== 'denied') throw error; return 'denied-child-program'; } })();"
            } else {
                "export default await tools.echo({text: 'child-value-🦀'});"
            }),
            ScriptedTurn::Text("child done".into()),
        ]);
        let AgentReply::Spawned { agent: child, .. } = connection
            .execute(&AgentCommand::Fork {
                continuation: None,
                parent: worker,
                id: crate::agent::IntentId::try_from("program-reader")
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                at: 1,
                brief: ModelContent::Text("Call the inherited program".into()),
                role: "worker".into(),
            })
            .await?
        else {
            return Err(Error::Invalid("program fork did not open".into()));
        };
        agents::settled(&harness, &child).await?;
        let requests = provider.requests();
        assert_eq!(requests.len(), forked_first + 2);
        assert_eq!(
            requests[first].model_tools().collect::<Vec<_>>(),
            requests[forked_first].model_tools().collect::<Vec<_>>()
        );
        if denied {
            assert_eq!(
                result(&requests[forked_first + 1], "inherited-call")?,
                "denied-child-program"
            );
        } else {
            echoed(
                &requests[forked_first + 1],
                "inherited-call",
                "child-value-🦀",
            )?;
        }
        if !denied {
            provider.extend([
                code("replace-parent-program", r#"
                    import {write} from 'acyclic';
                    await write('volume:///home/tools/echo/main.sh', 'printf parent-changed', 'any');
                    await write('volume:///home/tools/echo/main.ps1', "[Console]::Out.Write('parent-changed')", 'any');
                    export default 'replaced';
                "#),
                ScriptedTurn::Text("parent changed".into()),
            ]);
            assert!(
                observed
                    .send("replace-program", "Replace the parent program")
                    .await?
                    .outcome
                    .is_completed()
            );
            provider.extend([
                program_code(
                    "call-frozen-program",
                    "export default await tools.echo({text: 'frozen-child-value-🦀'});",
                ),
                ScriptedTurn::Text("child still isolated".into()),
            ]);
            let conversation = Conversation::open(connection.select(child).await?);
            let mut inherited = ObservedConversation::observe(conversation, None).await?;
            assert!(
                inherited
                    .send("call-frozen-program", "Call your inherited program again")
                    .await?
                    .outcome
                    .is_completed()
            );
            let last = provider
                .requests()
                .pop()
                .ok_or_else(|| Error::Invalid("child request is absent".into()))?;
            echoed(&last, "call-frozen-program", "frozen-child-value-🦀")?;
        }
    }
    Ok(())
}

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
        let harness = Arc::new(LocalHarness::open(config).await?);
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
        provider.extend([
            code("author", r#"
                import {write} from 'acyclic';
                await write('volume:///home/tools/echo/program.ts', {
                    version: 1, name: 'echo', description: 'Echo argument data',
                    inputSchema: {type: 'object', properties: {text: {type: 'string'}}, required: ['text'], additionalProperties: false},
                    entry: {sh: 'main.sh', ps1: 'main.ps1'}
                }, 'absent');
                await write('volume:///home/tools/echo/main.sh', 'cat', 'absent');
                await write('volume:///home/tools/echo/main.ps1', '[Console]::Out.Write([Console]::In.ReadToEnd())', 'absent');
                export default 'authored';
            "#),
            code("invoke", if denied {
                "export default await (async () => { try { await tools.echo({text: 'program-value'}); throw new Error('unexpected execution'); } catch (error) { if (error.class !== 'denied') throw error; return 'denied-program'; } })();"
            } else {
                "export default await tools.echo({text: 'program-value'});"
            }),
            ScriptedTurn::Text("done".into()),
        ]);
        let conversation = Conversation::open(connection.select(worker).await?);
        let mut observed = ObservedConversation::observe(conversation, None).await?;
        let entry = observed
            .send("author-and-call", "Write and call a program")
            .await?;
        assert!(entry.outcome.is_completed(), "{:?}", entry.outcome);
        let requests = provider.requests();
        assert_eq!(requests.len(), first + 3);
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
        let output = request_text(&harness, &requests[first + 2]).await?;
        assert!(
            output.contains(if denied {
                "denied-program"
            } else {
                "program-value"
            }),
            "{output}"
        );
    }
    Ok(())
}

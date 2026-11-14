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
        .ok_or_else(|| {
            let shape: Vec<_> = request
                .messages
                .iter()
                .map(|message| {
                    let parts: Vec<_> = message
                        .content
                        .parts()
                        .iter()
                        .map(|part| match part {
                            crate::model::ModelContentPart::ToolResult { call_id, .. } => {
                                format!("result:{call_id}")
                            }
                            crate::model::ModelContentPart::ToolCall(_) => "call".into(),
                            crate::model::ModelContentPart::File { .. } => "file".into(),
                            crate::model::ModelContentPart::Text { .. } => "text".into(),
                            crate::model::ModelContentPart::Notices { .. } => "notices".into(),
                            crate::model::ModelContentPart::Configuration { .. } => {
                                "configuration".into()
                            }
                            _ => "other".into(),
                        })
                        .collect();
                    (
                        message.role,
                        matches!(message.content, ModelContent::Text(_)),
                        parts,
                    )
                })
                .collect();
            Error::NotFound(format!("program result {id}; request shape: {shape:?}"))
        })?;
    match content {
        crate::model::ToolResultContent::Json { value } => Ok(value),
        other => Err(Error::Invalid(format!("program result {id}: {other:?}"))),
    }
}

fn system_prefix(request: &crate::model::ModelRequest) -> Result<Vec<u8>> {
    let prefix: Vec<_> = request
        .messages
        .iter()
        .take_while(|message| message.role == crate::model::ModelRole::System)
        .collect();
    serde_json::to_vec(&prefix).map_err(|error| Error::Invalid(error.to_string()))
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

async fn started_calls(conversation: &Conversation, inbox: u64) -> Result<Vec<String>> {
    let mut after = 0;
    let mut calls = Vec::new();
    loop {
        let page = conversation
            .execution_page(inbox, after, false)
            .await?
            .page
            .ok_or_else(|| Error::Invalid("original program execution page is absent".into()))?;
        for record in page.items {
            if let crate::executor::ExecutionEvent::ToolStarted { call_id, .. } = record.event {
                calls.push(call_id);
            }
        }
        let Some(next) = page.next else {
            return Ok(calls);
        };
        assert!(next > after, "execution page did not advance");
        after = next;
    }
}

#[tokio::test]
async fn authored_programs_apply_on_the_next_step_with_a_stable_prefix_and_shell_permissions()
-> Result<()> {
    for denied in [false, true] {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        std::fs::create_dir(directory.path().join(".git"))
            .map_err(|error| Error::Storage(error.to_string()))?;
        let pause = Arc::new(PauseBeforeReply::default());
        let provider = pause.provider(
            ScriptedModelProvider::new([ScriptedTurn::Text("ready".into())])
                .with_context_capacity(capacity()),
        );
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
        pause.arm(&harness, connection.root())?;
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
        assert_eq!(
            system_prefix(&requests[first])?,
            system_prefix(&requests[first + 1])?
        );
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
        pause.arm(&harness, &worker)?;
        let AgentReply::Spawned { agent: child, .. } = connection
            .execute(&AgentCommand::Fork {
                continuation: None,
                parent: worker.clone(),
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
            provider.extend([ScriptedTurn::Text("Child notification received".into())]);
            let (agents, _) = harness.agents().await?;
            agents
                .pause(&worker, false)
                .await
                .map_err(|error| Error::Storage(error.to_string()))?;
            super::agents::settled(&harness, &worker).await?;
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
            assert_eq!(
                result(&provider.last_request().unwrap(), "replace-parent-program")?,
                "replaced"
            );
            provider.extend([
                program_code(
                    "call-frozen-program",
                    "export default await tools.echo({text: 'frozen-child-value-🦀'});",
                ),
                ScriptedTurn::Text("child still isolated".into()),
            ]);
            pause.arm(&harness, &worker)?;
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

#[tokio::test]
async fn authored_programs_wait_for_the_original_shell_approval_before_native_dispatch()
-> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::create_dir(directory.path().join(".git"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    let pause = Arc::new(PauseBeforeReply::default());
    let provider = pause.provider(
        ScriptedModelProvider::new([ScriptedTurn::Text("ready".into())])
            .with_context_capacity(capacity()),
    );
    let policy = crate::approvals::ApprovalPolicy {
        approvers: vec![
            crate::approvals::Approver::Rules {
                principal: "owner".into(),
                rules: vec![crate::approvals::ApprovalRule {
                    tool: Some("shell".into()),
                    arguments: None,
                    selectors: Vec::new(),
                    target: None,
                    verdict: crate::approvals::RuleVerdict::Ask,
                }],
                fallback: crate::approvals::RuleVerdict::Allow,
            },
            crate::approvals::Approver::Human {
                principal: crate::approvals::HUMAN_APPROVER.into(),
            },
        ],
    };
    let config = checkout_config(directory.path(), (model()?, provider.clone()))?
        .approval_policy(policy)
        .home_file("tools/echo/program.ts", acyclic_stream::OwnedBytes::from_static(
            b"export default {version: 1, name: 'echo', description: 'Echo data', inputSchema: {type: 'object'}, entry: {sh: 'main.sh', ps1: 'main.ps1'}};"
        ))
        .home_file("tools/echo/main.sh", acyclic_stream::OwnedBytes::from_static(b"cat"))
        .home_file("tools/echo/main.ps1", acyclic_stream::OwnedBytes::from_static(
            b"[Console]::Out.Write([Console]::In.ReadToEnd())"
        ));
    let harness = Arc::new(super::open_scripted(config).await?);
    let connection = harness.connect().await?;
    pause.arm(&harness, connection.root())?;
    let AgentReply::Spawned { agent: worker, .. } = connection
        .execute(&AgentCommand::Spawn {
            parent: connection.root().clone(),
            id: crate::agent::IntentId::try_from("approved-program")
                .map_err(|error| Error::Invalid(error.to_string()))?,
            brief: ModelContent::Text("Prepare to call a program".into()),
            role: "worker".into(),
        })
        .await?
    else {
        return Err(Error::Invalid("program worker did not open".into()));
    };
    agents::settled(&harness, &worker).await?;
    let first = provider.request_count();
    provider.extend([
        code(
            "approved-program-call",
            "export default await tools.echo({text: 'approved-🦀'});",
        ),
        ScriptedTurn::Text("approved".into()),
    ]);
    let conversation = Conversation::open(connection.select(worker).await?);
    let mut observed = ObservedConversation::observe(conversation.clone(), None).await?;
    let inbox = conversation
        .send("approve-program", "Call the authored program")
        .await?;
    let (id, binding) = loop {
        match observed.observer.next().await? {
            Event::Status(crate::agent::StatusRecord {
                phase:
                    crate::agent::Phase::WaitingApproval {
                        id,
                        binding: Some(binding),
                    },
                ..
            }) => break (id, binding),
            Event::Turn { entry, .. } if entry.inbox == inbox => {
                return Err(Error::Invalid(format!(
                    "program settled before approval: {:?}",
                    entry.outcome
                )));
            }
            Event::Status(crate::agent::StatusRecord {
                phase: crate::agent::Phase::Done { outcome },
                ..
            }) => {
                return Err(Error::Invalid(format!(
                    "program ended before approval: {outcome:?}"
                )));
            }
            Event::Status(crate::agent::StatusRecord {
                phase: crate::agent::Phase::WaitingApproval { binding: None, .. },
                ..
            }) => {
                return Err(Error::Invalid(
                    "program approval has no original binding".into(),
                ));
            }
            _ => {}
        }
    };
    assert_eq!(provider.request_count(), first + 1);
    assert_eq!(
        started_calls(&conversation, inbox).await?,
        ["approved-program-call"],
        "inner native shell started before approval"
    );
    let pending = conversation
        .pending_approval(&id)
        .await?
        .ok_or_else(|| Error::Invalid("program approval is absent".into()))?;
    assert_eq!(pending.binding, binding);
    let mut different = binding.clone();
    different.action_digest[0] ^= 1;
    assert!(
        conversation
            .decide_with_binding(&id, different, true.into())
            .await
            .is_err()
    );
    assert_eq!(provider.request_count(), first + 1);
    conversation
        .decide_with_binding(&id, binding, true.into())
        .await?;
    let entry = observed.settled(inbox).await?;
    assert!(entry.outcome.is_completed(), "{:?}", entry.outcome);
    assert!(conversation.pending_approval(&id).await?.is_none());
    assert_eq!(provider.request_count(), first + 2);
    let starts = started_calls(&conversation, inbox).await?;
    assert_eq!(
        starts.len(),
        2,
        "original code and native call must start once each"
    );
    assert_eq!(starts[0], "approved-program-call");
    assert_ne!(starts[1], starts[0]);
    echoed(
        &provider.request(first + 1).unwrap(),
        "approved-program-call",
        "approved-🦀",
    )?;
    Ok(())
}

#[tokio::test]
async fn authored_settings_select_the_next_provider_without_restarting_the_turn() -> Result<()> {
    use crate::model::{
        ModelDispatch, ModelRequest, ModelStream, NativeToolProfile, PreparedModelRequest,
    };

    struct Native {
        family: NativeToolProfile,
        profile: crate::model::ModelProfileBinding,
        provider: Arc<dyn ModelProvider>,
    }
    impl ModelProvider for Native {
        fn native_tool_profile(&self, _: &Model) -> Result<Option<NativeToolProfile>> {
            Ok(Some(self.family))
        }
        fn model_profile(&self, _: &Model) -> Result<Option<crate::model::ModelProfileBinding>> {
            Ok(Some(self.profile))
        }
        fn bind_content(
            &self,
            reader: Arc<dyn ContentResidencyVerifier>,
            maximum: Option<u64>,
        ) -> Option<Arc<dyn ModelProvider>> {
            self.provider.bind_content(reader, maximum).map(|provider| {
                Arc::new(Self {
                    family: self.family,
                    profile: self.profile,
                    provider,
                }) as Arc<dyn ModelProvider>
            })
        }
        fn context_capacity(&self, model: &Model) -> Result<crate::context::ModelContextCapacity> {
            self.provider.context_capacity(model)
        }
        fn count_tokens<'a>(
            &'a self,
            request: &'a PreparedModelRequest,
            ceiling: u64,
        ) -> ModelStream<'a> {
            self.provider.count_tokens(request, ceiling)
        }
        fn generate<'a>(
            &'a self,
            request: &'a PreparedModelRequest,
            dispatch: ModelDispatch,
        ) -> ModelStream<'a> {
            self.provider.generate(request, dispatch)
        }
    }

    fn reasoning_profile(name: &str, sampling: &str) -> Result<crate::model::ModelProfileBinding> {
        use crate::profile::{AgentOverride, ModelProfile, ProjectLayer, RoleFile, resolve};
        let model = ModelProfile::parse(
            &format!(
                "---\nversion: 1\ntools: []\nreasoning: {{mode: effort, effort: medium}}\nsampling: {sampling}\ncache: automatic\nwatermark_tokens: 4096\n---\n"
            ),
            &format!("---\nversion: 1\nkey: scripted/{name}\n---\n"),
            capacity(),
            "settings-profile",
        )?;
        let role = RoleFile::parse(&format!(
            "---\nversion: 1\nname: worker\nmodel: scripted/{name}\ncontext: fork\nmay_spawn: []\n---\n"
        ))?;
        let project = ProjectLayer::parse("---\nversion: 1\n---\n")?;
        let approvals = crate::approvals::ApprovalPolicy::default().bind()?;
        let agent = AgentOverride::parse("---\nversion: 1\n---\n", &approvals)?;
        resolve(&model, &role, &project, &agent)?.bind_model(
            &Model::new("scripted".into(), name.into(), "1".into())?,
            capacity(),
        )
    }

    fn efforts(request: &ModelRequest) -> Vec<String> {
        request
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .filter_map(|part| {
                if matches!(part, crate::model::ModelContentPart::Configuration { .. }) {
                    Some(
                        serde_json::to_value(part).unwrap()["effort"]
                            .as_str()
                            .unwrap()
                            .to_owned(),
                    )
                } else {
                    None
                }
            })
            .collect()
    }

    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::create_dir(directory.path().join(".git"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::write(
        directory.path().join("source.txt"),
        "A source retained by the request",
    )
    .map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::write(directory.path().join("AGENTS.md"), "Initial instruction.\n")
        .map_err(|error| Error::Storage(error.to_string()))?;
    let pause = Arc::new(PauseBeforeReply::default());
    let first = pause.provider(ScriptedModelProvider::new([
        ScriptedTurn::Text("ready".into()),
        code("select-settings", r#"
            import {write} from 'acyclic';
            await write('volume:///project/AGENTS.md', 'Updated instruction.\n', 'any');
            await write('volume:///home/plugins/step-guard/plugin.ts', "export default {name:'step-guard',description:'Selected tool context',matcher:{tool:{action:['exec'],command:'selected-native'}}};", 'any');
            await write('volume:///home/plugins/step-guard/hooks/before-tool.sh', `printf '%s' '{"hookSpecificOutput":{"additionalContext":"selected-step-hook"}}'`, 'any');
            await write('volume:///home/plugins/step-guard/hooks/before-tool.ps1', `[Console]::Out.Write('{"hookSpecificOutput":{"additionalContext":"selected-step-hook"}}')`, 'any');
            await write('volume:///home/plugins.ts', "export default base => ({...base,'step-guard':{enabled:true}});", 'any');
            await write('volume:///home/agent.ts', "export default {model: 'scripted/second', effort: 'low', sampling: {temperature: 0.75}};", 'any');
            export default 'selected';
        "#),
    ]).reading_content().with_context_capacity(capacity()));
    let second = pause.provider(
        ScriptedModelProvider::new([])
            .reading_content()
            .with_context_capacity(capacity()),
    );
    let first_route: Arc<dyn ModelProvider> = Arc::new(Native {
        family: NativeToolProfile::Anthropic,
        profile: reasoning_profile("test", "{}")?,
        provider: first.clone(),
    });
    let second_route: Arc<dyn ModelProvider> = Arc::new(Native {
        family: NativeToolProfile::Openai,
        profile: reasoning_profile("second", "{temperature: 0.75}")?,
        provider: second.clone(),
    });
    let reasoning_baseline = second_route.model_profile(&model()?)?.unwrap().reasoning();
    let models = Arc::new(crate::model::ModelResolver::new("scripted/test", {
        let first = Arc::clone(&first_route);
        let second = Arc::clone(&second_route);
        move |selection| {
            let (name, provider) = match selection {
                "scripted/test" => ("test", Arc::clone(&first)),
                "scripted/second" => ("second", Arc::clone(&second)),
                _ => return Err(Error::NotFound(selection.into())),
            };
            Ok((
                Model::new("scripted".into(), name.into(), "1".into())?,
                provider,
            ))
        }
    }));
    let mut config = checkout_config(directory.path(), (model()?, first_route))?;
    config.model = None;
    config.models = Some((models, None));
    let harness = Arc::new(super::open_scripted(config).await?);
    let executable = acyclic_native_runtime::platform_shell()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let mut command = vec![
        executable
            .to_str()
            .ok_or_else(|| Error::Invalid("test shell is not Unicode".into()))?
            .to_owned(),
    ];
    command.extend(
        crate::tools::shell::PLATFORM
            .arguments
            .iter()
            .map(|argument| (*argument).to_owned()),
    );
    command.push(
        if cfg!(windows) {
            "[Console]::Out.Write('selected-native')"
        } else {
            "printf selected-native"
        }
        .to_owned(),
    );
    second.extend([
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                "selected-read".into(),
                "read".into(),
                json!({"path": "volume:///home/agent.ts"}),
            ))),
            ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                "selected-shell".into(),
                "shell".into(),
                json!({"command": command}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }),
        ]),
        ScriptedTurn::Text("selected provider".into()),
        ScriptedTurn::Text("inherited provider".into()),
    ]);
    let connection = harness.connect().await?;
    pause.arm(&harness, connection.root())?;
    let AgentReply::Spawned { agent: worker, .. } = connection
        .execute(&AgentCommand::Spawn {
            parent: connection.root().clone(),
            id: crate::agent::IntentId::try_from("settings-author")
                .map_err(|error| Error::Invalid(error.to_string()))?,
            brief: ModelContent::Text("Prepare to select a model".into()),
            role: "worker".into(),
        })
        .await?
    else {
        return Err(Error::Invalid("settings worker did not open".into()));
    };
    agents::settled(&harness, &worker).await?;
    assert_eq!(first.request_count(), 1);
    let conversation = Conversation::open(connection.select(worker.clone()).await?);
    let mut observed = ObservedConversation::observe(conversation, None).await?;
    let prompt = "Write and use your model settings @source.txt";
    let entry = observed.send("select-model", prompt).await?;
    assert!(entry.outcome.is_completed(), "{:?}", entry.outcome);
    assert_eq!(
        observed.conversation.output_text(&entry).await?,
        "selected provider"
    );
    assert_eq!(
        first.request_count(),
        2,
        "the preceding provider must not run another step"
    );
    assert_eq!(second.request_count(), 2);
    let before = first.request(1).unwrap();
    let after = second.request(0).unwrap();
    let finished = second.request(1).unwrap();
    let hook_contexts = |request: &ModelRequest| {
        request.messages.iter().filter(|message| {
            matches!(&message.content, ModelContent::Text(text) if text == "selected-step-hook")
        }).count()
    };
    assert_eq!(hook_contexts(&after), 0);
    assert_eq!(hook_contexts(&finished), 1);
    assert!(
        super::request_text(&harness, &before)
            .await?
            .contains("Initial instruction.")
    );
    let instruction_updates = after
        .messages
        .iter()
        .filter_map(|message| message.content.notice_parts())
        .flat_map(|(notices, _)| notices)
        .filter(|notice| notice.kind == crate::context::CONTEXT_CHANGED)
        .filter_map(|notice| notice.body.get("text").and_then(serde_json::Value::as_str))
        .filter(|text| text.contains("Updated instruction."))
        .count();
    assert_eq!(instruction_updates, 1);
    assert_eq!(result(&after, "select-settings")?, "selected");
    assert_eq!(after.model.name, "second");
    assert_eq!(after.sampling.temperature, Some(0.75));
    assert!(efforts(&before).is_empty());
    assert_eq!(efforts(&after), ["low"]);
    assert_eq!(efforts(&finished), ["low"]);
    assert!(
        after
            .messages
            .iter()
            .any(|message| message.content.file_parts().next().is_some()),
        "the selected provider must consume retained files"
    );
    assert_eq!(system_prefix(&before)?, system_prefix(&after)?);
    assert_eq!(system_prefix(&before)?, system_prefix(&finished)?);
    let before_shell = before
        .model_tools()
        .find(|tool| tool.name == "shell")
        .unwrap();
    let after_shell = after
        .model_tools()
        .find(|tool| tool.name == "shell")
        .unwrap();
    assert_eq!(
        before_shell.input_schema["properties"]["command"]["type"],
        "string"
    );
    assert_eq!(
        after_shell.input_schema["properties"]["command"]["type"],
        "array"
    );
    let read = finished
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .find_map(|part| match part {
            crate::model::ModelContentPart::ToolResult {
                call_id, content, ..
            } if call_id == "selected-read" => Some(content),
            _ => None,
        });
    assert!(
        matches!(
            read,
            Some(
                crate::model::ToolResultContent::Json { .. }
                    | crate::model::ToolResultContent::Parts { .. }
            )
        ),
        "restored canonical read failed: {read:?}"
    );
    let shell = result(&finished, "selected-shell")?;
    assert_eq!(shell["exit_code"], 0);
    assert_eq!(shell["stdout"], "selected-native");
    let before = first.dispatches()[1];
    let after = second.dispatches()[0];
    assert_eq!(before.operation_id, after.operation_id);
    assert_eq!((before.step, after.step), (0, 1));
    assert_eq!(second.dispatches()[1].operation_id, after.operation_id);
    assert_eq!(second.dispatches()[1].step, 2);
    assert_eq!(
        observed.conversation.send("select-model", prompt).await?,
        entry.inbox
    );
    assert_eq!((first.request_count(), second.request_count()), (2, 2));
    pause.arm(&harness, &worker)?;
    let AgentReply::Spawned { agent: child, .. } = connection
        .execute(&AgentCommand::Fork {
            continuation: None,
            parent: worker.clone(),
            id: crate::agent::IntentId::try_from("settings-reader")
                .map_err(|error| Error::Invalid(error.to_string()))?,
            at: 1,
            brief: ModelContent::Text("Use your inherited settings".into()),
            role: "worker".into(),
        })
        .await?
    else {
        return Err(Error::Invalid("settings fork did not open".into()));
    };
    agents::settled(&harness, &child).await?;
    assert_eq!((first.request_count(), second.request_count()), (2, 3));
    let inherited = second.request(2).unwrap();
    assert_eq!(inherited.model.name, "second");
    assert_eq!(inherited.sampling.temperature, Some(0.75));
    assert_ne!(second.dispatches()[2].operation_id, after.operation_id);
    assert_eq!(efforts(&inherited), ["low"]);
    let frozen_prefix = system_prefix(&inherited)?;
    second.extend([ScriptedTurn::Text("Child notification received".into())]);
    let (agents, _) = harness.agents().await?;
    agents
        .pause(&worker, false)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    super::agents::settled(&harness, &worker).await?;
    second.extend([
        code("clear-effort", r#"
            import {write} from 'acyclic';
            await write('volume:///home/agent.ts', "export default {model: 'scripted/second', effort: null, sampling: {temperature: 0.75}};", 'any');
            export default 'cleared';
        "#),
        ScriptedTurn::Text("default effort".into()),
        code("restore-effort", r#"
            import {write} from 'acyclic';
            await write('volume:///home/agent.ts', "export default {model: 'scripted/second', effort: 'low', sampling: {temperature: 0.75}};", 'any');
            export default 'restored';
        "#),
        ScriptedTurn::Text("low effort".into()),
    ]);
    let cleared = observed
        .send("clear-effort", "Clear your effort override")
        .await?;
    assert!(cleared.outcome.is_completed(), "{:?}", cleared.outcome);
    assert_eq!(
        observed.conversation.output_text(&cleared).await?,
        "default effort"
    );
    let default_request = second.request(5).unwrap();
    assert_eq!(efforts(&default_request), ["low", "medium"]);
    assert_eq!(system_prefix(&default_request)?, frozen_prefix);
    let restored = observed
        .send("restore-effort", "Restore your effort override")
        .await?;
    assert!(restored.outcome.is_completed(), "{:?}", restored.outcome);
    assert_eq!(
        observed.conversation.output_text(&restored).await?,
        "low effort"
    );
    let restored_request = second.request(7).unwrap();
    assert_eq!(efforts(&restored_request), ["low", "medium", "low"]);
    assert_eq!(system_prefix(&restored_request)?, frozen_prefix);
    assert_eq!(
        observed
            .conversation
            .send("clear-effort", "Clear your effort override")
            .await?,
        cleared.inbox
    );
    assert_eq!(
        observed
            .conversation
            .send("restore-effort", "Restore your effort override")
            .await?,
        restored.inbox
    );
    assert_eq!((first.request_count(), second.request_count()), (2, 8));
    first.extend([
        ScriptedTurn::Text("first model with high effort".into()),
        code("return-second", r#"
            import {write} from 'acyclic';
            await write('volume:///home/agent.ts', "export default {model: 'scripted/second', effort: null, sampling: {temperature: 0.75}};", 'any');
            export default 'second';
        "#),
        ScriptedTurn::Text("first model default effort".into()),
    ]);
    second.extend([
        code("return-first", r#"
            import {write} from 'acyclic';
            await write('volume:///home/agent.ts', "export default {model: 'scripted/test', effort: 'high', sampling: null};", 'any');
            export default 'first';
        "#),
        ScriptedTurn::Text("second model default effort".into()),
        code("reset-first", r#"
            import {write} from 'acyclic';
            await write('volume:///home/agent.ts', "export default {model: 'scripted/test', effort: null, sampling: null};", 'any');
            export default 'reset';
        "#),
    ]);
    for (identity, prompt, output) in [
        (
            "return-first",
            "Return to the first model with high effort",
            "first model with high effort",
        ),
        (
            "return-second",
            "Return to the second model with default effort",
            "second model default effort",
        ),
        (
            "reset-first",
            "Return to the first model with default effort",
            "first model default effort",
        ),
    ] {
        let entry = observed.send(identity, prompt).await?;
        assert!(entry.outcome.is_completed(), "{:?}", entry.outcome);
        assert_eq!(observed.conversation.output_text(&entry).await?, output);
    }
    let returned = first.request(4).unwrap();
    assert_eq!(returned.model.name, "test");
    assert_eq!(returned.sampling.temperature, None);
    assert_eq!(system_prefix(&returned)?, frozen_prefix);
    let configuration: Vec<_> = returned
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            crate::model::ModelContentPart::Configuration { prefix, .. } => Some(*prefix),
            _ => None,
        })
        .collect();
    assert_eq!(configuration.len(), 6);
    assert_ne!(configuration[0], configuration[3]);
    assert_eq!(configuration[0], configuration[4]);
    assert_eq!(configuration[3], configuration[5]);
    assert_eq!(
        efforts(&returned),
        ["low", "medium", "low", "high", "medium", "medium"]
    );
    assert_eq!((first.request_count(), second.request_count()), (5, 11));
    assert_eq!(
        second_route.model_profile(&model()?)?.unwrap().reasoning(),
        reasoning_baseline
    );
    let command = if cfg!(windows) {
        "[Console]::Out.Write('selected-native')"
    } else {
        "printf selected-native"
    };
    let probe = format!(
        "export default await tools.shell({{command:{}}});",
        serde_json::to_string(command).map_err(|error| Error::Invalid(error.to_string()))?
    );
    first.extend([
        code("disable-step-hook", &format!(
            "import {{write}} from 'acyclic'; await write('volume:///home/plugins.ts', \"export default base => ({{...base,'step-guard':{{enabled:false}}}});\", 'any'); {probe}"
        )),
        code("probe-disabled-hook", &probe),
        ScriptedTurn::Text("hook disabled".into()),
    ]);
    let disabled = observed
        .send(
            "disable-step-hook",
            "Disable the plugin without changing models",
        )
        .await?;
    assert!(disabled.outcome.is_completed(), "{:?}", disabled.outcome);
    assert_eq!(
        observed.conversation.output_text(&disabled).await?,
        "hook disabled"
    );
    let before_disable = first.request(5).unwrap();
    let after_disable = first.request(6).unwrap();
    let disabled_result = first.request(7).unwrap();
    assert_eq!(
        hook_contexts(&after_disable),
        hook_contexts(&before_disable) + 1
    );
    assert_eq!(
        hook_contexts(&disabled_result),
        hook_contexts(&after_disable)
    );
    assert_eq!(
        result(&after_disable, "disable-step-hook")?["stdout"],
        "selected-native"
    );
    assert_eq!(
        result(&disabled_result, "probe-disabled-hook")?["stdout"],
        "selected-native"
    );
    assert_eq!(system_prefix(&after_disable)?, frozen_prefix);
    assert_eq!(system_prefix(&disabled_result)?, frozen_prefix);
    assert_eq!((first.request_count(), second.request_count()), (8, 11));
    Ok(())
}

#[tokio::test]
async fn refused_settings_preserve_the_published_file_and_running_model() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::create_dir(directory.path().join(".git"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    let pause = Arc::new(PauseBeforeReply::default());
    let provider = pause.provider(
        ScriptedModelProvider::new([ScriptedTurn::Text("ready".into())])
            .with_context_capacity(capacity()),
    );
    let harness = Arc::new(
        super::open_scripted(checkout_config(
            directory.path(),
            (model()?, provider.clone()),
        )?)
        .await?,
    );
    let connection = harness.connect().await?;
    pause.arm(&harness, connection.root())?;
    let AgentReply::Spawned { agent: worker, .. } = connection
        .execute(&AgentCommand::Spawn {
            parent: connection.root().clone(),
            id: crate::agent::IntentId::try_from("settings-writer")
                .map_err(|error| Error::Invalid(error.to_string()))?,
            brief: ModelContent::Text("Prepare to edit your settings".into()),
            role: "worker".into(),
        })
        .await?
    else {
        return Err(Error::Invalid("settings worker did not open".into()));
    };
    agents::settled(&harness, &worker).await?;
    assert_eq!(provider.request_count(), 1);
    provider.extend([
        code("prepare-settings", r#"
            import {write} from 'acyclic';
            await write('volume:///home/agent.ts', "export default {model:'scripted/test'};", 'any');
            export default 'prepared';
        "#),
        code("refuse-settings", r#"
            import {read, write} from 'acyclic';
            const path = 'volume:///home/agent.ts';
            const before = await read(path);
            if (before === null) throw new Error('original settings are absent');
            let refused = false;
            try { await write(path, 'export default {', before.version); }
            catch (error) {
                if (!(error instanceof Error) || error.class !== 'invalid') throw error;
                refused = true;
            }
            const after = await read(path);
            export default {
                refused,
                unchanged: after !== null && after.text() === before.text(),
                sameVersion: after !== null && after.version === before.version,
            };
        "#),
        ScriptedTurn::Text("configuration unchanged".into()),
    ]);
    let conversation = Conversation::open(connection.select(worker).await?);
    let mut observed = ObservedConversation::observe(conversation, None).await?;
    let entry = observed
        .send("refuse-settings", "Try an invalid settings change")
        .await?;
    assert!(entry.outcome.is_completed(), "{:?}", entry.outcome);
    assert_eq!(
        observed.conversation.output_text(&entry).await?,
        "configuration unchanged"
    );
    assert_eq!(provider.request_count(), 4);
    let before = provider.request(1).unwrap();
    let after = provider.request(3).unwrap();
    assert_eq!(
        result(&provider.request(2).unwrap(), "prepare-settings")?,
        "prepared"
    );
    assert_eq!(
        result(&after, "refuse-settings")?,
        &json!({
            "refused": true, "unchanged": true, "sameVersion": true,
        })
    );
    assert_eq!(after.model, before.model);
    assert_eq!(system_prefix(&after)?, system_prefix(&before)?);
    assert_eq!(
        observed
            .conversation
            .send("refuse-settings", "Try an invalid settings change")
            .await?,
        entry.inbox
    );
    assert_eq!(provider.request_count(), 4);
    Ok(())
}

#[tokio::test]
async fn authored_lifecycle_hooks_run_once_through_the_original_turn() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::create_dir(directory.path().join(".git"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    let pause = Arc::new(PauseBeforeReply::default());
    let provider = pause.provider(
        ScriptedModelProvider::new([ScriptedTurn::Text("ready".into())])
            .with_context_capacity(capacity()),
    );
    let harness = Arc::new(
        super::open_scripted(checkout_config(
            directory.path(),
            (model()?, provider.clone()),
        )?)
        .await?,
    );
    let connection = harness.connect().await?;
    pause.arm(&harness, connection.root())?;
    let AgentReply::Spawned { agent: worker, .. } = connection
        .execute(&AgentCommand::Spawn {
            parent: connection.root().clone(),
            id: crate::agent::IntentId::try_from("hook-writer")
                .map_err(|error| Error::Invalid(error.to_string()))?,
            brief: ModelContent::Text("Prepare to author a hook".into()),
            role: "worker".into(),
        })
        .await?
    else {
        return Err(Error::Invalid("hook worker did not open".into()));
    };
    agents::settled(&harness, &worker).await?;
    assert_eq!(provider.request_count(), 1);
    provider.extend([
        code("author-hook", r#"
            import {write} from 'acyclic';
            await write('volume:///home/hooks/context/program.ts', {
                version: 1, name: 'context', description: 'Contribute tool context',
                inputSchema: {type: 'object'},
                entry: {sh: 'main.sh', ps1: 'main.ps1'},
                on: {kind: 'lifecycle', event: 'PreToolUse'},
            }, 'absent');
            await write('volume:///home/hooks/context/main.sh', `printf '%s' '{"hookSpecificOutput":{"additionalContext":"authored-hook-context"}}'`, 'absent');
            await write('volume:///home/hooks/context/main.ps1', `[Console]::Out.Write('{"hookSpecificOutput":{"additionalContext":"authored-hook-context"}}')`, 'absent');
            export default 'authored';
        "#),
        code("trigger-hook", "export default 'triggered';"),
        ScriptedTurn::Text("hook delivered".into()),
    ]);
    let conversation = Conversation::open(connection.select(worker).await?);
    let mut observed = ObservedConversation::observe(conversation, None).await?;
    let entry = observed
        .send("author-lifecycle-hook", "Write and run a lifecycle hook")
        .await?;
    assert!(entry.outcome.is_completed(), "{:?}", entry.outcome);
    assert_eq!(provider.request_count(), 4);
    assert_eq!(
        observed.conversation.output_text(&entry).await?,
        "hook delivered"
    );
    let before = provider.request(2).unwrap();
    let after = provider.request(3).unwrap();
    assert_eq!(result(&before, "author-hook")?, "authored");
    assert_eq!(result(&after, "trigger-hook")?, "triggered");
    let contributions = |request: &crate::model::ModelRequest| {
        request.messages.iter().filter(|message| {
            matches!(&message.content, ModelContent::Text(text) if text == "authored-hook-context")
        }).count()
    };
    assert_eq!(contributions(&before), 0);
    assert_eq!(contributions(&after), 1);
    assert_eq!(before.operation_id, after.operation_id);
    assert_eq!(system_prefix(&before)?, system_prefix(&after)?);
    assert_eq!(
        observed
            .conversation
            .send("author-lifecycle-hook", "Write and run a lifecycle hook")
            .await?,
        entry.inbox
    );
    assert_eq!(provider.request_count(), 4);
    Ok(())
}

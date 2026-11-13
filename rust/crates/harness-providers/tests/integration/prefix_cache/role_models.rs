//! Full shared-host run: files select roles, model spawns commit their selection,
//! both HTTP adapters execute, and a new host resumes the retained child.
use super::*;
use acyclic_harness::model::{ModelBinding, ModelResolver};

struct Routes {
    /// The shared fixture catalog: the root's recorded binding serves its own
    /// selection and every shipped role model.
    roles: ModelResolver,
    child: Arc<acyclic_harness_providers::ResolvedModel>,
    misroute: bool,
}

impl Routes {
    fn resolve(&self, selection: &str) -> acyclic_harness::Result<ModelBinding> {
        match selection {
            "workers/luna" if self.misroute => self.roles.resolve("recorded/model"),
            "workers/luna" => Ok((self.child.model().clone(), self.child.clone())),
            _ => self.roles.resolve(selection),
        }
    }
}

fn scripted(path: &'static str, steps: Vec<Step>) -> Arc<Scripted> {
    Arc::new(Scripted {
        path,
        state: Mutex::new(("agent", steps.into(), Vec::new())),
        cache: Mutex::default(),
    })
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Budgets {
    Automatic,

    AutomaticUnknown,
}

async fn run_roles(adapter: Adapter, misroute: bool, budgets: Budgets) -> TestResult<bool> {
    let parent = scripted(
        "messages",
        vec![
            text("root prelude"),
            retrospective(),
            Step {
                tools: [
                    ("kid", json!({"task":"Count","name":"kid","role":"worker"})),
                    ("explicit", json!({"task":"Override","name":"explicit","role":"advisor","model":"recorded/model"})),
                    ("inherited", json!({"task":"Inherit","name":"inherited","role":"advisor"})),
                ].into_iter().map(|(id, arguments)| Tool {
                    id: id.into(), name: "spawn".into(), namespace: None, arguments,
                }).collect(),
                ..Step::default()
            },
            text("delegated"),
            retrospective(),
        ],
    );
    let native_parts = vec![
        json!({"thought":true,"text":"Count privately","thoughtSignature":"signed-thought+/=="}),
        json!({"functionCall":{"id":"add","name":"code","args":{"code":"1 + 1"}},"thoughtSignature":"signed-tool+/=="}),
    ];
    let (path, first) = if adapter == Adapter::Gemini {
        (
            "models/luna:streamGenerateContent",
            Step {
                gemini: Some(harness_mock_model::Gemini {
                    parts: native_parts.clone(),
                    thought_tokens: 2,
                    ..Default::default()
                }),
                ..Step::default()
            },
        )
    } else {
        ("responses", program("add", "1 + 1"))
    };
    let child = scripted(path, vec![first, text("two"), retrospective()]);
    let root_model = Arc::new(resolve(
        Adapter::Anthropic,
        PromptCache::Native,
        parent.clone(),
    )?);
    let mut catalog = registry(adapter, PromptCache::Native, child.clone())?;
    let mut spec = catalog
        .catalog()
        .get("recorded")
        .ok_or("catalog")?
        .as_ref()
        .clone();
    let mut model = spec.models.remove("model").ok_or("model")?;
    spec.models.insert("replacement".into(), model.clone());
    // Synthetic provider units count wire bytes; this smaller child capacity
    // still admits the full real tool prefix.
    model.capacity.context_tokens = 16_384;
    model.capacity.output_tokens = match budgets {
        Budgets::AutomaticUnknown => None,
        _ => Some(1024),
    };
    spec.models.insert("luna".into(), model);
    catalog.register("workers".into(), spec)?;
    let routes = Arc::new(Routes {
        roles: acyclic_harness::test_support::role_models((
            root_model.model().clone(),
            root_model.clone(),
        ))?,
        child: Arc::new(catalog.resolve("workers/luna")?),
        misroute,
    });
    let selected = ["code", "spawn"].map(String::from);
    let make_executors = |root: Arc<acyclic_harness_providers::ResolvedModel>| -> TestResult<Arc<dyn TurnExecutors>> {
        let tools = acyclic_harness::tools::registry(&selected, |_| false, Vec::new())?;
        let limits = Limits::default();
        let grants = acyclic_harness::tools::call_grants(&tools, selected.iter().map(String::as_str))?;
        let compaction = None;
        let executors = acyclic_harness::filesystem::StockExecutors::new(
            (root.model().clone(), root), tools, (limits, compaction),
            Arc::new(acyclic_harness::filesystem::Grants(grants)),
        ).with_models(Arc::new(ModelResolver::new("recorded/model", {
            let routes = routes.clone();
            move |selection| routes.resolve(selection)
        }).with_binding(routes.resolve("workers/luna")?)?)).with_agent_tools(&selected).with_templates();
        Ok(Arc::new(executors))
    };
    let stream = Arc::new(MemoryStream::default());
    let provider = ProviderRef::new("agents".into(), "filesystem".into(), "2".into(), &mut None)?;
    let filesystem = acyclic_fs::Fs::new(
        acyclic_fs::StreamAuthorityStore::new(stream.clone()),
        acyclic_fs::LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory())),
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    let make_bound = |executors| -> TestResult<_> {
        Ok(Bound::local_root(
            acyclic_harness::core::AuthorityRoot::new(
                "agents",
                [7; 32],
                acyclic_harness::core::Authority {
                    kind: acyclic_harness::core::AggregateKind::Conversation,
                    id: "local".into(),
                },
            ),
            Placement {
                filesystem: filesystem.clone(),
                provider: provider.clone(),
                within: acyclic_actors::addressing::ActorReference::new(
                    b"account", b"agents", b"owner",
                )?,
            },
            Turns {
                routing: None,
                executors,
                limits: TurnLimits {
                    maximum_bytes: Some(1 << 20),
                    history_turns: 8,
                },
                actors: acyclic_actors::domain::ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
                clock: Arc::new(acyclic_actors::local::SystemClock),
                approval_policy: ApprovalPolicy::accept_all("roles"),
            },
        )?)
    };
    let bound = make_bound(make_executors(root_model.clone())?)?;
    let id = AgentId::from_bytes([1; 16]);
    let RootSubmission::Created(root) = bound
        .create_root(
            &intent("root"),
            RootRequest {
                peer: true,
                agent: id.clone(),
                fresh: vec![acyclic_harness::agent::WorkspaceEntry::initial_volume(
                    acyclic_harness_core::agent::WorkspaceRole::Private,
                    PRIVATE_VOLUME.into(),
                    VolumeRef::new(
                        provider.clone(),
                        "agent-private".into(),
                        VolumeClass::AgentPrivate,
                        VolumeOwner::Agent(acyclic_harness::AgentId::from_bytes(id.to_bytes())),
                        &mut None,
                    )?,
                )],
                retained: Vec::new(),
                brief: None,
                policy: None,
            },
        )
        .await?
    else {
        return Err("uncertain root".into());
    };
    bound.agents().drain(&root).await?;
    assert_eq!(
        bound
            .agents()
            .state(&root)
            .await?
            .profile
            .ok_or("root profile")?
            .model,
        acyclic_harness::templates::root()?.model
    );
    // A private role file, read by the original authorized template reader.
    let state = bound.agents().state(&root).await?;
    let authority = acyclic_harness::filesystem::agent_authority(
        bound.issuer(),
        bound.admission(),
        &root,
        &state.volumes,
        &[],
    )
    .await?;
    let files =
        acyclic_harness::filesystem::FilesystemHost::new(filesystem.clone(), &provider, None)?;
    let writer = acyclic_harness::filesystem::FilesystemContentPublisher::new(
        files,
        authority.private.volume()?,
        &authority.verifier,
        &authority.scope,
        1 << 20,
        None,
    )?;
    use acyclic_harness::conversation::{ContentAccess, ContentPublisher};
    writer.stage(acyclic_harness::OperationId::new(),"templates/worker/AGENT.md",
        b"---\nversion: 1\ncontext: fork\nmay_spawn: [worker]\nmodel: workers/luna\n---\nCount carefully.",
        "text/markdown","AGENT.md",ContentAccess::default())?.await?;
    // A role without a model: an explicit request is its fallback, otherwise
    // the child inherits its parent's model.
    writer
        .stage(
            acyclic_harness::OperationId::new(),
            "templates/advisor/AGENT.md",
            b"---\nversion: 1\ncontext: fresh\nmay_spawn: []\n---\nAdvise.",
            "text/markdown",
            "AGENT.md",
            ContentAccess::default(),
        )?
        .await?;
    let prepare = bound
        .stage_text(
            &root,
            "message",
            &user(),
            &intent("prepare"),
            ("Prepare", "Prepare"),
        )
        .await?;
    bound
        .agents()
        .send(
            &root,
            &user(),
            &InboxRecord::Message(Message {
                from: user(),
                id: intent("prepare"),
                body: Box::new(prepare),
                re: None,
                attachments: None,
            }),
        )
        .await?;
    bound.agents().drain(&root).await?;
    let body = bound
        .stage_text(
            &root,
            "message",
            &user(),
            &intent("work"),
            ("Count", "Count"),
        )
        .await?;
    bound
        .agents()
        .send(
            &root,
            &user(),
            &InboxRecord::Message(Message {
                from: user(),
                id: intent("work"),
                body: Box::new(body),
                re: None,
                attachments: None,
            }),
        )
        .await?;
    bound.agents().drain(&root).await?;
    if let acyclic_harness::agent::Phase::Done {
        outcome: acyclic_harness::agent::AgentOutcome::Failed { message },
    } = bound.agents().state(&root).await?.phase
    {
        return Err(format!(
            "root failed ({adapter:?}, {budgets:?}, misroute={misroute}): {message}"
        )
        .into());
    }
    let kid = bound
        .agents()
        .resolve_spawn_name(&root, "kid")
        .await?
        .live()
        .ok_or_else(|| {
            let requests = parent.state.lock().unwrap_or_else(|p| p.into_inner());
            format!(
                "missing child ({adapter:?}, {budgets:?}, misroute={misroute}); root requests: {:?}",
                requests.2.iter().map(|request| String::from_utf8_lossy(&request.body)).collect::<Vec<_>>()
            )
        })?;
    let inherited = acyclic_harness::templates::root()?.model;
    for (name, model) in [
        ("explicit", Some("recorded/model")),
        ("inherited", inherited.as_deref()),
    ] {
        let path = bound
            .agents()
            .resolve_spawn_name(&root, name)
            .await?
            .live()
            .ok_or("missing sibling")?;
        assert_eq!(
            bound
                .agents()
                .state(&path)
                .await?
                .profile
                .ok_or("profile")?
                .model
                .as_deref(),
            model
        );
        bound.agents().drain(&path).await?;
    }
    let creation = bound
        .agents()
        .state(&kid)
        .await?
        .profile
        .ok_or("child profile")?;
    assert_eq!(creation.model.as_deref(), Some("workers/luna"));
    assert_eq!(creation.context, acyclic_harness::agent::SpawnContext::Fork);
    // Role changes cannot change an admitted child. Resume on a new composition
    // with a different root default; the child's stream remains authoritative.
    writer
        .stage(
            acyclic_harness::OperationId::new(),
            "templates/worker/AGENT.md",
            b"---\nversion: 1\ncontext: fork\nmay_spawn: []\nmodel: recorded/model\n---\nChanged.",
            "text/markdown",
            "AGENT.md",
            ContentAccess::default(),
        )?
        .await?;
    let parent_calls = parent
        .state
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .2
        .len();
    drop(bound);
    let resumed = make_bound(make_executors(Arc::new(
        catalog.resolve("workers/replacement")?,
    ))?)?;
    resumed.agents().drain(&kid).await?;
    drop(resumed);
    let resumed = make_bound(make_executors(root_model.clone())?)?;
    // The kid's own home is fresh, so its worker is the seed role: its model
    // wins, and without one descendants inherit their actual parent.
    let role = acyclic_harness::templates::seed_model("worker")?;
    assert_eq!(
        resumed
            .child_profile(&kid, "worker")
            .await?
            .model
            .as_deref(),
        acyclic_harness::templates::resolve_model(role.as_deref(), creation.model.as_deref())
    );
    let body = resumed
        .stage_text(
            &kid,
            "message",
            &user(),
            &intent("continue"),
            ("Continue", "Continue"),
        )
        .await?;
    resumed
        .agents()
        .send(
            &kid,
            &user(),
            &InboxRecord::Message(Message {
                from: user(),
                id: intent("continue"),
                body: Box::new(body),
                re: None,
                attachments: None,
            }),
        )
        .await?;
    resumed.agents().drain(&kid).await?;
    assert!(matches!(
        resumed.agents().state(&kid).await?.phase,
        acyclic_harness::agent::Phase::Idle
    ));
    let sent = child.state.lock().unwrap_or_else(|p| p.into_inner());
    // The negative control binds every child to the root provider. It completes
    // the same lifecycle but cannot satisfy this HTTP-boundary routing oracle.
    if misroute {
        let misrouted = parent
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .2
            .len()
            > parent_calls;
        return Ok(sent.2.is_empty() && misrouted);
    }
    assert!(sent.2.len() >= 3, "child never used its own provider");
    assert!(
        holds(&sent.2[0].body, b"root prelude"),
        "child did not fork the parent's settled history"
    );
    for request in &sent.2 {
        let body: Value = serde_json::from_slice(&request.body)?;
        if adapter == Adapter::Gemini {
            assert!(
                request
                    .url
                    .ends_with("/models/luna:streamGenerateContent?alt=sse")
            );
        } else {
            assert_eq!(body["model"], "luna");
        }
    }
    extends(
        "responses",
        "resumed child",
        &sent.2[0].body,
        &sent.2[1].body,
    )?;
    extends("responses", "later turn", &sent.2[1].body, &sent.2[2].body)?;
    assert!(sent.2[1].cached.read > 0);
    if adapter == Adapter::Gemini {
        let body: Value = serde_json::from_slice(&sent.2[2].body)?;
        let parts: Vec<_> = body["contents"]
            .as_array()
            .ok_or("contents")?
            .iter()
            .flat_map(|content| content["parts"].as_array().into_iter().flatten())
            .collect();
        for original in &native_parts {
            assert!(
                parts.contains(&original),
                "native reasoning or tool signature changed: {original}"
            );
        }
    }
    let parent = parent.state.lock().unwrap_or_else(|p| p.into_inner());
    extends(
        "anthropic",
        "root second turn",
        &parent.2[0].body,
        &parent.2[1].body,
    )?;
    extends(
        "anthropic",
        "root spawn continuation",
        &parent.2[1].body,
        &parent.2[2].body,
    )?;
    for request in &parent.2 {
        let body: Value = serde_json::from_slice(&request.body)?;
        assert_eq!(body["model"], "model");
    }
    Ok(true)
}

#[tokio::test]
async fn role_models_route_across_providers_and_resume_with_stable_prefixes() -> TestResult {
    assert!(run_roles(Adapter::Responses, false, Budgets::Automatic).await?);
    assert!(run_roles(Adapter::Gemini, false, Budgets::Automatic).await?);
    for budgets in [Budgets::AutomaticUnknown] {
        assert!(run_roles(Adapter::Responses, false, budgets).await?);
    }
    assert!(
        run_roles(Adapter::Responses, true, Budgets::Automatic).await?,
        "negative control did not expose forced parent routing"
    );
    Ok(())
}

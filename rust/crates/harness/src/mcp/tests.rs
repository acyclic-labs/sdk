use super::remote::RemoteTool;
use super::*;
use crate::{
    Capabilities, Outcome,
    conversation::Limits,
    runtime::{Bindings, RuntimeScope, TaskContext, TaskDefinition},
};
use crate::{
    Error, InteractionId, OperationId, Result,
    runtime::ToolContext,
    tool::{
        JsonToolProjection, Tool, ToolApprovalVerifier, ToolExecutor, ToolInvocation,
        ToolProjection, ToolRegistry,
    },
};
use acyclic_stream::BoxProviderFuture;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Transport(AtomicUsize);
impl McpToolTransport for Transport {
    fn call<'a>(
        &'a self,
        _: OperationId,
        _: &'a str,
        arguments: Value,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Value>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if let Some(context) = context {
                return Ok(context_result(&context, arguments));
            }
            Ok(
                json!({"content":[{"type":"text","text":"ok"}], "structuredContent":{"value":1}, "isError":false}),
            )
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: OperationId,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<Value>>> {
        Box::pin(
            async move { Ok(context.map(|context| context_result(&context, json!({"value":1})))) },
        )
    }
}
fn catalog(revision: &str, names: &[&str]) -> McpCatalog {
    McpCatalog { server:"fixture".into(), revision:revision.into(), schema_exposure:McpSchemaExposure::Eager,
        discovery:McpDiscoveryPolicy::Search, projection_schema:crate::tool::json_projection_schema(json!({})), tools:names.iter().map(|name| McpToolDefinition {
        name:(*name).into(), title:None, description:format!("Find {name}"), input_schema:json!({"type":"object"}),
        output_schema:Some(json!({"type":"object","required":["value"],"properties":{"value":{"type":"integer"}}})) }).collect() }
}

#[test]
fn catalog_pins_replaceable_projection_independently_of_remote_output() -> Result<()> {
    let mut selected = catalog("native", &["a"]);
    selected.projection_schema = json!({"type":"object","properties":{
        "kind":{"const":"parts"},"parts":{"type":"array","items":{"type":"object"}}},
        "required":["kind","parts"],"additionalProperties":false});
    let definition = selected.model_definitions(8, 8192)?.remove(0);
    assert_eq!(definition.projection_schema, selected.projection_schema);
    let digest = selected.installation_digest(None, 8, 8192)?;
    selected.projection_schema = crate::tool::json_projection_schema(json!({}));
    assert_ne!(digest, selected.installation_digest(None, 8, 8192)?);
    selected.tools.clear();
    selected.projection_schema = json!({"type":"invalid-type"});
    assert!(selected.validate(8, 8192).is_err());
    Ok(())
}

struct CatalogApproval {
    operation: OperationId,
    interaction: InteractionId,
    digest: [u8; 32],
    reads: AtomicUsize,
}

impl ToolApprovalVerifier for CatalogApproval {
    fn verify<'a>(
        &'a self,
        interaction: InteractionId,
        operation: OperationId,
        digest: [u8; 32],
    ) -> BoxProviderFuture<'a, Result<()>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if interaction != self.interaction
                || operation != self.operation
                || digest != self.digest
            {
                return Err(Error::Unauthorized("catalog approval differs".into()));
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn scoped_catalog_installation_and_replacement_require_exact_approval() -> Result<()> {
    use crate::core::{AggregateKind, Authority, AuthorityIssuer};
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "catalog".into(),
    };
    let issuer = AuthorityIssuer::new("catalog", [8; 32], authority.clone());
    let foreign = AuthorityIssuer::new("catalog", [9; 32], authority);
    let scope = issuer.issue(
        "installer",
        &Capabilities::new(["mcp:install:fixture"]),
        None,
        None,
        &mut None,
    )?;
    let missing = issuer.issue("installer", &Capabilities::default(), None, None, &mut None)?;
    let forged = foreign.issue(
        "installer",
        &Capabilities::new(["mcp:install:fixture"]),
        None,
        None,
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None).expect("verifier copy");
    let original = catalog("1", &["a", "b"]);
    let approval = CatalogApproval {
        operation: OperationId::from_bytes([46; 16]),
        interaction: InteractionId::from_bytes([47; 16]),
        digest: original.installation_digest(None, 8, 8192)?,
        reads: AtomicUsize::new(0),
    };
    let transport = Arc::new(Transport::default());
    let bound: Arc<dyn McpToolTransport> = transport.clone();
    let projection: Arc<dyn ToolProjection> = Arc::new(JsonToolProjection);
    let mut registry = ToolRegistry::default();
    for control in 0..=13 {
        let mut input = original.clone();
        let mut operation = approval.operation;
        let mut interaction = approval.interaction;
        let mut selected_scope = &scope;
        let mut maximum_tools = 8;
        let mut maximum_bytes = 8192;
        match control {
            0 => selected_scope = &missing,
            1 => selected_scope = &forged,
            2 => operation = OperationId::from_bytes([48; 16]),
            3 => interaction = InteractionId::from_bytes([48; 16]),
            4 => maximum_tools = 9,
            5 => maximum_bytes = 8193,
            6 => input.revision = "different".into(),
            7 => input.schema_exposure = McpSchemaExposure::Selected { names: vec![] },
            8 => input.discovery = McpDiscoveryPolicy::Disabled,
            9 => input.tools.pop().map(|_| ()).unwrap_or_default(),
            12 => {
                input.projection_schema =
                    crate::tool::json_projection_schema(json!({"type":"string"}));
            }
            10 => input
                .tools
                .first_mut()
                .ok_or_else(|| Error::Invalid("fixture".into()))?
                .description
                .push('!'),
            11 => {
                input
                    .tools
                    .first_mut()
                    .ok_or_else(|| Error::Invalid("fixture".into()))?
                    .input_schema = json!({"type":"object","additionalProperties":false});
            }
            _ => {}
        }
        let reads = approval.reads.load(Ordering::SeqCst);
        let result = input
            .install_scoped(
                &mut registry,
                None,
                &bound,
                &projection,
                operation,
                interaction,
                selected_scope,
                &verifier,
                &approval,
                maximum_tools,
                maximum_bytes,
            )
            .await;
        if control == 13 {
            result?;
        } else {
            assert!(result.is_err());
            assert!(registry.definitions(None)?.is_empty());
            assert!(registry.get("mcp.fixture.a", Some("1"), None)?.is_none());
            if control < 2 {
                assert_eq!(approval.reads.load(Ordering::SeqCst), reads);
            }
        }
        assert_eq!(transport.0.load(Ordering::SeqCst), 0);
    }
    let invocation = ToolInvocation {
        operation_id: OperationId::from_bytes([49; 16]),
        call_id: "call".into(),
        name: "mcp.fixture.a".into(),
        arguments: json!({}),
    };
    let installer = RuntimeScope::new(scope.capabilities().clone(), Limits::default())?;
    let tool = registry
        .get("mcp.fixture.a", None, None)?
        .ok_or_else(|| Error::NotFound("installed tool".into()))?;
    assert!(matches!(
        tool.executor.authorize(Some(&installer), &invocation),
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(registry.definitions(None)?.len(), 2);
    assert_eq!(transport.0.load(Ordering::SeqCst), 0);
    let mut replacement = catalog("2", &["a"]);
    replacement.schema_exposure = McpSchemaExposure::Selected { names: vec![] };
    let approval = CatalogApproval {
        operation: OperationId::from_bytes([50; 16]),
        interaction: InteractionId::from_bytes([51; 16]),
        digest: replacement.installation_digest(Some(&original), 8, 8192)?,
        reads: AtomicUsize::new(0),
    };
    let mut stale = original.clone();
    stale.discovery = McpDiscoveryPolicy::Disabled;
    for previous in [None, Some(&stale), Some(&original)] {
        let result = replacement
            .install_scoped(
                &mut registry,
                previous,
                &bound,
                &projection,
                approval.operation,
                approval.interaction,
                &scope,
                &verifier,
                &approval,
                8,
                8192,
            )
            .await;
        if previous == Some(&original) {
            result?;
        } else {
            assert!(matches!(result, Err(Error::Unauthorized(_))));
            assert_eq!(
                registry.definitions(None)?,
                original.model_definitions(8, 8192)?
            );
            assert!(registry.get("mcp.fixture.a", Some("2"), None)?.is_none());
        }
    }
    assert!(registry.definitions(None)?.is_empty());
    assert!(registry.get("mcp.fixture.a", Some("1"), None)?.is_some());
    assert!(registry.get("mcp.fixture.b", Some("1"), None)?.is_some());
    assert!(registry.get("mcp.fixture.a", Some("2"), None)?.is_some());
    Ok(())
}

fn context_result(context: &ToolContext, structured_content: Value) -> Value {
    json!({"content":[], "structuredContent":structured_content, "isError":false,
        "_meta":{"task":context.task().id(), "operation":context.operation_id(),
            "call":context.call_id(), "serverGrant":context.task().scope().grants().contains("mcp:call:fixture"),
            "durableTask":context.task().durable_task_id()}})
}

async fn check_context_dispatch(task: TaskContext, tool: &Tool) -> Result<()> {
    let operation = OperationId::from_bytes([17; 16]);
    let denied = Arc::new(task.scoped(Capabilities::new([] as [String; 0]), Limits::default())?);
    let task = Arc::new(task);
    let context = ToolContext::new(Arc::clone(&task), operation, "provider-call", None)?;
    let invocation = ToolInvocation {
        operation_id: operation,
        call_id: "provider-call".into(),
        name: tool.definition.name.clone(),
        arguments: json!({"value":1}),
    };
    let expected = json!({"task":task.id(), "operation":operation,
        "call":"provider-call", "serverGrant":true, "durableTask":null});
    let result = tool
        .executor
        .execute_with_context(context.clone(), invocation.clone())
        .await?;
    assert_eq!(result.value["_meta"], expected);
    let recovered = tool
        .executor
        .reconcile_with_context(context.clone(), invocation.clone())
        .await?
        .ok_or_else(|| Error::NotFound("context result".into()))?;
    assert_eq!(recovered.value["_meta"], expected);
    for wrong in [
        ToolContext::new(
            Arc::clone(&task),
            OperationId::from_bytes([18; 16]),
            "provider-call",
            None,
        )?,
        ToolContext::new(task, operation, "different-call", None)?,
        ToolContext::new(denied, operation, "provider-call", None)?,
    ] {
        assert!(matches!(
            tool.executor
                .execute_with_context(wrong.clone(), invocation.clone())
                .await,
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            tool.executor
                .reconcile_with_context(wrong, invocation.clone())
                .await,
            Err(Error::Unauthorized(_))
        ));
    }
    let mut invalid = invocation;
    invalid.arguments = json!({"value":"wrong"});
    assert!(matches!(
        tool.executor.execute_with_context(context, invalid).await,
        Err(Error::Invalid(_))
    ));
    Ok(())
}

#[tokio::test]
async fn admitted_mcp_tools_preserve_context_and_reject_identity_or_scope_changes() -> Result<()> {
    let transport = Arc::new(Transport::default());
    let bound: Arc<dyn McpToolTransport> = transport.clone();
    let projection: Arc<dyn ToolProjection> = Arc::new(JsonToolProjection);
    let mut registry = ToolRegistry::default();
    catalog("1", &["a"]).install(&mut registry, None, &bound, &projection, 8, 8192)?;
    let tool = registry
        .get("mcp.fixture.a", None, None)?
        .cloned()
        .ok_or_else(|| Error::NotFound("context fixture".into()))?;
    let definition = TaskDefinition::live("mcp.context", "1", move |task, (): ()| {
        let tool = tool.clone();
        async move {
            check_context_dispatch(task, &tool).await?;
            Ok(1_u32)
        }
    })?;
    let mut bindings = Bindings::local();
    bindings.tasks.register(definition)?;
    bindings.scope = RuntimeScope::new(
        Capabilities::new(["task:spawn:mcp.context@1", "mcp:call:fixture"]),
        Limits::default(),
    )?;
    let harness = bindings.build(None)?;
    let definition = harness.task::<(), u32>("mcp.context")?;
    assert_eq!(
        harness.spawn(&definition, ()).await?.result().await?,
        Outcome::Succeeded(1)
    );
    Ok(())
}
#[tokio::test]
async fn search_grants_no_authority_and_reconciliation_never_dispatches() -> Result<()> {
    let catalog = catalog("1", &["c", "a", "b"]);
    assert_eq!(
        catalog.search("find", None, 1, 8, 8192)?[0].name,
        "mcp.fixture.a"
    );
    assert_eq!(
        catalog.search("find", Some("a"), 1, 8, 8192)?[0].name,
        "mcp.fixture.b"
    );
    let transport = Arc::new(Transport::default());
    let bound: Arc<dyn McpToolTransport> = transport.clone();
    let projection: Arc<dyn ToolProjection> = Arc::new(JsonToolProjection);
    let mut registry = ToolRegistry::default();
    catalog.install(&mut registry, None, &bound, &projection, 8, 8192)?;
    let tool = registry
        .get("mcp.fixture.a", None, None)?
        .ok_or_else(|| Error::NotFound("fixture".into()))?;
    let invocation = ToolInvocation {
        operation_id: OperationId::from_bytes([1; 16]),
        call_id: "call".into(),
        name: tool.definition.name.clone(),
        arguments: json!({}),
    };
    assert!(tool.executor.authorize(None, &invocation).is_err());
    let scope = RuntimeScope::new(Capabilities::new(["mcp:call:other"]), Limits::default())?;
    assert!(tool.executor.authorize(Some(&scope), &invocation).is_err());
    let scope = RuntimeScope::new(Capabilities::new(["mcp:call:fixture"]), Limits::default())?;
    tool.executor.authorize(Some(&scope), &invocation)?;
    assert!(tool.executor.reconcile(invocation.clone()).await?.is_none());
    assert_eq!(transport.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        tool.executor.execute(invocation).await?.value["structuredContent"]["value"],
        1
    );
    assert_eq!(transport.0.load(Ordering::SeqCst), 1);
    Ok(())
}
#[test]
fn schema_bounds_namespace_and_result_negative_controls() {
    let mut input = catalog("1", &["a"]);
    assert!(input.validate(1, 1).is_err());
    input.server = "fixture.other".into();
    assert!(input.validate(1, 8192).is_err());
    input.server = "fixture".into();
    input.tools.push(input.tools[0].clone());
    assert!(input.validate(8, 8192).is_err());
    input.tools.pop();
    input.tools[0].input_schema = json!({"type":"unknown"});
    assert!(input.validate(1, 8192).is_err());
    let tool = RemoteTool {
        server: "fixture".into(),
        name: "a".into(),
        output_schema: catalog("1", &["a"]).tools.remove(0).output_schema,
        transport: Arc::new(Transport::default()),
    };
    assert!(tool.result(json!({"content":[], "isError":false})).is_err());
    assert!(
        tool.result(json!({"content":[], "structuredContent":{"value":"wrong"}, "isError":false}))
            .is_err()
    );
    assert!(tool.result(json!({"content":[], "isError":true})).is_ok());
}

#[tokio::test]
async fn reload_during_remote_call_retains_admitted_transport() -> Result<()> {
    struct PendingTransport {
        response: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<Value>>>,
    }
    impl McpToolTransport for PendingTransport {
        fn call<'a>(
            &'a self,
            _: OperationId,
            _: &'a str,
            _: Value,
            _: Option<ToolContext>,
        ) -> BoxProviderFuture<'a, Result<Value>> {
            Box::pin(async move {
                let response = self
                    .response
                    .lock()
                    .map_err(|_| Error::Invalid("fixture lock".into()))?
                    .take()
                    .ok_or_else(|| Error::Invalid("fixture dispatched twice".into()))?;
                response
                    .await
                    .map_err(|_| Error::Invalid("fixture canceled".into()))
            })
        }
        fn reconcile<'a>(
            &'a self,
            _: OperationId,
            _: Option<ToolContext>,
        ) -> BoxProviderFuture<'a, Result<Option<Value>>> {
            Box::pin(async { Ok(None) })
        }
    }
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let old_transport: Arc<dyn McpToolTransport> = Arc::new(PendingTransport {
        response: std::sync::Mutex::new(Some(receiver)),
    });
    let new_transport: Arc<dyn McpToolTransport> = Arc::new(Transport::default());
    let projection: Arc<dyn ToolProjection> = Arc::new(JsonToolProjection);
    let mut registry = ToolRegistry::default();
    let old_catalog = catalog("1", &["a"]);
    old_catalog.install(&mut registry, None, &old_transport, &projection, 8, 8192)?;
    let admitted = registry
        .get("mcp.fixture.a", None, None)?
        .cloned()
        .ok_or_else(|| Error::NotFound("fixture".into()))?;
    let invocation = ToolInvocation {
        operation_id: OperationId::from_bytes([3; 16]),
        call_id: "in-flight".into(),
        name: "mcp.fixture.a".into(),
        arguments: json!({}),
    };
    let scope = RuntimeScope::new(Capabilities::new(["mcp:call:fixture"]), Limits::default())?;
    admitted.executor.authorize(Some(&scope), &invocation)?;
    let mut call = admitted.executor.execute(invocation.clone());
    assert!(futures::poll!(&mut call).is_pending());
    catalog("2", &["a"]).install(
        &mut registry,
        Some(&old_catalog),
        &new_transport,
        &projection,
        8,
        8192,
    )?;
    sender
        .send(json!({"content":[], "structuredContent":{"value":11}, "isError":false}))
        .map_err(|_| Error::Invalid("fixture dropped".into()))?;
    assert_eq!(call.await?.value["structuredContent"]["value"], 11);
    let next = registry
        .get("mcp.fixture.a", None, None)?
        .ok_or_else(|| Error::NotFound("fixture".into()))?;
    assert_eq!(next.definition.revision, "2");
    assert_eq!(
        next.executor.execute(invocation).await?.value["structuredContent"]["value"],
        1
    );
    Ok(())
}

#[test]
fn every_selected_exposure_reload_is_atomic_including_empty_visibility() -> Result<()> {
    let transport: Arc<dyn McpToolTransport> = Arc::new(Transport::default());
    let projection: Arc<dyn ToolProjection> = Arc::new(JsonToolProjection);
    let names = ["a", "b", "c"];
    let selected = |revision: &str, mask: u8| {
        let mut value = catalog(revision, &names);
        value.schema_exposure = McpSchemaExposure::Selected {
            names: names
                .iter()
                .enumerate()
                .filter(|(index, _)| mask & (1 << *index) != 0)
                .map(|(_, name)| (*name).into())
                .collect(),
        };
        value
    };
    for initial in 0_u8..8 {
        for replacement in 0_u8..8 {
            let old = selected("1", initial);
            let new = selected("2", replacement);
            let mut registry = ToolRegistry::default();
            old.install(&mut registry, None, &transport, &projection, 3, 8192)?;
            assert_eq!(registry.definitions(None)?, old.model_definitions(3, 8192)?);
            new.install(&mut registry, Some(&old), &transport, &projection, 3, 8192)?;
            assert_eq!(registry.definitions(None)?, new.model_definitions(3, 8192)?);
            assert_eq!(
                registry.definitions(None)?.len(),
                replacement.count_ones() as usize
            );
            assert_eq!(new.search("", None, 3, 3, 8192)?.len(), 3);
            for name in names {
                assert!(
                    registry
                        .get(&format!("mcp.fixture.{name}"), Some("1"), None)?
                        .is_some()
                );
                assert!(
                    registry
                        .get(&format!("mcp.fixture.{name}"), Some("2"), None)?
                        .is_some()
                );
            }
            let before = registry.definitions(None)?;
            assert!(
                selected("3", replacement)
                    .install(&mut registry, Some(&old), &transport, &projection, 3, 8192)
                    .is_err()
            );
            assert!(
                selected("3", replacement)
                    .install(&mut registry, None, &transport, &projection, 3, 8192)
                    .is_err()
            );
            assert_eq!(registry.definitions(None)?, before);
        }
    }
    let mut invalid = selected("1", 1);
    let mut registry = ToolRegistry::default();
    for names in [vec!["absent".into()], vec!["a".into(), "a".into()]] {
        invalid.schema_exposure = McpSchemaExposure::Selected { names };
        assert!(
            invalid
                .install(&mut registry, None, &transport, &projection, 3, 8192)
                .is_err()
        );
        assert!(registry.definitions(None)?.is_empty());
    }
    let mut disabled = selected("1", 0);
    disabled.discovery = McpDiscoveryPolicy::Disabled;
    disabled.install(&mut registry, None, &transport, &projection, 3, 8192)?;
    assert!(registry.definitions(None)?.is_empty());
    assert!(matches!(
        disabled.search("", None, 3, 3, 8192),
        Err(Error::Unsupported(_))
    ));
    Ok(())
}

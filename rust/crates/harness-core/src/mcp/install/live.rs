//! One event-invalidated catalog per installed connection, shared by agents.
use super::*;
use crate::tool::ToolSource;
use std::{collections::BTreeSet, sync::Mutex};

struct Snapshot {
    generation: u64,
    tools: ToolRegistry,
    names: BTreeSet<String>,
    removed: BTreeSet<String>,
}

pub(super) struct LiveCatalog {
    name: String,
    capability: String,
    server: McpServer,
    transport: Arc<dyn McpToolTransport>,
    binding: [u8; 32],
    identity: String,
    limits: McpClientLimits,
    refresh: tokio::sync::Mutex<()>,
    snapshot: Mutex<Snapshot>,
}

impl LiveCatalog {
    pub(super) fn new(
        name: String,
        server: McpServer,
        connected: Connected,
        limits: McpClientLimits,
    ) -> Result<(ToolRegistry, Capabilities)> {
        // Observe before discovery/publication; a racing notification remains dirty.
        let generation = connected.generation;
        let transport = connected.transport.clone();
        let binding = connected.binding;
        let identity = recovery_binding(binding, &server, limits)?;
        let (tools, capabilities) = register(name.clone(), &server, connected, limits, &|| Ok(()))?;
        let names = tools
            .definitions(None)?
            .into_iter()
            .map(|tool| tool.name)
            .collect();
        let source = Arc::new(Self {
            capability: format!("mcp:call:{name}"),
            name,
            server,
            transport,
            binding,
            identity,
            limits,
            refresh: tokio::sync::Mutex::new(()),
            snapshot: Mutex::new(Snapshot {
                generation,
                tools,
                names,
                removed: BTreeSet::new(),
            }),
        });
        Ok((ToolRegistry::default().with_source(source), capabilities))
    }
}

impl ToolSource for LiveCatalog {
    fn refresh(&self) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
            let deadline = tokio::time::Instant::now()
                + std::time::Duration::from_millis(u64::from(self.server.startup_timeout_ms));
            let refresh = async {
                let _refresh = self.refresh.lock().await;
                let generation = self.transport.tools_generation();
                if self
                    .snapshot
                    .lock()
                    .map_err(|_| Error::Storage("MCP source lock poisoned".into()))?
                    .generation
                    == generation
                {
                    return Ok(());
                }
                let tools = match native::discover(|cursor| self.transport.list(cursor)).await {
                    Ok(tools) => tools,
                    // An optional source's last admitted catalog remains usable.
                    // Leave its generation dirty for the next request's refresh.
                    Err(_) if !self.server.required => return Ok(()),
                    Err(error) => return Err(error),
                };
                let (tools, _) = register(
                    self.name.clone(),
                    &self.server,
                    Connected {
                        transport: self.transport.clone(),
                        tools,
                        binding: self.binding,
                        generation,
                    },
                    self.limits,
                    &|| Ok(()),
                )?;
                let names: BTreeSet<String> = tools
                    .definitions(None)?
                    .into_iter()
                    .map(|tool| tool.name)
                    .collect();
                let mut snapshot = self
                    .snapshot
                    .lock()
                    .map_err(|_| Error::Storage("MCP source lock poisoned".into()))?;
                let removed: Vec<_> = snapshot.names.difference(&names).cloned().collect();
                snapshot.removed.extend(removed);
                snapshot.removed.retain(|name| !names.contains(name));
                snapshot.names = names;
                snapshot.tools = tools;
                snapshot.generation = generation;
                Ok(())
            };
            match tokio::time::timeout_at(deadline, refresh).await {
                Ok(result) => result,
                Err(_) if !self.server.required => Ok(()),
                Err(_) => Err(startup_timeout()),
            }
        })
    }
    fn snapshot(&self) -> Result<ToolRegistry> {
        Ok(self
            .snapshot
            .lock()
            .map_err(|_| Error::Storage("MCP source lock poisoned".into()))?
            .tools
            .clone())
    }
    fn was_removed(&self, name: &str) -> Result<bool> {
        Ok(self
            .snapshot
            .lock()
            .map_err(|_| Error::Storage("MCP source lock poisoned".into()))?
            .removed
            .contains(name))
    }
    fn capability(&self) -> Option<&str> {
        Some(&self.capability)
    }
    fn recover(
        &self,
        definition: &crate::tool::ToolDefinition,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<Arc<crate::tool::Tool>>> {
        let Some(name) = definition.name.strip_prefix(&format!("mcp.{}.", self.name)) else {
            return Ok(None);
        };
        if definition.revision.len() != 128 || !definition.revision.starts_with(&self.identity) {
            return Ok(None);
        }
        let definition = crate::model::clone_tool_definition(definition, io)?;
        let output_schema = definition
            .output_schema
            .pointer("/then/properties/structuredContent")
            .cloned();
        Ok(Some(Arc::new(crate::tool::Tool {
            hook_names: &[],
            executor: Arc::new(super::super::remote::RemoteTool {
                server: self.name.clone(),
                name: name.to_owned(),
                output_schema,
                transport: self.transport.clone(),
            }),
            definition,
            projection: Arc::new(crate::tool::JsonToolProjection),
        })))
    }
}

use crate::{Result, tool::ToolRegistry};

/// The standard coding toolset. Custom tools can be added to this registry.
pub fn defaults() -> Result<ToolRegistry> {
    registry(&names(), |_| true, Vec::new())
}

/// Builds the host's registry from its admitted catalog selection and custom
/// tools. Agent tools bind later, to the original turn effects.
pub fn registry(
    names: &[String],
    workspace: impl Fn(&str) -> bool,
    tools: Vec<crate::tool::Tool>,
) -> Result<ToolRegistry> {
    if let Some(unknown) = names.iter().find(|name| {
        !super::catalog().any(|spec| spec.name == name.as_str())
            && !super::AGENT_TOOLS.contains(&name.as_str())
    }) {
        return Err(crate::Error::Invalid(format!("unknown tool {unknown}")));
    }
    let mut registry = ToolRegistry::default();
    for spec in super::catalog() {
        if !names.iter().any(|name| name == spec.name) {
            continue;
        }
        if spec.binding == super::ToolBinding::Workspace && !workspace(spec.name) {
            continue;
        }
        registry.register((spec.bind()?, None), &mut None, None)?;
    }
    for tool in tools {
        registry.register((tool, None), &mut None, None)?;
    }
    Ok(registry)
}

/// Call grants for the same registered tools and selected per-turn agent tools.
pub fn call_grants<'a>(
    registry: &ToolRegistry,
    names: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<String>> {
    Ok(registry
        .definitions(None)?
        .into_iter()
        .map(|definition| crate::contract::capability::tool_call(&definition.name))
        .chain(
            names
                .into_iter()
                .filter(|name| super::AGENT_TOOLS.contains(name))
                .map(crate::contract::capability::tool_call),
        )
        .collect())
}

/// Names of every standard tool: the catalog's, in order, then the agent tools.
#[must_use]
pub fn names() -> Vec<String> {
    super::catalog()
        .map(|spec| spec.name)
        .chain(super::AGENT_TOOLS)
        .map(str::to_owned)
        .collect()
}

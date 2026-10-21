use crate::{Result, runtime::RuntimeScope, tool::ToolRegistry};

/// The standard coding toolset. Custom tools can be added to this registry.
pub fn defaults() -> Result<ToolRegistry> {
    let mut registry = ToolRegistry::default();
    bind_defaults(&mut registry, &mut Vec::new(), None, true, true)?;
    Ok(registry)
}

#[doc(hidden)]
pub fn bind_defaults(
    registry: &mut ToolRegistry,
    grants: &mut Vec<String>,
    bound: Option<&RuntimeScope>,
    volume: bool,
    workspace: bool,
) -> Result<()> {
    for spec in super::catalog() {
        let available = match spec.binding {
            super::ToolBinding::Volume => volume,
            super::ToolBinding::Workspace => workspace,
        };
        if !available {
            continue;
        }
        let capability = spec.capability();
        if bound.is_some_and(|scope| {
            !scope.grants().contains(&capability)
                || (super::children::family(spec.name)
                    && !scope.grants().contains("tool:call:spawn"))
        }) {
            continue;
        }
        registry.register((spec.bind()?, None), &mut None, None)?;
        if bound.is_none() && !grants.contains(&capability) {
            grants.push(capability);
        }
    }
    Ok(())
}

use super::variants::FileVariant;
use crate::{Result, runtime::RuntimeScope, tool::ToolRegistry};

// Trained argument variants retain the original volume executor.
const TRAINED_FILES: [FileVariant; 3] = [
    FileVariant::Replace,
    FileVariant::WriteFile,
    FileVariant::TextEditor,
];

const PATCH: &str = "apply_patch";
#[cfg(feature = "filesystem")]
const PROFILE_FUNCTIONS: [&str; 1] = ["call"];
#[cfg(not(feature = "filesystem"))]
const PROFILE_FUNCTIONS: [&str; 0] = [];

/// The standard coding toolset. Custom tools can be added to this registry.
pub fn defaults() -> Result<ToolRegistry> {
    let mut registry = ToolRegistry::default();
    bind_defaults(&mut registry, &mut Vec::new(), None, &names(), true, |_| {
        true
    })?;
    Ok(registry)
}

/// Names of standard coding functions followed by the agent tools.
#[must_use]
pub fn names() -> Vec<String> {
    super::catalog()
        .map(|spec| spec.name)
        .chain(TRAINED_FILES.into_iter().map(FileVariant::name))
        .chain([PATCH])
        .chain(PROFILE_FUNCTIONS)
        .chain(super::AGENT_TOOLS)
        .map(str::to_owned)
        .collect()
}

/// Binds the standard functions `names` selects that the task's resources can
/// back. Agent tool names are bound per turn by [`super::bind_agent_tools`];
/// any other name is refused.
#[doc(hidden)]
pub fn bind_defaults(
    registry: &mut ToolRegistry,
    grants: &mut Vec<String>,
    bound: Option<&RuntimeScope>,
    names: &[String],
    volume: bool,
    workspace: impl Fn(&str) -> bool,
) -> Result<()> {
    if let Some(unknown) = names.iter().find(|name| {
        !super::catalog().any(|spec| spec.name == name.as_str())
            && !TRAINED_FILES
                .iter()
                .any(|variant| variant.name() == name.as_str())
            && name != PATCH
            && !PROFILE_FUNCTIONS.contains(&name.as_str())
            && !super::AGENT_TOOLS.contains(&name.as_str())
    }) {
        return Err(crate::Error::Invalid(format!("unknown tool {unknown}")));
    }
    for spec in super::catalog() {
        if !names.iter().any(|name| name == spec.name) {
            continue;
        }
        let available = match spec.binding {
            super::ToolBinding::Volume => volume,
            super::ToolBinding::Workspace => workspace(spec.name),
        };
        if !available {
            continue;
        }
        let capability = spec.capability();
        if bound.is_some_and(|scope| !scope.grants().contains(&capability)) {
            continue;
        }
        registry.register((spec.bind()?, None), &mut None, None)?;
        if bound.is_none() && !grants.contains(&capability) {
            grants.push(capability);
        }
    }
    if volume {
        for variant in TRAINED_FILES {
            if !names.iter().any(|name| name == variant.name()) {
                continue;
            }
            let capability = crate::contract::capability::tool_call(variant.name());
            if bound.is_some_and(|scope| !scope.grants().contains(&capability)) {
                continue;
            }
            registry.register((variant.bind()?, None), &mut None, None)?;
            if bound.is_none() && !grants.contains(&capability) {
                grants.push(capability);
            }
        }
    }
    for name in [PATCH].into_iter().chain(PROFILE_FUNCTIONS) {
        if !names.iter().any(|selected| selected == name) || (name == PATCH && !volume) {
            continue;
        }
        let capability = crate::contract::capability::tool_call(name);
        if bound.is_some_and(|scope| !scope.grants().contains(&capability)) {
            continue;
        }
        let tool = match name {
            PATCH => super::variants::apply_patch()?,
            #[cfg(feature = "filesystem")]
            "call" => crate::executor::call_tool()?,
            _ => unreachable!("only declared profile functions are iterated"),
        };
        registry.register((tool, None), &mut None, None)?;
        if bound.is_none() && !grants.contains(&capability) {
            grants.push(capability);
        }
    }
    Ok(())
}
